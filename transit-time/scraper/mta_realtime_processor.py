import asyncio
import logging
import os
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Optional

import dateparser
import sqlalchemy as sa
from flask import Flask, Response
from sqlalchemy.dialects.postgresql import insert

import db
import logger
from scraper.batch_process import BatchProcessor
from scraper.gtfs import gtfs, gtfs_realtime_pb2
from scraper.mta_realtime_parser import MTARealtimeParser
from scraper.nyc import nyct_subway_pb2  # noqa: F401

logger.setup()


class MTARealtimeProcessor(BatchProcessor):
    system = gtfs.TransitSystem.NYC_MTA
    parser: MTARealtimeParser

    def __init__(self, from_time: Optional[datetime] = None):
        table = db.get_table("realtime_raw")
        where_clauses = [table.c.system == self.system.value]
        if from_time:
            where_clauses.append(table.c.update_time >= from_time)
        super().__init__(
            "realtime_raw",
            ("update_time", "feed_id"),
            sa.and_(*where_clauses),
            groups=("feed_id",),
        )
        self.parser = MTARealtimeParser(self.system)

    async def process_row(self, row):
        feed_message_pb = gtfs_realtime_pb2.FeedMessage()
        feed_message_pb.ParseFromString(row["raw"])
        feed_message = gtfs.parse_feed_message(
            self.system, row["feed_id"], feed_message_pb
        )
        await self.parser.fix_feed_mesesage(feed_message)

        # Only write trip updates.  Vehicle positions are unusable because
        # stop_id is not provided and cannot be determined reliably from
        # current_stop_sequence.
        await asyncio.gather(
            *[
                self.process_trip_update(update, feed_message)
                for update in feed_message.trip_updates
            ]
        )

    async def process_trip_update(
        self, update: gtfs.TripUpdate, message: gtfs.FeedMessage
    ):
        trip = await self.parser.get_trip_row_from_descriptor(update.trip)
        if trip is None:
            if not message.is_trip_replaced(update.trip.route_id):
                logging.warning(
                    "TripUpdate trip  not scheduled or replaced: (%s, %s, %s, %s) at %s",
                    update.trip.trip_id,
                    update.trip.train_id,
                    update.trip.route_id,
                    update.trip.start_date,
                    message.timestamp,
                )

        table = db.get_table("realtime_stop_times2")
        raw_table = db.get_table("realtime_raw_stop_times")
        # All updates in this function will share the same update_time
        now = datetime.now(timezone.utc)
        route_id = update.trip.route_id
        start_date = update.trip.start_date
        trip_id = trip["trip_id"] if trip else update.trip.trip_id
        # train_id is a primary key and cannot be None
        train_id = update.trip.train_id or ""

        # Get last few entries for this trip
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                raw_table.select()
                .where(raw_table.c.system == self.system.value)
                .where(raw_table.c.route_id == route_id)
                .where(raw_table.c.start_date == start_date)
                .where(raw_table.c.trip_id == trip_id)
                .where(raw_table.c.train_id == train_id)
                .where(raw_table.c.time < message.timestamp)
                .order_by(raw_table.c.time.desc())
                .limit(5)
            )
            rows = await res.fetchall()
        previous_stop_times = [
            [self.deserialize_stop_time(obj) for obj in row.stop_times] for row in rows
        ]

        # Determine most likely stop order

        # Remove any stops that are out of order
        stop_time_updates = update.stop_time_updates.copy()

        # Update stop information in into realtime_stop_times2
        if len(stop_time_updates) > 0:
            values = []
            for stu in stop_time_updates:
                values.append(
                    {
                        "system": self.system.value,
                        "route_id": route_id,
                        "start_date": start_date,
                        "trip_id": trip_id,
                        "train_id": train_id,
                        "stop_id": stu.stop_id,
                        "arrival": stu.arrival,
                        "departure": stu.departure,
                        "departure_or_arrival": stu.departure
                        if stu.departure is not None
                        else stu.arrival,
                        "time": update.timestamp or message.timestamp,
                    }
                )
            stmt = insert(table).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    table.c.system,
                    table.c.route_id,
                    table.c.start_date,
                    table.c.trip_id,
                    table.c.train_id,
                    table.c.stop_id,
                ],
                set_={
                    "arrival": stmt.excluded.arrival,
                    "departure": stmt.excluded.departure,
                    "departure_or_arrival": stmt.excluded.departure_or_arrival,
                    "time": stmt.excluded.time,
                },
                where=(table.c.time <= stmt.excluded.time),
            )
            async with db.acquire_conn() as conn:
                await conn.execute(stmt)

        # Deleted any outdated stop info from the last entry
        # 1. If there are no stops in the current stop list, the train
        #    has completed it's route and we do not remove any stops.
        # 2. If the train arrived before the current time, assume it
        #    arrived.
        # 3. Otherwise, see if it would arrive before the earliest train in the
        #    current stop list.  If it does, assume it is omitted from the
        #    current stop list because it already arrived early.
        if len(previous_stop_times) > 0 and len(stop_time_updates) > 0:
            current_stop_ids = [stu.stop_id for stu in stop_time_updates]
            earliest_stop_time = min(
                [
                    stu.arrival if stu.arrival is not None else stu.departure
                    for stu in stop_time_updates
                ]
            )
            outdated_stop_ids = []
            for stu in previous_stop_times[0]:
                time = stu.arrival if stu.arrival is not None else stu.departure
                if (
                    stu.stop_id not in current_stop_ids
                    and time > message.timestamp
                    and time >= earliest_stop_time
                ):
                    outdated_stop_ids.append(stu.stop_id)
            if len(outdated_stop_ids) > 0:
                logging.info(
                    "Removing stops [%s] from trip (%s, %s, %s, %s) based on "
                    "updated information at %s",
                    ", ".join(outdated_stop_ids),
                    trip_id,
                    train_id,
                    route_id,
                    start_date,
                    message.timestamp,
                )
                async with db.acquire_conn() as conn:
                    await conn.execute(
                        table.delete()
                        .where(table.c.system == self.system.value)
                        .where(table.c.route_id == route_id)
                        .where(table.c.start_date == start_date)
                        .where(table.c.trip_id == trip_id)
                        .where(table.c.train_id == train_id)
                        .where(table.c.stop_id.in_(outdated_stop_ids))
                    )

        # Insert stops data into realtime_raw_stop_times
        values = {
            "system": self.system.value,
            "route_id": route_id,
            "start_date": start_date,
            "trip_id": trip_id,
            "train_id": train_id,
            "time": message.timestamp,
            "stop_times": [self.serialize_stop_time(stu) for stu in stop_time_updates],
            "update_time": now,
        }
        stmt = insert(raw_table).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                raw_table.c.system,
                raw_table.c.route_id,
                raw_table.c.start_date,
                raw_table.c.trip_id,
                raw_table.c.train_id,
                raw_table.c.time,
            ],
            set_={
                "stop_times": stmt.excluded.stop_times,
                "update_time": stmt.excluded.update_time,
            },
        )
        async with db.acquire_conn() as conn:
            await conn.execute(stmt)

    def serialize_stop_time(self, stu: gtfs.StopTimeUpdate) -> dict:
        """Converts StopTimeUpdate to JSON compatible object to store"""
        return {
            "stop_id": stu.stop_id,
            "arrival": stu.arrival.timestamp() if stu.arrival else None,
            "departure": stu.departure.timestamp() if stu.departure else None,
        }

    def deserialize_stop_time(self, obj: dict) -> gtfs.StopTimeUpdate:
        """Deserializes JSON compatible object into StopTimeUpdate"""
        arrival = (
            datetime.fromtimestamp(obj["arrival"], timezone.utc)
            if obj["arrival"] is not None
            else None
        )
        departure = (
            datetime.fromtimestamp(obj["departure"], timezone.utc)
            if obj["departure"] is not None
            else None
        )
        return gtfs.StopTimeUpdate(
            stop_id=obj["stop_id"], arrival=arrival, departure=departure
        )


def main_flask_wrapper():
    processor = MTARealtimeProcessor()
    asyncio.run(processor.main())
    return Response(status=HTTPStatus.OK)


if __name__ == "__main__":
    env = os.environ.get("ENV")
    if env == "GCP_RUN":
        # Google Cloud Run wants us to listen and respond to a HTTP request
        app = Flask(__name__)
        app.add_url_rule("/", "main", main_flask_wrapper, methods=["POST"])
        app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    else:
        from_time_str = "2020-02-13"
        from_time = None
        if from_time_str:
            from_time = dateparser.parse(
                from_time_str, settings={"RETURN_AS_TIMEZONE_AWARE": True}
            )

        processor = MTARealtimeProcessor(from_time=from_time)
        asyncio.run(processor.main())
