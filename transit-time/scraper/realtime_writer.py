import asyncio
import logging
from typing import Dict, Optional, Tuple

import sqlalchemy as sa
from async_lru import alru_cache
from sqlalchemy.dialects.postgresql import insert

import db
from scraper import nyc
from scraper.gtfs import gtfs


class RealtimeWriter:
    system: gtfs.TransitSystem

    def __init__(self, system: gtfs.TransitSystem):
        self.system = system

    async def write_message(self, message: gtfs.FeedMessage):
        assert self.system == message.system
        # for trip_update in message.trip_updates:
        #     await self._write_trip_update(trip_update, message)
        # for position in message.vehicle_positions:
        #     await self._write_vehicle_position(position, message)
        coros = []
        coros.extend(
            [
                self._write_trip_update(update, message)
                for update in message.trip_updates
            ]
        )
        coros.extend(
            [
                self._write_vehicle_position(position, message)
                for position in message.vehicle_positions
            ]
        )
        await asyncio.gather(*coros)

    async def _write_trip_update(
        self, update: gtfs.TripUpdate, message: gtfs.FeedMessage
    ):
        if not self._is_valid_trip_descriptor(update.trip, message):
            return

        trip = await self._get_trip_row_from_descriptor(update.trip)
        if trip is None:
            if not message.is_trip_replaced(update.trip.route_id):
                logging.warning(
                    "TripUpdate trip  not scheduled or replaced: (%s, %s, %s) at %s",
                    update.trip.trip_id,
                    update.trip.route_id,
                    update.trip.start_date,
                    message.timestamp,
                )

        async def get_insert_values(stop_time_update) -> Optional[Tuple[str, Dict]]:
            # For some reason, stop_id does not always exist (wtf MTA?).
            # In that case, don't do any writes because that will fail on the
            # foreign key constraint.
            stop_exists = await self._get_stop_exists(stop_time_update.stop_id)
            if not stop_exists:
                logging.debug(
                    "Encountered nonexistent stop %s in trip update (%s, %s, %s) at %s",
                    stop_time_update.stop_id,
                    update.trip.trip_id,
                    update.trip.route_id,
                    update.trip.start_date,
                    message.timestamp,
                )
                return None

            if stop_time_update.arrival is None and stop_time_update.departure is None:
                logging.warning(
                    "No arrival or departure for stop %s in trip update "
                    "(%s, %s, %s) at %s",
                    stop_time_update.stop_id,
                    update.trip.trip_id,
                    update.trip.route_id,
                    update.trip.start_date,
                    message.timestamp,
                )
                return None

            values = {
                "system": self.system.value,
                "route_id": update.trip.route_id,
                "stop_id": stop_time_update.stop_id,
                "start_date": update.trip.start_date,
                "trip_id": trip["trip_id"] if trip is not None else update.trip.trip_id,
                "timestamp": update.timestamp,
                "arrival": stop_time_update.arrival,
                "departure": stop_time_update.departure,
                "update_time": message.timestamp,
            }
            key = "||".join(
                [
                    values["system"],
                    values["route_id"],
                    values["stop_id"],
                    str(values["start_date"]),
                ]
            )
            return (key, values)

        insert_key_values = await asyncio.gather(
            *[get_insert_values(update) for update in update.stop_time_updates]
        )
        insert_key_values = [v for v in insert_key_values if v is not None]
        if len(insert_key_values) == 0:
            return

        # Sometimes we get data that updates the same trip twice for the same
        # stop.  We can't update them both in the same DB update because that
        # can conflict.  Resolve by picking one semi-arbitrarily.
        insert_values = list(dict(insert_key_values).values())

        table = db.get_table("realtime_stop_times")
        stmt = insert(table).values(insert_values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                table.c.system,
                table.c.route_id,
                table.c.stop_id,
                table.c.start_date,
                table.c.trip_id,
            ],
            set_={
                "timestamp": stmt.excluded.timestamp,
                "arrival": stmt.excluded.arrival,
                "departure": stmt.excluded.departure,
                "update_time": stmt.excluded.update_time,
            },
            where=(table.c.update_time <= stmt.excluded.update_time),
        )
        async with db.acquire_conn() as conn:
            await conn.execute(stmt)

    async def _write_vehicle_position(
        self, position: gtfs.VehiclePosition, message: gtfs.FeedMessage
    ):
        if not self._is_valid_trip_descriptor(position.trip, message):
            return

        trip = await self._get_trip_row_from_descriptor(position.trip)
        if trip is None:
            if not message.is_trip_replaced(position.trip.route_id):
                logging.warning(
                    "VehiclePosition trip  not scheduled or replaced: "
                    "(%s, %s, %s) at %s",
                    position.trip.trip_id,
                    position.trip.route_id,
                    position.trip.start_date,
                    message.timestamp,
                )

        if position.stop_id is not None:
            stop_id = position.stop_id
        else:
            # Guess from current_stop_sequence and stop_times
            # For some reason this is not always consistent with stop_id,
            # which is why we use stop_id if it exists (above)
            assert position.current_stop_sequence is not None
            if trip is None:
                logging.debug(
                    "Cannot write VehiclePosition: no stop_id or trip_id for "
                    "(%s, %s, %s) at %s",
                    position.trip.trip_id,
                    position.trip.route_id,
                    position.trip.start_date,
                    message.timestamp,
                )
                return

            current_stop_sequence = position.current_stop_sequence
            # Give up on current_stop_sequence because it is weird.  It starts
            # at 0 which is invalid.  It also goes over the number of stops in
            # the table.  For the L train, there are no entries with
            # current_stop_sequence larger than 22, even though there are trips
            # with 24 stops (e.g. BFA19SUPP-L047-Weekday-99_048500_L..S01R).
            # It seems like others have this problem as well:
            # https://groups.google.com/forum/#!topic/mtadeveloperresources/x8-f1biU-l0
            if self.system == gtfs.TransitSystem.NYC_MTA:
                return

            stop_id = await self._get_stop_id_from_stop_seq(
                trip["trip_id"], current_stop_sequence
            )

        # For some reason, stop_id does not always exist (wtf MTA?).
        # In that case, don't do any writes because that will fail on the
        # foreign key constraint.
        stop_exists = await self._get_stop_exists(stop_id)
        if not stop_exists:
            logging.debug(
                "Encountered nonexistent stop %s in vehicle position "
                "(%s, %s, %s) at %s",
                stop_id,
                position.trip.trip_id,
                position.trip.route_id,
                position.trip.start_date,
                message.timestamp,
            )
            return

        table = db.get_table("realtime_vehicle_positions")
        values = {
            "system": self.system.value,
            "route_id": position.trip.route_id,
            "stop_id": stop_id,
            "start_date": position.trip.start_date,
            "trip_id": trip["trip_id"] if trip is not None else position.trip.trip_id,
            "timestamp": position.timestamp or message.timestamp,
            "status": position.current_status,
            "update_time": message.timestamp,
        }
        stmt = insert(table).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                table.c.system,
                table.c.route_id,
                table.c.stop_id,
                table.c.start_date,
                table.c.trip_id,
                table.c.timestamp,
            ],
            set_={
                "status": stmt.excluded.status,
                "update_time": stmt.excluded.update_time,
            },
            where=(table.c.update_time <= stmt.excluded.update_time),
        )
        async with db.acquire_conn() as conn:
            await conn.execute(stmt)

    def _is_valid_trip_descriptor(
        self, trip: gtfs.TripDescriptor, message: gtfs.FeedMessage
    ) -> bool:
        if trip.route_id is None:
            # wtf MTA
            logging.warning(
                "No route for trip (%s, %s, %s) at %s",
                trip.trip_id,
                trip.route_id,
                trip.start_date,
                message.timestamp,
            )
            return False

        return True

    @alru_cache(maxsize=None)
    async def _get_trip_row_from_descriptor(self, trip_descriptor: gtfs.TripDescriptor):
        row = await self._get_trip_row_from_id(trip_descriptor.trip_id)
        if row is not None:
            return row

        # table = db.get_table('trips')
        # async with db.acquire_conn() as conn:
        #     res = await conn.execute(
        #         table.select()
        #         .where(table.c.system == self.system.value)
        #         .where(table.c.trip_id.like("%" + trip_descriptor.trip_id + "%"))
        #     )
        #     row = await res.fetchone()
        #     if row is not None:
        #         return row

        if self.system is not gtfs.TransitSystem.NYC_MTA:
            return None

        # Monday = 0, Sunday = 6
        day_of_week = trip_descriptor.start_date.weekday()
        if day_of_week < 5:
            service_day = nyc.ServiceDay.WEEKDAY
        elif day_of_week == 5:
            service_day = nyc.ServiceDay.SATURDAY
        elif day_of_week == 6:
            service_day = nyc.ServiceDay.SUNDAY
        else:
            raise ValueError(
                "Unexpected day of week {} for {}".format(
                    day_of_week, trip_descriptor.start_date
                )
            )

        table = db.get_table("mta_trip_id")
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                table.select()
                .where(table.c.system == self.system.value)
                .where(table.c.alternate_trip_id == trip_descriptor.trip_id)
                .where(table.c.service_day == service_day)
            )
            rows = await res.fetchall()
            if len(rows) == 0:
                return None
            if len(rows) > 1:
                trip_ids = [row["trip_id"] for row in rows]
                logging.info(
                    "%s: (%s, %s/%s) has multiple trip IDs: (%s)",
                    self.system,
                    trip_descriptor.trip_id,
                    trip_descriptor.start_date,
                    service_day,
                    ", ".join(trip_ids),
                )
                return None
            trip_id = rows[0]["trip_id"]
        return await self._get_trip_row_from_id(trip_id)

    @alru_cache(maxsize=None)
    async def _get_trip_row_from_id(self, trip_id: str):
        trips_table = db.get_table("trips")
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                trips_table.select()
                .where(trips_table.c.system == self.system.value)
                .where(trips_table.c.trip_id == trip_id)
            )
            return await res.fetchone()

    @alru_cache(maxsize=4096)
    async def _get_stop_id_from_stop_seq(self, trip_id: str, stop_seq: int) -> str:
        table = db.get_table("stop_times")
        async with db.acquire_conn() as conn:
            stop_id = await conn.scalar(
                sa.select([table.c.stop_id])
                .where(table.c.system == self.system.value)
                .where(table.c.trip_id == trip_id)
                .where(table.c.stop_sequence == stop_seq)
            )
            if stop_id is None:
                raise Exception(
                    "No stop_id for (trip_id, seq): ({}, {})".format(trip_id, stop_seq)
                )
            return stop_id

    @alru_cache(maxsize=None)
    async def _get_stop_exists(self, stop_id: str) -> bool:
        table = db.get_table("stops")
        async with db.acquire_conn() as conn:
            res = await conn.scalar(
                sa.select([table.c.stop_id])
                .where(table.c.system == self.system.value)
                .where(table.c.stop_id == stop_id)
            )
            return res is not None
