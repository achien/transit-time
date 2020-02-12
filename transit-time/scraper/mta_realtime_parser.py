import logging

import sqlalchemy as sa
from async_lru import alru_cache

import db
from scraper import nyc
from scraper.gtfs import gtfs

CACHE_SIZE = 4096


class MTARealtimeParser:
    system: gtfs.TransitSystem

    def __init__(self, system: gtfs.TransitSystem):
        self.system = system

    def is_valid_trip_descriptor(
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

    @alru_cache(maxsize=CACHE_SIZE)
    async def get_trip_row_from_descriptor(self, trip_descriptor: gtfs.TripDescriptor):
        row = await self.get_trip_row_from_id(trip_descriptor.trip_id)
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
        return await self.get_trip_row_from_id(trip_id)

    @alru_cache(maxsize=CACHE_SIZE)
    async def get_trip_row_from_id(self, trip_id: str):
        trips_table = db.get_table("trips")
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                trips_table.select()
                .where(trips_table.c.system == self.system.value)
                .where(trips_table.c.trip_id == trip_id)
            )
            return await res.fetchone()

    @alru_cache(maxsize=CACHE_SIZE)
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

    @alru_cache(maxsize=CACHE_SIZE)
    async def get_stop_exists(self, stop_id: str) -> bool:
        table = db.get_table("stops")
        async with db.acquire_conn() as conn:
            res = await conn.scalar(
                sa.select([table.c.stop_id])
                .where(table.c.system == self.system.value)
                .where(table.c.stop_id == stop_id)
            )
            return res is not None
