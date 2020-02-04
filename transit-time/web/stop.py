import asyncio
from datetime import datetime, time, timedelta
from typing import List

import sqlalchemy as sa
from async_lru import alru_cache
from dateutil.tz import gettz
from starlette.endpoints import HTTPEndpoint

import db
from scraper.gtfs import gtfs
from web import templates


class Stop(HTTPEndpoint):
    async def get(self, request):
        system = gtfs.TransitSystem(request.path_params["system"])
        route_id = request.path_params["route_id"]
        stop_id = request.path_params["stop_id"]

        # TODO: refactor
        (timezone, route, stop, stop_and_parents) = await asyncio.gather(
            self.query_timezone(system),
            self.query_route(system, route_id),
            self.query_stop(system, stop_id),
            self.query_stop_and_parents(system, stop_id),
        )

        realtime_stop_times = await self.query_realtime_stop_times(
            system, route_id, [stop["stop_id"] for stop in stop_and_parents]
        )

        # MTA is in New York, I am in New York (lol)
        minus_twelve_hours = timedelta(hours=-12)
        realtime_stop_times = [dict(r) for r in realtime_stop_times]
        for r in realtime_stop_times:
            start_datetime = (
                datetime.combine(r["start_date"], time(hour=12), tzinfo=timezone)
                + minus_twelve_hours
            )
            if r["scheduled_departure"] is not None:
                r["scheduled_departure"] = start_datetime + r["scheduled_departure"]
                departure_delta = r["departure"] - r["scheduled_departure"]
                r["departure_delta_minutes"] = round(
                    (departure_delta.total_seconds() / 60)
                )

        realtime_stop_times = sorted(
            realtime_stop_times, key=lambda r: r["departure"], reverse=True
        )
        stop_times_by_stop_id = {}
        for rst in realtime_stop_times:
            stop_id = rst["stop_id"]
            if stop_id not in stop_times_by_stop_id:
                stop_times_by_stop_id[stop_id] = []
            stop_times_by_stop_id[stop_id].append(rst)

        return templates.get().TemplateResponse(
            "stop.html.j2",
            {
                "request": request,
                "route": route,
                "stop": stop,
                "stop_times_by_stop_id": stop_times_by_stop_id,
                "timezone": timezone,
            },
        )

    async def query_realtime_stop_times(
        self, system: gtfs.TransitSystem, route_id: str, stop_ids: List[str]
    ):
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                """
                select
                    rst.stop_id,
                    rst.departure,
                    rst.start_date,
                    st.departure_time as scheduled_departure
                from realtime_stop_times as rst
                left outer join stop_times as st
                on
                    rst.trip_id = st.trip_id
                    and rst.stop_id = st.stop_id
                where
                    rst.system = %s
                    and rst.route_id = %s
                    and rst.stop_id in %s
                """,
                system.value,
                route_id,
                tuple(stop_ids),
            )
            return await res.fetchall()

    @alru_cache
    async def query_timezone(self, system: gtfs.TransitSystem):
        agency = db.get_table("agency")
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                agency.select().where(agency.c.system == system.value)
            )
            row = await res.fetchone()
            return gettz(row.agency_timezone)

    @alru_cache
    async def query_stop_and_parents(self, system: gtfs.TransitSystem, stop_id: str):
        stops = db.get_table("stops")
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                stops.select()
                .where(stops.c.system == system.value)
                .where(
                    sa.or_(
                        stops.c.stop_id == stop_id, stops.c.parent_station == stop_id
                    )
                )
            )
            return await res.fetchall()

    @alru_cache
    async def query_stop(self, system: gtfs.TransitSystem, stop_id: str):
        stops = db.get_table("stops")
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                stops.select()
                .where(stops.c.system == system.value)
                .where(stops.c.stop_id == stop_id)
            )
            stop = await res.fetchone()
        assert stop is not None
        return stop

    @alru_cache
    async def query_route(self, system: gtfs.TransitSystem, route_id: str):
        routes = db.get_table("routes")
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                routes.select()
                .where(routes.c.system == system.value)
                .where(routes.c.route_id == route_id)
            )
            route = await res.fetchone()
        assert route is not None
        return route
