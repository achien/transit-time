import asyncio
from datetime import datetime, time, timedelta, timezone
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
                r["departure_delta"] = self.departure_delta(
                    r["departure"], start_datetime + r["scheduled_departure"]
                )

            r["departure_str"] = self.friendly_time(r["departure"].astimezone(timezone))

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

    def friendly_time(self, dt: datetime):
        (sign, time_part, total_seconds) = self._relative_time_helper(
            dt, datetime.now(timezone.utc)
        )
        if total_seconds >= 24 * 60 * 60:
            # If more than 1 day ago, show day
            return dt.strftime("%-m/%-d/%y %-I:%M %p")
        elif total_seconds >= 60 * 60:
            # If more than 1 hour ago, show hours and minutes
            return dt.strftime("%-I:%M %p")

        hmm = dt.strftime("%-I:%M %p")
        if sign == 0:
            return "Departing now // {}".format(hmm)
        elif sign == 1:
            return "Departs in {} // {}".format(time_part, hmm)
        else:
            return "Departed {} ago // {}".format(time_part, hmm)

    def departure_delta(self, scheduled: datetime, actual: datetime):
        (sign, time_part, _) = self._relative_time_helper(scheduled, actual)
        if sign == 0:
            return "on time"
        elif sign == 1:
            return "{} late".format(time_part)
        else:
            return "{} early".format(time_part)

    def _relative_time_helper(self, diff, base):
        if diff > base:
            sign = +1
        elif diff == base:
            sign = 0
        else:
            sign = -1
        total_seconds = int(abs((diff - base).total_seconds()))
        seconds = total_seconds % 60
        total_minutes = total_seconds // 60
        minutes = total_minutes % 60
        hours = total_minutes // 60

        time_parts = []
        if hours > 1:
            time_parts.append("{}h".format(hours))
        elif hours == 1:
            time_parts.append("1h")
        if minutes > 1:
            time_parts.append("{}m".format(minutes))
        elif minutes == 1:
            time_parts.append("1m")
        # Don't display seconds if interval >= 10 minutes
        if minutes < 10 and hours == 0:
            if seconds > 1:
                time_parts.append("{}s".format(seconds))
            elif seconds == 1:
                time_parts.append("1s")

        return (sign, " ".join(time_parts), total_seconds)

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
