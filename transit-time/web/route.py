import asyncio

import sqlalchemy as sa
from async_lru import alru_cache
from starlette.endpoints import HTTPEndpoint

import c
import db
from scraper.gtfs import gtfs
from web import templates


class Route(HTTPEndpoint):
    async def get(self, request):
        system = gtfs.TransitSystem(request.path_params["system"])
        route_id = request.path_params["route_id"]
        (route, stop_ids) = await asyncio.gather(
            self.query_route(system, route_id), self.query_stop_ids(system, route_id)
        )
        stops = await asyncio.gather(
            *[self.query_station(system, stop_id) for stop_id in stop_ids]
        )
        # Some of these stops are duplicates, because the north/south platforms
        # roll up to the same stop.  Dedupe and sort by stop name
        stops = c.unique(stops, lambda stop: stop["stop_id"])

        # Luckily for us MTA stops are ordered by stop ID
        stops = sorted(stops, key=lambda stop: stop["stop_id"])

        return templates.get().TemplateResponse(
            "route.html.j2",
            {
                "request": request,
                "system": system,
                "system_name": gtfs.get_system_name(system),
                "route": route,
                "stops": stops,
            },
        )

    async def query_stop_ids(self, system: gtfs.TransitSystem, route_id: str):
        trips = db.get_table("trips")
        stop_times = db.get_table("stop_times")

        async with db.acquire_conn() as conn:
            res = await conn.execute(
                sa.select([stop_times.c.stop_id])
                .distinct()
                .where(
                    sa.and_(
                        trips.c.system == system.value,
                        trips.c.route_id == route_id,
                        trips.c.trip_id == stop_times.c.trip_id,
                    )
                )
            )
            rows = await res.fetchall()

        return [row["stop_id"] for row in rows]

    @alru_cache
    async def query_station(self, system: gtfs.TransitSystem, stop_id: str):
        stop = await self.query_stop(system, stop_id)
        while (
            stop["location_type"] != gtfs.LocationType.STATION
            and stop["parent_station"] is not None
        ):
            stop = await self.query_stop(system, stop["parent_station"])
        return stop

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
