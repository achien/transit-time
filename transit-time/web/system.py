from starlette.endpoints import HTTPEndpoint

import db
from scraper.gtfs import gtfs
from web import templates


class System(HTTPEndpoint):
    async def get(self, request):
        system = gtfs.TransitSystem(request.path_params["system"])
        routes = await self.get_routes(system)
        routes_by_color = {}
        # Group by color
        for route in routes:
            color = route["route_color"]
            if color not in routes_by_color:
                routes_by_color[color] = []
            routes_by_color[color].append(route)
        # Sort by short name
        for color in routes_by_color:
            routes_by_color[color] = sorted(
                routes_by_color[color], key=(lambda route: route["route_short_name"])
            )
        return templates.get().TemplateResponse(
            "system.html.j2",
            {
                "request": request,
                "routes_by_color": routes_by_color,
                "system": system,
                "system_name": gtfs.get_system_name(system),
            },
        )

    async def get_routes(self, system: gtfs.TransitSystem):
        async with db.acquire_conn() as conn:
            table = db.get_table("routes")
            res = await conn.execute(
                table.select().where(table.c.system == system.value)
            )
            return await res.fetchall()
