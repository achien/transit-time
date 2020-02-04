from starlette.endpoints import HTTPEndpoint
from scraper.gtfs import gtfs
from web import templates


class Home(HTTPEndpoint):
    async def get(self, request):
        system_names = {}
        for system in gtfs.TransitSystem:
            system_names[system.value] = gtfs.get_system_name(system)
        return templates.get().TemplateResponse(
            "home.html.j2", {"request": request, "system_names": system_names}
        )
