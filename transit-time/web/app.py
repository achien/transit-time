import os

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

import db
import logger
from web import home, route, stop, system
from web.timelapse import graph, realtime_trips

logger.setup()


routes = [
    Route("/", home.Home),
    Mount("/data", app=StaticFiles(directory="web/static/data")),
    Mount("/timelapse", app=StaticFiles(directory="web/dist/timelapse", html=True)),
    Mount(
        "/api",
        name="api",
        routes=[
            Route("/graph", graph.GraphEndpoint, name="graph"),
            Route(
                "/realtime-trips",
                realtime_trips.RealtimeTripsEndpoint,
                name="realtime-trips",
            ),
        ],
    ),
    Mount(
        "/schedule",
        name="schedule",
        routes=[
            Route("/{system}", system.System, name="system"),
            Route("/{system}/{route_id}", route.Route, name="route"),
            Route("/{system}/{route_id}/{stop_id}", stop.Stop, name="stop"),
        ],
    ),
]

app = Starlette(
    routes=routes,
    on_startup=[db.setup],
    on_shutdown=[db.teardown],
    debug=(os.environ.get("ENV") != "PROD"),
)
