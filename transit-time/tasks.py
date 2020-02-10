import logging
import os
from http import HTTPStatus

from starlette.applications import Starlette
from starlette.datastructures import Headers
from starlette.middleware import Middleware
from starlette.responses import Response
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

import db
import logger
from scraper.scraper_job import scrape

logger.setup()


GAE_TASK_HEADERS = (
    "X-AppEngine-QueueName",
    "X-AppEngine-TaskName",
    "X-AppEngine-TaskRetryCount",
    "X-AppEngine-TaskExecutionCount",
    "X-AppEngine-TaskETA",
)


class GAETaskMiddleware:
    app: ASGIApp

    def __init__(self, app: ASGIApp) -> None:
        self.app = app
        # All headers are converted to lowercase (maybe as part of ASGI?)
        self.required_headers = [header.lower() for header in GAE_TASK_HEADERS]

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)

        headers = Headers(scope=scope)
        for header in headers:
            if header in self.required_headers:
                return await self.app(scope, receive, send)

        logging.error("No App Engine task header found.  Headers: %s", headers)
        response = Response(None, HTTPStatus.FORBIDDEN)
        return await response(scope, receive, send)


async def scraper(request):
    await scrape()
    return Response(None, HTTPStatus.OK)


routes = [Route("/scraper", scraper)]

app = Starlette(
    routes=routes,
    middleware=[Middleware(GAETaskMiddleware)],
    on_startup=[db.setup],
    on_shutdown=[db.teardown],
    debug=(os.environ.get("ENV") != "prod"),
)
