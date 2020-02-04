import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import dateparser
from starlette.endpoints import HTTPEndpoint
from starlette.responses import JSONResponse

import db
from scraper.gtfs import gtfs

TRANSIT_SYSTEM = gtfs.TransitSystem.NYC_MTA


DEFAULT_START = "1 hour ago"
DEFAULT_END = "now"


class RealtimeTripsEndpoint(HTTPEndpoint):
    async def get(self, request):
        start_str = request.query_params.get("start", DEFAULT_START)
        end_str = request.query_params.get("end", DEFAULT_END)
        now = datetime.now(timezone.utc)
        dateparser_settings = {"RELATIVE_BASE": now, "RETURN_AS_TIMEZONE_AWARE": True}
        start = dateparser.parse(start_str, settings=dateparser_settings)
        if start is None:
            start = dateparser.parse(DEFAULT_START, settings=dateparser_settings)
        end = dateparser.parse(end_str, settings=dateparser_settings)
        if end is None:
            end = dateparser.parse(DEFAULT_END, settings=dateparser_settings)

        logging.info("Fetching realtime data between %s and %s", start, end)

        # Break down the query into chunks.  For some reason with larger time
        # ranges the Postgres queries take much much longer.
        chunks = self.get_query_chunks(start, end)
        chunk_rows = await asyncio.gather(
            *[
                self.query_chunk_rows(chunk_start, chunk_end, idx, len(chunks))
                for (idx, (chunk_start, chunk_end)) in enumerate(chunks)
            ]
        )
        rows = []
        for cr in chunk_rows:
            rows.extend(cr)
        logging.info("RealtimeTripsEndpoint: %d total rows", len(rows))

        trip_data = {}
        for row in rows:
            key = "__".join((row.route_id, row.start_date.isoformat(), row.trip_id))
            if key not in trip_data:
                trip_data[key] = {
                    "id": key,
                    "routeID": row.route_id,
                    "stops": [],
                }
            trip_data[key]["stops"].append(
                {
                    "stopID": row.stop_id,
                    "arrival": (
                        row.arrival.timestamp()
                        if row.arrival
                        else row.departure.timestamp()
                    ),
                    "departure": (
                        row.departure.timestamp()
                        if row.departure
                        else row.arrival.timestamp()
                    ),
                }
            )
        logging.info("RealtimeTripsEndpoint: constructed %d trip datas", len(trip_data))

        # graph = await load_graph(TRANSIT_SYSTEM)
        # for key in trip_data:
        #     stops = trip_data[key]["stops"]
        #     for i in range(len(stops) - 1):
        #         stop = stops[i]
        #         next_stop = stops[i + 1]
        #         path = graph.shortest_path(
        #             stop["stopID"], next_stop["stopID"], trip_data[key]["routeID"]
        #         )
        #         stop["path"] = (
        #             None
        #             if path is None or len(path) == 0
        #             else [
        #                 {"edgeID": str(x.edge_id), "direction": x.direction}
        #                 for x in path
        #             ]
        #         )

        return JSONResponse(
            {"start": start.timestamp(), "end": end.timestamp(), "tripData": trip_data}
        )

    async def query_chunk_rows(
        self, start: datetime, end: datetime, chunk_idx: int, total_chunks: int
    ):
        # We chunk the query into smaller pieces.  We need to make sure
        # the queries return distinct rows.  We both sort by and filter on
        # COALESCE(departure, arrival).  For filtering we write it with
        # departure BETWEEN and arrival BETWEEN statements because departure
        # and arrival are indexed.
        query = """
            select route_id, start_date, trip_id, stop_id, arrival, departure
            from realtime_stop_times
            where
                system=%s
                and (
                    departure between %s and %s
                    or (departure is null and arrival between %s and %s)
                )
            order by coalesce(departure, arrival)
        """
        async with db.acquire_conn() as conn:
            res = await conn.execute(
                query, TRANSIT_SYSTEM.value, start, end, start, end,
            )
            rows = await res.fetchall()
        logging.info(
            "RealtimeTripsEndpoint: %d rows in chunk %d/%d",
            len(rows),
            chunk_idx + 1,
            total_chunks,
        )
        return rows

    def get_query_chunks(
        self, start: datetime, end: datetime
    ) -> List[Tuple[datetime, datetime]]:
        # Pad start/end in the query so we see trains at those times
        query_start = start - timedelta(minutes=30)
        query_end = end + timedelta(minutes=30)

        # Large queries time out, so break into a few smaller queries
        chunks = []
        query_chunk_interval = timedelta(hours=1)
        while True:
            if query_start + query_chunk_interval < query_end:
                chunks.append([query_start, query_start + query_chunk_interval])
                query_start += query_chunk_interval
            else:
                chunks.append([query_start, query_end])
                return chunks
