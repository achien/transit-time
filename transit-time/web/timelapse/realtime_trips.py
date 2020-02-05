import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import dateparser
from starlette.endpoints import HTTPEndpoint
from starlette.responses import JSONResponse

import db
from scraper.gtfs import gtfs

TRANSIT_SYSTEM = gtfs.TransitSystem.NYC_MTA
TIMEZONE = "America/New_York"

DEFAULT_START = "1 hour ago"
DEFAULT_END = "now"

PAGE_SIZE = timedelta(hours=5)


class RealtimeTripsEndpoint(HTTPEndpoint):
    async def get(self, request):
        (start, end, query_start, query_end) = self.get_time_range(request)

        next_cursor = None
        paginate = int(request.query_params.get("paginate"))
        if paginate:
            if query_end - query_start > PAGE_SIZE:
                # This query will be (query_start, query_start + PAGE_SIZE),
                # so the next cursor encodes the remaining query of
                # (query_start + PAGE_SIZE, query_end).
                next_cursor = self.encode_cursor(
                    start, end, query_start + PAGE_SIZE, query_end
                )
                # Shrink query range for this query to be one page
                query_end = query_start + PAGE_SIZE

        logging.info("Querying realtime data between %s and %s", query_start, query_end)

        # Break down the query into chunks.  For some reason with larger time
        # ranges the Postgres queries take much much longer.
        chunks = self.get_query_chunks(query_start, query_end)
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
            key = "__".join(
                (row["route_id"], row["start_date"].isoformat(), row["trip_id"])
            )
            if key not in trip_data:
                trip_data[key] = {
                    "id": key,
                    "routeID": row["route_id"],
                    "stops": [],
                }
            arrival = row["arrival"]
            departure = row["departure"]
            trip_data[key]["stops"].append(
                {
                    "stopID": row["stop_id"],
                    "arrival": (
                        arrival.timestamp() if arrival else departure.timestamp()
                    ),
                    "departure": (
                        departure.timestamp() if departure else arrival.timestamp()
                    ),
                }
            )
        logging.info("RealtimeTripsEndpoint: constructed %d trip datas", len(trip_data))

        return JSONResponse(
            {
                "start": start.timestamp(),
                "end": end.timestamp(),
                "tripData": trip_data,
                "cursor": next_cursor,
            }
        )

    def encode_cursor(
        self, start: datetime, end: datetime, query_start: datetime, query_end: datetime
    ) -> str:
        return json.dumps(
            {
                # Start time for the original query
                "start": start.timestamp(),
                # End time for the original query
                "end": end.timestamp(),
                # Start time of remaining data we query (initially start -
                # 30 minutes, then shrinks for each call we make)
                "query_start": query_start.timestamp(),
                # End time of remaining data we query (should always be
                # end + 30 minutes)
                "query_end": query_end.timestamp(),
            }
        )

    def decode_cursor(
        self, cursor: str
    ) -> Tuple[datetime, datetime, datetime, datetime]:
        cursor_dict = json.loads(cursor)
        start = datetime.fromtimestamp(cursor_dict["start"], timezone.utc)
        end = datetime.fromtimestamp(cursor_dict["end"], timezone.utc)
        query_start = datetime.fromtimestamp(cursor_dict["query_start"], timezone.utc)
        query_end = datetime.fromtimestamp(cursor_dict["query_end"], timezone.utc)
        return (start, end, query_start, query_end)

    def get_time_range(self, request) -> Tuple[datetime, datetime, datetime, datetime]:
        cursor = request.query_params.get("cursor")
        if cursor is not None:
            return self.decode_cursor(cursor)

        start_str = request.query_params.get("start", DEFAULT_START)
        end_str = request.query_params.get("end", DEFAULT_END)
        now = datetime.now(timezone.utc)
        dateparser_settings = {
            "RELATIVE_BASE": now,
            "RETURN_AS_TIMEZONE_AWARE": True,
            "TIMEZONE": TIMEZONE,
        }
        start = dateparser.parse(start_str, settings=dateparser_settings)
        if start is None:
            start = dateparser.parse(DEFAULT_START, settings=dateparser_settings)
        end = dateparser.parse(end_str, settings=dateparser_settings)
        if end is None:
            end = dateparser.parse(DEFAULT_END, settings=dateparser_settings)
        # Pad start/end in the query so we see trains at those times
        query_start = start - timedelta(minutes=30)
        query_end = end + timedelta(minutes=30)
        return (start, end, query_start, query_end)

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
                system=$1
                and (
                    departure >= $2 and departure < $3
                    or (departure is null and arrival >= $2 and arrival < $3)
                )
            order by coalesce(departure, arrival)
        """
        async with db.acquire_asyncpg_conn() as conn:
            rows = await conn.fetch(query, TRANSIT_SYSTEM.value, start, end)
        logging.info(
            "RealtimeTripsEndpoint: %d rows in chunk %d/%d",
            len(rows),
            chunk_idx + 1,
            total_chunks,
        )
        return rows

    def get_query_chunks(
        self, query_start: datetime, query_end: datetime
    ) -> List[Tuple[datetime, datetime]]:
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
