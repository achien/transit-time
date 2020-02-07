import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from http import HTTPStatus
from typing import NamedTuple, Optional

import sqlalchemy as sa
from flask import Flask, Response

import c
import db
import logger
from scraper.gtfs import gtfs, gtfs_realtime_pb2
from scraper.nyc import nyct_subway_pb2  # noqa: F401
from scraper.realtime_writer import RealtimeWriter

logger.setup()

# Number of rows to fetch from DB at once
BATCH_SIZE = 100
# Number of rows we process in parallel.  For some reason it's faster to
# process the rows 20 at a time instead of 100 at a time.
CHUNK_SIZE = 20


class Checkpoint(NamedTuple):
    update_time: datetime
    feed_id: str


def format_checkpoint(checkpoint: Checkpoint) -> str:
    return "({}, {})".format(checkpoint.update_time.astimezone(), checkpoint.feed_id)


def dumps_checkpoint(checkpoint: Checkpoint) -> str:
    return json.dumps(
        {
            "update_time": checkpoint.update_time.isoformat(),
            "feed_id": checkpoint.feed_id,
        },
        separators=(",", ":"),
    )


def loads_checkpoint(serialized: str) -> Checkpoint:
    obj = json.loads(serialized)
    return Checkpoint(
        update_time=datetime.fromisoformat(obj["update_time"]), feed_id=obj["feed_id"]
    )


def get_query(
    transit_system: gtfs.TransitSystem,
    checkpoint: Optional[Checkpoint],
    limit: Optional[int],
):
    table = db.get_table("realtime_raw")
    query = sa.select(
        [table.c.feed_id, table.c.time, table.c.raw, table.c.update_time]
    ).where(table.c.system == transit_system.value)
    if checkpoint is not None:
        logging.info(
            "Starting checkpoint: %s", format_checkpoint(checkpoint),
        )
        query = query.where(
            sa.tuple_(table.c.update_time, table.c.feed_id)
            > sa.tuple_(checkpoint.update_time, checkpoint.feed_id)
        )
    query = query.order_by("update_time", "feed_id")
    if limit is not None:
        query = query.limit(limit)
    return query


async def process_row(transit_system: gtfs.TransitSystem, writer: RealtimeWriter, row):
    feed_message_pb = gtfs_realtime_pb2.FeedMessage()
    feed_message_pb.ParseFromString(row["raw"])
    feed_message = gtfs.parse_feed_message(
        transit_system, row["feed_id"], feed_message_pb
    )
    await writer.write_message(feed_message)


def write_db_checkpoint(engine, job_name: str, checkpoint: Checkpoint):
    table = db.get_table("batch_checkpoints")
    with engine.connect() as conn:
        conn.execute(
            table.insert().values(
                job_name=job_name,
                time=datetime.now(timezone.utc),
                checkpoint=dumps_checkpoint(checkpoint),
            )
        )


def read_db_checkpoint(engine, job_name: str) -> Optional[Checkpoint]:
    table = db.get_table("batch_checkpoints")
    with engine.connect() as conn:
        checkpoint_str = conn.scalar(
            sa.select([table.c.checkpoint])
            .where(table.c.job_name == job_name)
            .order_by(sa.desc(table.c.time))
            .limit(1)
        )
        if checkpoint_str is None:
            return None
        return loads_checkpoint(checkpoint_str)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoint", type=str)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--job-name", type=str)
    args = parser.parse_args()

    db.create_tables()

    # For batch jobs, use SQLAlchemy directly instead of aiopg.  aiopg does
    # not support server-side cursors and we need those to iterate through
    # the DB.
    engine = db.get_sa_engine()

    checkpoint = None
    if args.checkpoint:
        checkpoint = loads_checkpoint(args.checkpoint)
    elif args.job_name:
        checkpoint = read_db_checkpoint(engine, args.job_name)

    with engine.connect() as conn:
        transit_system = gtfs.TransitSystem.NYC_MTA
        query = get_query(transit_system, checkpoint, args.limit)
        res = conn.execution_options(stream_results=True).execute(query)
        rowcount = 0
        while True:
            rows = res.fetchmany(BATCH_SIZE)
            if len(rows) == 0:
                # results are exhausted
                break

            writer = RealtimeWriter(transit_system)
            for rows_chunk in c.chunk(rows, CHUNK_SIZE):
                await asyncio.gather(
                    *[process_row(transit_system, writer, row) for row in rows_chunk]
                )
            # for row in rows:
            #     print("{} / {}".format(row["feed_id"], row["time"]))
            #     await process_row(transit_system, writer, row)
            # print(writer._get_trip_row_from_descriptor.cache_info())
            # print(writer._get_trip_row_from_id.cache_info())

            rowcount += len(rows)
            checkpoint = Checkpoint(
                rows[-1]["update_time"].astimezone(timezone.utc), rows[-1]["feed_id"]
            )
            logging.info(
                "Processed %d rows, ending at %s",
                rowcount,
                format_checkpoint(checkpoint),
            )
            logging.info("Checkpoint: %s", dumps_checkpoint(checkpoint))

            if args.job_name:
                write_db_checkpoint(engine, args.job_name, checkpoint)


async def main_wrapper():
    try:
        await db.setup()
        await main()
    finally:
        await db.teardown()


def main_flask_wrapper():
    asyncio.run(main_wrapper())
    return Response(status=HTTPStatus.OK)


if __name__ == "__main__":
    env = os.environ.get("ENV")
    if env == "GCP_RUN":
        # Google Cloud Run wants us to listen and respond to a HTTP request
        app = Flask(__name__)
        app.add_url_rule("/", "main", main_flask_wrapper, methods=["POST"])
        app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    else:
        asyncio.run(main_wrapper())
