import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import google.protobuf.json_format
from sqlalchemy.dialects.postgresql import insert

import a
import db
import logger
from scraper import nyc
from scraper.gtfs import gtfs

logger.setup()

MAX_RETRIES = 5
RETRY_INTERVAL = 2


async def process_feed(feed_id: str):
    feed_message = await nyc.get_data(feed_id)
    request_time = datetime.now(timezone.utc)
    if not feed_message.IsInitialized():
        raise Exception(
            "Unable to parse NYC MTA feed {}: (FeedMessage not initialized)".format(
                feed_id
            )
        )

    timestamp = feed_message.header.timestamp
    json_str = google.protobuf.json_format.MessageToJson(feed_message)
    logging.info(
        "NYC MTA feed {}: {} ({:.3f} seconds ago), {} JSON bytes".format(
            feed_id, timestamp, time.time() - timestamp, len(json_str)
        )
    )

    async with db.acquire_conn() as conn:
        table = db.get_table("realtime_raw")
        await conn.execute(
            insert(table)
            .values(
                system=gtfs.TransitSystem.NYC_MTA.value,
                feed_id=feed_id,
                time=datetime.fromtimestamp(timestamp, timezone.utc),
                json=json.loads(json_str),
                raw=feed_message.SerializeToString(),
                update_time=request_time,
            )
            .on_conflict_do_update(
                table.primary_key, set_=dict(update_time=request_time)
            )
        )


async def process_feed_with_retries(feed_id: str, max_retries: int):
    retries = 0
    exceptions = []
    while True:
        try:
            return (await process_feed(feed_id), retries)
        except Exception as e:
            exceptions.append(e)
            logging.warning("Error fetching feed %s: %s", feed_id, e)
        retries += 1
        if retries > max_retries:
            logging.error(
                "Unable to parse NYC MTA feed %s after %d retries, giving up now",
                feed_id,
                max_retries,
            )
            raise exceptions[0]
        # wait a bit to see if the error resolves itself
        await asyncio.sleep(RETRY_INTERVAL)
        logging.info("Retrying feed %s", feed_id)


async def scrape():
    results = await a.gatherd(
        {
            feed_id: process_feed_with_retries(feed_id, MAX_RETRIES)
            for feed_id in nyc.FEED_IDS
        },
        return_exceptions=True,
    )
    e = None
    for feed_id, result in results.items():
        if isinstance(result, Exception):
            logging.error(
                "Error with NYC MTA feed %s (%d retries): %s",
                feed_id,
                MAX_RETRIES,
                result,
            )
            if e is None:
                e = result
        else:
            (_, retries) = result
            logging.info(
                "Successfully processed NYC MTA feed %s (%d retries)", feed_id, retries,
            )


async def main():
    try:
        db.create_tables()
        await db.setup()
        await scrape()
    finally:
        await db.teardown()


if __name__ == "__main__":
    asyncio.run(main())
