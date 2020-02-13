import argparse
import asyncio
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Any, ClassVar, Dict, List, Optional, Tuple

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


class Checkpoint:
    """Checkpoint for batch processor"""

    fields: Dict[str, Any]

    def __init__(self, fields):
        self.fields = fields

    def __str__(self):
        return self.dumps()

    def dumps(self) -> str:
        """Serializes the checkpoint to a string"""
        return json.dumps(
            {k: self._serialize_value(k, v) for (k, v) in self.fields.items()},
            separators=(",", ":"),
        )

    @classmethod
    def loads(cls, serialized: str) -> "Checkpoint":
        """Deserializes checkpoint from string"""
        fields = {
            k: cls._deserialize_value(k, v) for (k, v) in json.loads(serialized).items()
        }
        return cls(fields)

    def as_list(self, cols: Tuple[str, ...]) -> List:
        """Returns checkpoint as a tuple in the specific order"""
        if len(cols) != len(self.fields):
            raise Exception(
                "Mismatch between columns {} and fields {}".format(
                    cols, list(self.fields.keys())
                )
            )
        return [self.fields[col] for col in cols]

    def _serialize_value(self, field_name, value):
        """
        Serializes an individual field.

        Required for fields that cannot be serialized natively.  Override for
        custom processing.
        """
        if isinstance(value, dict):
            return {
                "type": "dict",
                "value": value,
            }
        if isinstance(value, datetime):
            return {
                "type": "datetime",
                "value": value.astimezone(timezone.utc).isoformat(),
            }
        return value

    @classmethod
    def _deserialize_value(cls, field_name, serialized_value):
        if isinstance(serialized_value, dict):
            t = serialized_value["type"]
            v = serialized_value["value"]
            if t == "dict":
                return v
            if t == "datetime":
                return datetime.fromisoformat(v)
            raise Exception("Cannot deserialize type {}, value {}".format(t, v))
        return serialized_value


class BatchProcessor(ABC):
    CheckpointCls: ClassVar = Checkpoint

    table_name: str
    cols: Tuple[str, ...]
    # Additional filtering on table
    where: Optional
    # Column names that form a group (e.g. feed_id).  If groups exist,
    # each group is processed serially.  This is used if processing requires
    # all previous rows to be processed first.
    groups: Tuple[str, ...]

    def __init__(
        self,
        table_name: str,
        cols: Tuple[str, ...],
        where: Optional = None,
        groups: Optional[Tuple[str, ...]] = None,
    ):
        self.table_name = table_name
        self.cols = cols
        self.where = where
        self.groups = groups if groups is not None else ()

    @abstractmethod
    async def process_row(row):
        pass

    def get_query(self, checkpoint: Optional[Checkpoint], limit: Optional[int]):
        table = db.get_table(self.table_name)
        query = table.select()
        if self.where is not None:
            query = query.where(self.where)
        if checkpoint is not None:
            logging.info("Starting checkpoint: %s", checkpoint)
            query = query.where(
                sa.tuple_(*[table.c[col] for col in self.cols])
                > sa.tuple_(*checkpoint.as_list(self.cols))
            )
        query = query.order_by(*self.cols)
        if limit is not None:
            query = query.limit(limit)
        return query

    def write_checkpoint(self, engine, job_name: str, checkpoint: Checkpoint):
        table = db.get_table("batch_checkpoints")
        with engine.connect() as conn:
            conn.execute(
                table.insert().values(
                    job_name=job_name,
                    time=datetime.now(timezone.utc),
                    checkpoint=checkpoint.dumps(),
                )
            )

    def read_checkpoint(self, engine, job_name: str) -> Optional[Checkpoint]:
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
            return self.CheckpointCls.loads(checkpoint_str)

    async def main(self):
        try:
            await db.setup()
            await self._main()
        finally:
            await db.teardown()

    async def _main(self):
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
            checkpoint = self.CheckpointCls.loads(args.checkpoint)
        elif args.job_name:
            checkpoint = self.read_checkpoint(engine, args.job_name)

        with engine.connect() as conn:
            query = self.get_query(checkpoint, args.limit)
            res = conn.execution_options(stream_results=True).execute(query)
            rowcount = 0
            while True:
                rows = res.fetchmany(BATCH_SIZE)
                if len(rows) == 0:
                    # results are exhausted
                    break
                logging.debug("Processing batch of %d rows", len(rows))

                if len(self.groups) > 0:
                    await self.process_as_groups(rows)
                else:
                    await self.process_as_chunks(rows)

                rowcount += len(rows)
                checkpoint_data = {col: rows[-1][col] for col in self.cols}
                checkpoint = self.CheckpointCls(checkpoint_data)
                logging.info(
                    "Processed %d rows, ending at %s", rowcount, checkpoint,
                )
                logging.info("Checkpoint: %s", checkpoint.dumps())

                if args.job_name:
                    self.write_checkpoint(engine, args.job_name, checkpoint)

    async def process_as_chunks(self, rows):
        chunks = c.chunk(rows, CHUNK_SIZE)
        for (i, rows_chunk) in enumerate(chunks):
            logging.debug(
                "Processing chunk %d/%d with %d rows",
                i + 1,
                len(chunks),
                len(rows_chunk),
            )
            await asyncio.gather(*[self.process_row(row) for row in rows_chunk])

    async def process_as_groups(self, rows):
        groups = {}
        for row in rows:
            group_key = tuple([row[col] for col in self.groups])
            if group_key not in groups:
                groups[group_key] = [row]
            else:
                groups[group_key].append(row)

        logging.debug("Found %d groups of (%s)", len(groups), ",".join(self.groups))

        async def process_group(group_name, group_rows):
            for row in group_rows:
                await self.process_row(row)
            logging.debug(
                "Finished processing group %s with %d rows", group_name, len(group_rows)
            )

        await asyncio.gather(
            *[
                process_group(group_name, group_rows)
                for (group_name, group_rows) in groups.items()
            ]
        )


class RealtimeRawCheckpointDEPRECATED(Checkpoint):
    def _serialize_value(self, field_name, value):
        if field_name == "update_time":
            return value.isoformat()
        return super()._serialize_value(field_name, value)

    @classmethod
    def _deserialize_value(cls, field_name, serialized_value):
        if field_name == "update_time":
            return datetime.fromisoformat(serialized_value)
        return super()._deserialize_value(field_name, serialized_value)


class RealtimeBatchProcessorDEPRECATED(BatchProcessor):
    CheckpointCls = RealtimeRawCheckpointDEPRECATED
    transit_system = gtfs.TransitSystem.NYC_MTA

    def __init__(self):
        table = db.get_table("realtime_raw")
        super().__init__(
            "realtime_raw",
            ("update_time", "feed_id"),
            table.c.system == self.transit_system.value,
        )
        self.writer = RealtimeWriter(self.transit_system)

    async def process_row(self, row):
        feed_message_pb = gtfs_realtime_pb2.FeedMessage()
        feed_message_pb.ParseFromString(row["raw"])
        feed_message = gtfs.parse_feed_message(
            self.transit_system, row["feed_id"], feed_message_pb
        )
        await self.writer.write_message(feed_message)


def main_flask_wrapper():
    processor = RealtimeBatchProcessorDEPRECATED()
    asyncio.run(processor.main())
    return Response(status=HTTPStatus.OK)


if __name__ == "__main__":
    env = os.environ.get("ENV")
    if env == "GCP_RUN":
        # Google Cloud Run wants us to listen and respond to a HTTP request
        app = Flask(__name__)
        app.add_url_rule("/", "main", main_flask_wrapper, methods=["POST"])
        app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    else:
        processor = RealtimeBatchProcessorDEPRECATED()
        asyncio.run(processor.main())
