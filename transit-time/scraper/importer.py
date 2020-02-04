import asyncio
import csv
import logging
import os.path
import sys
from datetime import timedelta
from typing import Any, Callable, Dict, Iterable, List, NamedTuple

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

import c
import db
import logger
from scraper import nyc
from scraper.gtfs import gtfs

logger.setup()


INSERT_CHUNK_SIZE = 1000


class CartesianProduct(NamedTuple):
    options: Iterable


class ImportSpec(NamedTuple):
    # CSV File to import from
    csv: str
    # Function that returns an SQLAlchemy table
    table: str
    # Columns to copy directly from the CSV file
    copy: List[str]
    # Columns we compute from the CSV file
    compute: Dict[str, Callable[[Dict[str, str]], Any]]


IMPORT_SPECS: List[ImportSpec] = [
    ImportSpec(
        csv="agency.txt",
        table="agency",
        copy=[
            "agency_id",
            "agency_name",
            "agency_url",
            "agency_timezone",
            "agency_lang",
            "agency_phone",
        ],
        compute={},
    ),
    ImportSpec(
        csv="stops.txt",
        table="stops",
        copy=[
            "stop_id",
            "stop_code",
            "stop_name",
            "stop_desc",
            "zone_id",
            "stop_url",
            "parent_station",
        ],
        compute={
            "stop_loc": lambda row: "POINT({} {})".format(
                row["stop_lon"], row["stop_lat"]
            ),
            "location_type": lambda row: gtfs.LocationType(int(row["location_type"])),
        },
    ),
    ImportSpec(
        csv="routes.txt",
        table="routes",
        copy=[
            "route_id",
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_desc",
            "route_type",
            "route_url",
            "route_color",
            "route_text_color",
        ],
        compute={},
    ),
    ImportSpec(
        csv="trips.txt",
        table="trips",
        copy=[
            "trip_id",
            "route_id",
            "service_id",
            "trip_headsign",
            "direction_id",
            "block_id",
            "shape_id",
        ],
        compute={},
    ),
    ImportSpec(
        csv="trips.txt",
        table="mta_trip_id",
        copy=["trip_id"],
        compute={
            "alternate_trip_id": lambda row: CartesianProduct(
                nyc.short_trip_ids(row["trip_id"])
            ),
            "service_day": lambda row: nyc.parse_trip_id(row["trip_id"]).service_day,
        },
    ),
    ImportSpec(
        csv="stop_times.txt",
        table="stop_times",
        copy=[
            "trip_id",
            "stop_sequence",
            "stop_id",
            "stop_headsign",
            "pickup_type",
            "drop_off_type",
            "shape_dist_traveled",
        ],
        compute={
            "arrival_time": lambda row: get_timedelta(row["arrival_time"]),
            "departure_time": lambda row: get_timedelta(row["departure_time"]),
        },
    ),
]


# Converts 12:22:30 or 24:52:00 into a datetime.timedelta
def get_timedelta(gtfs_time: str) -> timedelta:
    parts = gtfs_time.split(":")
    assert len(parts) == 3
    (h, m, s) = parts
    return timedelta(seconds=int(s), minutes=int(m), hours=int(h))


async def delete_tables(transit_system: gtfs.TransitSystem):
    # Order matters due to foreign keys
    tables = reversed([db.get_table(spec.table) for spec in IMPORT_SPECS])
    async with db.acquire_conn() as conn:
        for table in tables:
            res = await conn.execute(
                table.delete().where(table.c.system == transit_system.value)
            )
            logging.info("Deleted %d rows from %s", res.rowcount, table.name)


def get_db_rows(db_row_spec: Dict) -> List[Dict]:
    """Converts row spec with CartesianProducts into rows"""
    rows = [{}]
    for col, val in db_row_spec.items():
        if isinstance(val, CartesianProduct):
            next_rows = []
            for v in val.options:
                for row in rows:
                    row_copy = row.copy()
                    row_copy[col] = v
                    next_rows.append(row_copy)
            rows = next_rows
        else:
            for row in rows:
                row[col] = val
    return rows


async def import_generic(
    transit_system: gtfs.TransitSystem,
    csv_path: str,
    table: sa.Table,
    spec: ImportSpec,
):
    with open(csv_path) as csv_file:
        reader = csv.DictReader(csv_file)
        db_rows = []
        for csv_row in reader:
            db_row_spec = {}
            db_row_spec["system"] = transit_system.value
            for col in spec.copy:
                db_row_spec[col] = csv_row[col] if csv_row[col] != "" else None
            for col_name, generator in spec.compute.items():
                db_row_spec[col_name] = generator(csv_row)
            db_rows.extend(get_db_rows(db_row_spec))

        async with db.acquire_conn() as conn:
            await import_rows_batch(conn, table, db_rows)


async def import_rows_batch(conn, table, db_rows: List[Dict]):
    rows_inserted = 0
    for rows_chunk in c.chunk(db_rows, INSERT_CHUNK_SIZE):
        # We should update existing values instead of doing nothing, but
        # that requires specifying an index and I don't know how to do that
        # generically.
        await conn.execute(insert(table).values(rows_chunk).on_conflict_do_nothing())
        rows_inserted += len(rows_chunk)
        logging.info(
            "Imported %d/%d rows into %s", rows_inserted, len(db_rows), table.name
        )


async def main():
    # To run this script, comment out this line.  If importing MTA data,
    # you need to drop the mta_trip_id table, since it does not have a
    # primary key.  Ideally we'd also get some on_conflict_do_update
    # up in this file if we are updating any existing data with new data.
    raise Exception("This is not safe to run because it will mess up existing jobs.")
    db.create_tables()

    transit_system = gtfs.TransitSystem.NYC_MTA
    base_dir = sys.argv[1]

    # This does not work anymore because we have other tables with foreign
    # keys.
    # await delete_tables(transit_system)

    for spec in IMPORT_SPECS:
        csv_path = os.path.join(base_dir, spec.csv)
        if os.path.exists(csv_path):
            await import_generic(
                transit_system, csv_path, db.get_table(spec.table), spec,
            )


async def main_wrapper():
    try:
        await db.setup()
        await main()
    finally:
        await db.teardown()


if __name__ == "__main__":
    asyncio.run(main_wrapper())
