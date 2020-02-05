import logging
import os

import asyncpg
import aiopg.sa
import geoalchemy2 as ga
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from scraper import nyc
from scraper.gtfs import gtfs


def _get_postgres_info():
    return {
        "database": "transit_time",
        "host": os.environ["POSTGRES_HOST"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


_aiopg_engine = None
_asyncpg_pool = None


async def setup():
    global _aiopg_engine
    _aiopg_engine = await aiopg.sa.create_engine(**_get_postgres_info())
    logging.info("[aiopg] database connected: %s", _aiopg_engine.dsn)

    global _asyncpg_pool
    _asyncpg_pool = await asyncpg.create_pool(**_get_postgres_info())
    logging.info("[asyncpg] database connected")


async def teardown():
    global _aiopg_engine
    _aiopg_engine.close()
    await _aiopg_engine.wait_closed()
    logging.info("[aiopg] database connection closed: %s", _aiopg_engine.dsn)
    _aiopg_engine = None

    global _asyncpg_pool
    await _asyncpg_pool.close()
    logging.info("[async] database connection closed")
    _asyncpg_pool = None


def acquire_conn():
    assert _aiopg_engine is not None
    return _aiopg_engine.acquire()


def acquire_asyncpg_conn():
    assert _asyncpg_pool is not None
    return _asyncpg_pool.acquire()


def get_sa_engine():
    return sa.create_engine(
        "postgresql+psycopg2://{user}:{password}@{host}/{database}".format(
            **_get_postgres_info()
        )
    )


_initted = False
_metadata = None
_tables = {}


def get_metadata():
    global _metadata
    _init()
    return _metadata


def get_table(table_name: str):
    global _tables
    _init()
    return _tables[table_name]


def _init():
    global _initted
    if _initted:
        return
    _initted = True

    global _metadata
    global _tables
    _metadata = sa.MetaData()
    _tables = init_tables(_metadata)


def init_tables(metadata):
    tables = {}

    tables["batch_checkpoints"] = sa.Table(
        "batch_checkpoints",
        metadata,
        sa.Column("job_name", sa.String, primary_key=True),
        sa.Column("time", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("checkpoint", sa.String, nullable=False),
    )

    tables["realtime_raw"] = sa.Table(
        "realtime_raw",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("feed_id", sa.String, primary_key=True),
        sa.Column("time", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("json", postgresql.JSONB, nullable=False),
        sa.Column("raw", sa.LargeBinary, nullable=False),
        sa.Column(
            "update_time", sa.DateTime(timezone=True), nullable=False, index=True
        ),
    )

    tables["agency"] = sa.Table(
        "agency",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("agency_id", sa.String, primary_key=True),
        sa.Column("agency_name", sa.String, nullable=False),
        sa.Column("agency_url", sa.String, nullable=False),
        sa.Column("agency_timezone", sa.String, nullable=False),
        sa.Column("agency_lang", sa.String),
        sa.Column("agency_phone", sa.String),
    )

    tables["stops"] = sa.Table(
        "stops",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("stop_id", sa.String, primary_key=True),
        sa.Column("stop_code", sa.String),
        sa.Column("stop_name", sa.String),
        sa.Column("stop_desc", sa.String),
        sa.Column("stop_loc", ga.Geometry("POINT")),
        sa.Column("zone_id", sa.String),
        sa.Column("stop_url", sa.String),
        sa.Column("location_type", sa.Enum(gtfs.LocationType)),
        sa.Column("parent_station", sa.String),
    )

    tables["routes"] = sa.Table(
        "routes",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("route_id", sa.String, primary_key=True),
        sa.Column("agency_id", sa.String),
        sa.Column("route_short_name", sa.String),
        sa.Column("route_long_name", sa.String),
        sa.Column("route_desc", sa.String),
        sa.Column("route_type", sa.Integer),
        sa.Column("route_url", sa.String),
        sa.Column("route_color", sa.String),
        sa.Column("route_text_color", sa.String),
    )

    tables["trips"] = sa.Table(
        "trips",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("trip_id", sa.String, primary_key=True),
        sa.Column("route_id", sa.String, nullable=False),
        sa.Column("service_id", sa.String, nullable=False),
        sa.Column("trip_headsign", sa.String),
        sa.Column("direction_id", sa.Integer),
        sa.Column("block_id", sa.String),
        sa.Column("shape_id", sa.String),
        sa.ForeignKeyConstraint(
            ["system", "route_id"], ["routes.system", "routes.route_id"]
        ),
        # When we import shape_id, add ForeignKeyConstraint here
    )

    # Table for MTA trip_id lookups in realtime data.  Realtime trip_ids are a
    # substring of the full trip_id.  It looks like it is based on origin time,
    # route, and direction, and it is unique for Weekday/Saturday/Sunday.
    tables["mta_trip_id"] = sa.Table(
        "mta_trip_id",
        metadata,
        # (system, alternate_trip_id, service_day) is not unique and cannot
        # be the primary key
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("alternate_trip_id", sa.String, primary_key=True),
        sa.Column("service_day", sa.Enum(nyc.ServiceDay), primary_key=True),
        sa.Column("trip_id", sa.String, primary_key=True),
        sa.Index(
            "idx_lookup_mta_trip_id", "system", "alternate_trip_id", "service_day"
        ),
        sa.ForeignKeyConstraint(
            ["system", "trip_id"], ["trips.system", "trips.trip_id"]
        ),
    )

    # Scheduled stops
    tables["stop_times"] = sa.Table(
        "stop_times",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("trip_id", sa.String, primary_key=True),
        sa.Column("stop_sequence", sa.Integer, primary_key=True),
        sa.Column("stop_id", sa.String, nullable=False),
        # We cannot store these as Time, because these can have a value
        # >= 24:00:00 for trips starting/ending after midnight.
        # Instead, store as an interval.
        # For GTFS, times are measured relatively to "noon - 12h" which is
        # midnight except for days with daylight savings time changes.
        sa.Column("arrival_time", sa.Interval),
        sa.Column("departure_time", sa.Interval),
        sa.Column("stop_headsign", sa.String),
        sa.Column("pickup_type", sa.Integer),
        sa.Column("drop_off_type", sa.Integer),
        sa.Column("shape_dist_traveled", sa.Float),
        sa.ForeignKeyConstraint(
            ["system", "trip_id"], ["trips.system", "trips.trip_id"]
        ),
        sa.ForeignKeyConstraint(
            ["system", "stop_id"], ["stops.system", "stops.stop_id"]
        ),
    )

    tables["trip_paths"] = sa.Table(
        "trip_paths",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("shape_id", sa.String, primary_key=True),
        sa.Column("routes", sa.ARRAY(sa.String)),
        sa.Column("shape", ga.Geometry("LINESTRING")),
    )

    # Actual stops, based on realtime data.  Can be past or future.
    tables["realtime_stop_times"] = sa.Table(
        "realtime_stop_times",
        metadata,
        sa.Column("system", sa.String, nullable=False),
        sa.Column("route_id", sa.String, nullable=False),
        sa.Column("stop_id", sa.String, nullable=False),
        sa.Column("start_date", sa.Date, nullable=False),
        sa.Column("trip_id", sa.String, nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True)),
        sa.Column("arrival", sa.DateTime(timezone=True), index=True),
        sa.Column("departure", sa.DateTime(timezone=True), index=True),
        sa.Column("update_time", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("arrival IS NOT NULL OR departure IS NOT NULL"),
        # Index for lookups.  Not primary key because we might need to add seq
        # in the future to support trips that reuse stops.
        sa.Index(
            "idx_lookup_realtime_stop_times",
            "system",
            "route_id",
            "stop_id",
            "start_date",
            "trip_id",
            unique=True,
        ),
        sa.Index(
            "ix_realtime_stop_times_lookup_trip",
            "system",
            "route_id",
            "start_date",
            "trip_id",
        ),
        sa.ForeignKeyConstraint(
            ["system", "route_id"],
            [tables["routes"].c.system, tables["routes"].c.route_id],
        ),
        sa.ForeignKeyConstraint(
            ["system", "stop_id"],
            [tables["stops"].c.system, tables["stops"].c.stop_id],
        ),
    )

    tables["realtime_vehicle_positions"] = sa.Table(
        "realtime_vehicle_positions",
        metadata,
        sa.Column("system", sa.String, nullable=False),
        sa.Column("route_id", sa.String, nullable=False),
        sa.Column("stop_id", sa.String, nullable=False),
        sa.Column("start_date", sa.Date, nullable=False),
        sa.Column("trip_id", sa.String, nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.Enum(gtfs.VehicleStopStatus), nullable=False),
        sa.Column("update_time", sa.DateTime(timezone=True), nullable=False),
        # Index for unique vehicle positions.  Not primary key because we
        # might need to add seq in the future to support trips that reuse stops.
        sa.Index(
            "idx_unique_realtime_vehicle_positions",
            "system",
            "route_id",
            "stop_id",
            "start_date",
            "trip_id",
            "timestamp",
            unique=True,
        ),
        sa.ForeignKeyConstraint(
            ["system", "route_id"],
            [tables["routes"].c.system, tables["routes"].c.route_id],
        ),
        sa.ForeignKeyConstraint(
            ["system", "stop_id"],
            [tables["stops"].c.system, tables["stops"].c.stop_id],
        ),
    )

    tables["nyc_subway_stations"] = sa.Table(
        "nyc_subway_stations",
        metadata,
        sa.Column("objectid", sa.String, primary_key=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("notes", sa.String, nullable=False),
        sa.Column("lines", sa.ARRAY(sa.String), nullable=False),
        sa.Column("loc", ga.Geometry("POINT"), nullable=False),
    )

    tables["nyc_subway_lines"] = sa.Table(
        "nyc_subway_lines",
        metadata,
        sa.Column("objectid", sa.String, primary_key=True),
        sa.Column("lines", sa.ARRAY(sa.String), nullable=False),
        sa.Column("shape_len", sa.Float, nullable=False),
        sa.Column("path", ga.Geometry("LINESTRING"), nullable=False),
    )

    # Stops that we render on a map.  This does not need to be all stops in
    # the stops table.
    tables["map_stops"] = sa.Table(
        "map_stops",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("stop_id", sa.String, primary_key=True),
        # This does not need to be the same locatiotn as stops.stop_loc,
        # e.g. if we are glueing different datasets together
        sa.Column("loc", ga.Geometry("POINT"), nullable=False),
        sa.ForeignKeyConstraint(
            ["system", "stop_id"],
            [tables["stops"].c.system, tables["stops"].c.stop_id],
        ),
    )

    tables["map_nodes"] = sa.Table(
        "map_nodes",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("edge_ids", sa.ARRAY(sa.Integer), nullable=False),
        sa.Column("loc", ga.Geometry("POINT"), nullable=False),
        sa.Column("stop_ids", sa.ARRAY(sa.String)),
    )

    tables["map_edges"] = sa.Table(
        "map_edges",
        metadata,
        sa.Column("system", sa.String, primary_key=True),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("node_id1", sa.Integer, nullable=False),
        sa.Column("node_id2", sa.Integer, nullable=False),
        sa.Column("routes", sa.ARRAY(sa.String), nullable=False),
        sa.Column("path", ga.Geometry("LINESTRING"), nullable=False),
    )

    return tables


def create_tables():
    engine = get_sa_engine()
    metadata = sa.MetaData()
    init_tables(metadata)
    metadata.create_all(engine)
