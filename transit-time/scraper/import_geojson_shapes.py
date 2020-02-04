"""
MTA shapes.txt is not up to date, but NYC open data has subway shapes
too which look more correct (they include the 2nd Ave extension).
https://data.cityofnewyork.us/Transportation/Subway-Lines/3qz8-muuu/data
"""

import argparse
import asyncio
import json
import logging
import re

import c
import db
import logger
from scraper.gtfs import gtfs

logger.setup()

STATION_NAME_NORMALIZED_WORDS = {
    "Av": "Ave",
    "Avs": "Aves",
    "Avenue": "Ave",
    "Avenues": "Aves",
    "Bklyn": "Brooklyn",
    "Ctr": "Center",
    "Ft": "Fort",
    "Jct": "Junction",
    "Pkwy": "Parkway",
    "Pky": "Parkway",
    "Plz": "Plaza",
    "Yds": "Yards",
    "Barclay's": "Barclays",
    "Beverly Rd": "Beverley Rd",
    "Sound View": "Soundview",
    "Broadway - Lafayette": "Broadway-Lafayette",
    "Myrtle - Willoughby": "Myrtle-Willoughby",
    "Delancy St": "Delancey St",
    "Bus Term": "Bus Terminal",
}

STATION_EQUIVALENTS = {
    "716": ("33rd St", "7"),
    "R08": ("39th Ave", "N-W"),
    "715": ("40th St", "7"),
    "714": ("46th St", "7"),
    "R14": ("57th St", "N-Q-R-W"),
    "724": ("5th Ave - Bryant Pk", "7-7 Express"),
    "G10": ("63rd Dr - Rego Park", "E-M-R"),
    "J17": ("75th St - Eldert Ln", "J-Z"),
    "A21": ("81st St", "A-B-C"),
    "J14": ("104th-102nd Sts", "J-Z"),
    "414": ("161st St - Yankee Stadium", "4"),
    "D11": ("161st St - Yankee Stadium", "B-D"),
    "112": ("168th St", "1"),
    "H02": ("Aqueduct - North Conduit Av", "A"),
    "H01": ("Aqueduct Racetrack", "A"),
    "F05": ("Briarwood - Van Wyck Blvd", "E-F"),
    "L21": ("Bushwick - Aberdeen", "L"),
    # Confusion with rest of Canal St
    "639": ("Canal St", "4-6-6 Express"),
    "Q01": ("Canal St", "N-Q"),
    "A34": ("Canal St - Holland Tunnel", "A-C-E"),
    "N12": ("Coney Island - Stillwell Av", "D-F-N-Q"),
    "F09": ("Court Sq - 23rd St", "E-M"),
    "S01": ("Franklin Ave - Fulton St", "S"),
    # Confusion with stop placement
    "A38": ("Fulton St", "A-C"),
    "M22": ("Fulton St", "J-Z"),
    "N10": ("Gravesend - 86th St", "N"),
    "629": ("Lexington Ave - 59th St", "4-5-6-6 Express"),
    "G22": ("Long Island City - Court Sq", "G"),
    "F14": ("Lower East Side - 2nd Ave", "F"),
    "L17": ("Myrtle - Wyckoff Aves", "L"),
    "M08": ("Myrtle - Wyckoff Aves", "M"),
    "204": ("Nereid Ave (238 St)", "2-5"),
    "D31": ("Newkirk Ave", "B-Q"),
    # Confusion with Rector St (1)
    "R26": ("Rector St", "R-W"),
    "B06": ("Roosevelt Island - Main St", "F"),
    # Confustion with South Ferry Loop which is unused
    "142": ("South Ferry", "1"),
    "G06": ("Sutphin Blvd - Archer Av", "E-J-Z"),
    "251": ("Sutter Ave - Rutland Road", "3-4"),
    "D20": ("W 4th St - Washington Sq (Lower)", "B-D-F-M"),
    "A32": ("W 4th St - Washington Sq (Upper)", "A-C-E"),
    "R27": ("Whitehall St", "R-W"),
    "G11": ("Woodhaven Blvd - Queens Mall", "E-M-R"),
}

NO_STATION = [
    # South Ferry Loop is not used in stop_times, 142 is used instead
    "140",
    # Weird Broad Channel Station floating in space, H10 is used instead
    "H19",
]


def normalize_name(stop_name: str):
    name = stop_name
    for (w1, w2) in STATION_NAME_NORMALIZED_WORDS.items():
        name = re.sub("\\b" + w1 + "\\b", w2, name)
    name = re.sub(r"(\w)- ", r"\1 - ", name)
    name = re.sub(r"(\w)/(\w)", r"\1 / \2", name)
    name = re.sub(r"(\w) / (\w)", r"\1 - \2", name)
    name = re.sub(r"(\b|[02-9])1(-| Ave| St)", r"\g<1>1st\2", name)
    name = re.sub(r"(\b|[02-9])2(-| Ave| St)", r"\g<1>2nd\2", name)
    name = re.sub(r"(\b|[02-9])3(-| Ave| St)", r"\g<1>3rd\2", name)
    name = re.sub(r"(\d)(-| Ave| St)", r"\1th\2", name)
    name = " - ".join(sorted(name.split(" - ")))
    return name


async def import_stations(filename: str):
    with open(filename) as file:
        geodata = json.load(file)

    assert geodata["type"] == "FeatureCollection"
    logging.info("%s has %d stations", filename, len(geodata["features"]))

    async with db.acquire_conn() as conn:
        res = await conn.execute(db.get_table("nyc_subway_stations").delete())
        logging.info("Deleted %d stations", res.rowcount)

    insert_stmt = """
        insert into nyc_subway_stations (objectid, name, notes, lines, loc)
        values (
            %(objectid)s,
            %(name)s,
            %(notes)s,
            %(lines)s,
            ST_GeomFromGeoJSON(%(loc)s)
        )
    """
    values = []
    for feature in geodata["features"]:
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] == "Point"
        props = feature["properties"]
        values.append(
            {
                "objectid": props["objectid"],
                "name": props["name"],
                "notes": props["notes"],
                "lines": props["line"].split("-"),
                "loc": json.dumps(feature["geometry"]),
            }
        )

    async def insert(value):
        async with db.acquire_conn() as conn:
            await conn.execute(insert_stmt, value)

    await asyncio.gather(*[insert(value) for value in values])
    logging.info("Inserted %d stations", len(values))

    stmt = """
        select
            distinct on (stop_id)
            name,
            objectid,
            array_to_string(lines, '-') as line,
            loc,
            stop_name,
            stop_id,
            stop_loc,
            ST_Distance(loc::geography, stop_loc::geography) as distance
        from stops s
        cross join nyc_subway_stations nss
        where parent_station is null
        order by
            stop_id,
            distance asc
    """
    async with db.acquire_conn() as conn:
        res = await conn.execute(stmt)
        rows = await res.fetchall()

    failed = False
    o_to_s = {}
    s_to_o = {}
    for row in rows:
        # Staten Island is very far away and has no subway data, so let's ignore
        # everything more than 1km away from the nearest GeoJSON station
        if row.distance >= 1000:
            continue
        # Some stops have no station
        if row.stop_id in NO_STATION:
            continue
        if row.stop_id in STATION_EQUIVALENTS:
            (name, line) = STATION_EQUIVALENTS[row.stop_id]
            features = [
                f
                for f in geodata["features"]
                if f["properties"]["name"] == name and f["properties"]["line"] == line
            ]
            if len(features) != 1:
                raise Exception(
                    "{}, {} does not uniquely describe station: [{}]".format(
                        name,
                        line,
                        ", ".join([f["properties"]["objectid"] for f in features]),
                    )
                )
            o = features[0]["properties"]["objectid"]
            s = row.stop_id
            if o not in o_to_s:
                o_to_s[o] = []
            o_to_s[o].append(s)
            if s not in s_to_o:
                s_to_o[s] = []
            s_to_o[s].append(o)
            continue
        if normalize_name(row.name) == normalize_name(row.stop_name):
            o = row.objectid
            s = row.stop_id
            s = row.stop_id
            if o not in o_to_s:
                o_to_s[o] = []
            o_to_s[o].append(s)
            if s not in s_to_o:
                s_to_o[s] = []
            s_to_o[s].append(o)
            continue
        print(
            "Name mismatch: {} ({}), {}, {}, {} ({}), {}".format(
                row.stop_name,
                row.stop_id,
                normalize_name(row.stop_name),
                normalize_name(row.name),
                row.name,
                row.line,
                row.distance,
            )
        )
        failed = True
    if failed:
        raise Exception("Cannot write data, resolve mismatches first")

    # Duplicates expected here becuase stops.txt has finer granularity
    # on some stations where platforms are stacked (e.g. a station is a single
    # level of platforms, instead of both of them)
    expected_duplicate_objectids = {
        # Queensboro Plaza
        "103": ["718", "R09"],
        # 145 St
        "295": ["A12", "D13"],
        # Coney Island - Stillwell Ave
        "469": ["D43", "N12"],
    }
    overloaded_objects = {
        k: v
        for (k, v) in o_to_s.items()
        if len(set(v)) > 1 and (k, v) not in expected_duplicate_objectids.items()
    }
    if len(overloaded_objects) > 0:
        logging.info(
            "Multiple stop_ids for objectid: %s", overloaded_objects,
        )
        raise Exception("Cannot write data, resolve overloaded stop_ids")
    overloaded_stops = {k: v for (k, v) in s_to_o.items() if len(set(v)) > 1}
    if len(overloaded_stops) > 0:
        logging.info("Multiple objectids for stop_id: %s", overloaded_stops)
        raise Exception("Cannot write data, resolve overloaded objectids")

    async with db.acquire_conn() as conn:
        res = await conn.execute(db.get_table("stops").select())
        all_stops = await res.fetchall()
        res = await conn.execute(db.get_table("nyc_subway_stations").select())
        all_imported_stations = await res.fetchall()
    # all_stops_by_id = {s.stop_id: s for s in all_stops}
    all_imported_stations_by_id = {s.objectid: s for s in all_imported_stations}

    stmt = db.get_table("map_stops").insert()
    values = []
    for stop in all_stops:
        station_id = stop.parent_station or stop.stop_id
        # station = all_stops_by_id[station_id]
        if station_id in s_to_o:
            # We just imported this station and will use it's data in the map
            objectid = c.only(s_to_o[station_id])
            loc = all_imported_stations_by_id[objectid].loc
        elif station_id in NO_STATION:
            # This station is not in the map
            logging.info("Skipping stop %s (%s) in map", stop.stop_id, stop.stop_name)
            continue
        else:
            # We did not import any data for this station, use the existing
            # location data (e.g. for the Staten Island Railway)
            # loc = stop.stop_loc
            continue
        values.append(
            {
                "system": gtfs.TransitSystem.NYC_MTA.value,
                "stop_id": stop.stop_id,
                "loc": loc,
            }
        )
    async with db.acquire_conn() as conn:
        deleted = await conn.execute(db.get_table("map_stops").delete())
        logging.info("Deleted %d stops from map_stops", deleted.rowcount)
        await conn.execute(db.get_table("map_stops").insert().values(values))
        logging.info("Wrote %d stops to map_stops", len(values))


async def import_lines(filename: str):
    with open(filename) as file:
        geodata = json.load(file)

    assert geodata["type"] == "FeatureCollection"
    logging.info("%s has %d lines", filename, len(geodata["features"]))

    async with db.acquire_conn() as conn:
        res = await conn.execute(db.get_table("nyc_subway_lines").delete())
        logging.info("Deleted %d lines", res.rowcount)

    insert_stmt = """
        insert into nyc_subway_lines (objectid, lines, shape_len, path)
        values (
            %(objectid)s,
            %(lines)s,
            %(shape_len)s,
            ST_GeomFromGeoJSON(%(path)s)
        )
    """
    values = []
    for feature in geodata["features"]:
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] == "LineString"
        props = feature["properties"]
        values.append(
            {
                "objectid": props["objectid"],
                "lines": props["name"].split("-"),
                "shape_len": float(props["shape_len"]),
                "path": json.dumps(feature["geometry"]),
            }
        )

    async def insert(value):
        async with db.acquire_conn() as conn:
            await conn.execute(insert_stmt, value)

    await asyncio.gather(*[insert(value) for value in values])
    logging.info("Inserted %d lines", len(values))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stations", type=str)
    parser.add_argument("--lines", type=str)
    args = parser.parse_args()

    db.create_tables()

    if args.stations:
        await import_stations(args.stations)

    if args.lines:
        await import_lines(args.lines)


async def main_wrapper():
    try:
        await db.setup()
        await main()
    finally:
        await db.teardown()


if __name__ == "__main__":
    asyncio.run(main_wrapper())
