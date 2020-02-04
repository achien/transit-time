"""
Imports shapes.txt
"""
import asyncio
import csv
import logging
import sys
from typing import Dict, List, NamedTuple, Set, Tuple

from sqlalchemy.dialects.postgresql import insert

import db
import logger
from scraper.gtfs import gtfs
from timelapse.graph_writer import Edge, Node, Point, RouteGraph

logger.setup()

TRANSIT_SYSTEM = gtfs.TransitSystem.NYC_MTA


class RouteSegment(NamedTuple):
    points: List[Point]
    routes: Set[str]


def make_graph(segments, stops):
    nodes = []
    edges = []

    nodes.extend([Node(pt, set([stop_id]), []) for (stop_id, pt) in stops])

    for segment in segments:
        (seg_nodes, seg_edges) = explode_segment(segment)
        nodes.extend(seg_nodes)
        edges.extend(seg_edges)

    return RouteGraph(nodes, edges)


def explode_segment(segment: RouteSegment) -> Tuple[List[Node], List[Edge]]:
    nodes = [Node(p, set(), []) for p in segment.points]
    edges = []
    for i in range(len(nodes) - 1):
        edge = Edge(
            segment.points[i : i + 2], segment.routes.copy(), [nodes[i], nodes[i + 1]],
        )
        nodes[i].edges.append(edge)
        nodes[i + 1].edges.append(edge)
        edges.append(edge)
    return (nodes, edges)


def get_stops() -> List[Tuple[str, Point]]:
    # Maybe we should load from DB, but this seems safer because we don't
    # deal with floating point nonsense
    stops_txt = sys.argv[2]
    stops = []
    with open(stops_txt) as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Fix some data for the MTA.  There are two stations named
            # "Broad Channel" in Far Rockaway and one of them is disconnected
            # from the graph.  Move them so both stations are in the same
            # place.
            if (
                row["stop_id"].startswith("H19")
                and row["stop_lon"] == "-73.816024"
                and row["stop_lat"] == "40.609014"
            ):
                stops.append((row["stop_id"], Point(x="-73.815925", y="40.608382")))
            else:
                stops.append(
                    (row["stop_id"], Point(x=row["stop_lon"], y=row["stop_lat"]))
                )
    logging.info("Loaded %d stops", len(stops))
    return stops


async def get_routes_for_shape_id() -> Dict[str, Set[str]]:
    query = """
        select shape_id, array_agg(distinct route_id) as routes
        from trips
        where shape_id is not null
        group by shape_id
    """
    routes_for_shape_id = {}
    async with db.acquire_conn() as conn:
        async for row in conn.execute(query):
            routes_for_shape_id[row.shape_id] = set(row.routes)
    logging.info("%d shapes have routes", len(routes_for_shape_id))
    return routes_for_shape_id


def load_paths() -> Dict[str, List[Point]]:
    shapes_txt = sys.argv[1]
    with open(shapes_txt) as file:
        reader = csv.DictReader(file)
        rows = 0
        # All shapes are vehicle paths (linestrings)
        unordered_paths = {}
        for row in reader:
            rows += 1
            shape_id = row["shape_id"]
            seq = int(row["shape_pt_sequence"])
            lat = row["shape_pt_lat"]
            lon = row["shape_pt_lon"]
            if shape_id not in unordered_paths:
                unordered_paths[shape_id] = {}
            unordered_paths[shape_id][seq] = Point(x=lon, y=lat)

    logging.info("%d rows in shapes.txt", rows)

    paths = {}
    for (shape_id, unordered_path) in unordered_paths.items():
        path = []
        last_point = None
        for seq in sorted(unordered_path.keys()):
            # Sometimes the data is bad and 2 consecutive points are the same
            # This creates a degenerate segment and causes issues, so we
            # skip it
            point = unordered_path[seq]
            if point == last_point:
                continue

            # Hardcode exceptions here for the MTA.
            # - Arthur Kill on the SI is not connected to nodes but is right
            #   next to a long edge.  Split this edge into two pieces.
            # - Same thing for York St on the F in Brooklyn
            p1 = Point(x="-74.241023", y="40.517071")
            p2 = Point(x="-74.242872", y="40.516194")
            p_one_point_five = Point(x="-74.242096", y="40.516578")
            p3 = Point(x="-73.986587", y="40.704099")
            p4 = Point(x="-73.986885", y="40.699743")
            p_three_point_five = Point(x="-73.986751", y="40.701397")
            if (point, last_point) == (p1, p2) or (last_point, point) == (p1, p2):
                path.append(p_one_point_five)
            elif (point, last_point) == (p3, p4) or (last_point, point) == (p3, p4):
                path.append(p_three_point_five)

            path.append(point)
            last_point = point

        paths[shape_id] = path
    return paths


def load_additional_paths(stops) -> Dict[str, List[Point]]:
    stops_by_id = {id: pt for (id, pt) in stops}
    # Q train
    stop_paths = {
        # Q train
        "Q___0000": [
            # 57 St - 7 Av
            "R14",
            # Lexington Av/63 St
            "B08",
            # 72 St
            "Q03",
            # 86 St
            "Q04",
            # 96 St
            "Q05",
        ]
    }
    return {
        shape_id: [stops_by_id[stop_id] for stop_id in stop_ids]
        for (shape_id, stop_ids) in stop_paths.items()
    }


async def write_trip_paths(routes_for_shape_id, paths):
    values = []
    for (shape_id, path) in paths.items():
        values.append(
            {
                "system": TRANSIT_SYSTEM.value,
                "shape_id": shape_id,
                "routes": routes_for_shape_id.get(shape_id),
                "shape": "LINESTRING({})".format(
                    ", ".join(["{} {}".format(p.x, p.y) for p in path])
                ),
            }
        )

    table = db.get_table("trip_paths")
    stmt = insert(table).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[table.c.system, table.c.shape_id],
        set_={"shape": stmt.excluded.shape},
    )
    async with db.acquire_conn() as conn:
        res = await conn.execute(
            table.delete().where(table.c.system == TRANSIT_SYSTEM.value)
        )
        logging.info("Deleted %d rows from %s", res.rowcount, table.name)
        await conn.execute(stmt)
        logging.info("Inserted %d rows into %s", len(values), table.name)


async def main():
    routes_for_shape_id = await get_routes_for_shape_id()

    stops = get_stops()
    paths = load_paths()
    additional_paths = load_additional_paths(stops)
    for (k, v) in additional_paths.items():
        assert k not in paths
        paths[k] = v
    logging.info("%d distinct shapes", len(paths))

    segments = [
        RouteSegment(points=path, routes=routes_for_shape_id.get(shape_id, set()))
        for (shape_id, path) in paths.items()
    ]
    graph = make_graph(segments, stops)
    graph.dedupe()
    node_count = len(graph.nodes)
    edge_count = len(graph.edges)
    # Second dedupe should do nothing if implementation is correct
    graph.dedupe()
    assert len(graph.nodes) == node_count
    assert len(graph.edges) == edge_count

    graph.coalesce()
    node_count = len(graph.nodes)
    edge_count = len(graph.edges)
    # Second coalesce should do nothing if implementation is correct
    graph.coalesce()
    assert len(graph.nodes) == node_count
    assert len(graph.edges) == edge_count

    for node in graph.nodes.values():
        if len(node.stop_ids) > 0 and len(node.edges) == 0:
            logging.warning(
                "Stop IDs %s (%s) disconnected from graph", node.stop_ids, node.point
            )

    db.create_tables()
    await graph.write(TRANSIT_SYSTEM)
    await write_trip_paths(routes_for_shape_id, paths)


async def main_wrapper():
    try:
        await db.setup()
        await main()
    finally:
        await db.teardown()


if __name__ == "__main__":
    asyncio.run(main_wrapper())
