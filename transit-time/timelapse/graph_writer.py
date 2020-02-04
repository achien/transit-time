import functools
import logging
from typing import Dict, List, Set

import c
import db
from scraper.gtfs.gtfs import TransitSystem


@functools.total_ordering
class Point:
    # coordinates are stored as strings so we don't lose any information
    x: str
    y: str
    # coordinates are compared using floats to allow comparison when fields
    # have different precisions (e.g. 3.40 vs 3.4)
    xf: float
    yf: float

    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.xf = float(x)
        self.yf = float(y)

    # Returns a float tuple for comparison purposes
    def key(self):
        return (self.xf, self.yf)

    def __eq__(self, other):
        if other is None:
            return False
        return self.key() == other.key()

    def __lt__(self, other):
        return self.key() < other.key()


id_counter = 0


def next_id():
    global id_counter
    id_counter += 1
    return id_counter


class Node:
    point: Point
    stop_ids: Set[str]
    edges: List["Edge"]

    def __init__(
        self, point: Point, stop_ids: Set[str], edges: List["Edge"],
    ):
        self.id = next_id()
        self.point = point
        self.stop_ids = stop_ids
        self.edges = edges

    def merge(self, other: "Node"):
        assert self.point == other.point
        self.stop_ids |= other.stop_ids
        for edge in other.edges:
            edge.nodes.remove(other)
            edge.nodes.append(self)
            if edge not in self.edges:
                self.edges.append(edge)

    def can_coalesce(self):
        if len(self.stop_ids) > 0:
            return False
        if len(self.edges) != 2:
            return False
        (e1, e2) = self.edges
        return e1.routes == e2.routes

    def postgis_point(self):
        return "POINT({} {})".format(self.point.x, self.point.y)

    def __str__(self):
        return "Node({}, {}, <{} edges>)".format(
            str(self.point), self.stop_ids, len(self.edges)
        )


class Edge:
    id: int
    points: List[Point]
    routes: Set[str]
    nodes: List[Node]

    def __init__(
        self, points: List[Point], routes: Set[str], nodes: List[Node],
    ):
        self.id = next_id()
        self.routes = routes
        self.nodes = nodes
        # Order the points so points <= reversed(points) lexicographically
        # This makes for easy comparison of edges
        points_reversed = list(reversed(points))
        self.points = points if points <= points_reversed else points_reversed

    def merge(self, other: "Edge"):
        assert self.points == other.points
        assert len(self.nodes) == 2
        assert self.nodes == other.nodes or self.nodes == list(reversed(other.nodes))
        self.routes |= other.routes
        for node in other.nodes:
            assert self in node.edges
            node.edges.remove(other)

    def postgis_linestring(self):
        return "LINESTRING({})".format(
            ", ".join(["{} {}".format(p.x, p.y) for p in self.points])
        )


class RouteGraph:
    nodes: Dict[str, Node]
    edges: Dict[str, Edge]

    def __init__(self, nodes, edges):
        self.nodes = {node.id: node for node in nodes}
        self.edges = {edge.id: edge for edge in edges}
        logging.info("Created graph with %d nodes, %d edges", len(nodes), len(edges))

    def dedupe(self):
        """
        Dedupe nodes representing the same points and edges representing the
        same segments.
        """
        nodes_by_point = {}
        for node in self.nodes.values():
            key = node.point.key()
            if key not in nodes_by_point:
                nodes_by_point[key] = [node]
            else:
                nodes_by_point[key].append(node)

        for nodes in nodes_by_point.values():
            for i in range(1, len(nodes)):
                nodes[0].merge(nodes[i])
                del self.nodes[nodes[i].id]
        logging.info("Dedupe nodes: %d nodes remain", len(self.nodes))

        # This code only works when deduping edges after nodes.  In this
        # dedupe process, we group edges by their points.  If there are
        # duplicate nodes we cannot merge edges because each edge can only
        # have 2 nodes and we don't know which nodes the edges should be
        # connected to after merging.
        edges_by_points = {}
        for edge in self.edges.values():
            key = str([pt.key() for pt in edge.points])
            if key not in edges_by_points:
                edges_by_points[key] = [edge]
            else:
                edges_by_points[key].append(edge)

        for edges in edges_by_points.values():
            for i in range(1, len(edges)):
                edges[0].merge(edges[i])
                del self.edges[edges[i].id]
        logging.info("Dedupe edges: %d edges remain", len(self.edges))

    def coalesce(self):
        """
        Simplify graph by removing nodes of degree two, if the node is not
        a station and both edges are for the same routes.
        """
        # The list should be fully simplified after a single pass through all
        # nodes.  Copy the list because we remove nodes as we coalesce.
        for node in self.nodes.copy().values():
            if node.can_coalesce():
                (e1, e2) = node.edges
                del self.edges[e1.id]
                del self.edges[e2.id]
                del self.nodes[node.id]

                # Normalize the lists so e1.points ends in node and e2.points
                # starts with node
                if e1.points[0] == node.point:
                    e1.points.reverse()
                assert e1.points[-1] == node.point
                if e2.points[-1] == node.point:
                    e2.points.reverse()
                assert e2.points[0] == node.point
                # Combine the lists, removing the shared point
                points = e1.points
                points.pop()
                points.extend(e2.points)

                assert e1.routes == e2.routes

                # Combine the lists of nodes
                e1.nodes.remove(node)
                e1_node = c.only(e1.nodes)
                e2.nodes.remove(node)
                e2_node = c.only(e2.nodes)
                nodes = [e1_node, e2_node]

                edge = Edge(points, e1.routes, nodes)
                self.edges[edge.id] = edge

                e1_node.edges.remove(e1)
                e1_node.edges.append(edge)
                e2_node.edges.remove(e2)
                e2_node.edges.append(edge)
        logging.info(
            "Coalesce: %d nodes remain, %d edges remain",
            len(self.nodes),
            len(self.edges),
        )

    async def write(self, transit_system: TransitSystem):
        nodes_table = db.get_table("map_nodes")
        edges_table = db.get_table("map_edges")
        nodes_values = [
            {
                "system": transit_system.value,
                "id": node.id,
                "edge_ids": [edge.id for edge in node.edges],
                "loc": node.postgis_point(),
                "stop_ids": list(node.stop_ids) or None,
            }
            for node in self.nodes.values()
        ]
        edges_values = []
        for edge in self.edges.values():
            # The nodes are in the same order as the path
            (node1, node2) = edge.nodes
            if node1.point == edge.points[-1]:
                (node1, node2) = (node2, node1)
            edges_values.append(
                {
                    "system": transit_system.value,
                    "id": edge.id,
                    "node_id1": node1.id,
                    "node_id2": node2.id,
                    "path": edge.postgis_linestring(),
                    "routes": list(edge.routes),
                }
            )

        async with db.acquire_conn() as conn:
            res = await conn.execute(
                nodes_table.delete().where(nodes_table.c.system == transit_system.value)
            )
            logging.info("Deleted %d rows from %s", res.rowcount, nodes_table.name)
            res = await conn.execute(nodes_table.insert().values(nodes_values))
            logging.info("Inserted %d rows into %s", res.rowcount, nodes_table.name)

            res = await conn.execute(
                edges_table.delete().where(edges_table.c.system == transit_system.value)
            )
            logging.info("Deleted %d rows from %s", res.rowcount, edges_table.name)
            res = await conn.execute(edges_table.insert().values(edges_values))
            logging.info("Inserted %d rows into %s", res.rowcount, edges_table.name)
