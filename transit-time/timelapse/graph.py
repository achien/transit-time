import asyncio
from typing import Dict, List, NamedTuple, Optional

from async_lru import alru_cache

import db
from scraper.gtfs.gtfs import TransitSystem


class Node(NamedTuple):
    id: int
    geojson: object
    edge_ids: List[int]
    stop_ids: Optional[List[str]]


class Edge(NamedTuple):
    id: int
    geojson: object
    node_id1: int
    node_id2: int
    length: float
    route_ids: List[str]


class PathEdge(NamedTuple):
    edge_id: int
    direction: int


class Graph:
    nodes: Dict[int, Node]
    nodes_by_stop_id: Dict[str, Node]
    edges: Dict[int, Edge]

    def __init__(
        self, nodes: List[Node], edges: List[Edge],
    ):
        self.nodes = {}
        self.nodes_by_stop_id = {}
        for node in nodes:
            assert node.id not in self.nodes
            self.nodes[node.id] = node
            if node.stop_ids is not None:
                for stop_id in node.stop_ids:
                    assert stop_id not in self.nodes_by_stop_id
                    self.nodes_by_stop_id[stop_id] = node

        self.edges = {}
        for edge in edges:
            assert edge.id not in self.edges
            self.edges[edge.id] = edge


@alru_cache
async def load_graph(transit_system: TransitSystem) -> Graph:
    (nodes, edges) = await asyncio.gather(
        _load_nodes(transit_system), _load_edges(transit_system),
    )
    return Graph(nodes, edges)


async def _load_nodes(transit_system: TransitSystem) -> List[Node]:
    nodes = []
    async with db.acquire_conn() as conn:
        query = """
            select
                id,
                edge_ids,
                ST_AsGeoJSON(loc)::json as geojson,
                stop_ids
            from map_nodes
            where system=%s
        """
        async for row in conn.execute(query, transit_system.value):
            nodes.append(
                Node(
                    id=row.id,
                    edge_ids=row.edge_ids,
                    geojson=row.geojson,
                    stop_ids=row.stop_ids,
                )
            )
    return nodes


async def _load_edges(transit_system: TransitSystem) -> List[Edge]:
    edges = []
    async with db.acquire_conn() as conn:
        query = """
            select
                id,
                node_id1,
                node_id2,
                routes,
                ST_AsGeoJSON(path)::json as geojson,
                ST_Length(path::geography) as length
            from map_edges
            where system=%s
        """
        async for row in conn.execute(query, transit_system.value):
            edges.append(
                Edge(
                    id=row.id,
                    node_id1=row.node_id1,
                    node_id2=row.node_id2,
                    geojson=row.geojson,
                    length=row.length,
                    route_ids=row.routes,
                )
            )
    return edges
