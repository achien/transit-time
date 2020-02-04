from starlette.endpoints import HTTPEndpoint
from starlette.responses import JSONResponse

from scraper.gtfs import gtfs
from timelapse.graph import Edge, Node, load_graph

TRANSIT_SYSTEM = gtfs.TransitSystem.NYC_MTA


class GraphEndpoint(HTTPEndpoint):
    async def get(self, _request):
        graph = await load_graph(TRANSIT_SYSTEM)
        return JSONResponse(
            {
                "nodes": {
                    id: self.format_node(node) for (id, node) in graph.nodes.items()
                },
                "edges": {
                    id: self.format_edge(edge) for (id, edge) in graph.edges.items()
                },
            }
        )

    def format_node(self, node: Node) -> object:
        return {
            "id": str(node.id),
            "point": node.geojson,
            "edgeIDs": [str(edge_id) for edge_id in node.edge_ids],
            "stopIDs": node.stop_ids,
        }

    def format_edge(self, edge: Edge) -> object:
        return {
            "id": str(edge.id),
            "linestring": edge.geojson,
            "nodeID1": str(edge.node_id1),
            "nodeID2": str(edge.node_id2),
            "length": edge.length,
            "routeIDs": edge.route_ids,
        }
