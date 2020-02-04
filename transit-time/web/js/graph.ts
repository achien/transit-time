import * as d3 from 'd3';
import assert from 'assert';
import Heap from 'heap';

type GraphNode = Readonly<{
  id: string;
  point: Point;
  edgeIDs: readonly string[];
  stopIDs: readonly string[];
}>;

type GraphEdge = Readonly<{
  id: string;
  linestring: LineString;
  nodeID1: string;
  nodeID2: string;
  length: number;
  routeIDs: readonly string[];
}>;

type Path = Array<{
  edgeID: string;
  direction: 1 | -1;
}>;

interface Point {
  type: 'Point';
  coordinates: [number, number];
}

interface LineString {
  type: 'LineString';
  coordinates: [number, number][];
}

export class Graph {
  readonly nodes: Readonly<Record<string, GraphNode>>;
  readonly edges: Readonly<Record<string, GraphEdge>>;
  readonly nodesByStopID: Readonly<Record<string, GraphNode>>;

  constructor(nodes: GraphNode[], edges: GraphEdge[]) {
    let _nodes: Record<string, GraphNode> = {};
    let _nodesByStopID: Record<string, GraphNode> = {};
    for (let node of nodes) {
      _nodes[node.id] = node;
      if (node.stopIDs != null) {
        for (let stopID of node.stopIDs) {
          _nodesByStopID[stopID] = node;
        }
      }
    }
    this.nodes = _nodes;
    this.nodesByStopID = _nodesByStopID;

    let _edges: Record<string, GraphEdge> = {};
    for (let edge of edges) {
      _edges[edge.id] = edge;
    }
    this.edges = _edges;
  }

  shortestPathCache: Record<string, Path | null> = {};
  shortestPath(stopID1: string, stopID2: string, routeID: string): Path | null {
    // Just pick some characters that are probably not in IDs
    const key = [stopID1, stopID2, routeID].join('\0\n');
    if (!(key in this.shortestPathCache)) {
      this.shortestPathCache[key] = this._shortestPath(
        stopID1,
        stopID2,
        routeID,
      );
    }
    return this.shortestPathCache[key];
  }

  _shortestPath(
    stopID1: string,
    stopID2: string,
    routeID: string,
  ): Path | null {
    const node1 = this.nodesByStopID[stopID1];
    const node2 = this.nodesByStopID[stopID2];

    let visited = new Set();
    let heap = new Heap<[number, string, boolean, Path]>((a, b) => a[0] - b[0]);
    heap.push([0, node1.id, false, []]);
    while (heap.size() > 0) {
      const [traveled, nodeID, offRoute, path] = heap.pop();
      if (nodeID === node2.id) {
        return path;
      }
      if (visited.has(nodeID)) {
        // Already visited node means we considered a shorter path
        // This is not quite right because we have the penalty and might
        // be able to find a shorter path here (if we applied penalty here
        // already and have not yet in the other path) but should be fine
        // as long as the penalty is larger than any possible path.
        continue;
      }
      visited.add(nodeID);
      const node = this.nodes[nodeID];
      for (let edgeID of node.edgeIDs) {
        const edge = this.edges[edgeID];
        let nextNodeID;
        let direction: 1 | -1;
        if (edge.nodeID1 === nodeID) {
          nextNodeID = edge.nodeID2;
          direction = 1;
        } else {
          nextNodeID = edge.nodeID1;
          direction = -1;
        }
        let nextTraveled = traveled + edge.length;
        let nextOffRoute = offRoute;
        if (!offRoute && !edge.routeIDs.includes(routeID)) {
          // Apply a one-time penalty for going off-route
          nextOffRoute = true;
          nextTraveled += 100000; // 100km
        }
        let nextPath = path.slice();
        nextPath.push({ edgeID: edge.id, direction: direction });
        heap.push([nextTraveled, nextNodeID, nextOffRoute, nextPath]);
      }
    }
    console.error('Path not found between ' + stopID1 + ' and ' + stopID2);
    return null;
  }

  interpolatePath(
    path: Path,
    fraction: number,
    projection: d3.GeoProjection,
  ): [number, number] {
    assert(fraction >= 0 && fraction <= 1);
    let totalLength = 0;
    for (let x of path) {
      const edge = this.edges[x.edgeID];
      totalLength += edge.length;
    }

    let targetLength = fraction * totalLength;
    for (let x of path) {
      const edge = this.edges[x.edgeID];
      if (targetLength <= edge.length) {
        return this.interpolateLine(
          edge.linestring,
          x.direction,
          targetLength / edge.length,
          projection,
        );
      }
      targetLength -= edge.length;
    }
    console.error(fraction, totalLength, targetLength, path);
    assert.fail('Failed to interpolate path');
  }

  interpolateLine(
    linestring: LineString,
    direction: 1 | -1,
    fraction: number,
    projection: d3.GeoProjection,
  ): [number, number] {
    assert(fraction >= 0 && fraction <= 1);
    let projected = linestring.coordinates.map(coords => projection(coords));
    let lengths = [];
    let totalLength = 0.0;
    for (let i = 0; i < projected.length - 1; i++) {
      let point, nextPoint;
      if (direction == 1) {
        point = projected[i];
        nextPoint = projected[i + 1];
      } else {
        point = projected[projected.length - 1 - i];
        nextPoint = projected[projected.length - 2 - i];
      }
      const dx = nextPoint[0] - point[0];
      const dy = nextPoint[1] - point[1];
      const length = Math.sqrt(dx * dx + dy * dy);
      lengths.push(length);
      totalLength += length;
    }

    let idx = 0;
    let targetLength = fraction * totalLength;
    for (idx = 0; idx < projected.length - 1; idx++) {
      if (targetLength <= lengths[idx]) {
        const segmentFraction = targetLength / lengths[idx];
        assert(segmentFraction >= 0 && segmentFraction <= 1);
        let x, y, x2, y2;
        if (direction == 1) {
          [x, y] = projected[idx];
          [x2, y2] = projected[idx + 1];
        } else {
          [x, y] = projected[projected.length - 1 - idx];
          [x2, y2] = projected[projected.length - 2 - idx];
        }
        return projection.invert([
          x + (x2 - x) * segmentFraction,
          y + (y2 - y) * segmentFraction,
        ]);
      }
      targetLength -= lengths[idx];
    }
    console.error(fraction, totalLength, targetLength);
    assert.fail('Failed to interpolate LineString');
  }
}

export async function fetchGraph(): Promise<Graph> {
  const resp = await fetch('/api/graph');
  const graphData = await resp.json();
  return new Graph(
    Object.values(graphData.nodes),
    Object.values(graphData.edges),
  );
}
