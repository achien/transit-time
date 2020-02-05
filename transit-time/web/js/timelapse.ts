import * as d3 from 'd3';
import assert from 'assert';
import { Graph, fetchGraph } from './graph';

const ROUTE_COLORS: Record<string, string> = {
  A: '#2850AD',
  C: '#2850AD',
  E: '#2850AD',
  H: '#2850AD',

  B: '#FF6319',
  D: '#FF6319',
  F: '#FF6319',
  FX: '#FF6319',
  M: '#FF6319',

  G: '#6CBE45',

  J: '#996633',
  Z: '#996633',

  L: '#A7A9AC',

  N: '#FCCC0A',
  Q: '#FCCC0A',
  R: '#FCCC0A',
  W: '#FCCC0A',

  GS: '#808183',
  FS: '#808183',

  '1': '#EE352E',
  '2': '#EE352E',
  '3': '#EE352E',

  '4': '#00933C',
  '5': '#00933C',
  '5X': '#00933C',
  '6': '#00933C',
  '6X': '#00A65C',

  '7': '#B933AD',
  '7X': '#B933AD',

  SI: '#0039A6',
};

async function fetchJson(uri: string): Promise<any> {
  let res = await fetch(uri);
  return await res.json();
}

function drawMap(
  ctx: CanvasRenderingContext2D,
  path: d3.GeoPath,
  nycBoundaries: any,
  graph: Graph,
) {
  ctx.beginPath();
  ctx.lineWidth = 1;
  ctx.strokeStyle = '#d0d0d0';
  path(nycBoundaries);
  ctx.stroke();

  ctx.strokeStyle = '#808080';
  ctx.beginPath();
  for (let edgeID in graph.edges) {
    const edge = graph.edges[edgeID];
    path(edge.linestring);
  }
  ctx.stroke();

  ctx.beginPath();
  path.pointRadius(1);
  for (let nodeID in graph.nodes) {
    const node = graph.nodes[nodeID];
    if (node.stopIDs != null) {
      path(node.point);
      ctx.fill();
    }
  }
  ctx.stroke();
}

const TRAIN_POINT_RADIUS = 3;
let timeRatio = 60;
let tripsStart: number;
let start: number;
let startWall: number;
let currentTripData: any = null;
let drawCurrent: () => void;
let animationFrameRequestID: number | null = null;
let paused = false;
let indexes: Record<string, number>;

function toggleDraw() {
  if (paused) {
    paused = false;
    startWall = Date.now() / 1000;
    drawCurrent();
  } else {
    paused = true;
    start += (Date.now() / 1000 - startWall) * timeRatio;
    if (animationFrameRequestID != null) {
      window.cancelAnimationFrame(animationFrameRequestID);
    }
  }
}

function reset() {
  start = tripsStart;
  startWall = Date.now() / 1000;
  indexes = {};
  for (let tripID in currentTripData) {
    indexes[tripID] = -1;
  }
}

let tripLocations: Record<string, [number, number]> = {};
function findNearbyTrips(x: number, y: number) {
  let lines = [];
  for (const tripID in tripLocations) {
    const [tripX, tripY] = tripLocations[tripID];
    const dx = x - tripX;
    const dy = y - tripY;
    const radius = TRAIN_POINT_RADIUS;
    if (dx * dx + dy * dy <= radius * radius) {
      lines.push(tripID);
    }
  }
  let elem = document.getElementById('coords');
  if (lines.length) {
    elem.textContent = lines.join('\n\n');
    elem.style.display = '';
    elem.style.left = x + 'px';
    elem.style.top = y + 'px';
  } else {
    elem.style.display = 'none';
  }
}

async function loadTrains() {
  const startElem = <HTMLInputElement>document.getElementById('start-input');
  const endElem = <HTMLInputElement>document.getElementById('end-input');

  let url = new URL('/api/realtime-trips', new URL(window.location.href));
  url.searchParams.set('start', startElem.value);
  url.searchParams.set('end', endElem.value);
  url.searchParams.set('paginate', '1');
  let resp = await fetch(url.href);
  let { start, end, cursor, tripData } = await resp.json();
  console.log(
    'Fetched tripData page: ' +
      Object.keys(tripData).length +
      ' trips, cursor: ' +
      cursor,
  );

  while (cursor != null) {
    url = new URL('/api/realtime-trips', new URL(window.location.href));
    url.searchParams.set('cursor', cursor);
    url.searchParams.set('paginate', '1');
    resp = await fetch(url.href);
    const respJson = await resp.json();
    // Every page has the same start/end information, only the cursor and data
    // will be different
    assert.equal(respJson.start, start);
    assert.equal(respJson.end, end);
    const pageTripData = respJson.tripData;
    cursor = respJson.cursor;
    console.log(
      'Fetched tripData page: ' +
        Object.keys(pageTripData).length +
        ' trips, cursor: ' +
        cursor,
    );

    for (let tripID in pageTripData) {
      if (!(tripID in tripData)) {
        tripData[tripID] = pageTripData[tripID];
      } else {
        assert.equal(tripData[tripID].routeID, pageTripData[tripID].routeID);
        tripData[tripID].stops = tripData[tripID].stops.concat(
          pageTripData[tripID].stops,
        );
      }
    }
  }
  return { start, end, tripData };
}

async function drawTrains(
  ctx: CanvasRenderingContext2D,
  path: d3.GeoPath,
  projection: d3.GeoProjection,
  graph: Graph,
) {
  const trainsData = await loadTrains();
  tripsStart = <number>trainsData.start;
  const { end, tripData } = trainsData;

  console.log('Fetched tripData: ' + Object.keys(tripData).length + ' trips');
  currentTripData = tripData;

  const timer = Date.now();
  for (let tripID in tripData) {
    let stops = tripData[tripID].stops;
    for (let i = 0; i < stops.length - 1; i++) {
      let stop = stops[i];
      const nextStop = stops[i + 1];
      const path = graph.shortestPath(
        stop.stopID,
        nextStop.stopID,
        tripData[tripID].routeID,
      );
      if (path != null && path.length > 0) {
        stop.path = path;
      }
    }
  }
  const elapsed = Date.now() - timer;
  console.log('Computed paths in ' + elapsed + 'ms');

  console.log(
    'Drawing trips between ' +
      new Date(tripsStart * 1000).toLocaleString() +
      ' and ' +
      new Date(end * 1000).toLocaleString(),
  );

  reset();
  let routeIDs: Record<string, boolean> = {};
  for (let tripID in tripData) {
    routeIDs[tripData[tripID].routeID] = true;
  }
  const routeIDArray = Object.keys(routeIDs);
  let colors: Record<string, string> = {};
  for (let routeID of routeIDArray) {
    // Color with NYC subway line color
    if (!(routeID in ROUTE_COLORS)) {
      console.error('Cannot color routeID ' + routeID + ' with a MTA color');
      colors[routeID] = 'hsl(' + Math.floor(Math.random() * 256) + ', 100, 50)';
    }
    colors[routeID] = ROUTE_COLORS[routeID];

    // Apply 75% opacity
    colors[routeID] += 'B0';
  }

  tripLocations = {};
  startWall = Date.now() / 1000;
  let lastWall = startWall;
  let frameCounter = 0;
  let draw = function() {
    animationFrameRequestID = null;
    // Abort if we've started drawing something else
    if (currentTripData !== tripData) {
      console.log('Canceling old draw');
      return;
    }

    const curWall = Date.now() / 1000;
    const elapsedWall = curWall - startWall;
    const simTime = timeRatio * elapsedWall + start;

    document.getElementById('time').textContent = new Date(
      simTime * 1000,
    ).toLocaleString();

    frameCounter++;
    if (curWall - lastWall > 0.5) {
      const msDelta = (curWall - lastWall) * 1000;
      document.getElementById('frame-time').textContent =
        Math.round(msDelta / frameCounter) +
        'ms (' +
        Math.round((10000 * frameCounter) / msDelta) / 10 +
        ' fps)';
      lastWall = curWall;
      frameCounter = 0;
    }

    ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
    ctx.strokeStyle = 'black';
    ctx.lineWidth = 0.5;
    path.pointRadius(TRAIN_POINT_RADIUS);

    for (let tripID in tripData) {
      const stops = tripData[tripID].stops;
      // Move index forward until it is in between stops
      while (
        indexes[tripID] + 1 < stops.length &&
        stops[indexes[tripID] + 1].arrival <= simTime
      ) {
        indexes[tripID]++;
      }
      if (
        indexes[tripID] == stops.length - 1 &&
        stops[indexes[tripID]].departure <= simTime
      ) {
        // Train departed last station
        indexes[tripID] += 1;
      }
      const index = indexes[tripID];
      if (index == stops.length - 1) {
        const node = graph.nodesByStopID[stops[index].stopID];
        ctx.beginPath();
        ctx.fillStyle = colors[tripData[tripID].routeID];
        path(node.point);
        tripLocations[tripID] = projection(node.point.coordinates);
        ctx.fill();
        ctx.stroke();
      } else if (index >= 0 && index < stops.length - 1) {
        ctx.beginPath();
        ctx.fillStyle = colors[tripData[tripID].routeID];
        const stop = stops[index];
        const nextStop = stops[index + 1];
        if (simTime < stop.departure || stop.path == null) {
          // Train is still at the first stop
          // Or: we do not know how to get from one stop to the next
          const node = graph.nodesByStopID[stops[index].stopID];
          path(node.point);
          tripLocations[tripID] = projection(node.point.coordinates);
        } else {
          // Train is between stops, interpolate location
          const totalDuration = nextStop.arrival - stop.departure;
          const elapsedDuration = simTime - stop.departure;
          const location = graph.interpolatePath(
            stop.path,
            elapsedDuration / totalDuration,
            projection,
          );
          path({ type: 'Point', coordinates: location });
          tripLocations[tripID] = projection(location);
        }
        ctx.fill();
        ctx.stroke();
      } else {
        delete tripLocations[tripID];
      }
    }

    if (simTime < end) {
      animationFrameRequestID = window.requestAnimationFrame(draw);
    }
  };
  drawCurrent = draw;
  // toggleDraw twice to preserve paused state (instead of just calling draw())
  toggleDraw();
  toggleDraw();
}

function setupCanvas(
  id: string,
  nycBoundaries: any,
): [CanvasRenderingContext2D, d3.GeoPath, d3.GeoProjection] {
  const pixelRatio = window.devicePixelRatio || 1;
  const canvas = <HTMLCanvasElement>document.getElementById(id);
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * pixelRatio;
  canvas.height = rect.height * pixelRatio;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = rect.height + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(pixelRatio, pixelRatio);

  let projection = d3.geoMercator();
  projection.fitSize([rect.width, rect.height], nycBoundaries);
  let path = d3.geoPath(projection, ctx);

  let mousemoveListener = (e: MouseEvent) => {
    findNearbyTrips(e.offsetX, e.offsetY);
  };
  let frameTime = document.getElementById('frame-time');
  let debugInput = <HTMLInputElement>document.getElementById('debug-input');
  function updateDebugInput() {
    if (debugInput.checked) {
      canvas.addEventListener('mousemove', mousemoveListener);
      frameTime.hidden = false;
    } else {
      canvas.removeEventListener('mousemove', mousemoveListener);
      frameTime.hidden = true;
    }
  }
  debugInput.addEventListener('change', updateDebugInput);
  updateDebugInput();

  return [ctx, path, projection];
}

async function main() {
  const [nycBoundaries, graph] = await Promise.all([
    fetchJson('/data/nyc-boundaries.geojson'),
    fetchGraph(),
  ]);

  let [mapCtx, path] = setupCanvas('map-layer', nycBoundaries);
  drawMap(mapCtx, path, nycBoundaries, graph);

  let [trainsCtx, trainsPath, trainsProjection] = setupCanvas(
    'trains-layer',
    nycBoundaries,
  );
  let _drawTrains = () =>
    drawTrains(trainsCtx, trainsPath, trainsProjection, graph);
  let form = <HTMLFormElement>document.getElementById('query');
  form.addEventListener('submit', e => {
    e.preventDefault();
    _drawTrains();
  });
  await _drawTrains();

  const speedInput = <HTMLInputElement>document.getElementById('speed-input');
  timeRatio = parseInt(speedInput.value, 10);
  speedInput.addEventListener('change', e => {
    const newSpeed = parseInt(speedInput.value, 10);
    // Make sure the input is an integer, otherwise revert
    if (newSpeed.toString() == speedInput.value) {
      // If currently drawing, pause or else the simulationbreaks
      if (!paused) {
        toggleDraw();
        timeRatio = newSpeed;
        toggleDraw();
      } else {
        timeRatio = newSpeed;
      }
      console.log('Set new speed: ' + newSpeed);
    } else {
      speedInput.value = timeRatio.toString();
    }
  });

  document.addEventListener('keydown', e => {
    if (e.target == document.body) {
      if (e.key == ' ') {
        e.preventDefault();
        toggleDraw();
      } else if (e.key == '0') {
        e.preventDefault();
        reset();
        if (paused) {
          toggleDraw();
          toggleDraw();
        }
      }
    }
  });
}

document.addEventListener('DOMContentLoaded', e => main());
