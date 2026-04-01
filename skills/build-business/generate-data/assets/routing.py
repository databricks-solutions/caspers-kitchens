"""
Routing Reference Implementation

Provides real road routes (OSM graph + Dijkstra), great-circle routes (air),
address resolution (Nominatim), and derived field profiles for tracking events.

This is reference code for the LLM to draw on when generating canonical dataset
generators. It should be inlined into generated code, not imported at runtime.

Primary road routing: osmnx graph load + networkx Dijkstra (fast, local)
Fallback road routing: OSRM public API (no install, but slow)
Air routing: great-circle interpolation (pure math, zero deps)

Dependencies for road routing: osmnx, networkx (pip install osmnx networkx)
Dependencies for air routing: none (math only)
Dependencies for address resolution: requests
"""

import math
import time
import json
import hashlib
import pickle
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any

# ── Types ────────────────────────────────────────────────────────────────────

Coordinate = Tuple[float, float]  # (lat, lon)
Route = List[Coordinate]


# ══════════════════════════════════════════════════════════════════════════════
# OSM Graph Road Routing (PRIMARY — fast, local Dijkstra)
# ══════════════════════════════════════════════════════════════════════════════

class RoadGraph:
    """
    Loads an OSM road network for an area and computes routes via Dijkstra.

    Usage:
        graph = RoadGraph.load(center=(37.77, -122.42), radius_km=6.4)
        route, distance = graph.route(origin, destination)
        route, distance = graph.route_multi([A, B, C, D])  # multi-stop
        location = graph.random_node(rng)  # random routable point
    """

    def __init__(self, G, nodes_df, center_node):
        self.G = G
        self.nodes_df = nodes_df
        self.center_node = center_node
        self._route_cache: Dict[str, Tuple[Route, float]] = {}

    @classmethod
    def load(
        cls,
        center: Coordinate,
        radius_km: float = 6.4,
        network_type: str = "drive",
        cache_dir: Optional[str] = None,
    ) -> "RoadGraph":
        """
        Load an OSM road network centered on a point.

        Args:
            center: (lat, lon) center of the area
            radius_km: Radius in kilometers to download
            network_type: "drive" for car roads, "walk" for pedestrian paths,
                          "bike" for cycling, "all" for everything
            cache_dir: Directory to cache the graph pickle. None = no caching.

        Returns:
            RoadGraph instance ready for routing.
        """
        import osmnx as ox
        import networkx as nx
        import pandas as pd

        cache_file = None
        if cache_dir:
            safe_name = f"{center[0]:.4f}_{center[1]:.4f}_{radius_km}_{network_type}"
            cache_file = Path(cache_dir) / f"graph_{safe_name}.pkl"
            if cache_file.exists():
                print(f"Loading cached graph: {cache_file}")
                with open(cache_file, "rb") as f:
                    G = pickle.load(f)
                center_node = ox.distance.nearest_nodes(G, center[1], center[0])
                nodes_df = cls._extract_nodes(G, center_node, nx)
                return cls(G, nodes_df, center_node)

        print(f"Downloading OSM road network: center={center}, radius={radius_km}km, type={network_type}")
        ox.settings.log_console = False
        radius_m = radius_km * 1000
        G = ox.graph_from_point(center, dist=radius_m, network_type=network_type)
        print(f"Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

        if cache_file:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file, "wb") as f:
                pickle.dump(G, f)
            print(f"Cached graph: {cache_file}")

        center_node = ox.distance.nearest_nodes(G, center[1], center[0])
        nodes_df = cls._extract_nodes(G, center_node, nx)

        return cls(G, nodes_df, center_node)

    @staticmethod
    def _extract_nodes(G, center_node, nx) -> "pd.DataFrame":
        """Extract all routable nodes in the connected component containing center."""
        import pandas as pd

        comp_map = {
            n: cid
            for cid, comp in enumerate(nx.connected_components(G.to_undirected()))
            for n in comp
        }
        center_comp = comp_map.get(center_node)

        nodes = []
        for node_id, data in G.nodes(data=True):
            if comp_map.get(node_id) == center_comp:
                nodes.append({
                    "node_id": node_id,
                    "lat": data["y"],
                    "lon": data["x"],
                })
        return pd.DataFrame(nodes)

    def nearest_node(self, lat: float, lon: float) -> int:
        """Find the nearest graph node to a coordinate."""
        import osmnx as ox
        return ox.distance.nearest_nodes(self.G, lon, lat)

    def random_node(self, rng) -> Dict[str, Any]:
        """
        Pick a random routable node from the graph.

        Returns:
            {"node_id": int, "lat": float, "lon": float}
        """
        row = self.nodes_df.sample(1, random_state=rng).iloc[0]
        return {"node_id": int(row["node_id"]), "lat": row["lat"], "lon": row["lon"]}

    def route(
        self, origin: Coordinate, destination: Coordinate
    ) -> Tuple[Route, float]:
        """
        Compute shortest path between two points via Dijkstra.

        Args:
            origin: (lat, lon)
            destination: (lat, lon)

        Returns:
            (route_points, distance_meters)
        """
        return self.route_multi([origin, destination])

    def route_multi(self, waypoints: List[Coordinate]) -> Tuple[Route, float]:
        """
        Compute road route through multiple waypoints (multi-stop).

        Chains shortest paths between consecutive waypoints.

        Args:
            waypoints: List of (lat, lon) tuples. Minimum 2.

        Returns:
            (route_points, total_distance_meters)
        """
        import networkx as nx

        cache_key = json.dumps([(round(w[0], 6), round(w[1], 6)) for w in waypoints])
        if cache_key in self._route_cache:
            return self._route_cache[cache_key]

        all_coords = []
        total_dist = 0.0

        for i in range(len(waypoints) - 1):
            origin_node = self.nearest_node(waypoints[i][0], waypoints[i][1])
            dest_node = self.nearest_node(waypoints[i + 1][0], waypoints[i + 1][1])

            try:
                path = nx.shortest_path(self.G, origin_node, dest_node, weight="length")
            except nx.NetworkXNoPath:
                # Fall back to undirected
                path = nx.shortest_path(
                    self.G.to_undirected(), origin_node, dest_node, weight="length"
                )

            coords = [(self.G.nodes[n]["y"], self.G.nodes[n]["x"]) for n in path]
            dist = sum(
                min(d["length"] for d in self.G[u][v].values())
                for u, v in zip(path[:-1], path[1:])
            )

            # Avoid duplicating junction points
            if i > 0 and all_coords:
                coords = coords[1:]

            all_coords.extend(coords)
            total_dist += dist

        result = (all_coords, total_dist)
        self._route_cache[cache_key] = result
        return result

    def generate_location_in_area(
        self,
        rng,
        center: Optional[Coordinate] = None,
        with_address: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate a random location by picking a graph node.

        Much better than random-point-in-radius because the point is guaranteed
        to be on a real road. No OSRM snap needed.

        Args:
            rng: numpy random generator
            center: ignored (uses graph's built-in node set)
            with_address: if True, reverse geocode via Nominatim

        Returns:
            {"node_id": int, "lat": float, "lon": float, "address": str or None}
        """
        node = self.random_node(rng)
        address = None
        if with_address:
            try:
                address = reverse_geocode(node["lat"], node["lon"])
            except Exception:
                address = f"{node['lat']:.6f}, {node['lon']:.6f}"
        node["address"] = address
        return node


# ══════════════════════════════════════════════════════════════════════════════
# OSRM Road Routing (FALLBACK — no install needed, but slow per-call)
# ══════════════════════════════════════════════════════════════════════════════

import requests

OSRM_BASE = "https://router.project-osrm.org"

_osrm_route_cache: Dict[str, Route] = {}


def osrm_route(waypoints: List[Coordinate], retries: int = 3) -> Tuple[Route, float]:
    """
    Compute a road route through ordered waypoints via OSRM public API.

    Slower than RoadGraph (one HTTP call per route), but zero install dependencies.
    Use as fallback when osmnx is not available.

    Args:
        waypoints: List of (lat, lon) tuples. Minimum 2.
        retries: Number of retry attempts on failure.

    Returns:
        (route_points, total_distance_meters)
    """
    raw = json.dumps(waypoints, sort_keys=True)
    key = hashlib.md5(raw.encode()).hexdigest()
    if key in _osrm_route_cache:
        return _osrm_route_cache[key], 0.0

    # OSRM expects lon,lat (not lat,lon)
    coords_str = ";".join(f"{lon},{lat}" for lat, lon in waypoints)
    url = f"{OSRM_BASE}/route/v1/driving/{coords_str}"
    params = {"overview": "full", "geometries": "geojson"}

    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data.get("code") != "Ok":
                raise ValueError(f"OSRM error: {data.get('code')} - {data.get('message', '')}")

            route = data["routes"][0]
            coords = [(c[1], c[0]) for c in route["geometry"]["coordinates"]]
            distance = route["distance"]

            _osrm_route_cache[key] = coords
            return coords, distance

        except (requests.RequestException, ValueError) as e:
            if attempt < retries - 1:
                time.sleep(1 * (attempt + 1))
                continue
            raise RuntimeError(f"OSRM routing failed after {retries} attempts: {e}")


def osrm_nearest(lat: float, lon: float) -> Coordinate:
    """Snap a coordinate to the nearest routable road point via OSRM."""
    url = f"{OSRM_BASE}/nearest/v1/driving/{lon},{lat}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != "Ok" or not data.get("waypoints"):
        raise ValueError(f"OSRM nearest failed: {data.get('code')}")

    snapped = data["waypoints"][0]["location"]
    return (snapped[1], snapped[0])


# ══════════════════════════════════════════════════════════════════════════════
# Great-Circle (Air) Routing
# ══════════════════════════════════════════════════════════════════════════════

def great_circle_route(
    waypoints: List[Coordinate],
    points_per_segment: int = 50,
) -> Route:
    """
    Interpolate a great-circle route through ordered waypoints.

    Suitable for flights, drones, or any non-road movement.
    """
    route = []
    for i in range(len(waypoints) - 1):
        segment = _interpolate_great_circle(waypoints[i], waypoints[i + 1], points_per_segment)
        if i > 0:
            segment = segment[1:]
        route.extend(segment)
    return route


def _interpolate_great_circle(
    start: Coordinate, end: Coordinate, num_points: int
) -> Route:
    """Interpolate points along a great-circle arc between two coordinates."""
    lat1, lon1 = math.radians(start[0]), math.radians(start[1])
    lat2, lon2 = math.radians(end[0]), math.radians(end[1])

    d = 2 * math.asin(
        math.sqrt(
            math.sin((lat2 - lat1) / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
        )
    )

    if d < 1e-10:
        return [start] * num_points

    points = []
    for i in range(num_points):
        f = i / (num_points - 1) if num_points > 1 else 0
        a = math.sin((1 - f) * d) / math.sin(d)
        b = math.sin(f * d) / math.sin(d)
        x = a * math.cos(lat1) * math.cos(lon1) + b * math.cos(lat2) * math.cos(lon2)
        y = a * math.cos(lat1) * math.sin(lon1) + b * math.cos(lat2) * math.sin(lon2)
        z = a * math.sin(lat1) + b * math.sin(lat2)
        lat = math.degrees(math.atan2(z, math.sqrt(x**2 + y**2)))
        lon = math.degrees(math.atan2(y, x))
        points.append((lat, lon))

    return points


def great_circle_distance_km(start: Coordinate, end: Coordinate) -> float:
    """Haversine distance in kilometers between two coordinates."""
    R = 6371.0
    lat1, lon1 = math.radians(start[0]), math.radians(start[1])
    lat2, lon2 = math.radians(end[0]), math.radians(end[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ══════════════════════════════════════════════════════════════════════════════
# Address Resolution (Nominatim)
# ══════════════════════════════════════════════════════════════════════════════

NOMINATIM_BASE = "https://nominatim.openstreetmap.org"
NOMINATIM_HEADERS = {"User-Agent": "caspers-data-generator/1.0"}


def geocode(address: str) -> Coordinate:
    """Forward geocode: address string → (lat, lon). Rate limited 1 req/sec."""
    resp = requests.get(
        f"{NOMINATIM_BASE}/search",
        params={"q": address, "format": "json", "limit": 1},
        headers=NOMINATIM_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    results = resp.json()
    if not results:
        raise ValueError(f"Geocoding failed: no results for '{address}'")
    time.sleep(1)
    return (float(results[0]["lat"]), float(results[0]["lon"]))


def reverse_geocode(lat: float, lon: float) -> str:
    """Reverse geocode: (lat, lon) → address string. Rate limited 1 req/sec."""
    resp = requests.get(
        f"{NOMINATIM_BASE}/reverse",
        params={"lat": lat, "lon": lon, "format": "json"},
        headers=NOMINATIM_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    time.sleep(1)
    return data.get("display_name", f"{lat:.6f}, {lon:.6f}")


# ══════════════════════════════════════════════════════════════════════════════
# Route Position & Heading
# ══════════════════════════════════════════════════════════════════════════════

def route_position_at(route: Route, progress: float) -> Coordinate:
    """Get (lat, lon) at a given progress (0.0 to 1.0) along a route."""
    if not route:
        raise ValueError("Empty route")
    progress = max(0.0, min(1.0, progress))
    idx = int(progress * (len(route) - 1))
    return route[idx]


def route_heading_at(route: Route, progress: float) -> float:
    """Get heading (bearing, 0=north, 90=east) at a progress point along a route."""
    if len(route) < 2:
        return 0.0

    progress = max(0.0, min(1.0, progress))
    idx = int(progress * (len(route) - 1))
    next_idx = min(idx + 1, len(route) - 1)

    if idx == next_idx:
        idx = max(0, idx - 1)

    lat1, lon1 = math.radians(route[idx][0]), math.radians(route[idx][1])
    lat2, lon2 = math.radians(route[next_idx][0]), math.radians(route[next_idx][1])

    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    bearing = math.degrees(math.atan2(x, y))
    return (bearing + 360) % 360


# ══════════════════════════════════════════════════════════════════════════════
# Derived Field Profiles
# ══════════════════════════════════════════════════════════════════════════════

def climb_cruise_descend(
    progress: float,
    max_value: float,
    climb_end: float = 0.15,
    descend_start: float = 0.85,
) -> float:
    """
    Profile: climb, cruise, descend.
    Common for: aircraft altitude, train speed between stations.
    """
    if progress <= climb_end:
        return max_value * (progress / climb_end)
    elif progress >= descend_start:
        return max_value * (1.0 - (progress - descend_start) / (1.0 - descend_start))
    else:
        return max_value


def constant_with_jitter(value: float, jitter: float, rng) -> float:
    """Constant value with random noise. Common for: cruise speed, sensor readings."""
    return value + rng.uniform(-jitter, jitter)


def linear_ramp(progress: float, start_value: float, end_value: float) -> float:
    """Linear interpolation. Common for: fuel consumption, battery drain."""
    return start_value + (end_value - start_value) * progress
