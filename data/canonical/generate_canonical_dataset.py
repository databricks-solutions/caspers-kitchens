"""
Canonical Dataset Generator for Casper's Kitchens
Generates 90 days of realistic ghost kitchen event data across 4 cities.

Uses real OpenStreetMap road networks for routing.
Outputs compact parquet files (orders.parquet, events.parquet).
"""

import datetime as dt
import json
import math
import pickle
import random
from pathlib import Path
from typing import Dict, List, Tuple

import networkx as nx
import numpy as np
import osmnx as ox
import pandas as pd
from geopy.geocoders import Nominatim

# ============================================================================
# CONFIGURATION
# ============================================================================

DAYS = 90
RANDOM_SEED = 42
PING_INTERVAL_SEC = 60
DRIVER_MPH = 25

# Service time parameters (minutes): [mean, std_dev]
SVC_TIMES = {
    "created_to_started": [2, 1],
    "started_to_finished": [10, 3],
    "finished_to_ready": [2, 1],
    "ready_to_pickup": [6, 2],
}

# Driver arrival distribution (beta distribution parameters)
DRIVER_ARRIVAL = {
    "after_ready_pct": 0.5,  # 50% chance driver arrives after food is ready
    "alpha": 3,
    "beta": 3,
}

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# ============================================================================
# LOAD DIMENSION TABLES
# ============================================================================

print("📊 Loading dimension tables...")
locations_df = pd.read_parquet("canonical_dataset/locations.parquet")
brands_df = pd.read_parquet("canonical_dataset/brands.parquet")
brand_locations_df = pd.read_parquet("canonical_dataset/brand_locations.parquet")
categories_df = pd.read_parquet("canonical_dataset/categories.parquet")
items_df = pd.read_parquet("canonical_dataset/items.parquet")

# Build lookup structures
LOCATIONS = locations_df.to_dict('records')
BRANDS_BY_ID = brands_df.set_index('brand_id').to_dict('index')
ITEMS_BY_BRAND = {bid: grp.to_dict('records') for bid, grp in items_df.groupby('brand_id')}

# ============================================================================
# DEMAND PATTERNS
# ============================================================================

def minute_weights():
    """Generate minute-by-minute demand weights for 24h period."""
    w = np.ones(1440)
    # Lunch peak: 11am-1:30pm (3x multiplier)
    for h, m, mult in [(11, 0, 3.0), (17, 0, 3.5)]:
        if h == 11:  # Lunch
            start_m, end_m = 11*60, 13*60 + 30
        else:  # Dinner
            start_m, end_m = 17*60, 20*60
        span = end_m - start_m
        for mi in range(start_m, end_m):
            x = (mi - start_m) / span
            w[mi] += (mult - 1) * (math.sin(math.pi * x) ** 2)
    return w

MINUTE_WEIGHTS = minute_weights()

def minute_weights_sv():
    """Silicon Valley has late-night spike."""
    w = minute_weights()
    # Add late-night spike 9pm-1am (2x multiplier)
    start_m, end_m = 21*60, 24*60  # 9pm-midnight
    span = end_m - start_m
    for mi in range(start_m, end_m):
        x = (mi - start_m) / span
        w[mi] += (2.0 - 1) * (math.sin(math.pi * x) ** 2)
    # Add 12am-1am
    for mi in range(0, 60):
        w[mi] += 1.0
    return w

MINUTE_WEIGHTS_SV = minute_weights_sv()

def day_of_week_multiplier(date: dt.date) -> float:
    """Weekend boost, except no pattern for SV."""
    dow = date.strftime("%a").lower()
    mult = {
        "mon": 1.0,
        "tue": 1.05,
        "wed": 1.08,
        "thu": 1.10,
        "fri": 1.25,
        "sat": 1.35,
        "sun": 1.15,
    }
    return mult[dow]

def orders_for_day(day_num: int, location: dict) -> int:
    """Calculate target orders for a given day and location."""
    base = location['base_orders_day']
    growth_rate = location['growth_rate_daily']

    # Apply growth
    orders = base * ((1 + growth_rate) ** day_num)

    # Day of week variation
    date = dt.date(2024, 1, 1) + dt.timedelta(days=day_num)
    if location['location_code'] != 'sv':  # SV has no weekly pattern
        orders *= day_of_week_multiplier(date)

    # Add noise ±10%
    orders *= random.uniform(0.9, 1.1)

    return int(orders)

# ============================================================================
# ROAD NETWORK & ROUTING
# ============================================================================

print("🗺️  Loading road networks (this may take a few minutes)...")

GRAPHS = {}
NODES_DF = {}

def load_graph_and_nodes(location: dict) -> Tuple[nx.MultiDiGraph, pd.DataFrame]:
    """Load OSM road network and addressable nodes for a location."""
    loc_code = location['location_code']
    cache_file = Path(f"cache_{loc_code}_graph.pkl")

    if cache_file.exists():
        print(f"  Loading cached graph for {loc_code}...")
        with open(cache_file, 'rb') as f:
            G = pickle.load(f)
    else:
        print(f"  Downloading road network for {location['name']}...")
        ox.settings.log_console = False
        center = (location['lat'], location['lon'])
        G = ox.graph_from_point(center, dist=4 * 1609.34, network_type="drive")
        with open(cache_file, 'wb') as f:
            pickle.dump(G, f)

    # Get GK node
    gk_node = ox.distance.nearest_nodes(G, location['lon'], location['lat'])

    # Get connected component containing GK
    comp_map = {n: cid for cid, comp in enumerate(nx.connected_components(G.to_undirected())) for n in comp}
    gk_component = comp_map[gk_node]

    # Extract nodes in same component as GK
    nodes = []
    for node_id, data in G.nodes(data=True):
        if comp_map.get(node_id) == gk_component:
            nodes.append({
                'node_id': node_id,
                'lat': data['y'],
                'lon': data['x'],
            })

    nodes_df = pd.DataFrame(nodes)
    print(f"  {loc_code}: {len(nodes_df)} addressable nodes")

    return G, nodes_df, gk_node

for loc in LOCATIONS:
    G, nodes_df, gk_node = load_graph_and_nodes(loc)
    GRAPHS[loc['location_code']] = {
        'graph': G,
        'nodes': nodes_df,
        'gk_node': gk_node,
    }

def shortest_route(G: nx.MultiDiGraph, gk_node, customer_node) -> Tuple[List[Tuple[float, float]], float]:
    """Calculate shortest route and return (route_points, distance_meters)."""
    try:
        path = nx.shortest_path(G, gk_node, customer_node, weight="length")
        graph = G
    except nx.NetworkXNoPath:
        graph = G.to_undirected()
        path = nx.shortest_path(graph, gk_node, customer_node, weight="length")

    # Extract coordinates
    coords = [(graph.nodes[n]["y"], graph.nodes[n]["x"]) for n in path]

    # Calculate distance
    dist = sum(
        min(d["length"] for d in graph[u][v].values())
        for u, v in zip(path[:-1], path[1:])
    )

    return coords, dist

# ============================================================================
# BRAND & ITEM SELECTION
# ============================================================================

def active_brands_for_day(location_id: int, day: int) -> List[int]:
    """Get list of brand IDs active at this location on this day."""
    active = brand_locations_df[
        (brand_locations_df['location_id'] == location_id) &
        (brand_locations_df['start_day'] <= day) &
        ((brand_locations_df['end_day'].isna()) | (brand_locations_df['end_day'] > day))
    ]
    return active['brand_id'].tolist()

def brand_weight(brand_id: int, location_id: int, day: int) -> float:
    """Calculate brand popularity weight based on growth trajectory."""
    bl = brand_locations_df[
        (brand_locations_df['brand_id'] == brand_id) &
        (brand_locations_df['location_id'] == location_id)
    ]

    if len(bl) == 0:
        return 0.0

    bl = bl.iloc[0]
    growth_rate = bl['growth_rate_monthly']
    days_since_start = day - bl['start_day']

    # Convert monthly to daily growth
    daily_rate = (1 + growth_rate) ** (1/30) - 1
    weight = (1 + daily_rate) ** days_since_start

    return max(0.1, weight)  # Minimum weight 0.1

def select_basket(location_id: int, day: int) -> List[Dict]:
    """Select items for an order."""
    active_brands = active_brands_for_day(location_id, day)
    if not active_brands:
        return []

    # Calculate brand weights
    weights = np.array([brand_weight(bid, location_id, day) for bid in active_brands])
    weights = weights / weights.sum()

    # Single brand 70% of time, 2-3 brands otherwise
    if random.random() < 0.7:
        num_brands = 1
    else:
        num_brands = random.randint(2, min(3, len(active_brands)))

    chosen_brands = np.random.choice(active_brands, size=num_brands, replace=False, p=weights)

    # Select items from each brand
    items = []
    for brand_id in chosen_brands:
        brand_items = ITEMS_BY_BRAND.get(brand_id, [])
        if not brand_items:
            continue

        # Pick 1-3 items from this brand
        num_items = random.randint(1, min(3, len(brand_items)))
        selected = random.sample(brand_items, num_items)

        for item in selected:
            item_copy = item.copy()
            item_copy['qty'] = random.randint(1, 2)
            items.append(item_copy)

    return items

# ============================================================================
# ORDER GENERATION
# ============================================================================

def gauss_time(mean_std: List[float]) -> float:
    """Sample from gaussian, minimum 0.1 minutes."""
    return max(0.1, random.gauss(mean_std[0], mean_std[1]))

def driver_arrival_time(order_ts: dt.datetime, ready_ts: dt.datetime, pickup_ts: dt.datetime) -> dt.datetime:
    """Calculate when driver arrives at kitchen."""
    if random.random() < DRIVER_ARRIVAL['after_ready_pct']:
        # Arrives between ready and pickup
        base, span = ready_ts, pickup_ts - ready_ts
    else:
        # Arrives between order creation and ready
        base, span = order_ts, ready_ts - order_ts

    # Beta distribution for variation
    frac = np.random.beta(DRIVER_ARRIVAL['alpha'], DRIVER_ARRIVAL['beta'])
    arrival = base + span * frac

    # Ensure ordering: arrival < pickup
    if arrival >= pickup_ts:
        arrival = pickup_ts - dt.timedelta(microseconds=1)

    return arrival

def generate_random_order_id() -> str:
    """Generate random 6-character alphanumeric order ID."""
    import string
    chars = string.ascii_uppercase + string.digits
    return ''.join(random.choice(chars) for _ in range(6))

def generate_order(order_id: str, location: dict, day: int, minute_of_day: int) -> Tuple[Dict, List[Dict]]:
    """Generate one complete order with all events."""
    loc_code = location['location_code']
    graph_info = GRAPHS[loc_code]
    G = graph_info['graph']
    gk_node = graph_info['gk_node']
    nodes = graph_info['nodes']

    # Random customer location
    customer = nodes.sample(1).iloc[0]
    customer_node = int(customer['node_id'])
    customer_lat = customer['lat']
    customer_lon = customer['lon']

    # Calculate route
    route_points, dist_m = shortest_route(G, gk_node, customer_node)
    drive_time_min = (dist_m / 1609.34) / DRIVER_MPH * 60

    # Select items
    items = select_basket(location['location_id'], day)
    if not items:
        return None, None

    # Create order timestamp
    date = dt.date(2024, 1, 1) + dt.timedelta(days=day)
    order_ts = dt.datetime.combine(date, dt.time(0, 0)) + dt.timedelta(minutes=minute_of_day, seconds=random.randint(0, 59))

    # Calculate event timestamps
    ts_started = order_ts + dt.timedelta(minutes=gauss_time(SVC_TIMES["created_to_started"]))
    ts_finished = ts_started + dt.timedelta(minutes=gauss_time(SVC_TIMES["started_to_finished"]))
    ts_ready = ts_finished + dt.timedelta(minutes=gauss_time(SVC_TIMES["finished_to_ready"]))
    ts_pickup = ts_ready + dt.timedelta(minutes=gauss_time(SVC_TIMES["ready_to_pickup"]))
    ts_delivered = ts_pickup + dt.timedelta(minutes=drive_time_min)
    ts_arrival = driver_arrival_time(order_ts, ts_ready, ts_pickup)

    # Common data for all events
    customer_lat_f = float(customer_lat)
    customer_lon_f = float(customer_lon)
    customer_addr_str = f"{random.randint(1, 9999)} Main St"
    items_json_str = json.dumps(items)

    # Build event records - ALL data embedded in events
    events = []
    seq = 0

    def add_event(ts, event_type_id, **kwargs):
        nonlocal seq
        events.append({
            'order_id': order_id,
            'location_id': location['location_id'],
            'event_type_id': event_type_id,
            'ts_seconds': int(ts.timestamp()),  # Absolute timestamp
            'sequence': seq,
            **kwargs
        })
        seq += 1

    # Event type IDs: 1=created, 2=started, 3=finished, 4=ready, 5=arrived, 6=picked_up, 7=ping, 8=delivered
    add_event(order_ts, 1,
              customer_lat=customer_lat_f, customer_lon=customer_lon_f,
              customer_addr=customer_addr_str, items_json=items_json_str,
              route_json=None, ping_lat=None, ping_lon=None, ping_progress=None)

    add_event(ts_started, 2,
              customer_lat=None, customer_lon=None, customer_addr=None, items_json=None,
              route_json=None, ping_lat=None, ping_lon=None, ping_progress=None)

    add_event(ts_finished, 3,
              customer_lat=None, customer_lon=None, customer_addr=None, items_json=None,
              route_json=None, ping_lat=None, ping_lon=None, ping_progress=None)

    add_event(ts_ready, 4,
              customer_lat=None, customer_lon=None, customer_addr=None, items_json=None,
              route_json=None, ping_lat=None, ping_lon=None, ping_progress=None)

    add_event(ts_arrival, 5,
              customer_lat=None, customer_lon=None, customer_addr=None, items_json=None,
              route_json=None, ping_lat=None, ping_lon=None, ping_progress=None)

    add_event(ts_pickup, 6,
              customer_lat=None, customer_lon=None, customer_addr=None, items_json=None,
              route_json=json.dumps(route_points), ping_lat=None, ping_lon=None, ping_progress=None)

    # Driver pings
    num_pings = max(1, int(drive_time_min * 60 / PING_INTERVAL_SEC))
    for i in range(1, num_pings):
        progress = i / num_pings
        ping_ts = ts_pickup + dt.timedelta(seconds=i * PING_INTERVAL_SEC)
        route_idx = int(progress * (len(route_points) - 1))
        ping_lat_val, ping_lon_val = route_points[route_idx]
        add_event(
            ping_ts, 7,
            customer_lat=None, customer_lon=None, customer_addr=None, items_json=None,
            route_json=None,
            ping_lat=float(ping_lat_val),
            ping_lon=float(ping_lon_val),
            ping_progress=float(progress * 100)
        )

    add_event(ts_delivered, 8,
              customer_lat=customer_lat_f, customer_lon=customer_lon_f,
              customer_addr=None, items_json=None,
              route_json=None, ping_lat=None, ping_lon=None, ping_progress=None)

    return events

# ============================================================================
# MAIN GENERATION LOOP
# ============================================================================

print(f"\n🏭 Generating {DAYS} days of orders across 4 cities...")

all_events = []
generated_order_ids = set()  # Track to ensure uniqueness

for day in range(DAYS):
    if day % 10 == 0:
        print(f"  Day {day}/{DAYS}...")

    for location in LOCATIONS:
        loc_code = location['location_code']
        target_orders = orders_for_day(day, location)

        # Get minute weights for this location
        if loc_code == 'sv':
            minute_weights = MINUTE_WEIGHTS_SV
        else:
            minute_weights = MINUTE_WEIGHTS

        # Normalize to get probabilities
        lambda_by_minute = target_orders / minute_weights.sum() * minute_weights

        # Generate orders using Poisson distribution per minute
        for minute in range(1440):
            num_orders = np.random.poisson(lambda_by_minute[minute])

            for _ in range(num_orders):
                # Generate unique random order ID
                order_id = generate_random_order_id()
                while order_id in generated_order_ids:
                    order_id = generate_random_order_id()
                generated_order_ids.add(order_id)

                events = generate_order(order_id, location, day, minute)
                if events:
                    all_events.extend(events)

# Count unique orders
unique_orders = len(set(e['order_id'] for e in all_events))
print(f"\n✅ Generated {unique_orders} orders with {len(all_events)} events")

# ============================================================================
# SAVE TO PARQUET
# ============================================================================

print("\n💾 Saving to parquet file...")

events_df = pd.DataFrame(all_events)

# Optimize dtypes for compression
# order_id is already string, keep as-is
events_df['location_id'] = events_df['location_id'].astype('int8')
events_df['event_type_id'] = events_df['event_type_id'].astype('int8')
events_df['ts_seconds'] = events_df['ts_seconds'].astype('int64')  # Unix timestamp
events_df['sequence'] = events_df['sequence'].astype('int8')
events_df['customer_lat'] = events_df['customer_lat'].astype('float32')
events_df['customer_lon'] = events_df['customer_lon'].astype('float32')
events_df['ping_lat'] = events_df['ping_lat'].astype('float32')
events_df['ping_lon'] = events_df['ping_lon'].astype('float32')
events_df['ping_progress'] = events_df['ping_progress'].astype('float32')

events_df.to_parquet("canonical_dataset/events.parquet", compression='snappy', index=False)

print(f"\n🎉 Canonical dataset generated successfully!")
print(f"\nDataset summary:")
print(f"  - Orders: {unique_orders:,}")
print(f"  - Events: {len(events_df):,}")
print(f"  - Time period: 90 days (2024-01-01 to 2024-03-30)")
print(f"  - Locations: {len(LOCATIONS)}")
print(f"\nFile size:")
import os
events_size = os.path.getsize("canonical_dataset/events.parquet") / 1024 / 1024
print(f"  - events.parquet: {events_size:.1f} MB")
