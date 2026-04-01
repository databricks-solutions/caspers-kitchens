# Canonical Dataset Generator Pattern

How to generate a canonical event dataset from a state machine definition + seed data.

## Overview

The canonical generator creates a parquet file of pre-generated events covering N days. This file is then replayed by the replay engine. Generating offline (rather than live) gives you: instant backfill, reproducibility, and crash resilience.

## Inputs

1. **State machine** — states, transitions (with weights), delay distributions
2. **Seed data** — entity DataFrames (the dimensional tables)
3. **Demand config** — how many entities per day, time-of-day curves, growth rates
4. **Body generators** — per-event-type functions that produce compact columns from seed data
5. **Config** — number of days, random seed for reproducibility

## The Algorithm

```python
import pandas as pd
import numpy as np
from datetime import datetime

def generate_canonical(
    state_machine,     # dict defining states, transitions, delays
    seed_data,         # dict of DataFrames: {"brands": df, "items": df, ...}
    demand,            # DemandConfig object
    body_generators,   # dict: {"event_type_name": fn(context, seed_data) -> dict}
    context_factory,   # fn(seed_data) -> context dict for one entity lifecycle
    days=40,
    epoch=datetime(2024, 1, 1),
    random_seed=42,
):
    rng = np.random.default_rng(random_seed)
    epoch_ts = int(epoch.timestamp())
    all_events = []

    for day in range(days):
        # How many entities start today?
        n_entities = demand.count_for_day(day, rng)

        for _ in range(n_entities):
            # When does this entity's lifecycle begin?
            time_of_day_seconds = demand.sample_start_time(day, rng)
            start_ts = epoch_ts + (day * 86400) + time_of_day_seconds

            # Create context: pick which seed data applies to this entity
            # e.g., pick a location, pick brands at that location, pick items
            context = context_factory(seed_data, rng)

            # Walk the state machine
            current_state = state_machine["initial"]
            current_ts = start_ts
            sequence = 0

            while True:
                # Generate body columns for this event
                body_cols = body_generators[current_state](context, seed_data, rng)

                all_events.append({
                    "ts_seconds": current_ts,
                    "event_type": current_state,
                    "sequence": sequence,
                    **context["ids"],    # e.g., {"order_id": "A7K2M9", "location_id": 1}
                    **body_cols,         # e.g., {"customer_lat": 37.75, "items_json": "..."}
                })
                sequence += 1

                # Terminal state? Done with this entity.
                if current_state in state_machine["terminal"]:
                    break

                # Pick next transition
                transitions = state_machine["states"][current_state]["transitions"]
                weights = [t["weight"] for t in transitions]
                chosen = rng.choice(len(transitions), p=weights)
                transition = transitions[chosen]

                # Sample delay
                delay = sample_delay(transition["delay"], rng)

                # Emit tracking events during this transition
                tracking = state_machine.get("tracking", {})
                tracking_key = f"{current_state}->{transition['to']}"
                if tracking_key in tracking:
                    track_cfg = tracking[tracking_key]
                    t = current_ts
                    while t < current_ts + delay:
                        t += track_cfg["interval_seconds"]
                        if t >= current_ts + delay:
                            break
                        progress = (t - current_ts) / delay
                        track_cols = body_generators[track_cfg["event_type"]](
                            context, seed_data, rng, progress=progress
                        )
                        all_events.append({
                            "ts_seconds": t,
                            "event_type": track_cfg["event_type"],
                            "sequence": sequence,
                            **context["ids"],
                            **track_cols,
                        })
                        sequence += 1

                current_ts += int(delay)
                current_state = transition["to"]

    df = pd.DataFrame(all_events)
    df = df.sort_values("ts_seconds").reset_index(drop=True)
    return df


def sample_delay(delay_config, rng):
    """Sample a delay in seconds from a distribution config."""
    dist = delay_config.get("distribution", "lognormal")
    if dist == "lognormal":
        median_sec = delay_config["median_seconds"]
        cv = delay_config.get("cv", 0.3)
        sigma = np.sqrt(np.log(1 + cv**2))
        mu = np.log(median_sec) - sigma**2 / 2
        return max(1, int(rng.lognormal(mu, sigma)))
    elif dist == "fixed":
        return delay_config["seconds"]
    elif dist == "uniform":
        return int(rng.uniform(delay_config["min_seconds"], delay_config["max_seconds"]))
    else:
        raise ValueError(f"Unknown distribution: {dist}")
```

## Spatial Routing in the Generator

When tracking events have a `route` config, the context factory computes the route once per entity lifecycle, and tracking body generators index into it.

### Route Computation in Context Factory

```python
from routing import RoadGraph, great_circle_route

# Load graph ONCE at generator startup (not per-entity)
# For multiple service areas, load one graph per area
road_graph = RoadGraph.load(center=(37.77, -122.42), radius_km=6.4, network_type="drive")

def context_factory_with_routing(seed_data, rng, route_config):
    """Extended context factory that computes routes for spatial tracking."""
    # ... pick seed data as usual ...

    # Generate destination if needed
    if destination_mode == "generated_in_area":
        # For road businesses, use the graph's random_node — guaranteed routable
        dest = road_graph.generate_location_in_area(rng, with_address=True)
        dest_lat, dest_lon = dest["lat"], dest["lon"]
        dest_address = dest["address"]

    # Compute route based on mode
    waypoints = [(origin_lat, origin_lon), (dest_lat, dest_lon)]
    # Multi-stop: just add more waypoints to the list

    if route_config["mode"] == "road":
        route_points, distance_m = road_graph.route_multi(waypoints)
    elif route_config["mode"] == "air":
        route_points = great_circle_route(waypoints, points_per_segment=100)
        distance_m = None  # use great_circle_distance_km if needed

    context["route_points"] = route_points
    context["route_json"] = [[p[0], p[1]] for p in route_points]
    # ... return context ...
```

Route results are cached automatically by `osrm_route` — identical waypoint pairs return the cached route without another API call.

### Tracking Body Generators with Routes

```python
from routing import route_position_at, route_heading_at, climb_cruise_descend, constant_with_jitter

# Road-based (ghost kitchen)
"driver_ping": lambda ctx, seed, rng, progress=0: {
    "ping_lat": route_position_at(ctx["route_points"], progress)[0],
    "ping_lon": route_position_at(ctx["route_points"], progress)[1],
    "ping_progress": progress * 100,
},

# Air-based (airline) with derived field profiles
"flight_position": lambda ctx, seed, rng, progress=0: {
    "ping_lat": route_position_at(ctx["route_points"], progress)[0],
    "ping_lon": route_position_at(ctx["route_points"], progress)[1],
    "ping_progress": progress * 100,
    "ping_altitude_ft": climb_cruise_descend(progress, max_value=35000),
    "ping_speed_knots": constant_with_jitter(450, 20, rng),
    "ping_heading": route_heading_at(ctx["route_points"], progress),
},
```

### Graph Loading Strategy

Load ONE graph per service area at generator startup. The graph stays in memory for the entire run.

```python
# Multiple service areas = multiple graphs
graphs = {}
for loc in seed_data["locations"].itertuples():
    graphs[loc.location_id] = RoadGraph.load(
        center=(loc.lat, loc.lon),
        radius_km=6.4,
        network_type="drive",  # or "walk" for pedestrian businesses
    )
```

Route caching is built into `RoadGraph` — same origin→destination pair returns the cached result. For businesses with fixed origins, there are only `N_origins × N_destinations` unique routes.

For air routes, no graph needed — `great_circle_route` is pure math.

---

## The Business-Specific Parts

### 1. State Machine Definition

```python
# Ghost kitchen example
ghost_kitchen_sm = {
    "initial": "order_created",
    "terminal": ["delivered"],
    "states": {
        "order_created": {
            "transitions": [
                {"to": "gk_started", "weight": 1.0, "delay": {"median_seconds": 120, "cv": 0.5}}
            ]
        },
        "gk_started": {
            "transitions": [
                {"to": "gk_finished", "weight": 1.0, "delay": {"median_seconds": 600, "cv": 0.3}}
            ]
        },
        "gk_finished": {
            "transitions": [
                {"to": "gk_ready", "weight": 1.0, "delay": {"median_seconds": 120, "cv": 0.5}}
            ]
        },
        "gk_ready": {
            "transitions": [
                {"to": "driver_arrived", "weight": 1.0, "delay": {"median_seconds": 360, "cv": 0.33}}
            ]
        },
        "driver_arrived": {
            "transitions": [
                {"to": "driver_picked_up", "weight": 1.0, "delay": {"median_seconds": 60, "cv": 0.3}}
            ]
        },
        "driver_picked_up": {
            "transitions": [
                {"to": "delivered", "weight": 1.0, "delay": {"median_seconds": 900, "cv": 0.4}}
            ]
        },
        "delivered": {}  # terminal
    },
    "tracking": {
        "driver_picked_up->delivered": {
            "event_type": "driver_ping",
            "interval_seconds": 60
        }
    }
}
```

### 2. Context Factory

Creates the "who/what/where" for each entity lifecycle:

```python
def ghost_kitchen_context(seed_data, rng):
    """Pick a location, brands, items, customer — everything for one order."""
    locations = seed_data["locations"]
    brands = seed_data["brands"]
    items = seed_data["items"]

    # Pick location (weighted by base_orders_day)
    loc = locations.sample(1, weights="base_orders_day", random_state=rng).iloc[0]

    # Pick 1-3 brands available at this location
    available = brands[brands["location_id"] == loc["location_id"]]
    n_brands = rng.choice([1, 1, 1, 2, 3])  # 70% single brand
    chosen_brands = available.sample(min(n_brands, len(available)), random_state=rng)

    # Pick items from chosen brands
    order_items = []
    for _, brand in chosen_brands.iterrows():
        brand_items = items[items["brand_id"] == brand["brand_id"]]
        n_items = rng.integers(1, 4)
        for _, item in brand_items.sample(min(n_items, len(brand_items)), random_state=rng).iterrows():
            order_items.append({
                "id": int(item["item_id"]),
                "brand_id": int(brand["brand_id"]),
                "name": item["name"],
                "price": float(item["price"]),
                "qty": int(rng.integers(1, 3)),
            })

    # Generate customer location (random within radius of kitchen)
    angle = rng.uniform(0, 2 * np.pi)
    dist = rng.uniform(0, loc["radius_mi"]) * 0.01449  # miles to degrees approx
    customer_lat = loc["lat"] + dist * np.cos(angle)
    customer_lon = loc["lon"] + dist * np.sin(angle)

    return {
        "ids": {
            "order_id": generate_order_id(rng),
            "location_id": int(loc["location_id"]),
        },
        "location": loc,
        "items": order_items,
        "customer_lat": customer_lat,
        "customer_lon": customer_lon,
    }
```

### 3. Body Generators

One function per event type that produces the compact columns:

```python
body_generators = {
    "order_created": lambda ctx, seed, rng: {
        "customer_lat": ctx["customer_lat"],
        "customer_lon": ctx["customer_lon"],
        "customer_addr": generate_address(rng),
        "items_json": json.dumps(ctx["items"]),
    },
    "gk_started": lambda ctx, seed, rng: {},
    "gk_finished": lambda ctx, seed, rng: {},
    "gk_ready": lambda ctx, seed, rng: {},
    "driver_arrived": lambda ctx, seed, rng: {},
    "driver_picked_up": lambda ctx, seed, rng: {
        "route_json": json.dumps(compute_route(ctx["location"], ctx["customer_lat"], ctx["customer_lon"])),
    },
    "driver_ping": lambda ctx, seed, rng, progress=0: {
        "ping_lat": interpolate_route(ctx, progress)[0],
        "ping_lon": interpolate_route(ctx, progress)[1],
        "ping_progress": progress * 100,
    },
    "delivered": lambda ctx, seed, rng: {
        "customer_lat": ctx["customer_lat"],
        "customer_lon": ctx["customer_lon"],
    },
}
```

## Demand Configuration

```python
class DemandConfig:
    def __init__(self, base_orders, growth_rate, time_of_day_curve, day_of_week_mult):
        self.base_orders = base_orders
        self.growth_rate = growth_rate          # daily compound
        self.tod_curve = time_of_day_curve      # list of (hour, multiplier) pairs
        self.dow_mult = day_of_week_mult        # [Mon, Tue, ..., Sun] multipliers

    def count_for_day(self, day, rng):
        base = self.base_orders * ((1 + self.growth_rate) ** day)
        dow = self.dow_mult[day % 7]
        noise = rng.normal(1.0, 0.1)
        return max(1, int(base * dow * noise))

    def sample_start_time(self, day, rng):
        """Sample a time-of-day in seconds using the demand curve."""
        # Rejection sampling against the time-of-day curve
        while True:
            hour = rng.uniform(0, 24)
            intensity = self._intensity_at(hour)
            if rng.uniform(0, 1) < intensity:
                minute = rng.uniform(0, 60)
                return int(hour * 3600 + minute * 60)

    def _intensity_at(self, hour):
        # Interpolate the time-of-day curve
        ...
```

## Output

The generator produces a single parquet file with:
- `ts_seconds` (int64) — required by replay engine
- `event_type` (string) or `event_type_id` (int) — business choice
- Entity ID column(s) — whatever the business needs
- Compact body columns — one column per field, nulls where not applicable

This compact format is more efficient than storing JSON bodies in parquet. The replay engine's transform function expands compact columns into JSON bodies at replay time.
