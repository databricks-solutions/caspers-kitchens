"""
Worked Example: Airline flight tracking transform.

This is the business-specific part that maps compact parquet columns
to the output event JSON format for an airline business. Shows great-circle
routing with altitude/speed profiles.

Complements example_ghost_kitchen_transform.py which shows road-based routing.
"""

from pyspark.sql import DataFrame, functions as F
import math

# Airline event type mapping
EVENT_TYPE_MAP = {
    1: "booking_created",
    2: "check_in",
    3: "boarding",
    4: "departed",
    5: "flight_position",
    6: "landed",
    7: "completed",
    8: "cancelled",
    9: "diverted",
}


def airline_transform(df: DataFrame, time_shift: int) -> DataFrame:
    """
    Transform compact parquet columns into airline event format.

    The canonical parquet has columns like:
    - flight_id, origin_airport, destination_airport
    - passenger_id, booking_class
    - route_json: [[lat,lon], ...] from great-circle
    - ping_lat, ping_lon, ping_progress (flight position updates)
    - ping_altitude_ft, ping_speed_knots (derived from profiles)

    Output: event_id, event_type, ts, flight_id, body (JSON string)
    """

    type_col = F.lit(None).cast("string")
    for type_id, type_name in EVENT_TYPE_MAP.items():
        type_col = F.when(F.col("event_type_id") == type_id, type_name).otherwise(type_col)

    return (
        df
        .withColumn("event_type", type_col)

        .withColumn(
            "ts",
            F.date_format(
                F.from_unixtime(F.col("virtual_ts_seconds") + F.lit(time_shift)),
                "yyyy-MM-dd HH:mm:ss.SSS",
            ),
        )

        .withColumn(
            "body",
            F.when(
                F.col("event_type") == "booking_created",
                F.to_json(
                    F.struct(
                        F.col("passenger_id"),
                        F.col("booking_class"),
                        F.col("origin_airport").alias("origin"),
                        F.col("destination_airport").alias("destination"),
                        F.col("seat_assignment"),
                    )
                ),
            )
            .when(
                F.col("event_type") == "departed",
                F.to_json(
                    F.struct(
                        F.col("origin_airport").alias("origin"),
                        F.col("destination_airport").alias("destination"),
                        F.col("aircraft_type"),
                        F.from_json(
                            F.col("route_json"),
                            "array<array<double>>",
                        ).alias("route_points"),
                    )
                ),
            )
            .when(
                F.col("event_type") == "flight_position",
                F.to_json(
                    F.struct(
                        F.col("ping_progress").cast("double").alias("progress_pct"),
                        F.col("ping_lat").cast("double").alias("lat"),
                        F.col("ping_lon").cast("double").alias("lon"),
                        F.col("ping_altitude_ft").cast("double").alias("altitude_ft"),
                        F.col("ping_speed_knots").cast("double").alias("speed_knots"),
                        F.col("ping_heading").cast("double").alias("heading"),
                    )
                ),
            )
            .when(
                F.col("event_type") == "landed",
                F.to_json(
                    F.struct(
                        F.col("destination_lat").cast("double").alias("lat"),
                        F.col("destination_lon").cast("double").alias("lon"),
                    )
                ),
            )
            .when(
                F.col("event_type") == "diverted",
                F.to_json(
                    F.struct(
                        F.col("diversion_airport").alias("diversion_airport"),
                        F.col("diversion_lat").cast("double").alias("lat"),
                        F.col("diversion_lon").cast("double").alias("lon"),
                        F.col("diversion_reason").alias("reason"),
                    )
                ),
            )
            .otherwise(F.lit("{}")),
        )

        .withColumn("event_id", F.expr("uuid()"))
        .select("event_id", "event_type", "ts", "flight_id", "sequence", "body")
    )


# ── Canonical Generator Snippets ─────────────────────────────────────────────
# These show how the canonical generator would compute tracking fields
# for the airline business. Used as context_factory + body_generator examples.

def airline_context_factory(seed_data, rng):
    """Pick route, aircraft, passenger — everything for one flight."""
    routes = seed_data["routes"]
    airports = seed_data["airports"]
    aircraft = seed_data["aircraft"]

    # Pick a route
    route = routes.sample(1, random_state=rng).iloc[0]
    origin = airports[airports["iata"] == route["origin_iata"]].iloc[0]
    destination = airports[airports["iata"] == route["destination_iata"]].iloc[0]

    # Compute great-circle route
    from routing import great_circle_route, great_circle_distance_km

    waypoints = [(origin["lat"], origin["lon"]), (destination["lat"], destination["lon"])]
    route_points = great_circle_route(waypoints, points_per_segment=100)
    distance_km = great_circle_distance_km(waypoints[0], waypoints[1])

    # Pick aircraft
    plane = aircraft.sample(1, random_state=rng).iloc[0]

    # Flight time based on distance (avg 800 km/h cruise)
    flight_time_hours = distance_km / 800.0
    flight_time_min = flight_time_hours * 60

    return {
        "ids": {
            "flight_id": f"{route['airline_iata']}{rng.integers(100, 9999):04d}",
        },
        "origin": origin,
        "destination": destination,
        "route_points": route_points,
        "distance_km": distance_km,
        "flight_time_min": flight_time_min,
        "aircraft": plane,
        "route_json": [[p[0], p[1]] for p in route_points],
    }


def airline_body_generators():
    """Body generators for each airline event type."""

    from routing import (
        route_position_at,
        route_heading_at,
        climb_cruise_descend,
        constant_with_jitter,
    )

    return {
        "booking_created": lambda ctx, seed, rng: {
            "passenger_id": f"PAX-{rng.integers(10000, 99999)}",
            "booking_class": rng.choice(["economy", "premium_economy", "business", "first"]),
            "origin_airport": ctx["origin"]["iata"],
            "destination_airport": ctx["destination"]["iata"],
            "seat_assignment": f"{rng.integers(1, 40)}{rng.choice(list('ABCDEF'))}",
        },
        "check_in": lambda ctx, seed, rng: {},
        "boarding": lambda ctx, seed, rng: {},
        "departed": lambda ctx, seed, rng: {
            "origin_airport": ctx["origin"]["iata"],
            "destination_airport": ctx["destination"]["iata"],
            "aircraft_type": ctx["aircraft"]["type"],
            "route_json": json.dumps(ctx["route_json"]),
        },
        "flight_position": lambda ctx, seed, rng, progress=0: {
            "ping_lat": route_position_at(ctx["route_points"], progress)[0],
            "ping_lon": route_position_at(ctx["route_points"], progress)[1],
            "ping_progress": progress * 100,
            "ping_altitude_ft": climb_cruise_descend(progress, max_value=35000),
            "ping_speed_knots": constant_with_jitter(450, 20, rng),
            "ping_heading": route_heading_at(ctx["route_points"], progress),
        },
        "landed": lambda ctx, seed, rng: {
            "destination_lat": ctx["destination"]["lat"],
            "destination_lon": ctx["destination"]["lon"],
        },
        "completed": lambda ctx, seed, rng: {},
        "cancelled": lambda ctx, seed, rng: {
            "reason": rng.choice(["weather", "mechanical", "crew", "passenger_request"]),
        },
        "diverted": lambda ctx, seed, rng: {
            "diversion_reason": rng.choice(["weather", "medical_emergency", "mechanical"]),
        },
    }


import json  # needed by body_generators
