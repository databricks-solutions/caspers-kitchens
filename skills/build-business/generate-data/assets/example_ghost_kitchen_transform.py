"""
Worked Example: Ghost Kitchen replay transform.

This is the business-specific part that maps compact parquet columns
to the output event JSON format. Passed as `transform_fn` to the replay engine.

Usage:
    from replay_engine import replay
    from example_ghost_kitchen_transform import ghost_kitchen_transform

    replay(
        canonical_path="./canonical_dataset/events.parquet",
        transform_fn=ghost_kitchen_transform,
        catalog="caspers",
        schema="simulator",
        volume="events",
        start_day=30,
        speed_multiplier=60.0,
        entity_id_column="order_id",
    )
"""

from pyspark.sql import DataFrame, functions as F


# Ghost kitchen event type mapping (compact int → string)
EVENT_TYPE_MAP = {
    1: "order_created",
    2: "gk_started",
    3: "gk_finished",
    4: "gk_ready",
    5: "driver_arrived",
    6: "driver_picked_up",
    7: "driver_ping",
    8: "delivered",
}


def ghost_kitchen_transform(df: DataFrame, time_shift: int) -> DataFrame:
    """
    Transform compact parquet columns into the output event format.

    The canonical parquet has domain-specific columns (customer_lat, items_json,
    route_json, ping_lat, etc.). This function assembles them into the universal
    event envelope: event_type, ts, body (JSON string).

    Args:
        df: Spark DataFrame with compact parquet columns + virtual_ts_seconds
        time_shift: Seconds to add to virtual timestamps to project events to "today"

    Returns:
        DataFrame with columns: event_id, event_type, ts, location_id, order_id, sequence, body
    """

    # Map event_type_id integers to string names
    event_type_expr = F.col("event_type_id")
    for type_id, type_name in EVENT_TYPE_MAP.items():
        event_type_expr = F.when(F.col("event_type_id") == type_id, type_name).otherwise(event_type_expr)

    # Rebuild as chained when/otherwise
    type_col = F.lit(None).cast("string")
    for type_id, type_name in EVENT_TYPE_MAP.items():
        type_col = F.when(F.col("event_type_id") == type_id, type_name).otherwise(type_col)

    return (
        df
        .withColumn("event_type", type_col)

        # Shift virtual timestamps to appear relative to "today"
        .withColumn(
            "ts",
            F.date_format(
                F.from_unixtime(F.col("virtual_ts_seconds") + F.lit(time_shift)),
                "yyyy-MM-dd HH:mm:ss.SSS",
            ),
        )

        # Assemble body JSON per event type
        .withColumn(
            "body",
            F.when(
                F.col("event_type") == "order_created",
                F.to_json(
                    F.struct(
                        F.col("customer_lat").cast("double").alias("customer_lat"),
                        F.col("customer_lon").cast("double").alias("customer_lon"),
                        F.col("customer_addr"),
                        F.from_json(
                            F.col("items_json"),
                            "array<struct<id:int,category_id:int,menu_id:int,brand_id:int,name:string,price:double,qty:int>>",
                        ).alias("items"),
                    )
                ),
            )
            .when(
                F.col("event_type") == "driver_picked_up",
                F.when(
                    F.col("route_json").isNotNull(),
                    F.to_json(
                        F.struct(
                            F.from_json(F.col("route_json"), "array<array<double>>").alias("route_points")
                        )
                    ),
                ).otherwise(F.lit("{}")),
            )
            .when(
                F.col("event_type") == "driver_ping",
                F.when(
                    F.col("ping_lat").isNotNull(),
                    F.to_json(
                        F.struct(
                            F.col("ping_progress").cast("double").alias("progress_pct"),
                            F.col("ping_lat").cast("double").alias("loc_lat"),
                            F.col("ping_lon").cast("double").alias("loc_lon"),
                        )
                    ),
                ).otherwise(F.lit("{}")),
            )
            .when(
                F.col("event_type") == "delivered",
                F.when(
                    F.col("customer_lat").isNotNull(),
                    F.to_json(
                        F.struct(
                            F.col("customer_lat").cast("double").alias("delivered_lat"),
                            F.col("customer_lon").cast("double").alias("delivered_lon"),
                        )
                    ),
                ).otherwise(F.lit("{}")),
            )
            .otherwise(F.lit("{}")),
        )

        # Generate unique event IDs
        .withColumn("event_id", F.expr("uuid()"))

        # Select output columns (these are ghost-kitchen-specific choices)
        .select("event_id", "event_type", "ts", "location_id", "order_id", "sequence", "body")
    )
