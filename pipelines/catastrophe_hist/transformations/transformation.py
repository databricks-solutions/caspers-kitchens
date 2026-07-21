import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# Catastrophe Hist Aggregation Pipeline
#
# Medallion architecture — Bronze → Silver → Gold
# Sources: {HIST_CATALOG}.{HIST_SCHEMA}.catastrophe_hist_* tables
# Target:  {pipeline catalog}.{pipeline schema}  (set on the pipeline;
#          the Catastrophe_History_Pipeline stage uses {CATALOG}.lakeflow)
#
# Source catalog/schema come from the pipeline configuration so nothing is
# pinned to a single catalog:
#   HIST_CATALOG -> ${CATALOG}          (e.g. the run's catalog)
#   HIST_SCHEMA  -> ${SIMULATOR_SCHEMA} (e.g. "simulator")
# ============================================================

HIST_CATALOG = spark.conf.get("HIST_CATALOG")
HIST_SCHEMA = spark.conf.get("HIST_SCHEMA")


def _src(table: str) -> str:
    return f"{HIST_CATALOG}.{HIST_SCHEMA}.{table}"


# ============================================================
# BRONZE LAYER — Raw pass-through views
# Reads directly from the {HIST_CATALOG}.{HIST_SCHEMA} source tables.
# No transformations applied; schema preserved as-is.
# These are lightweight temporary views (not persisted to UC).
# ============================================================


@dlt.view(name="bronze_orders", comment="Raw catastrophe_hist_orders source table.")
def bronze_orders():
    return spark.read.table(_src("catastrophe_hist_orders"))


@dlt.view(name="bronze_order_events", comment="Raw catastrophe_hist_order_events source table.")
def bronze_order_events():
    return spark.read.table(_src("catastrophe_hist_order_events"))


@dlt.view(name="bronze_refunds", comment="Raw catastrophe_hist_refunds source table.")
def bronze_refunds():
    return spark.read.table(_src("catastrophe_hist_refunds"))


@dlt.view(name="bronze_complaints", comment="Raw catastrophe_hist_complaints source table.")
def bronze_complaints():
    return spark.read.table(_src("catastrophe_hist_complaints"))


@dlt.view(name="bronze_actions", comment="Raw catastrophe_hist_actions source table.")
def bronze_actions():
    return spark.read.table(_src("catastrophe_hist_actions"))


# ============================================================
# SILVER LAYER — Cleaned and enriched datasets
# ============================================================


@dlt.expect("order_id is not null", "order_id IS NOT NULL")
@dlt.expect("order_value is positive", "order_value > 0")
@dlt.table(
    name="silver_orders_enriched",
    comment="Orders enriched with per-order refund totals and complaint counts.",
)
def silver_orders_enriched():
    orders = spark.read.table("bronze_orders")

    # Aggregate refund data per order_id
    refund_agg = (
        spark.read.table("bronze_refunds")
        .groupBy("order_id")
        .agg(
            F.sum("refund_amount").alias("total_refund_amount"),
            F.count("*").alias("refund_count"),
        )
    )

    # Aggregate complaint data per order_id; dominant_sentiment = first recorded sentiment
    complaint_agg = (
        spark.read.table("bronze_complaints")
        .groupBy("order_id")
        .agg(
            F.count("*").alias("complaint_count"),
            F.first("sentiment").alias("dominant_sentiment"),
        )
    )

    return (
        orders
        .join(refund_agg, on="order_id", how="left")
        .join(complaint_agg, on="order_id", how="left")
        .fillna({"total_refund_amount": 0.0, "refund_count": 0, "complaint_count": 0})
    )


@dlt.table(
    name="silver_order_events_latest",
    comment="Deduplicated order events retaining only the latest status per order_id.",
)
def silver_order_events_latest():
    # Partition by order_id, rank events newest-first
    w = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc())

    return (
        spark.read.table("bronze_order_events")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ============================================================
# GOLD LAYER — Aggregated metrics
# ============================================================


@dlt.table(
    name="gold_orders_by_city_day",
    comment="Daily aggregated order metrics per city: volume, revenue, quality signals, disruptions.",
)
def gold_orders_by_city_day():
    return (
        spark.read.table("silver_orders_enriched")
        .groupBy("city_id", "city_name", "order_date")
        .agg(
            # Volume
            F.count("*").alias("total_orders"),
            F.sum(F.when(F.col("status") == "delivered", 1).otherwise(0)).alias("delivered_orders"),
            F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
            # Revenue
            F.sum("order_value").alias("total_revenue"),
            F.avg("order_value").alias("avg_order_value"),
            # Lateness
            F.avg("late_min").alias("avg_late_min"),
            # Quality
            F.sum(F.when(F.col("refunded") == True, 1).otherwise(0)).alias("refunded_orders"),
            F.sum(F.when(F.col("complaint") == True, 1).otherwise(0)).alias("orders_with_complaints"),
            F.sum("total_refund_amount").alias("total_refund_amount"),
            # Disruptions
            F.sum(F.when(F.col("disrupted") == True, 1).otherwise(0)).alias("disrupted_orders"),
            F.sum(F.when(F.col("rerouted") == True, 1).otherwise(0)).alias("rerouted_orders"),
        )
    )


@dlt.table(
    name="gold_orders_by_kitchen_day",
    comment="Daily kitchen-level metrics: order throughput and delivery timing efficiency.",
)
def gold_orders_by_kitchen_day():
    return (
        spark.read.table("silver_orders_enriched")
        .groupBy("kitchen_id", "kitchen_name", "city_id", "order_date")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("order_value").alias("total_revenue"),
            F.avg("late_min").alias("avg_late_min"),
            F.avg("prep_min").alias("avg_prep_min"),
            F.avg("travel_min").alias("avg_travel_min"),
            F.avg("eta_min").alias("avg_eta_min"),
        )
    )


@dlt.table(
    name="gold_refunds_summary",
    comment="Refund breakdown by city, order kind, and reason including aggregate refund rate.",
)
def gold_refunds_summary():
    return (
        spark.read.table("bronze_refunds")
        .groupBy("city_id", "kind", "reason")
        .agg(
            F.count("*").alias("total_refunds"),
            F.sum("refund_amount").alias("total_refund_amount"),
            F.avg("refund_amount").alias("avg_refund_amount"),
            F.avg("order_value").alias("avg_order_value"),
            (F.sum("refund_amount") / F.sum("order_value")).alias("refund_rate"),
        )
    )


@dlt.table(
    name="gold_complaints_summary",
    comment="Complaint metrics grouped by city, order kind, sentiment, and resolution type.",
)
def gold_complaints_summary():
    return (
        spark.read.table("bronze_complaints")
        .groupBy("city_id", "kind", "sentiment", "resolution")
        .agg(
            F.count("*").alias("complaint_count"),
            F.avg(
                (F.unix_timestamp("resolved_at") - F.unix_timestamp("raised_at")) / 3600.0
            ).alias("avg_resolution_time_hours"),
            F.sum(F.when(F.col("cold") == True, 1).otherwise(0)).alias("cold_complaint_count"),
        )
    )


@dlt.table(
    name="gold_disruption_impact",
    comment="Disruption impact per city/scenario/day: disruption rates and lateness comparison.",
)
def gold_disruption_impact():
    return (
        spark.read.table("silver_orders_enriched")
        .groupBy("city_id", "city_name", "scenario_id", "order_date")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum(F.when(F.col("disrupted") == True, 1).otherwise(0)).alias("disrupted_orders"),
            F.sum(F.when(F.col("rerouted") == True, 1).otherwise(0)).alias("rerouted_orders"),
            F.sum(F.when(F.col("bridge_closed") == True, 1).otherwise(0)).alias("bridge_closed_orders"),
            F.avg(F.when(F.col("disrupted") == True, F.col("late_min"))).alias("avg_late_min_disrupted"),
            F.avg(F.when(F.col("disrupted") == False, F.col("late_min"))).alias("avg_late_min_normal"),
            (
                F.sum(F.when(F.col("disrupted") == True, 1).otherwise(0)).cast("double")
                / F.count("*")
            ).alias("disruption_rate"),
        )
    )
