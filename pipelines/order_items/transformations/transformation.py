# ──────────────────────────────────────────────────────────────
# Imports & common helpers
# ──────────────────────────────────────────────────────────────
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, ArrayType
)

# ──────────────────────────────────────────────────────────────
# 0. Bronze  – raw transaction event stream
# ──────────────────────────────────────────────────────────────
@dlt.table(
    comment = "Raw JSON transaction events as ingested (one file per event)."
)
def all_events():
    CATALOG = spark.conf.get("RAW_DATA_CATALOG")
    SCHEMA = spark.conf.get("RAW_DATA_SCHEMA")
    VOLUME = spark.conf.get("RAW_DATA_VOLUME")
    return (
        spark.readStream.format("cloudFiles") 
             .option("cloudFiles.format", "json")
             .load(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")
    )

# ──────────────────────────────────────────────────────────────
# 1. Silver – explode transaction items, add extended_price
# ──────────────────────────────────────────────────────────────
item_schema = StructType([
    StructField("id",          IntegerType()),
    StructField("category_id", IntegerType()),
    StructField("menu_id",     IntegerType()),
    StructField("brand_id",    IntegerType()),
    StructField("name",        StringType()),
    StructField("price",       DoubleType()),
    StructField("qty",         IntegerType())
])

body_schema = (
    StructType()
        .add("customer_lat",  DoubleType())
        .add("customer_lon",  DoubleType())
        .add("customer_addr", StringType())
        .add("items",         ArrayType(item_schema))
)

@dlt.table(
    name           = "silver_order_items",
    comment        = "Silver – one row per item per transaction, with extended_price.",
    partition_cols = ["order_day"]
)
def silver_order_items():
    df = (
        dlt.read_stream("all_events")
           .filter(F.col("event_type") == "order_created")
           .withColumn("event_ts",   F.to_timestamp("ts"))        # enforce TIMESTAMP
           .withColumn("body_obj",   F.from_json("body", body_schema))
           .withColumn("item",       F.explode("body_obj.items"))
           .withColumn("extended_price", F.col("item.price") * F.col("item.qty"))
           .withColumn("order_day",  F.to_date("event_ts"))
           .select(
               "order_id",
               "gk_id",
               "location",
               F.col("event_ts").alias("order_ts"),               # canonical event time
               "order_day",
               F.col("item.id").alias("item_id"),
               F.col("item.menu_id"),
               F.col("item.category_id"),
               F.col("item.brand_id"),
               F.col("item.name").alias("item_name"),
               F.col("item.price"),
               F.col("item.qty"),
               "extended_price"
           )
    )
    return df

# ──────────────────────────────────────────────────────────────
# 2-A. Gold – transaction header (one row per transaction)
# ──────────────────────────────────────────────────────────────
@dlt.table(
    name    = "gold_order_header",
    comment = "Gold – per-transaction revenue & counts."
)
def gold_order_header():
    return (
        dlt.read_stream("silver_order_items")
           .groupBy("order_id", "gk_id", "location", "order_day")
           .agg(
               F.sum("extended_price").alias("order_revenue"),
               F.sum("qty").alias("total_qty"),
               F.count("item_id").alias("total_items"),
               F.collect_set("brand_id").alias("brands_in_order")
           )
    )

# ──────────────────────────────────────────────────────────────
# 2-B. Gold – daily product transactions
# ──────────────────────────────────────────────────────────────
@dlt.table(
    name           = "gold_item_sales_day",
    partition_cols = ["day"],
    comment        = "Gold – product-level transaction volumes & revenue by day."
)
def gold_item_sales_day():
    return (
        dlt.read_stream("silver_order_items")
           .groupBy(
               "item_id", "menu_id", "category_id", "brand_id",
               F.col("order_day").alias("day")
           )
           .agg(
               F.sum("qty").alias("units_sold"),
               F.sum("extended_price").alias("gross_revenue")
           )
    )

# ──────────────────────────────────────────────────────────────
# 2-C. Gold – daily merchant transactions (stream-safe, HLL txn count)
# ──────────────────────────────────────────────────────────────
@dlt.table(
    name           = "gold_brand_sales_day",
    partition_cols = ["day"],
    comment        = "Gold – merchant-level transactions (approx), items, revenue by day."
)
def gold_brand_sales_day():
    return (
        dlt.read_stream("silver_order_items")
        .withWatermark("order_ts", "3 hours")
           .groupBy("brand_id", F.col("order_day").alias("day"))
           .agg(
               F.approx_count_distinct("order_id").alias("orders"),
               F.sum("qty").alias("items_sold"),
               F.sum("extended_price").alias("brand_revenue")
           )
    )

# ──────────────────────────────────────────────────────────────
# 2-D-1. Gold – hourly TRANSACTIONS per branch  (txns≈, revenue)
# ──────────────────────────────────────────────────────────────
@dlt.table(
    name           = "gold_location_sales_hourly",
    partition_cols = ["hour_ts"],
    comment        = "Gold – hourly transactions (approx) & revenue per branch location."
)
def gold_location_sales_hourly():
    return (
        dlt.read_stream("silver_order_items")
           .withWatermark("order_ts", "3 hours")
           .withColumn("hour_ts", F.date_trunc("hour", "order_ts"))
           .groupBy("location", "hour_ts")
           .agg(
               F.approx_count_distinct("order_id").alias("orders"),
               F.sum("extended_price").alias("revenue")
           )
    )