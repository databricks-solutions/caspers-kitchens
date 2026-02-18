# Databricks notebook source

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, array_contains, array_size, when,
    sum as spark_sum, avg as spark_avg, min as spark_min,
    max as spark_max, count, round as spark_round,
)

CATALOG = spark.conf.get("MENU_CATALOG")

BRANDS_TABLE = f"{CATALOG}.menu_documents.brands_metadata"
INSPECTIONS_TABLE = f"{CATALOG}.food_safety.inspections"
VIOLATIONS_TABLE = f"{CATALOG}.food_safety.violations"

# ============================================================
# BRONZE LAYER — Raw ingestion with data quality expectations
# ============================================================

# COMMAND ----------


@dlt.table(
    name="bronze_menu_raw",
    comment="Raw menu item data ingested from brands_metadata source table",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("valid_item_name", "item_name IS NOT NULL")
@dlt.expect_or_drop("valid_brand", "brand_name IS NOT NULL")
@dlt.expect("valid_price", "price > 0")
def bronze_menu_raw():
    return spark.read.table(BRANDS_TABLE)


# COMMAND ----------


@dlt.table(
    name="bronze_inspections_raw",
    comment="Raw food safety inspection records ingested from source table",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("valid_inspection_id", "inspection_id IS NOT NULL")
@dlt.expect("valid_score", "score BETWEEN 0 AND 100")
def bronze_inspections_raw():
    return spark.read.table(INSPECTIONS_TABLE)


# COMMAND ----------


@dlt.table(
    name="bronze_violations_raw",
    comment="Raw food safety violation records ingested from source table",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("valid_violation_inspection", "inspection_id IS NOT NULL")
@dlt.expect_or_drop("valid_violation_code", "code IS NOT NULL")
def bronze_violations_raw():
    return spark.read.table(VIOLATIONS_TABLE)


# ============================================================
# SILVER LAYER — Cleaned, enriched, standardized
# ============================================================

# COMMAND ----------


@dlt.table(
    name="silver_menu_items",
    comment="Cleaned menu items with price tiers, calorie categories, macronutrient ratios, and allergen counts",
    table_properties={"quality": "silver"},
)
@dlt.expect("reasonable_calories", "calories BETWEEN 50 AND 3000")
@dlt.expect("non_negative_macros", "protein_g >= 0 AND fat_g >= 0 AND carbs_g >= 0")
def silver_menu_items():
    return (
        dlt.read("bronze_menu_raw")
        .select(
            "brand_name", "cuisine", "pdf_filename",
            "item_name", "description", "category", "price",
            "calories", "protein_g", "fat_g", "carbs_g", "allergens",
        )
        .withColumn("allergen_count", array_size("allergens"))
        .withColumn("is_allergen_free", array_size("allergens") == 0)
        .withColumn(
            "price_tier",
            when(col("price") < 10, "budget")
            .when(col("price") < 18, "standard")
            .otherwise("premium"),
        )
        .withColumn(
            "calorie_category",
            when(col("calories") < 300, "light")
            .when(col("calories") < 600, "moderate")
            .otherwise("hearty"),
        )
        .withColumn(
            "protein_pct",
            spark_round((col("protein_g") * 4 / col("calories")) * 100, 1),
        )
        .withColumn("is_high_protein", (col("protein_g") * 4 / col("calories")) > 0.25)
        .withColumn("is_low_calorie", col("calories") < 400)
    )


# COMMAND ----------


@dlt.table(
    name="silver_inspections",
    comment="Cleaned inspections with pass/fail status, score bands, and composite severity index",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_grade", "grade IN ('A', 'B', 'C', 'F')")
def silver_inspections():
    return (
        dlt.read("bronze_inspections_raw")
        .withColumn("passed", col("score") >= 70)
        .withColumn(
            "score_band",
            when(col("score") >= 90, "excellent")
            .when(col("score") >= 80, "good")
            .when(col("score") >= 70, "acceptable")
            .otherwise("failing"),
        )
        .withColumn("has_critical", col("critical_count") > 0)
        .withColumn(
            "severity_index",
            col("critical_count") * 3 + col("major_count") * 2 + col("minor_count"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="silver_violations",
    comment="Enriched violations with severity flags, urgency score, and immediate-action indicators",
    table_properties={"quality": "silver"},
)
def silver_violations():
    return (
        dlt.read("bronze_violations_raw")
        .withColumn("is_critical", col("severity") == "critical")
        .withColumn("is_major", col("severity") == "major")
        .withColumn("is_minor", col("severity") == "minor")
        .withColumn(
            "urgency_score",
            when(col("severity") == "critical", 10)
            .when(col("severity") == "major", 5)
            .otherwise(2),
        )
        .withColumn("needs_immediate_action", col("deadline_days") <= 7)
    )


# ============================================================
# GOLD LAYER — Business-ready views and aggregates
# ============================================================

# COMMAND ----------

TRACKED_ALLERGENS = [
    "wheat", "milk", "egg", "soy", "peanut",
    "tree_nut", "fish", "shellfish", "sesame",
]


@dlt.table(
    name="menu_items",
    comment="Menu item catalog with brand, category, pricing tier, and allergen summary",
    table_properties={"quality": "gold"},
)
def menu_items():
    return (
        dlt.read("silver_menu_items")
        .select(
            "brand_name", "cuisine", "item_name", "description", "category",
            "price", "price_tier", "allergen_count", "is_allergen_free",
        )
    )


# COMMAND ----------


@dlt.table(
    name="nutritional_info",
    comment="Per-item nutritional breakdown with calorie category, protein ratio, and health flags",
    table_properties={"quality": "gold"},
)
def nutritional_info():
    return (
        dlt.read("silver_menu_items")
        .select(
            "brand_name", "item_name", "category",
            "calories", "protein_g", "fat_g", "carbs_g",
            "calorie_category", "protein_pct",
            "is_high_protein", "is_low_calorie",
        )
    )


# COMMAND ----------


@dlt.table(
    name="allergens",
    comment="Per-item allergen flags for all tracked allergen types with total count",
    table_properties={"quality": "gold"},
)
def allergens():
    df = dlt.read("silver_menu_items").select(
        "brand_name", "item_name", "category", "allergens", "allergen_count",
    )
    for a in TRACKED_ALLERGENS:
        df = df.withColumn(f"contains_{a}", array_contains(col("allergens"), a))
    return df.drop("allergens")


# COMMAND ----------


@dlt.table(
    name="brand_nutrition_summary",
    comment="Aggregate nutritional, pricing, and dietary statistics per brand",
    table_properties={"quality": "gold"},
)
def brand_nutrition_summary():
    return (
        dlt.read("silver_menu_items")
        .groupBy("brand_name", "cuisine")
        .agg(
            count("*").alias("total_items"),
            spark_round(spark_avg("calories"), 0).alias("avg_calories"),
            spark_min("calories").alias("min_calories"),
            spark_max("calories").alias("max_calories"),
            spark_round(spark_avg("protein_g"), 1).alias("avg_protein_g"),
            spark_round(spark_avg("fat_g"), 1).alias("avg_fat_g"),
            spark_round(spark_avg("carbs_g"), 1).alias("avg_carbs_g"),
            spark_round(spark_avg("price"), 2).alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            spark_sum(when(col("is_high_protein"), 1).otherwise(0)).alias("high_protein_items"),
            spark_sum(when(col("is_low_calorie"), 1).otherwise(0)).alias("low_calorie_items"),
            spark_sum(when(col("is_allergen_free"), 1).otherwise(0)).alias("allergen_free_items"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="inspection_details",
    comment="Food safety inspection records with scores, pass/fail, score band, and severity index",
    table_properties={"quality": "gold"},
)
def inspection_details():
    return (
        dlt.read("silver_inspections")
        .select(
            "inspection_id", "location_id", "location_name", "address",
            "inspection_date", "inspector_name",
            "score", "grade", "passed", "score_band",
            "violation_count", "critical_count", "major_count", "minor_count",
            "has_critical", "severity_index",
            "follow_up_status",
        )
    )


# COMMAND ----------


@dlt.table(
    name="violation_analysis",
    comment="Individual food safety violations with severity flags, urgency score, and action indicators",
    table_properties={"quality": "gold"},
)
def violation_analysis():
    return (
        dlt.read("silver_violations")
        .select(
            "inspection_id", "location_id", "location_name", "inspection_date",
            "code", "severity", "category", "description",
            "corrective_action", "deadline_days",
            "is_critical", "is_major", "is_minor",
            "urgency_score", "needs_immediate_action",
        )
    )


# COMMAND ----------


@dlt.table(
    name="location_compliance_summary",
    comment="Aggregate food safety compliance metrics per location with pass rate and severity index",
    table_properties={"quality": "gold"},
)
def location_compliance_summary():
    return (
        dlt.read("silver_inspections")
        .groupBy("location_id", "location_name")
        .agg(
            count("*").alias("total_inspections"),
            spark_round(spark_avg("score"), 1).alias("avg_score"),
            spark_min("score").alias("min_score"),
            spark_max("score").alias("max_score"),
            spark_sum("violation_count").alias("total_violations"),
            spark_sum("critical_count").alias("total_critical"),
            spark_sum("major_count").alias("total_major"),
            spark_sum("minor_count").alias("total_minor"),
            spark_sum(when(col("passed"), 1).otherwise(0)).alias("passed_inspections"),
            spark_round(
                spark_sum(when(col("passed"), 1).otherwise(0)) / count("*") * 100, 1,
            ).alias("pass_rate_pct"),
            spark_round(spark_avg("severity_index"), 1).alias("avg_severity_index"),
        )
    )
