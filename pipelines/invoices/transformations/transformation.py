# Databricks notebook source

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, lit, when, array_contains, array_size,
    sum as spark_sum, avg as spark_avg, min as spark_min,
    max as spark_max, count, countDistinct, round as spark_round,
    current_date, datediff, greatest,
)

CATALOG = spark.conf.get("PROCUREMENT_CATALOG")

INVOICES_TABLE = f"{CATALOG}.procurement.invoices_raw"
LINE_ITEMS_TABLE = f"{CATALOG}.procurement.invoice_line_items"
SUPPLIERS_TABLE = f"{CATALOG}.procurement.suppliers"

# ============================================================
# BRONZE LAYER — Raw ingestion with data quality expectations
# ============================================================

# COMMAND ----------


@dlt.table(
    name="bronze_invoices_raw",
    comment="Raw invoice records ingested from procurement.invoices source table",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("valid_invoice_id", "invoice_id IS NOT NULL")
@dlt.expect_or_drop("valid_supplier_id", "supplier_id IS NOT NULL")
@dlt.expect("valid_subtotal", "subtotal >= 0")
@dlt.expect("valid_total_due", "total_due >= 0")
@dlt.expect("valid_status", "status IN ('paid', 'outstanding', 'past_due', 'disputed')")
def bronze_invoices_raw():
    return spark.read.table(INVOICES_TABLE)


# COMMAND ----------


@dlt.table(
    name="bronze_line_items_raw",
    comment="Raw invoice line items ingested from procurement.invoice_line_items source table",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("valid_line_invoice_id", "invoice_id IS NOT NULL")
@dlt.expect_or_drop("valid_description", "description IS NOT NULL")
@dlt.expect("positive_qty", "qty > 0")
@dlt.expect("positive_unit_price", "unit_price > 0")
@dlt.expect("positive_contract_price", "contract_price > 0")
def bronze_line_items_raw():
    return spark.read.table(LINE_ITEMS_TABLE)


# COMMAND ----------


@dlt.table(
    name="bronze_suppliers_raw",
    comment="Raw supplier records ingested from procurement.suppliers source table",
    table_properties={"quality": "bronze"},
)
@dlt.expect_or_drop("valid_supplier_id", "supplier_id IS NOT NULL")
@dlt.expect_or_drop("valid_supplier_name", "name IS NOT NULL")
def bronze_suppliers_raw():
    return spark.read.table(SUPPLIERS_TABLE)


# ============================================================
# SILVER LAYER — Cleaned, enriched, standardized
# ============================================================

# COMMAND ----------


@dlt.table(
    name="silver_invoices",
    comment="Enriched invoices with status flags, financial exception indicators, and recoverable amounts",
    table_properties={"quality": "silver"},
)
def silver_invoices():
    return (
        dlt.read("bronze_invoices_raw")
        # Status flags
        .withColumn("is_paid", col("status") == "paid")
        .withColumn("is_outstanding", col("status") == "outstanding")
        .withColumn("is_overdue", col("status") == "past_due")
        .withColumn("is_disputed", col("status") == "disputed")
        .withColumn("is_open", col("status").isin("outstanding", "past_due", "disputed"))
        # Exception flags derived from flags array
        .withColumn("has_missed_discount", array_contains(col("flags"), "missed_discount"))
        .withColumn("has_missing_volume_discount", array_contains(col("flags"), "missing_volume_discount"))
        .withColumn("has_price_discrepancy", array_contains(col("flags"), "price_discrepancy"))
        .withColumn("has_late_fee", col("late_fee") > 0)
        .withColumn("has_sla_penalty", col("sla_penalty") > 0)
        .withColumn("attention_required", array_size(col("flags")) > 0)
        # Total money at risk / recoverable
        .withColumn(
            "recoverable_amount",
            spark_round(
                col("missed_discount") + col("volume_discount_owed") + col("price_discrepancy"),
                2,
            ),
        )
        # Days since invoice date (for aging analysis)
        .withColumn("days_since_invoice", datediff(current_date(), col("invoice_date")))
        .withColumn(
            "days_overdue",
            when(col("is_open"), datediff(current_date(), col("due_date"))).otherwise(lit(0)),
        )
    )


# COMMAND ----------


@dlt.table(
    name="silver_line_items",
    comment="Invoice line items with computed totals, price variance vs. contract, and discrepancy flags",
    table_properties={"quality": "silver"},
)
def silver_line_items():
    return (
        dlt.read("bronze_line_items_raw")
        .withColumn("line_total", spark_round(col("qty") * col("unit_price"), 2))
        .withColumn("contract_line_total", spark_round(col("qty") * col("contract_price"), 2))
        .withColumn(
            "price_variance",
            spark_round(col("line_total") - col("contract_line_total"), 2),
        )
        .withColumn("is_discrepant", col("unit_price") != col("contract_price"))
        .withColumn(
            "discrepancy_pct",
            when(
                col("contract_price") > 0,
                spark_round((col("unit_price") - col("contract_price")) / col("contract_price") * 100, 2),
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "variance_direction",
            when(col("price_variance") > 0, "overbilled")
            .when(col("price_variance") < 0, "underbilled")
            .otherwise("correct"),
        )
    )


# ============================================================
# GOLD LAYER — Business-ready tables for Genie space
# ============================================================

# COMMAND ----------


@dlt.table(
    name="invoices",
    comment="Invoice summary with supplier info, status, all financial amounts, and exception flags",
    table_properties={"quality": "gold"},
)
def invoices():
    inv = dlt.read("silver_invoices")
    sup = dlt.read("bronze_suppliers_raw").select(
        "supplier_id",
        col("name").alias("supplier_name"),
        col("category").alias("supplier_category"),
        col("payment_terms").alias("supplier_payment_terms"),
    )
    return inv.join(sup, on="supplier_id", how="left").select(
        "invoice_id", "supplier_id", "supplier_name", "supplier_category",
        "supplier_payment_terms", "purchase_order",
        "invoice_date", "due_date", "early_pay_deadline", "payment_date", "delivery_date",
        "status", "is_paid", "is_outstanding", "is_overdue", "is_disputed", "is_open",
        "attention_required",
        "has_missed_discount", "has_missing_volume_discount", "has_price_discrepancy",
        "has_late_fee", "has_sla_penalty",
        "subtotal", "discount_amount", "sla_penalty", "late_fee", "total_due",
        "missed_discount", "volume_discount_owed", "price_discrepancy", "recoverable_amount",
        "days_since_invoice", "days_overdue",
        "pdf_filename",
    )


# COMMAND ----------


@dlt.table(
    name="line_items",
    comment="Invoice line items with price variance analysis — the primary table for discrepancy investigation",
    table_properties={"quality": "gold"},
)
def line_items():
    items = dlt.read("silver_line_items")
    inv = dlt.read("silver_invoices").select(
        "invoice_id", "invoice_date", "status",
        "has_price_discrepancy",
    )
    sup = dlt.read("bronze_suppliers_raw").select(
        "supplier_id", col("name").alias("supplier_name"), col("category").alias("supplier_category"),
    )
    return (
        items
        .join(inv, on="invoice_id", how="left")
        .join(sup, on="supplier_id", how="left")
        .select(
            "invoice_id", "supplier_id", "supplier_name", "supplier_category", "invoice_date", "status",
            "description", "qty", "unit", "unit_price", "contract_price",
            "line_total", "contract_line_total",
            "price_variance", "discrepancy_pct", "variance_direction", "is_discrepant",
        )
    )


# COMMAND ----------


@dlt.table(
    name="spend_by_supplier",
    comment="Aggregate spend and invoice counts per supplier — total billed, total paid, and open balance",
    table_properties={"quality": "gold"},
)
def spend_by_supplier():
    inv = dlt.read("silver_invoices")
    sup = dlt.read("bronze_suppliers_raw").select(
        "supplier_id", col("name").alias("supplier_name"), col("category").alias("supplier_category"),
    )
    return (
        inv.join(sup, on="supplier_id", how="left")
        .groupBy("supplier_id", "supplier_name", "supplier_category")
        .agg(
            count("*").alias("total_invoices"),
            spark_sum(when(col("is_paid"), 1).otherwise(0)).alias("paid_invoices"),
            spark_sum(when(col("is_open"), 1).otherwise(0)).alias("open_invoices"),
            spark_sum(when(col("is_disputed"), 1).otherwise(0)).alias("disputed_invoices"),
            spark_round(spark_sum("subtotal"), 2).alias("total_billed"),
            spark_round(spark_sum(when(col("is_paid"), col("total_due")).otherwise(0)), 2).alias("total_paid"),
            spark_round(spark_sum(when(col("is_open"), col("total_due")).otherwise(0)), 2).alias("open_balance"),
            spark_round(spark_sum("recoverable_amount"), 2).alias("total_recoverable"),
            spark_round(spark_avg("total_due"), 2).alias("avg_invoice_value"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="spend_by_category",
    comment="Aggregate spend per supplier category (produce, meat, seafood, etc.)",
    table_properties={"quality": "gold"},
)
def spend_by_category():
    inv = dlt.read("silver_invoices")
    sup = dlt.read("bronze_suppliers_raw").select(
        "supplier_id", col("category").alias("supplier_category"),
    )
    return (
        inv.join(sup, on="supplier_id", how="left")
        .groupBy("supplier_category")
        .agg(
            count("*").alias("total_invoices"),
            countDistinct("supplier_id").alias("supplier_count"),
            spark_round(spark_sum("subtotal"), 2).alias("total_billed"),
            spark_round(spark_sum(when(col("is_open"), col("total_due")).otherwise(0)), 2).alias("open_balance"),
            spark_round(spark_avg("total_due"), 2).alias("avg_invoice_value"),
            spark_round(spark_min("total_due"), 2).alias("min_invoice_value"),
            spark_round(spark_max("total_due"), 2).alias("max_invoice_value"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="payment_aging",
    comment="Payment aging analysis — invoices bucketed by how long they have been outstanding",
    table_properties={"quality": "gold"},
)
def payment_aging():
    inv = dlt.read("silver_invoices")
    sup = dlt.read("bronze_suppliers_raw").select(
        "supplier_id", col("name").alias("supplier_name"),
    )
    return (
        inv.filter(col("is_open"))
        .join(sup, on="supplier_id", how="left")
        .withColumn(
            "aging_bucket",
            when(col("days_overdue") <= 0, "current")
            .when(col("days_overdue") <= 30, "1-30 days overdue")
            .when(col("days_overdue") <= 60, "31-60 days overdue")
            .otherwise("60+ days overdue"),
        )
        .select(
            "invoice_id", "supplier_id", "supplier_name",
            "invoice_date", "due_date", "days_overdue", "aging_bucket",
            "status", "total_due", "late_fee", "attention_required",
        )
    )


# COMMAND ----------


@dlt.table(
    name="invoice_exceptions",
    comment="All invoices with at least one financial exception — the primary AP review queue",
    table_properties={"quality": "gold"},
)
def invoice_exceptions():
    inv = dlt.read("silver_invoices")
    sup = dlt.read("bronze_suppliers_raw").select(
        "supplier_id", col("name").alias("supplier_name"),
    )
    return (
        inv.filter(col("attention_required"))
        .join(sup, on="supplier_id", how="left")
        .select(
            "invoice_id", "supplier_id", "supplier_name",
            "invoice_date", "due_date", "status",
            "has_price_discrepancy", "has_missed_discount",
            "has_missing_volume_discount", "has_late_fee", "has_sla_penalty",
            "subtotal", "total_due",
            "price_discrepancy", "missed_discount", "volume_discount_owed",
            "late_fee", "sla_penalty", "recoverable_amount",
            "days_overdue",
        )
    )


# COMMAND ----------


@dlt.table(
    name="supplier_scorecard",
    comment="Per-supplier performance metrics: on-time payment rate, dispute rate, SLA violations, discount capture",
    table_properties={"quality": "gold"},
)
def supplier_scorecard():
    inv = dlt.read("silver_invoices")
    sup = dlt.read("bronze_suppliers_raw").select(
        "supplier_id", col("name").alias("supplier_name"),
        col("category").alias("supplier_category"), "payment_terms",
    )
    return (
        inv.join(sup, on="supplier_id", how="left")
        .groupBy("supplier_id", "supplier_name", "supplier_category", "payment_terms")
        .agg(
            count("*").alias("total_invoices"),
            spark_sum(when(col("is_disputed"), 1).otherwise(0)).alias("disputed_invoices"),
            spark_sum(when(col("is_overdue"), 1).otherwise(0)).alias("overdue_invoices"),
            spark_sum(when(col("has_sla_penalty"), 1).otherwise(0)).alias("sla_violations"),
            spark_sum(when(col("has_price_discrepancy"), 1).otherwise(0)).alias("price_discrepancies"),
            spark_sum(when(col("has_missed_discount"), 1).otherwise(0)).alias("missed_discounts"),
            spark_sum(when(col("has_missing_volume_discount"), 1).otherwise(0)).alias("missing_vol_discounts"),
            spark_round(spark_sum("late_fee"), 2).alias("total_late_fees_accrued"),
            spark_round(spark_sum("sla_penalty"), 2).alias("total_sla_penalties"),
            spark_round(spark_sum("price_discrepancy"), 2).alias("total_overbilled"),
            spark_round(spark_sum("recoverable_amount"), 2).alias("total_recoverable"),
            spark_round(spark_sum("discount_amount"), 2).alias("discounts_captured"),
            spark_round(spark_sum("missed_discount") + spark_sum("volume_discount_owed"), 2).alias("discounts_missed"),
        )
    )


# COMMAND ----------


@dlt.table(
    name="discount_analysis",
    comment="Discount capture rate analysis — captured vs. forfeited early payment and volume discounts per supplier",
    table_properties={"quality": "gold"},
)
def discount_analysis():
    inv = dlt.read("silver_invoices")
    sup = dlt.read("bronze_suppliers_raw").select(
        "supplier_id", col("name").alias("supplier_name"),
        col("payment_terms").alias("supplier_payment_terms"),
    )
    return (
        inv.join(sup, on="supplier_id", how="left")
        .groupBy("supplier_id", "supplier_name", "supplier_payment_terms")
        .agg(
            count("*").alias("total_invoices"),
            spark_sum(when(col("discount_pct") > 0, 1).otherwise(0)).alias("discount_eligible_invoices"),
            spark_sum(when(col("discount_applied"), 1).otherwise(0)).alias("discounts_captured_count"),
            spark_sum(when(col("has_missed_discount"), 1).otherwise(0)).alias("early_pay_discounts_missed_count"),
            spark_sum(when(col("has_missing_volume_discount"), 1).otherwise(0)).alias("volume_discounts_missed_count"),
            spark_round(spark_sum("discount_amount"), 2).alias("total_discount_captured"),
            spark_round(spark_sum("missed_discount"), 2).alias("total_early_pay_discount_missed"),
            spark_round(spark_sum("volume_discount_owed"), 2).alias("total_volume_discount_missed"),
            spark_round(
                spark_sum("discount_amount") + spark_sum("missed_discount") + spark_sum("volume_discount_owed"),
                2,
            ).alias("total_discount_opportunity"),
            spark_round(
                spark_sum("discount_amount") /
                greatest(
                    spark_sum("discount_amount") + spark_sum("missed_discount") + spark_sum("volume_discount_owed"),
                    lit(0.01),
                ) * 100,
                1,
            ).alias("discount_capture_rate_pct"),
        )
    )
