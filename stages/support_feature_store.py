# Databricks notebook source
# MAGIC %pip install -U -qqqq typing_extensions

# COMMAND ----------

import json
import re
from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

try:
    from databricks.feature_engineering import FeatureEngineeringClient  # type: ignore
except Exception:
    try:
        from databricks.feature_store import FeatureStoreClient as FeatureEngineeringClient  # type: ignore
    except Exception:
        FeatureEngineeringClient = None

CATALOG = dbutils.widgets.get("CATALOG")
SUPPORT_ONLINE_STORE_NAME = dbutils.widgets.get("SUPPORT_ONLINE_STORE_NAME")
SUPPORT_ONLINE_STORE_CAPACITY = dbutils.widgets.get("SUPPORT_ONLINE_STORE_CAPACITY")
SUPPORT_ONLINE_TABLE_NAME = dbutils.widgets.get("SUPPORT_ONLINE_TABLE_NAME")
SUPPORT_RISK_ENDPOINT_NAME = dbutils.widgets.get("SUPPORT_RISK_ENDPOINT_NAME")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.support")

base = spark.sql(
    f"""
    WITH latest AS (
      SELECT support_request_id, user_id, order_id, request_text, ts, agent_response,
             ROW_NUMBER() OVER (PARTITION BY support_request_id ORDER BY ts DESC) AS rn
      FROM {CATALOG}.support.support_agent_reports
    )
    SELECT support_request_id, user_id, order_id, request_text, ts, agent_response
    FROM latest
    WHERE rn = 1
    """
)

history = spark.sql(
    f"""
    SELECT
      support_request_id,
      user_id,
      ts,
      COUNT(*) OVER (
        PARTITION BY user_id
        ORDER BY ts
        RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
      ) AS repeat_complaints_30d
    FROM {CATALOG}.support.raw_support_requests
    """
)

latest_repeat = (
    history.withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("support_request_id").orderBy(F.col("ts").desc())
        ),
    )
    .filter(F.col("rn") == 1)
    .select("support_request_id", "repeat_complaints_30d")
)

features = (
    base.join(latest_repeat, on="support_request_id", how="left")
    .fillna({"repeat_complaints_30d": 1})
    .withColumn(
        "policy_limit_usd",
        F.when(F.col("repeat_complaints_30d") >= 8, F.lit(10.0))
        .when(F.col("repeat_complaints_30d") >= 4, F.lit(20.0))
        .otherwise(F.lit(35.0)),
    )
)

def _extract_risk_score(text: str) -> Optional[float]:
    try:
        obj = json.loads(text)
        score = float(obj.get("risk_score"))
        return max(0.0, min(1.0, score))
    except Exception:
        match = re.search(r"([01](?:\\.\\d+)?)", text)
        if not match:
            return None
        try:
            score = float(match.group(1))
            return max(0.0, min(1.0, score))
        except Exception:
            return None


def score_risk(request_text: str, repeat_complaints_30d: int, policy_limit_usd: float) -> float:
    # Reliability fallback: score from deterministic signals until endpoint scoring is stable.
    text_boost = 0.1 if ("fraud" in (request_text or "").lower() or "chargeback" in (request_text or "").lower()) else 0.0
    repeat_boost = 0.08 * float(repeat_complaints_30d)
    limit_penalty = 0.05 if float(policy_limit_usd) <= 10.0 else 0.0
    return max(0.0, min(1.0, 0.15 + text_boost + repeat_boost + limit_penalty))


risk_udf = F.udf(score_risk, DoubleType())

features = features.withColumn(
    "risk_score",
    risk_udf(
        F.coalesce(F.col("request_text"), F.lit("")),
        F.coalesce(F.col("repeat_complaints_30d"), F.lit(1)),
        F.coalesce(F.col("policy_limit_usd"), F.lit(35.0)),
    ),
)

materialized = features.select(
    "support_request_id",
    "user_id",
    "order_id",
    "ts",
    "repeat_complaints_30d",
    "policy_limit_usd",
    "risk_score",
)

materialized.write.mode("overwrite").saveAsTable(f"{CATALOG}.support.support_request_features")

spark.sql(
    f"""
    ALTER TABLE {CATALOG}.support.support_request_features
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """
)

try:
    spark.sql(
        f"""
        ALTER TABLE {CATALOG}.support.support_request_features
        ALTER COLUMN support_request_id SET NOT NULL
        """
    )
except Exception:
    pass

try:
    spark.sql(
        f"""
        ALTER TABLE {CATALOG}.support.support_request_features
        ADD CONSTRAINT support_request_features_pk PRIMARY KEY (support_request_id)
        """
    )
except Exception:
    pass

if FeatureEngineeringClient is None:
    print(
        "Feature engineering client package is unavailable on this runtime; "
        "offline feature table was created but online publish was skipped."
    )
else:
    fe = FeatureEngineeringClient()
    try:
        fe.get_online_store(name=SUPPORT_ONLINE_STORE_NAME)
    except Exception:
        fe.create_online_store(
            name=SUPPORT_ONLINE_STORE_NAME,
            capacity=SUPPORT_ONLINE_STORE_CAPACITY,
        )

    online_store = fe.get_online_store(name=SUPPORT_ONLINE_STORE_NAME)
    fe.publish_table(
        online_store=online_store,
        source_table_name=f"{CATALOG}.support.support_request_features",
        online_table_name=SUPPORT_ONLINE_TABLE_NAME,
        publish_mode="TRIGGERED",
    )

print("Support feature store materialization complete")
