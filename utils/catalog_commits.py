"""Enable Unity Catalog-managed Delta commits on tables created by Casper's stages.

See: https://docs.databricks.com/aws/en/tables/features/catalog-commits

Used by stages/catalog_commits.ipynb (devconnect target only).
Catalog commits apply to managed/external Delta tables only — not views,
materialized views, or streaming tables (DLT bronze/gold outputs are often views).
"""

CATALOG_COMMITS_PROPERTY = "delta.feature.catalogManaged"
CATALOG_COMMITS_VALUE = "supported"

# UC information_schema.table_type values that support ALTER TABLE … SET TBLPROPERTIES.
_ALTERABLE_TYPES = frozenset({"MANAGED", "EXTERNAL"})


def _already_enabled(spark, table: str) -> bool:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        features = detail.asDict().get("tableFeatures") or []
        return "catalogManaged" in features
    except Exception:
        return False


def enable_catalog_commits(spark, *tables: str) -> None:
    """ALTER TABLE … SET TBLPROPERTIES for catalog commits (idempotent per table)."""
    for table in tables:
        if _already_enabled(spark, table):
            print(f"  {table}: catalog commits already enabled")
            continue
        try:
            spark.sql(
                f"ALTER TABLE {table} SET TBLPROPERTIES "
                f"('{CATALOG_COMMITS_PROPERTY}' = '{CATALOG_COMMITS_VALUE}')"
            )
            print(f"  ✅ {table}: catalog commits enabled")
        except Exception as exc:
            print(f"  ⚠️  {table}: could not enable catalog commits — {exc}")


def enable_catalog_commits_in_schema(spark, catalog: str, schema: str) -> None:
    """Enable catalog commits on managed/external Delta tables in a UC schema."""
    rows = spark.sql(
        f"""
        SELECT table_name, table_type
        FROM `{catalog}`.information_schema.tables
        WHERE table_schema = '{schema}'
        """
    ).collect()
    tables = []
    for row in rows:
        name = row.table_name
        ttype = str(row.table_type or "").upper()
        if ttype not in _ALTERABLE_TYPES:
            print(f"  skip {catalog}.{schema}.{name} ({ttype.lower()})")
            continue
        tables.append(f"`{catalog}`.`{schema}`.`{name}`")
    enable_catalog_commits(spark, *tables)
