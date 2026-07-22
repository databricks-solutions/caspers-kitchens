"""Enable Unity Catalog-managed Delta commits on tables created by Casper's stages.

See: https://docs.databricks.com/aws/en/tables/features/catalog-commits

Used by stages/catalog_commits.ipynb (devconnect target only).
Catalog commits apply to managed/external Delta tables only — not views,
materialized views, streaming tables, or DLT materialization backing tables.

Important: enabling catalogManaged on DLT silver/gold materialization tables
breaks subsequent pipeline updates with
DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED (DLT still path-loads
those tables). Never enable it on silver_*/gold_*/__materialization* names.
"""

CATALOG_COMMITS_PROPERTY = "delta.feature.catalogManaged"
CATALOG_COMMITS_VALUE = "supported"

# UC information_schema.table_type values that support ALTER TABLE … SET TBLPROPERTIES.
_ALTERABLE_TYPES = frozenset({"MANAGED", "EXTERNAL"})

# DLT outputs / internals — path-based access breaks once catalogManaged is on.
_SKIP_NAME_PREFIXES = (
    "__",
    "silver_",
    "gold_",
)


def _skip_name(name: str) -> bool:
    n = (name or "").lower()
    return any(n.startswith(p) for p in _SKIP_NAME_PREFIXES)


def _already_enabled(spark, table: str) -> bool:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
    except Exception:
        return False
    features = detail.asDict().get("tableFeatures") or []
    return "catalogManaged" in features


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
    """Enable catalog commits on managed/external Delta tables in a UC schema.

    Skips DLT silver/gold outputs and ``__materialization*`` backing tables —
    those must stay path-accessible to Lakeflow pipelines.
    """
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
        if _skip_name(name):
            print(f"  skip {catalog}.{schema}.{name} (dlt/pipeline output)")
            continue
        if ttype not in _ALTERABLE_TYPES:
            print(f"  skip {catalog}.{schema}.{name} ({ttype.lower()})")
            continue
        tables.append(f"`{catalog}`.`{schema}`.`{name}`")
    enable_catalog_commits(spark, *tables)
