---
name: generate-data
description: Generate coherent business data — seed dimensions as managed Delta tables with PK/FK, streaming events, and unstructured documents. Works standalone or as part of build-business.
user_invocable: true
---

# Generate Data

You help users generate coherent business data for Databricks demos. You can generate three types of data:

1. **Seed data** — static dimensional tables as managed Delta tables with primary/foreign key constraints
2. **Event stream** — a canonical dataset + replay engine for streaming simulation
3. **Unstructured documents** — PDFs with structured metadata for RAG/document intelligence

Read `assets/data-generation.md` for detailed patterns and reference examples.

## How You Work

### Standalone Mode

When invoked directly (`/generate-data`), you:
1. Ask what business domain and what kind of data they need (seed, events, documents, or all)
2. Work with the user to define entities, relationships, and event schemas
3. Set up the catalog (see Catalog Setup below)
4. Generate the data artifacts
5. Deploy, run jobs, provide test queries
6. STOP — wait for user to confirm data looks good before anything else

### As Part of build-business

When called by the `build-business` skill, you receive a Blueprint with entities and events already defined. Generate the data files that conform to the Blueprint. Don't re-ask questions — trust the Blueprint and generate.

## Catalog & Schema Convention

The catalog IS the business. All data lives in a `data` schema:

```
{business_catalog}
  └── data
      ├── seed_beers          (managed Delta table with PK)
      ├── seed_ingredients     (managed Delta table with PK + FK → ...)
      ├── ...
      └── [volumes]
          ├── events/          (streaming JSON from replay)
          ├── canonical/       (pre-generated parquet)
          └── misc/            (checkpoint files)
```

### Catalog Setup

Before generating code:
1. List existing catalogs: `databricks catalogs list --profile {profile}`
2. Suggest a catalog name based on the business (e.g., `cascade_creek_brewing`)
3. Create catalog and schema via SQL statements API (not CLI subcommands):
   ```bash
   databricks api post /api/2.0/sql/statements --profile {profile} --json '{
     "warehouse_id": "{warehouse_id}",
     "statement": "CREATE CATALOG IF NOT EXISTS {name}"
   }'
   ```
4. Confirm with the user

Never default to `main`. Always use the business-specific catalog. Volumes are declared in `databricks.yml`, not created via SQL.

## What You Generate

### Seed Data (Managed Delta Tables with PK/FK)
- A `data/seed_generator.py` that creates managed Delta tables in `{catalog}.data`
- Uses `spark.createDataFrame(df).write.saveAsTable("{catalog}.data.{table}")`
- After creating all tables, adds constraints in order (PKs first, then FKs):
  - **Primary keys**: `ALTER TABLE {catalog}.data.{table} ADD CONSTRAINT pk_{table} PRIMARY KEY ({id_col})`
  - **Foreign keys**: `ALTER TABLE {catalog}.data.{table} ADD CONSTRAINT fk_{table}_{ref} FOREIGN KEY ({fk_col}) REFERENCES {catalog}.data.{ref_table}({pk_col})`
- PK/FK constraints are **informational** (Unity Catalog metadata, not enforced at write time) but enable lineage, BI tools, and query optimization
- **PK columns must be NOT NULL.** Use `ALTER TABLE ... ALTER COLUMN {pk} SET NOT NULL` before adding the PK constraint. Do NOT rely on DataFrame schema `nullable=False` — `createDataFrame()` often overrides it.
- FK must reference an existing PK — so create all tables and add all PKs before adding any FKs

The exact pattern for constraints:
```python
# After all tables are created, add constraints in order:
for table, pk in [("seed_products", "id"), ("seed_suppliers", "id")]:
    spark.sql(f"ALTER TABLE {catalog}.data.{table} ALTER COLUMN {pk} SET NOT NULL")
    spark.sql(f"ALTER TABLE {catalog}.data.{table} ADD CONSTRAINT pk_{table} PRIMARY KEY ({pk})")

# Then FKs (all PKs must exist first):
spark.sql(f"""ALTER TABLE {catalog}.data.seed_products
    ADD CONSTRAINT fk_products_supplier FOREIGN KEY (supplier_id)
    REFERENCES {catalog}.data.seed_suppliers(id)""")
```
- The LLM generates realistic content using world knowledge (real names, codes, pricing)
- Keep it small (tens to hundreds of rows) — the event generator creates volume
- Every FK must resolve to a real parent row

### Event Stream
Two files:
1. **`data/canonical_generator.py`** — walks the state machine to produce N days of events as parquet in a UC volume
2. **`data/replay.py`** — replays canonical events at configurable speed into a UC volume as streaming JSON. **Self-contained** — inlines the replay engine logic, no external imports beyond PySpark/pandas.

The replay engine in `assets/replay_engine.py` is **reference material** for how the replay logic works. Generated code should inline it, not import from it.

### Timeline Configuration

The canonical generator and replay work together to create **instant historical data + live streaming**:

- **`dataset_days`** (Blueprint → `timeline.dataset_days`): How many days to pre-generate. Default 40.
- **`start_day`** (Blueprint → `timeline.start_day`): Where the replay cursor starts. Days before this = instant backfill on first run. Default `dataset_days - 10`.
- **`speed_multiplier`** (Blueprint → `timeline.speed_multiplier`): How fast new events flow after backfill. 60 = 1 real minute covers 1 sim hour.

The canonical generator accepts `--days=N`. The replay accepts `--start-day=N` and `--speed=X`.

Read `assets/data-generation.md` section "Timeline Strategy" for the full explanation. **Always configure this in the Blueprint** — if the user doesn't specify, suggest `dataset_days: 40, start_day: 30, speed_multiplier: 60.0` as sensible defaults.

### Unstructured Documents
- Files in `data/documents/` organized by type
- Metadata JSON alongside each document set
- LLM-generated markdown → PDF for demos, or templates filled with seed data

### Infrastructure
- Volume declarations in `databricks.yml` for events, canonical, and misc
- Job declarations for seed loader, canonical generator, and event replay (with schedule)
- Schema is created via SQL during catalog setup — do NOT declare it in `databricks.yml`

## Event Schema

The event envelope is minimal:

```
event_type: string    # what happened
ts: timestamp         # when it happened
body: string (JSON)   # everything else — fully domain-specific
```

There is no prescribed `location_id`, `entity_id`, or `sequence`. Those are body fields if the business needs them. The Blueprint declares which body fields serve which roles.

## Databricks Runtime Rules

All generated code runs on Databricks serverless. You MUST follow these rules:

1. **Never use `__file__` or `Path(__file__)`** — serverless runs via `exec()` so `__file__` is undefined.
2. **Seed data as managed Delta tables** — write to `{catalog}.data.{table}`, not parquet in volumes. Volumes are for events/canonical/misc only.
3. **Accept config via `sys.argv`** — use `--catalog=X` pattern. Schema is always `data`.
4. **No `if __name__ == "__main__":` guard** — call `main()` directly at module level.
5. **Every job needs `environments` block** — and every task needs `environment_key: default`.
6. **Every volume path in code needs a volume resource in `databricks.yml`**.

## Sample Query Rules

When providing DBSQL queries for the user to test data:

1. **Every column must exist in the actual generated schema.** Check the Blueprint and the generator code.
2. **For JSON body fields, use `get_json_object(body, '$.field_name')`** with `CAST()` as needed. Example:
   ```sql
   SELECT CAST(get_json_object(body, '$.total_amount') AS DOUBLE) as total
   FROM json.`/Volumes/{catalog}/data/events/`
   WHERE event_type = 'closed';
   ```
3. **Never use `get_json_field`** — it does not exist in Spark SQL.
4. **Never reference columns like `entity_id` unless the generator actually produces that column.** The entity ID (batch_id, order_id, visit_id, etc.) is typically inside the body JSON, not a top-level column. Check what the replay transform actually outputs as top-level columns.
5. **Validate every query against the Blueprint before presenting it.** For each column reference, trace it to either a top-level output column or a body field in the Blueprint.

## Spatial Tracking

When the Blueprint includes tracking events with a `route` config, generate spatial tracking code.

Read `assets/data-generation.md` for the full spatial section and `assets/routing.py` for the reference implementation.

### What to generate

1. **In the canonical generator**: inline routing functions from `assets/routing.py` (don't import — inline the functions you need). The context factory should compute routes, and tracking body generators should use `route_position_at()`.

2. **In the replay transform**: tracking events already have `ping_lat`, `ping_lon`, etc. as compact parquet columns. The transform just assembles them into the body JSON (same as non-spatial events).

3. **For `generated_in_area` entities**: the canonical generator creates random locations (with optional road snap + reverse geocode) during context setup.

### Route mode selection

- Use `road` (OSRM) for ground vehicles: delivery, ride share, fleet, ambulance
- Use `air` (great-circle) for aircraft, drones, ships (roughly), satellites
- Multi-stop: just more waypoints in the list — both modes handle it

### Dependencies

Road routing: `osmnx`, `networkx` — install via `%pip install osmnx networkx` at the top of the canonical generator. The graph is loaded once, then all routes are local Dijkstra (milliseconds each).

Air routing: no dependencies (pure math).

Address resolution: `requests` for Nominatim geocoding. For road businesses, prefer `RoadGraph.random_node()` over Nominatim — it's faster and guaranteed routable.

## Coherence Rules

You must validate after generation:
- Every entity FK resolves to a real parent row
- Every event body `ref()` uses valid IDs from seed data
- Timestamps are monotonically increasing within each entity lifecycle
- Every `/Volumes/...` path in code has a matching volume in `databricks.yml`
- All PK/FK constraints in seed generator match the entity relationships in the Blueprint
- If documents exist: metadata matches file contents
- Entities with `location_mode: fixed` have lat/lon in seed data
- Entities with `location_mode: generated_in_area` have a valid center ref and radius
- Tracking event waypoint refs resolve to real coordinates
- Route mode matches the domain (road for ground, air for flight)

## What You Never Do

- Generate seed data that references nonexistent entities
- Assume fields exist in the Blueprint without checking
- Generate unrealistic data (negative prices, impossible timestamps, random strings for names)
- Hardcode ghost-kitchen-specific concepts into the universal event schema
- Use `__file__`, write seed data to volumes as parquet, or skip volume declarations
- Import from `.agents/` assets at runtime — inline what you need
- Default the catalog to `main`
- Use `get_json_field` in SQL queries
- Reference non-existent columns in sample queries
- Use OSRM API as primary routing — use `RoadGraph` (osmnx + Dijkstra) instead
- Compute routes per-entity instead of loading the graph once at startup
- Generate tracking events without a route when the Blueprint specifies spatial tracking
