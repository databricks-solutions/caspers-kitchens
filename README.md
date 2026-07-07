<img src="./images/casperslogo.png" alt="Casper's" width="60"/> 

# Casper's Kitchens

A "demo" consumer brand built by the Developer Relations team to showcase the full Databricks platform. First imagined as a ghost kitchen and delivery service, the evolved concept can flex across multiple verticals. Casper's integrates every layer — Lakeflow, AI/BI, Genie, Agent Bricks, Apps, and Lakebase — into a unified live demo.

## Build Your Own

Want a demo for a different business? The **Caspers skill** generates streaming data, GPS routing, and replay engines for any domain:

```bash
git clone -b skill https://github.com/databricks-solutions/caspers-kitchens.git caspers-skill
```

See the [`skill` branch](../../tree/skill) for details.

## Prerequisites

- Databricks CLI installed and authenticated (`databricks auth login`)
- Workspace permissions to create catalogs

## Deploy

Replace `all` and `mycatalog` with your target and catalog:

```bash
databricks bundle deploy -t all --var catalog=mycatalog
databricks bundle run caspers -t all --params "CATALOG=mycatalog"
```

Use the same target for deploy and run. Omit `-t` to use `default` (refund demo only).

Available targets:

| Target | What it deploys |
|--------|----------------|
| `default` | Canonical data replay, Lakeflow pipeline, refund agent + stream, Lakebase Autoscale reverse-ETL, Refund Manager app |
| `support` | Canonical data replay, Lakeflow pipeline, support triage agent + streams, Lakebase + app |
| `complaints` | Canonical data replay, Lakeflow pipeline, complaint agent + streams, Lakebase |
| `free` | Canonical data replay, Lakeflow pipeline (Free Edition compatible) |
| `all` | Full platform demo: refund + complaints paths above, document intelligence pipeline, 3 Genies + 6 Knowledge Assistants + Multi-Agent Supervisor, Operational Dashboard app (Lakebase-backed), 5 AI/BI dashboards. See [`demos/dais2026-runbooks/SETUP.ipynb`](demos/dais2026-runbooks/SETUP.ipynb) for workspace prep. |

Refund and complaint evaluation tasks (`Refund_Evaluation`, `Complaint_Evaluation`) are **skipped by default** (`SKIP_EVAL=true`). Pass `--params "SKIP_EVAL=false"` to opt in. The supervisor/KA `Evaluation` task on `all` (`stages/operational_evaluation`) always runs — it is not gated by `SKIP_EVAL`.

Optionally specify a catalog (default: `caspersdev`).  There are **two** dials
that take a catalog name and they must agree:

| Dial | When | What it controls |
|---|---|---|
| `bundle deploy --var catalog=<name>` | deploy time | the catalog baked into every DABs-managed resource — the `all` target's `caspers_ops_warehouse` SQL warehouse, AI/BI dashboard names, dashboard `dataset_catalog`, and the *default* value of every job parameter that uses `${var.catalog}` (including `CATALOG`, `REFUND_AGENT_ENDPOINT_NAME`, `OPS_WAREHOUSE_NAME`, etc.) |
| `bundle run caspers --params "CATALOG=<name>"` | run time | only the value of the `CATALOG` widget inside stage notebooks.  Cannot rename anything DABs already created. |

If they disagree (e.g. `bundle deploy -t all` with the default + `bundle run
--params CATALOG=mycatalog`), the `all` target will fail at the
`Operational_App` stage because the warehouse DABs created (`caspersdev-ops-warehouse`)
is not what the stage looks up (`mycatalog-ops-warehouse`).  The fix is to
pass the same catalog to both:

```bash
databricks bundle deploy -t all --var catalog=mycatalog
databricks bundle run caspers -t all --params "CATALOG=mycatalog"
```

For targets other than `all` (no DABs-owned warehouse/dashboards),
`--params CATALOG=mycatalog` alone usually works, but passing both keeps the
deploy-time and run-time catalogs in sync and is the safer habit.

## Clean Up

Cleanup is **destructive**: it runs `destroy.ipynb`, which `DROP CATALOG … CASCADE` on the catalog you pass. It does **not** rebuild data or schemas afterward.

**Catalog default:** if you omit `BUNDLE_VAR_catalog=...`, cleanup uses `caspersdev` (the bundle default) — or whatever catalog was last passed to `bundle deploy --var catalog=...` on this machine.

Cleanup is a bundle **script** — use `BUNDLE_VAR_catalog=...` on the command line (not `--params CATALOG=...`, which is ignored for scripts). **`--var catalog=...` on `bundle run cleanup` does not work** — the CLI does not pass it into the script.

```bash
# 1. Delete runtime UC resources (catalog + everything in it)
BUNDLE_VAR_catalog=mycatalog databricks bundle run cleanup -t all
# With a non-default CLI profile, append --profile <name> at the end.

# 2. Delete bundle-managed resources (job, warehouses, dashboards)
databricks bundle destroy -t all
```

To **rebuild** after cleanup, deploy and run the job again (deploy alone does not recreate the catalog):

```bash
databricks bundle deploy -t all --var catalog=mycatalog
databricks bundle run caspers -t all --params "CATALOG=mycatalog"
```

The first task (`Canonical_Data`) runs `CREATE CATALOG IF NOT EXISTS` and repopulates schemas/tables.

## Blog

Check out the [Casper's Kitchens Blog](https://databricks-solutions.github.io/caspers-kitchens/).

## License

© 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
