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

```bash
databricks bundle deploy -t <target>
databricks bundle run caspers
```

Available targets:

| Target | What it deploys |
|--------|----------------|
| `default` | Data generation, Lakeflow pipeline, refund agent, Lakebase + app |
| `support` | Data generation, Lakeflow pipeline, support triage agent, Lakebase + app |
| `complaints` | Data generation, Lakeflow pipeline, complaint agent, Lakebase |
| `free` | Data generation, Lakeflow pipeline (Free Edition compatible) |
| `all` | Everything end-to-end: refund + complaints + Operational Dashboard (3 Genies + 6 Knowledge Assistants + Multi-Agent Supervisor + Lakebase-backed FastAPI app) |

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
databricks bundle run caspers --params "CATALOG=mycatalog"
```

For targets other than `all` (no DABs-owned warehouse/dashboards),
`--params CATALOG=mycatalog` alone usually works, but passing both keeps the
deploy-time and run-time catalogs in sync and is the safer habit.

## Clean Up

```bash
# Pass the target you deployed to; --var (not --params) overrides the catalog.
databricks bundle run cleanup -t <target> [--var catalog=<name>]
databricks bundle destroy     -t <target>
```

## Blog

Check out the [Casper's Kitchens Blog](https://databricks-solutions.github.io/caspers-kitchens/).

## License

© 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
