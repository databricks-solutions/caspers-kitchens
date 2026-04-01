# Casper's Kitchens

A fully Databricks-native ghost kitchen demo — streaming data, Spark Declarative Pipelines, AI agents, and apps, deployed in one command.

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
| `menus` | Document intelligence, DLT pipeline, Genie, Knowledge Assistants, Multi-Agent Supervisor |

Optionally specify a catalog (default: `caspersdev`):

```bash
databricks bundle run caspers --params "CATALOG=mycatalog"
```

## Clean Up

```bash
databricks bundle run cleanup
databricks bundle destroy
```

## Blog

Check out the [Casper's Kitchens Blog](https://databricks-solutions.github.io/caspers-kitchens/).

## License

© 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
