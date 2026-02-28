# 🍔 Casper's Kitchens

Casper’s Kitchens is a fully Databricks-native ghost kitchen and food-delivery platform built by the Developer Relations team. It brings together every layer of the Databricks platform — Lakeflow (ingestion, Spark Declarative Pipelines), AI & BI dashboards with Genie, Agent Bricks, and Apps powered by Lakebase (Postgres) — into a single, cohesive live demo.

Casper’s is more than a showcase. It’s a living playground for simulation, demos, and creative misuse — designed to push the Databricks platform past its comfort zone.

Everything is built to be easy to:

1. 🚀 **Deploy** — spin up the entire environment in minutes.
2. 🎬 **Demo** — run only the stages you need, powered by live streaming data.
3. 🧑‍💻 **Develop** — extend with new pipelines, agents, or apps effortlessly.

We build only with Databricks — by choice — so Casper’s serves as a shared sandbox for learning, experimentation, and storytelling across the platform.

## Prerequisites

- Databricks CLI installed on your local machine.
- Authenticated to your Databricks workspace. (can do interactively `databricks auth login`)
- Access to the repository containing Casper's Kitchens.
- Permissions in the Databricks workspace to create new catalogs.

## 🚀 Deploy

Casper’s Kitchens uses **Databricks Asset Bundles (DABs)** for one-command deployment.
Clone this repo, then run from the root directory:

```bash
databricks bundle deploy -t <target>
```

Each **target** represents a different flavor of Casper’s (for example, full demo, complaints-only, free tier, etc.).
Use whichever fits your needs:

```bash
databricks bundle deploy -t default     # full version: Data generation, Lakeflow, Agents, Lakebase & Apps
databricks bundle deploy -t complaints  # complaints agent: Data generation, Lakeflow, Agents, Lakebase
databricks bundle deploy -t free        # Databricks Free Edition: Data generation, Lakeflow
```

This creates the main job **Casper’s Initializer**, which orchestrates the full ecosystem, and places all assets in your workspace under
`/Workspace/Users/<your.email@databricks.com>/caspers-kitchens-demo`.

> 💡 You can also deploy from the Databricks UI by cloning this repo as a [Git-based folder](https://docs.databricks.com/repos/) and clicking [Deploy Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-tutorial#deploy-the-bundle).

For more about how bundles and targets work, see [databricks.yml](./databricks.yml) or the [Databricks Bundles docs](https://docs.databricks.com/en/dev-tools/bundles/index.html).

## 🎬 Run the Demo

![](./images/stages.gif)

Once deployed, run **any** target with the same command:

```bash
databricks bundle run caspers
```

Optionally, specify a catalog (default: `caspersdev`):

```bash
databricks bundle run caspers --params "CATALOG=mycatalog"
```

This spins up all the components—data generator, pipelines, agents, and apps—based on your selected target.

To clean up:

```bash
databricks bundle run cleanup (--params "CATALOG=mycatalog")
databricks bundle destroy
```

> 🧩 You can also run individual tasks or stages directly in the Databricks Jobs UI for finer control.

## 📊 Generated Event Types

The data generator produces the following realistic events for each order in the Volume `caspers.simulator.events`:

| Event | Description | Data Included |
|-------|-------------|---------------|
| `order_created` | Customer places order | Customer location (lat/lon), delivery address, ordered items with quantities |
| `gk_started` | Kitchen begins preparing food | Timestamp when prep begins |
| `gk_finished` | Kitchen completes food preparation | Timestamp when food is ready |
| `gk_ready` | Order ready for pickup | Timestamp when driver can collect |
| `driver_arrived` | Driver arrives at kitchen | Timestamp of driver arrival |
| `driver_picked_up` | Driver collects order | Full GPS route to customer, estimated delivery time |
| `driver_ping` | Driver location updates during delivery | Current GPS coordinates, delivery progress percentage |
| `delivered` | Order delivered to customer | Final delivery location coordinates |

Each event includes order ID, sequence number, timestamp, and location context. The system models realistic timing between events based on configurable service times, kitchen capacity, and real road network routing via OpenStreetMap data.

## 🎯 Use Cases

- **📚 Learning Databricks**: Complete end-to-end platform experience
- **🎓 Teaching**: Consistent narrative across different Databricks features  
- **🧪 CUJ Testing**: Run critical user journeys in realistic environment
- **🎨 UX Prototyping**: Fully loaded platform for design iteration
- **🎬 Demo Creation**: Unified narrative for new feature demonstrations

## Support Feature Store Notes

For the support scenario (`-t support` target), feature serving is split from app OLTP storage:

- App runtime tables (actions, replies, status) stay on Lakebase Autoscaling (v2).
- Features are materialized into the offline UC table `casperskitchens.support.support_request_features`.
- Current support features include deterministic signals (`repeat_complaints_30d`, `policy_limit_usd`) and a risk score currently computed via deterministic fallback logic in the stage.
- The support initializer now includes a dedicated `Support_Feature_Store` task and depends on it before `Support_Lakebase`.

### Current Platform Behavior (Feb 2026)

- `Support Response Evals Hourly` has been fixed and is now scheduled (`UNPAUSED`).
- The eval notebook was hardened to handle schema variations in `mlflow.genai.evaluate()` output without failing.
- Legacy Online Table creation is currently blocked by Databricks platform behavior ("Online Table is being deprecated"), so online publish is intentionally deferred.
- Next migration step is to map feature serving exposure to Synced Tables-compatible patterns.

## Check out the [Casper's Kitchens Blog](https://databricks-solutions.github.io/caspers-kitchens/)!

## License

© 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
