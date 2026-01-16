# 🏦 Casper's Bank

Spin up a fully working digital bank on Databricks in minutes.

Casper's Bank is a simulated digital banking platform that shows off the full power of Databricks: streaming transaction ingestion, Lakeflow Declarative Pipelines, AI/BI Dashboards and Genie, Agent Bricks, and real-time apps backed by Lakebase postgres — all stitched together into one narrative.

## 🚀 Quick Start

1. **Import to Databricks Workspace**: Create a new Git folder in your workspace and import this repository

2. **Initialize the demo**: Run `init.ipynb` to create the "Casper's Initializer" job
   - By default the job will use the catalog `caspers`
   - **Important**: If you're working in a metastore that spans multiple workspaces and another workspace has already used the catalog name `caspers`, you'll need to specify a different name using the `CATALOG` parameter. Catalog names must be unique within a metastore.
   - By default, only the San Francisco branch will generate data. To run additional branches (like Chicago) or create your own, see `data/generator/configs/README.md` and use the `LOCATIONS` parameter.

3. **Launch your digital bank**:
   - Navigate to **Jobs & Pipelines** in the left sidebar of your Databricks workspace
   - Find and run the `Casper's Initializer` job
   - You can pick a subset of tasks to run if you want. The `Raw_Data` and `Lakeflow_Declarative_Pipeline` tasks are required, but downstream tasks are demo-specific and you can run whichever ones you need.

Then open Databricks and watch:
- 💳 Transactions stream in from bank branches and credit card processors
- 🔄 Pipelines curate raw → bronze → silver → gold
- 📊 [Dashboards](https://github.com/databricks-solutions/caspers-kitchens/issues/13) & apps come alive with real-time insights
- 🤖 Transaction Review Agent decides whether transaction disputes should be approved

That's it! Your Casper's Bank environment will be up and running.

## 🏗️ What is Casper's Bank?

Casper's Bank is a fully functional digital bank running entirely on the Databricks platform. As a modern digital bank, Casper's operates multiple branch locations, processes credit card transactions, manages customer accounts, and handles real-time payment processing from various merchant partners across different transaction networks.

The platform serves dual purposes:
- **🎭 Narrative**: Provides a consistent business context for demos and training across the Databricks platform
- **⚙️ Technical**: Delivers complete infrastructure for learning Databricks, running critical user journeys (CUJs), and enabling UX prototyping

The platform generates realistic transaction data with full transaction lifecycle tracking - from initiation to settlement - including authorization status updates, payment network routing, and configurable business parameters.

## 🏗️ Architecture

![Stages](./images/stages.png)

The system is structured as **stages** (found in `./stages/`) orchestrated by a single Databricks Lakeflow Job called "Casper's Initializer". Each stage corresponds to a task in the job (pictured above), enabling:

- **🎯 Customizable demos**: Run only the stages relevant to your use case
- **🔧 Easy extensibility**: Add new demos that integrate seamlessly under the Casper's narrative  
- **⚡ Databricks-native**: Uses Databricks itself to bootstrap the demo environment

The dependencies between stages is reflected in the job's DAG. 

You can add new stages to this DAG to extend the demo but they do not NEED to be dependent on the existing DAG if they do not actually use assets produced by other stages.

### 📊 Generated Event Types

The data generator produces the following realistic events for each transaction in the Volume `caspers.simulator.events`:

| Event | Description | Data Included |
|-------|-------------|---------------|
| `order_created` | Transaction initiated | Customer location (lat/lon), account details, transaction items with amounts |
| `gk_started` | Authorization begins | Timestamp when authorization starts |
| `gk_finished` | Authorization completed | Timestamp when authorization is approved/denied |
| `gk_ready` | Transaction ready for processing | Timestamp when ready for settlement |
| `driver_arrived` | Payment processor engaged | Timestamp when processor begins routing |
| `driver_picked_up` | Transaction routed through network | Full payment route through network, estimated completion time |
| `driver_ping` | Transaction status updates during processing | Current processing node, transaction progress percentage |
| `delivered` | Transaction settled | Final settlement confirmation and location |

Each event includes transaction ID, sequence number, timestamp, and branch/location context. The system models realistic timing between events based on configurable processing times, branch capacity, and real payment network routing via OpenStreetMap data for branch locations.

### 🛠️ Available Stages

**📊 Raw Data**
- Starts realistic data generators for transaction streams
- Configurable branch locations, processing parameters, and simulation speed
- Tracks complete transaction lifecycle with payment network routing
- Default San Francisco branch with easy expansion via JSON configs

**🔄 Lakeflow**
- Medallion architecture pipeline (Bronze → Silver → Gold)
- Processes and normalizes transaction data
- Creates summary tables for downstream consumption

**🤖 Transaction Review Agent**
- ML model that scores transactions for dispute eligibility
- Uses processing time percentiles (P50, P75, P99) for scoring
- Classifies as no refund, partial refund, or full refund

**⚡ Transaction Review Agent Stream**
- Spark Streaming job for real-time dispute scoring
- Processes completed transactions and writes results to lakehouse

**🗄️ Lakebase and Reverse ETL**
- Creates Lakebase (PostgreSQL) instance
- Sets up reverse ETL for scored transactions

**📱 Transaction Manager App**
- Databricks application for human dispute review
- Allows managers to approve/deny AI recommendations

## ⚙️ Configuration

Business parameters are fully configurable via JSON files in `data/generator/configs/`:

- **📍 Branch Locations**: Add new cities/regions with custom parameters
- **⏱️ Simulation speed**: From real-time (1x) to accelerated (60x = 1 hour of data per minute)
- **💳 Transaction parameters**: Processing speeds, service radius, time distributions
- **🏢 Business settings**: Merchants, financial products, accounts, transaction volumes
- **📊 Data generation**: Historical data spans, noise levels, batch sizes

## 🎯 Use Cases

- **📚 Learning Databricks**: Complete end-to-end platform experience
- **🎓 Teaching**: Consistent narrative across different Databricks features  
- **🧪 CUJ Testing**: Run critical user journeys in realistic environment
- **🎨 UX Prototyping**: Fully loaded platform for design iteration
- **🎬 Demo Creation**: Unified narrative for new feature demonstrations

## 🙌 Why This Matters

Most demos show just one slice of Databricks. Casper's Bank shows how it all connects: ingestion, curation, analytics, and AI apps working together. Use it to learn, demo to customers, or build your own extensions.

## 🧹 Cleanup

Run `destroy.ipynb` to remove all Casper's Bank resources from your workspace.

## License

© 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
