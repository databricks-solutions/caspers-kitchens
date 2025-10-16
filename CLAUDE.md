# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Casper's Kitchens is a simulated ghost kitchen business platform that demonstrates the full power of Databricks. It simulates a food delivery platform with streaming data ingestion, medallion architecture pipelines, AI agents, real-time apps, and dashboards.

## Architecture

The system is structured as **stages** (found in `./stages/`) orchestrated by a single Databricks Lakeflow Job called "Casper's Initializer". Each stage corresponds to a task in the job, forming a dependency DAG:

- **Raw Data** → **Lakeflow Pipeline** → **AI Agents** (Refund/Complaint) → **Streaming** → **Lakebase** → **Apps**

### Key Components

- **Data Generator**: Creates realistic order lifecycle events with GPS tracking and configurable business parameters
- **Medallion Pipeline**: Bronze → Silver → Gold data processing using Databricks Lakeflow Declarative Pipelines
- **AI Agents**: ML models for refund scoring and complaint processing using UC functions
- **Real-time Streaming**: Spark Streaming jobs for continuous processing
- **Lakebase Integration**: PostgreSQL sync for operational database needs
- **Databricks Apps**: Web applications for business operations

## Essential Commands

### Initial Setup

```bash
# 1. Run init.ipynb to create the "Casper's Initializer" job
# 2. Navigate to Jobs & Pipelines in Databricks workspace
# 3. Run the "Casper's Initializer" job with desired tasks
```

### Cleanup

```bash
# Run destroy.ipynb to remove all resources
```

### Development

- **No traditional build/test commands** - this is a Databricks notebook-based project
- Development happens in Databricks workspace using `.ipynb` files
- Dependencies managed through `%pip install` commands in notebooks

## Configuration System

### Location Configs

Business parameters are configurable via JSON files in `data/generator/configs/`:

- **Default**: Only San Francisco location runs (`sanfrancisco.json`)
- **Custom locations**: Copy template and modify parameters
- **Selection**: Use `LOCATIONS` parameter in job (e.g., `"chicago.json,sanfrancisco.json"` or `"all"`)

### Key Parameters (Configurable in Job)

- `CATALOG`: Default "caspers" - must be unique within metastore
- `LOCATIONS`: Which location configs to run
- `LLM_MODEL`: AI model for complaint processing
- `COMPLAINT_RATE`: Percentage of orders generating complaints
- Simulation speed, order volumes, service times, driver behavior

### Simulation Configuration

Each location JSON controls:

- **Time window**: Historical data span and future simulation length
- **Speed**: Real-time (1x) to accelerated (60x = 1 hour per minute)
- **Volume**: Order counts with linear growth and day-of-week patterns
- **Service times**: Kitchen prep, cooking, packaging, driver pickup (Gaussian distributions)
- **Geography**: Ghost kitchen location, delivery radius, real road networks via OpenStreetMap

## Data Flow

### Event Types Generated

The system produces realistic order lifecycle events:

1. `order_created` → Customer places order with location/items
2. `gk_started` → Kitchen begins preparation
3. `gk_finished` → Kitchen completes cooking
4. `gk_ready` → Order ready for pickup
5. `driver_arrived` → Driver reaches kitchen
6. `driver_picked_up` → Driver collects order with GPS route
7. `driver_ping` → GPS updates during delivery
8. `delivered` → Final delivery confirmation

### Data Storage

- **Volume**: `/Volumes/{CATALOG}/simulator/events` (JSON event files)
- **Tables**: Medallion architecture in `{CATALOG}.lakeflow.*`
- **Lakebase**: PostgreSQL for operational queries

## State Management

The project uses a UC (Unity Catalog) state tracking system in `utils/uc_state/`:

- Tracks created jobs, pipelines, and resources
- Enables proper cleanup via `destroy.ipynb`
- Import pattern: `from uc_state import create_state_manager, add`
- State stored in `{CATALOG}._internal_state.resources` table
- Usage: Create manager → Add resources → Destroy reads state for cleanup

## Development Patterns

### Stage Development

When adding new stages:

1. Create notebook in `./stages/`
2. Add task to job DAG in `init.ipynb`
3. Use UC state tracking for resource management
4. Follow dependency patterns (Raw Data → Lakeflow → downstream)

### Agent Development

AI agents follow UC function patterns:

- Model serving endpoints for inference
- Streaming jobs for real-time processing
- Lakebase sync for operational access

### App Development

Databricks apps in `./apps/` directory:

- Python-based web applications
- Database connectivity through Lakebase
- Real-time data integration

## Important Notes

- **Catalog Uniqueness**: If working in shared metastore, use unique catalog name via `CATALOG` parameter
- **Resource Management**: Always use UC state tracking for proper cleanup
- **Dependencies**: Respect stage DAG - Raw Data and Lakeflow are required foundations
- **Location Selection**: Control data generation scope via `LOCATIONS` parameter to avoid unnecessary resource usage
- **Real Geography**: Uses actual road networks and addresses via OpenStreetMap for realistic routing