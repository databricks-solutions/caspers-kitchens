# UC-State: Unity Catalog State Management

A utility for managing Databricks resource state using Unity Catalog tables.

## Features

- **Catalog-based storage**: Stores state in Unity Catalog tables
- **Universal IDs**: Generates internal UUIDs for consistent resource referencing
- **Easy integration**: Simple API for state management
- **Safe cleanup**: Follows proper resource deletion order
- **Resource tracking**: Supports jobs, pipelines, apps, database instances, model endpoints, and catalogs

## Quick Start

```python
from uc_state import create_state_manager

# Initialize state manager
state = create_state_manager(catalog="your_catalog")

# Add resources (from API return objects)
job = w.jobs.create(...)
job_id = state.add("jobs", job)

pipeline = w.pipelines.create(...)
pipeline_id = state.add("pipelines", pipeline)

# Add catalogs (just pass catalog name as string)
catalog_id = state.add("catalogs", "my_catalog_name")

# List resources
all_resources = state.list()
just_jobs = state.list("jobs")

# Remove specific resource
state.remove(job_id)

# Clear everything (dry run first!)
preview = state.clear_all(dry_run=True)
state.clear_all()  # Actually delete everything
```

## Configuration

- **Catalog**: Required - the main catalog you're using
- **Schema**: Optional - defaults to `_caspers_state`
- **Table**: Optional - defaults to `resources`

## Installation

```bash
pip install -r requirements.txt
```

Or install dependencies manually:
```bash
pip install databricks-sdk mlflow
```

## Table Schema

The utility creates a Delta table with:
- `internal_id`: UUID primary key
- `resource_type`: Resource category (jobs, pipelines, etc.)
- `resource_data`: JSON-serialized API response object
- `created_at`: Timestamp

## Resource Types

Supported resource types and their cleanup methods:
- `jobs` → `w.jobs.delete(job_id)`
- `pipelines` → `w.pipelines.delete(pipeline_id)`  
- `models` → `mlflow.deployments.delete_endpoint(name)`
- `apps` → `w.apps.delete(name)`
- `dashboards` → `w.lakeview.trash(dashboard_id)` (Lakeview/AI BI dashboards)
- `databaseinstances` → `w.database.delete_database_instance(name)`
- `postgres_projects` → `w.postgres.delete_project(name)` (Lakebase Autoscaling)
- `catalogs` → `w.catalogs.delete(name, force=True)`