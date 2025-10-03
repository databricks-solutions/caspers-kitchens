# Developer Onboarding Guide - Casper's Kitchens

## Welcome to Casper's Kitchens! 🍔

This guide will help you understand the Ghost Kitchen data architecture and get you productive quickly. Casper's Kitchens is a comprehensive demo of the Databricks platform, showcasing streaming data processing, medallion architecture, AI/ML integration, and real-time applications.

## Quick Start Checklist

- [ ] Review the [dataflow diagram](./dataflow-diagram.md) to understand the overall architecture
- [ ] Examine the [technical reference](./technical-reference.md) for detailed schemas and implementations  
- [ ] Run through the demo setup process
- [ ] Explore key code files and notebooks
- [ ] Understand the data flow from events to applications

## Architecture Overview

Casper's Kitchens simulates a ghost kitchen delivery platform with the following key components:

### 🏗️ System Layers
1. **Event Sources**: Real-time order lifecycle events from ghost kitchens
2. **Bronze Layer**: Raw event ingestion via CloudFiles streaming
3. **Silver Layer**: Clean, normalized operational data
4. **Gold Layer**: Business intelligence aggregations
5. **Applications**: Real-time apps for operations and analytics

### 📊 Data Flow Pattern
```
Ghost Kitchen Events → Volume Storage → Bronze → Silver → Gold → Applications
                                          ↓
                              Dimensional Data (Parquet Files)
                                          ↓  
                              Streaming Intelligence (ML/AI)
                                          ↓
                              Lakebase (PostgreSQL) → Web Apps
```

## Key Files to Understand

### 1. Core Data Pipeline
**File**: `pipelines/order_items/transformations/transformation.py`
- **Purpose**: Defines the medallion architecture transformations
- **Key Functions**: 
  - `all_events()`: Bronze layer raw event ingestion
  - `silver_order_items()`: Silver layer item-level processing
  - `gold_*()`: Business intelligence aggregations

**What to Look For**:
- Delta Live Tables decorators (`@dlt.table`)
- Streaming transformations and watermarks
- Schema definitions and data quality rules
- Partitioning strategies

### 2. Data Generation and Setup
**File**: `stages/raw_data.ipynb`
- **Purpose**: Sets up data generation and dimensional tables
- **Key Sections**:
  - Catalog and schema creation
  - Dimensional data loading from parquet files
  - Event generation configuration

### 3. Pipeline Orchestration  
**File**: `stages/lakeflow.ipynb`
- **Purpose**: Creates and manages the Delta Live Tables pipeline
- **Key Concepts**:
  - Pipeline configuration and settings
  - Cluster and compute management
  - Continuous vs triggered execution

### 4. Streaming Intelligence
**File**: `jobs/refund_recommender_stream.ipynb`
- **Purpose**: Real-time ML-based refund recommendations
- **Key Components**:
  - LLM integration for decision making
  - Streaming data processing
  - Output to recommendation tables

### 5. Application Layer
**File**: `apps/refund-manager/app/main.py`
- **Purpose**: FastAPI web application for human review
- **Key Features**:
  - REST API endpoints
  - PostgreSQL integration via Lakebase
  - Human-in-the-loop decision making

### 6. Lakebase Integration
**File**: `stages/lakebase.ipynb`
- **Purpose**: Sets up PostgreSQL instance and synced tables
- **Key Concepts**:
  - Database instance creation
  - Continuous sync from lakehouse to PostgreSQL
  - Operational data serving

## Understanding the Data Model

### Event Types
The system processes 7 types of events in the order lifecycle:

1. **order_created** - Customer places order
2. **gk_started** - Kitchen begins preparation  
3. **gk_finished** - Kitchen completes preparation
4. **gk_ready** - Order ready for pickup
5. **driver_arrived** - Driver arrives at kitchen
6. **driver_picked_up** - Driver collects order
7. **driver_ping** - GPS updates during delivery
8. **delivered** - Order delivered to customer

### Table Relationships
```
all_events (Bronze)
    ↓ (filter: order_created, explode items)
silver_order_items (Silver)
    ↓ (aggregate by order)        ↓ (aggregate by item+day)
gold_order_header               gold_item_sales_day
    ↓ (aggregate by brand+day)     ↓ (aggregate by location+hour)
gold_brand_sales_day           gold_location_sales_hourly
```

### Dimensional Data
Static reference tables loaded from parquet files:
- **brands**: Restaurant brand information
- **categories**: Food category definitions
- **items**: Menu item details with pricing
- **menus**: Menu structure and organization

## Common Development Tasks

### 1. Adding New Event Types
To add a new event type to the system:

1. **Update the generator** (`data/generator/generator.ipynb`):
   - Add event generation logic
   - Define event schema and timing

2. **Modify transformations** (`pipelines/order_items/transformations/transformation.py`):
   - Add filtering logic for new event type
   - Create new silver/gold tables if needed

3. **Update applications** as needed to consume new data

### 2. Creating New Gold Tables
To add business intelligence aggregations:

1. **Define the table** in `transformation.py`:
```python
@dlt.table(
    name="gold_my_new_metric",
    partition_cols=["day"],
    comment="Description of the new metric"
)
def gold_my_new_metric():
    return (
        dlt.read_stream("silver_order_items")
           .groupBy("dimension1", "dimension2", "day")
           .agg(
               F.sum("measure1").alias("total_measure1"),
               F.count("*").alias("record_count")
           )
    )
```

2. **Consider partitioning** for query performance
3. **Add watermarks** for streaming if needed
4. **Update downstream applications** to consume the new data

### 3. Modifying the Web Application
The Refund Manager app is a standard FastAPI application:

1. **Add new endpoints** in `apps/refund-manager/app/main.py`
2. **Update the database schema** if needed (DDL in startup function)
3. **Modify the frontend** (`apps/refund-manager/index.html`)
4. **Test locally** using the development server

### 4. Configuring Data Generation
Modify `data/generator/configs/sanfrancisco.json` to:
- Change simulation speed (`speed_up`)
- Adjust order volumes (`orders_day_1`, `orders_last`)
- Modify delivery parameters (`radius_mi`, `driver_mph`)
- Add noise for testing (`noise_pct`)

## Running the Demo

### Full Demo Setup
1. **Initialize**: Run `init.ipynb` to create the "Casper's Initializer" job
2. **Execute**: Run the job with "Run All" for complete demo
3. **Monitor**: Watch pipelines process data in real-time
4. **Explore**: Access applications and dashboards

### Selective Stage Execution
You can run individual stages for focused development:
- **Raw Data**: Data generation and dimensional tables
- **Lakeflow**: Medallion architecture pipeline
- **Refund Agent**: ML model training and deployment
- **Refund Stream**: Real-time streaming intelligence
- **Lakebase**: PostgreSQL setup and sync
- **Apps**: Web application deployment

### Development Workflow
1. **Make changes** to transformation logic or applications
2. **Test locally** using notebook environments
3. **Deploy changes** through the pipeline orchestration
4. **Validate results** using SQL queries or application UI
5. **Monitor performance** using Databricks monitoring tools

## Useful SQL Queries

### Monitoring Data Flow
```sql
-- Check recent events
SELECT event_type, COUNT(*) as count, MAX(ts) as latest_event
FROM {CATALOG}.lakeflow.all_events 
WHERE ts >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
GROUP BY event_type;

-- Monitor silver layer processing
SELECT order_day, COUNT(*) as items, SUM(extended_price) as revenue
FROM {CATALOG}.lakeflow.silver_order_items
WHERE order_day >= CURRENT_DATE - 7
GROUP BY order_day
ORDER BY order_day DESC;

-- Check gold layer metrics
SELECT day, SUM(brand_revenue) as total_revenue, SUM(items_sold) as total_items
FROM {CATALOG}.lakeflow.gold_brand_sales_day
WHERE day >= CURRENT_DATE - 7  
GROUP BY day
ORDER BY day DESC;
```

### Debugging Streaming Jobs
```sql
-- Check streaming job health
DESCRIBE HISTORY {CATALOG}.lakeflow.all_events;

-- Monitor pipeline execution
SELECT * FROM system.lakeflow.pipeline_events 
WHERE pipeline_name LIKE '%casper%'
ORDER BY timestamp DESC;

-- Check for processing delays
SELECT 
    event_type,
    MIN(ts) as earliest_event,
    MAX(ts) as latest_event,
    COUNT(*) as event_count
FROM {CATALOG}.lakeflow.all_events
WHERE ts >= CURRENT_TIMESTAMP - INTERVAL 1 DAY
GROUP BY event_type;
```

### Application Data Queries
```sql
-- Check refund recommendations
SELECT 
    refund_class,
    COUNT(*) as count,
    AVG(CAST(JSON_EXTRACT(agent_response, '$.refund_usd') AS DOUBLE)) as avg_amount
FROM {CATALOG}.recommender.refund_recommendations
WHERE ts >= CURRENT_DATE - 1
GROUP BY refund_class;

-- Monitor human decisions
SELECT 
    refund_class,
    COUNT(*) as decisions,
    SUM(amount_usd) as total_amount,
    decided_by
FROM refunds.refund_decisions
WHERE decided_ts >= CURRENT_DATE - 7
GROUP BY refund_class, decided_by;
```

## Troubleshooting Common Issues

### Pipeline Not Processing Data
1. **Check data generation**: Verify events are being written to volumes
2. **Verify pipeline status**: Look for errors in pipeline execution logs
3. **Check permissions**: Ensure proper access to catalogs and schemas
4. **Monitor resource usage**: Verify cluster has sufficient resources

### Streaming Delays
1. **Check watermarks**: Ensure watermark settings allow for data latency
2. **Monitor checkpoints**: Verify streaming checkpoints are progressing
3. **Review batch sizes**: Adjust trigger intervals if needed
4. **Check for backpressure**: Monitor streaming metrics for bottlenecks

### Application Connectivity Issues
1. **Verify Lakebase instance**: Check PostgreSQL instance is running
2. **Check sync status**: Ensure synced tables are up to date
3. **Review permissions**: Verify app has proper database access
4. **Test connections**: Use health endpoints to validate connectivity

### Data Quality Issues
1. **Check schema evolution**: Verify schemas match between layers
2. **Monitor data quality metrics**: Look for parsing errors or null values
3. **Review transformation logic**: Validate business rules and calculations
4. **Check dimensional data**: Ensure reference tables are current

## Next Steps

### For Data Engineers
- Explore advanced Delta Live Tables features
- Implement data quality monitoring and alerting
- Optimize streaming performance and resource usage
- Add new business metrics and KPIs

### For Application Developers  
- Extend the Refund Manager with new features
- Build additional applications consuming gold layer data
- Implement real-time dashboards and monitoring
- Add authentication and authorization

### For Data Scientists
- Enhance the refund recommendation model
- Add new ML use cases (demand forecasting, route optimization)
- Implement A/B testing for model improvements
- Build feature stores for ML workflows

### For Platform Engineers
- Implement CI/CD for pipeline deployments
- Add comprehensive monitoring and alerting
- Optimize cost and performance across the platform
- Implement disaster recovery and backup strategies

## Resources and Support

- **Documentation**: This docs folder contains comprehensive technical references
- **Code Examples**: All notebooks include detailed comments and examples
- **Databricks Documentation**: Official platform documentation and best practices
- **Community**: Databricks community forums and user groups

Happy coding! 🚀
