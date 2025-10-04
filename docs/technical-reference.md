# Casper's Kitchens - Technical Reference

## Table Schemas and Implementation Details

### Bronze Layer Tables

#### all_events
**Location**: `{CATALOG}.lakeflow.all_events`  
**Type**: Streaming table (Delta Live Tables)  
**Source**: CloudFiles streaming from `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}`

```python
@dlt.table(comment="Raw JSON events as ingested (one file per event).")
def all_events():
    return (
        spark.readStream.format("cloudFiles") 
             .option("cloudFiles.format", "json")
             .load(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")
    )
```

**Schema**:
- `event_type`: STRING - Type of event (order_created, gk_started, etc.)
- `order_id`: STRING - Unique order identifier  
- `ts`: STRING - Event timestamp (raw)
- `body`: STRING - JSON payload with event-specific data
- `location`: STRING - Ghost kitchen location
- `gk_id`: STRING - Ghost kitchen identifier
- `seq`: INTEGER - Event sequence number

### Silver Layer Tables

#### silver_order_items
**Location**: `{CATALOG}.lakeflow.silver_order_items`  
**Type**: Streaming table (Delta Live Tables)  
**Partitioned by**: `order_day`

```python
@dlt.table(
    name="silver_order_items",
    comment="Silver – one row per item per order, with extended_price.",
    partition_cols=["order_day"]
)
```

**Schema Definition**:
```python
item_schema = StructType([
    StructField("id",          IntegerType()),
    StructField("category_id", IntegerType()),
    StructField("menu_id",     IntegerType()),
    StructField("brand_id",    IntegerType()),
    StructField("name",        StringType()),
    StructField("price",       DoubleType()),
    StructField("qty",         IntegerType())
])

body_schema = (
    StructType()
        .add("customer_lat",  DoubleType())
        .add("customer_lon",  DoubleType())
        .add("customer_addr", StringType())
        .add("items",         ArrayType(item_schema))
)
```

**Output Schema**:
- `order_id`: STRING
- `gk_id`: STRING  
- `location`: STRING
- `order_ts`: TIMESTAMP - Canonical event time
- `order_day`: DATE - Partition key
- `item_id`: INTEGER
- `menu_id`: INTEGER
- `category_id`: INTEGER
- `brand_id`: INTEGER
- `item_name`: STRING
- `price`: DOUBLE
- `qty`: INTEGER
- `extended_price`: DOUBLE - Calculated as price * qty

**Key Transformations**:
```python
.filter(F.col("event_type") == "order_created")
.withColumn("event_ts", F.to_timestamp("ts"))
.withColumn("body_obj", F.from_json("body", body_schema))
.withColumn("item", F.explode("body_obj.items"))
.withColumn("extended_price", F.col("item.price") * F.col("item.qty"))
.withColumn("order_day", F.to_date("event_ts"))
```

### Gold Layer Tables

#### gold_order_header
**Location**: `{CATALOG}.lakeflow.gold_order_header`  
**Type**: Streaming table (Delta Live Tables)

```python
@dlt.table(
    name="gold_order_header",
    comment="Gold – per-order revenue & counts."
)
```

**Aggregation Logic**:
```python
.groupBy("order_id", "gk_id", "location", "order_day")
.agg(
    F.sum("extended_price").alias("order_revenue"),
    F.sum("qty").alias("total_qty"),
    F.count("item_id").alias("total_items"),
    F.collect_set("brand_id").alias("brands_in_order")
)
```

**Schema**:
- `order_id`: STRING
- `gk_id`: STRING
- `location`: STRING  
- `order_day`: DATE
- `order_revenue`: DOUBLE - Total order value
- `total_qty`: LONG - Total items quantity
- `total_items`: LONG - Count of distinct items
- `brands_in_order`: ARRAY<INTEGER> - Set of brand IDs in order

#### gold_item_sales_day
**Location**: `{CATALOG}.lakeflow.gold_item_sales_day`  
**Type**: Streaming table (Delta Live Tables)  
**Partitioned by**: `day`

```python
@dlt.table(
    name="gold_item_sales_day",
    partition_cols=["day"],
    comment="Gold – item-level units & revenue by day."
)
```

**Schema**:
- `item_id`: INTEGER
- `menu_id`: INTEGER
- `category_id`: INTEGER
- `brand_id`: INTEGER
- `day`: DATE - Partition key
- `units_sold`: LONG - Total quantity sold
- `gross_revenue`: DOUBLE - Total revenue for item

#### gold_brand_sales_day
**Location**: `{CATALOG}.lakeflow.gold_brand_sales_day`  
**Type**: Streaming table (Delta Live Tables)  
**Partitioned by**: `day`

```python
@dlt.table(
    name="gold_brand_sales_day",
    partition_cols=["day"],
    comment="Gold – brand-level orders (approx), items, revenue by day."
)
```

**Streaming Configuration**:
```python
.withWatermark("order_ts", "3 hours")
.groupBy("brand_id", F.col("order_day").alias("day"))
.agg(
    F.approx_count_distinct("order_id").alias("orders"),
    F.sum("qty").alias("items_sold"),
    F.sum("extended_price").alias("brand_revenue")
)
```

**Schema**:
- `brand_id`: INTEGER
- `day`: DATE - Partition key
- `orders`: LONG - Approximate distinct order count (HyperLogLog)
- `items_sold`: LONG - Total items sold
- `brand_revenue`: DOUBLE - Total brand revenue

#### gold_location_sales_hourly
**Location**: `{CATALOG}.lakeflow.gold_location_sales_hourly`  
**Type**: Streaming table (Delta Live Tables)  
**Partitioned by**: `hour_ts`

```python
@dlt.table(
    name="gold_location_sales_hourly",
    partition_cols=["hour_ts"],
    comment="Gold – hourly orders (approx) & revenue per location."
)
```

**Streaming Configuration**:
```python
.withWatermark("order_ts", "3 hours")
.withColumn("hour_ts", F.date_trunc("hour", "order_ts"))
.groupBy("location", "hour_ts")
.agg(
    F.approx_count_distinct("order_id").alias("orders"),
    F.sum("extended_price").alias("revenue")
)
```

**Schema**:
- `location`: STRING
- `hour_ts`: TIMESTAMP - Partition key (truncated to hour)
- `orders`: LONG - Approximate distinct order count
- `revenue`: DOUBLE - Total hourly revenue

### Dimensional Tables

#### brands
**Location**: `{CATALOG}.{SIMULATOR_SCHEMA}.brands`  
**Source**: `data/dimensional/brands.parquet`

```python
spark.createDataFrame(pd.read_parquet("../data/dimensional/brands.parquet")) \
    .write.mode("overwrite").saveAsTable(f"{CATALOG}.{SIMULATOR_SCHEMA}.brands")
```

#### categories  
**Location**: `{CATALOG}.{SIMULATOR_SCHEMA}.categories`  
**Source**: `data/dimensional/categories.parquet`

#### items
**Location**: `{CATALOG}.{SIMULATOR_SCHEMA}.items`  
**Source**: `data/dimensional/items.parquet`

#### menus
**Location**: `{CATALOG}.{SIMULATOR_SCHEMA}.menus`  
**Source**: `data/dimensional/menus.parquet`

### Streaming Intelligence Tables

#### refund_recommendations
**Location**: `{CATALOG}.recommender.refund_recommendations`  
**Type**: Streaming table (Spark Structured Streaming)

```python
refund_recommendations = spark.readStream.table(f"{CATALOG}.lakeflow.all_events") \
    .filter("event_type = 'delivered'") \
    .filter(
        # For historical data (ts < current_time), sample 10%
        # For new data (ts >= current_time), process 100%
        (F.col("ts") >= current_time) | 
        ((F.col("ts") < current_time) & (F.rand() < 0.1))
    ) \
    .select(
        F.col("order_id"),
        F.current_timestamp().alias("ts"),
        get_chat_completion_udf(F.col("order_id")).alias("agent_response"),
    )
```

**Schema**:
- `order_id`: STRING
- `ts`: TIMESTAMP - Processing timestamp
- `agent_response`: STRING - LLM-generated JSON response

**Agent Response Format**:
```json
{
    "refund_class": "none|partial|full",
    "refund_usd": 0.00,
    "reason": "Explanation for refund decision"
}
```

### Lakebase Tables

#### pg_recommendations
**Location**: PostgreSQL in Lakebase instance `{CATALOG}refundmanager`  
**Database**: `caspers`  
**Sync Source**: `{CATALOG}.recommender.refund_recommendations`

```python
synced_table = w.database.create_synced_database_table(
    SyncedDatabaseTable(
        name=f"{CATALOG}.recommender.pg_recommendations",
        database_instance_name=instance.name,
        logical_database_name="caspers",
        spec=SyncedTableSpec(
            source_table_full_name=f"{CATALOG}.recommender.refund_recommendations",
            primary_key_columns=["order_id"],
            scheduling_policy=SyncedTableSchedulingPolicy.CONTINUOUS,
            create_database_objects_if_missing=True
        )
    )
)
```

#### refund_decisions
**Location**: PostgreSQL in Lakebase instance  
**Schema**: `refunds`  
**Purpose**: Human refund decisions

```sql
CREATE TABLE refunds.refund_decisions (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    decided_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    amount_usd NUMERIC(10,2) NOT NULL CHECK (amount_usd >= 0),
    refund_class TEXT NOT NULL CHECK (refund_class IN ('none','partial','full')),
    reason TEXT NOT NULL,
    decided_by TEXT,
    source_suggestion JSONB
);
CREATE INDEX idx_refund_decisions_order_id ON refunds.refund_decisions(order_id);
```

## Event Types and Schemas

### Order Lifecycle Events

1. **order_created**
   - Customer places order
   - Contains: customer location, delivery address, ordered items with quantities

2. **gk_started**  
   - Kitchen begins preparing food
   - Contains: timestamp when prep begins

3. **gk_finished**
   - Kitchen completes food preparation  
   - Contains: timestamp when food is ready

4. **gk_ready**
   - Order ready for pickup
   - Contains: timestamp when driver can collect

5. **driver_arrived**
   - Driver arrives at kitchen
   - Contains: timestamp of driver arrival

6. **driver_picked_up**
   - Driver collects order
   - Contains: full GPS route to customer, estimated delivery time

7. **driver_ping**
   - Driver location updates during delivery
   - Contains: current GPS coordinates, delivery progress percentage

8. **delivered**
   - Order delivered to customer
   - Contains: final delivery location coordinates

## Configuration Files

### Generator Configuration
**Location**: `data/generator/configs/sanfrancisco.json`

```json
{
    "start_days_ago": 3,
    "end_days_ahead": 362,
    "speed_up": 1,
    "orders_day_1": 50,
    "orders_last": 1000,
    "noise_pct": 10,
    "gk_location": "160 Spear St, San Francisco, CA 94105",
    "location_name": "sanfrancisco",
    "radius_mi": 4,
    "driver_mph": 25,
    "batch_rows": 10,
    "batch_seconds": 1,
    "ping_sec": 60,
    "random_seed": 72
}
```

**Key Parameters**:
- `speed_up`: Simulation speed multiplier (1x = real-time, 60x = 1 hour per minute)
- `orders_day_1` / `orders_last`: Order volume scaling
- `noise_pct`: Data quality variation percentage
- `radius_mi`: Delivery radius from ghost kitchen
- `driver_mph`: Average driver speed for routing
- `batch_rows` / `batch_seconds`: Event generation batching

## Application Configurations

### Refund Manager App
**Location**: `apps/refund-manager/app.yaml`

```yaml
display_name: "Refund Manager"
description: "Human review interface for AI-generated refund recommendations"
```

**Environment Variables**:
- `REFUNDS_SCHEMA`: Schema for refund decisions (default: "refunds")
- `RECS_SCHEMA`: Schema for recommendations (default: "recommender")
- `DEBUG`: Enable debug mode for detailed error responses

### API Endpoints

#### GET /api/summary
Returns aggregate statistics:
```json
{
    "recommendations_count": 1250,
    "suggestions_by_class": {"none": 800, "partial": 300, "full": 150},
    "suggested_total_usd": 15750.50,
    "decisions_count": 450,
    "decisions_by_class": {"none": 280, "partial": 120, "full": 50},
    "decided_total_usd": 5250.25,
    "pending_count": 800
}
```

#### GET /api/recommendations
Returns paginated recommendations with decisions:
```json
{
    "items": [
        {
            "order_id": "order_123",
            "ts": "2024-01-15T10:30:00Z",
            "suggestion": {
                "refund_class": "partial",
                "refund_usd": 12.50,
                "reason": "Delivery delayed by 25 minutes"
            },
            "decision": null,
            "status": "pending"
        }
    ],
    "limit": 50,
    "offset": 0
}
```

#### POST /api/refunds
Apply human refund decision:
```json
{
    "order_id": "order_123",
    "amount_usd": 10.00,
    "refund_class": "partial",
    "reason": "Approved partial refund for late delivery",
    "decided_by": "manager@caspers.com"
}
```

#### GET /api/orders/{order_id}/events
Returns complete event timeline for an order:
```json
{
    "order_id": "order_123",
    "events": [
        {
            "event_type": "order_created",
            "ts": "2024-01-15T09:00:00Z",
            "location": "160 Spear St, San Francisco, CA 94105"
        },
        {
            "event_type": "delivered", 
            "ts": "2024-01-15T10:25:00Z",
            "delivery_time_minutes": 85
        }
    ]
}
```

## Performance and Scaling

### Streaming Watermarks
- **Purpose**: Handle late-arriving data in streaming aggregations
- **Configuration**: 3 hours for brand and location hourly tables
- **Impact**: Allows events up to 3 hours late to be processed correctly

### Partitioning Strategy
- **Time-based partitioning**: All fact tables partitioned by date/hour
- **Benefits**: Query pruning, parallel processing, maintenance efficiency
- **Partition columns**: `order_day`, `day`, `hour_ts`

### Approximate Aggregations
- **HyperLogLog**: Used for distinct count estimates in streaming
- **Trade-off**: ~2% error rate for significant performance improvement
- **Use cases**: Order counts in brand and location aggregations

### Delta Lake Optimizations
- **Auto-compaction**: Enabled for all streaming tables
- **Z-ordering**: Applied to frequently queried columns
- **Vacuum**: Automated cleanup of old file versions
