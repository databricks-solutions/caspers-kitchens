# Casper's Kitchens - Canonical Dataset & Streaming Replay

This directory contains the **canonical dataset** and **streaming replay system** for Casper's Kitchens ghost kitchen simulation.

## Overview

The canonical dataset replaces the previous long-running generator notebook with:
1. **Pre-generated 90-day dataset** (34.5 MB) that can be shipped statically
2. **Custom PySpark Streaming data source** that replays events at configurable speeds
3. **Checkpoint-based state management** for resumable, scheduled streaming

This approach provides:
- ✅ **Reliability** - No more dying generators that can't restart
- ✅ **Flexibility** - Start at any day, run at any speed (1x, 60x, 3600x)
- ✅ **Portability** - Single 34.5 MB file, easy to ship and deploy
- ✅ **Reproducibility** - Same dataset across all environments

---

## Directory Structure

```
canonical/
├── README.md                           # This file
├── regenerate.sh                       # One-shot regen helper (venv + run)
├── generate_canonical_dataset.py       # Offline dataset generator
├── update_locations_parquet.py         # Add/edit locations (dim parquet only)
├── caspers_data_source.py              # Standalone Python streaming source
├── caspers_streaming_notebook.py       # Databricks notebook version
└── canonical_dataset/
    ├── events.parquet                  # All events, orders + driver pings
    ├── locations.parquet               # Ghost kitchen locations (4 US + 4 EMEA)
    ├── brands.parquet                  # 24 food brands (4 KB)
    ├── brand_locations.parquet         # Brand availability by city/time
    ├── categories.parquet              # 112 menu categories (4 KB)
    ├── menus.parquet                   # 24 menus (3 KB)
    └── items.parquet                   # 181 menu items with prices (9 KB)
```

---

## Quick Start

### Using the Streaming Notebook (Databricks)

1. **Upload dataset to DBFS:**
   ```bash
   # Upload canonical_dataset/ directory to DBFS
   databricks fs cp -r canonical_dataset/ dbfs:/caspers/canonical_dataset/
   ```

2. **Import notebook:**
   - Upload `caspers_streaming_notebook.py` to Databricks workspace

3. **Configure parameters:**
   ```python
   DATASET_PATH = "/dbfs/caspers/canonical_dataset"
   START_DAY = 70          # Day 0-89
   SPEED_MULTIPLIER = 60.0 # 60x realtime
   ```

4. **Schedule the notebook:**
   - Use `trigger(availableNow=True)` (already configured)
   - Schedule at any interval (5 min, 10 min, etc.)
   - Each run processes events based on elapsed time

### Local Testing

```bash
# Test locally with PySpark
python3 caspers_data_source.py
```

---

## Dataset Details

### Time Period
- **Start:** January 1, 2024 00:00:00
- **End:** March 30, 2024 23:59:59
- **Duration:** 90 days
- **Total Orders:** 75,780
- **Total Events:** 1,014,290

### Cities & Narratives

US (original 4):
| City | Location ID | Growth | Daily Orders | Narrative |
|------|-------------|--------|--------------|-----------|
| **San Francisco** | 1 | +74% | 171 → 297 | Health-focused brands, steady growth |
| **Silicon Valley** | 2 | +190% | 53 → 155 | Tech startup culture, explosive growth, late-night spike |
| **Bellevue** | 3 | +2% (flat) | 230 → 234 | Suburban comfort food, stagnant |
| **Chicago** | 4 | -25% | 318 → 240 | Legacy operation, declining quality |

EMEA (added for operational dashboard demo):
| City | Location ID | Growth | Daily Orders | Notes |
|------|-------------|--------|--------------|-------|
| **London** | 5 | +0.3%/day | ~22 baseline | Shoreditch, UK |
| **Munich** | 6 | +0.2%/day | ~18 baseline | Schwabing, Germany |
| **Amsterdam** | 7 | +0.25%/day | ~20 baseline | City center, Netherlands |
| **Vianen** | 8 | +0.4%/day | ~10 baseline | Smallest, fastest growth |

EMEA locations all inherit the location-1 (SF) brand mix in `brand_locations.parquet`. To customise per-city, edit `update_locations_parquet.py` (or hand-edit `brand_locations.parquet` after regen).

### Event Types

| Event | Count | % | Description |
|-------|------:|--:|-------------|
| `order_created` | 75,780 | 7.5% | New order placed with customer location + items |
| `gk_started` | 75,780 | 7.5% | Kitchen starts preparing food |
| `gk_finished` | 75,780 | 7.5% | Food preparation complete |
| `gk_ready` | 75,780 | 7.5% | Food packaged and ready for pickup |
| `driver_arrived` | 75,780 | 7.5% | Driver arrives at kitchen |
| `driver_picked_up` | 75,780 | 7.5% | Driver picks up food (includes full GPS route) |
| `driver_ping` | ~640K | 63% | Driver location updates every 60 seconds |
| `delivered` | 75,780 | 7.5% | Order delivered to customer |

**Average:** ~13.4 events per order

### Event Schema

```json
{
  "event_id": "a7f2c9e1-...",      // UUID
  "event_type": "order_created",    // One of 8 types above
  "ts": "2024-01-01 12:34:56.789",  // ISO timestamp
  "location_id": 1,                 // 1=SF, 2=SV, 3=Bellevue, 4=Chicago
  "order_id": "A7K2M9",             // Random 6-char alphanumeric
  "sequence": 0,                    // Event order within order lifecycle
  "body": "{...}"                   // JSON string, varies by event type
}
```

### Event Body Examples

**order_created:**
```json
{
  "customer_lat": 37.7499,
  "customer_lon": -122.3924,
  "customer_addr": "3612 Main St",
  "items": [
    {"id": 155, "name": "Thai Peanut Bowl", "price": 12.99, "qty": 1},
    {"id": 158, "name": "Build Your Own Bowl", "price": 10.5, "qty": 1}
  ]
}
```

**driver_picked_up:**
```json
{
  "route_points": [
    [37.7910, -122.3929],  // Kitchen location
    [37.7905, -122.3935],
    ...                    // Real OSM street coordinates
    [37.7500, -122.4406]   // Customer location
  ]
}
```

**driver_ping:**
```json
{
  "progress_pct": 42.5,
  "loc_lat": 37.7800,
  "loc_lon": -122.4100
}
```

**delivered:**
```json
{
  "delivered_lat": 37.7500,
  "delivered_lon": -122.4406
}
```

---

## Streaming Replay System

### How It Works

The streaming system uses **checkpoint-based replay** with configurable speed and infinite looping:

1. **First Run (Historical Catchup):**
   - Outputs ALL data from day 0 → START_DAY @ current time
   - Example: `START_DAY=70` at 21:00 → outputs 70 days + 21 hours
   - Speed multiplier NOT used (just dump historical data)

2. **Subsequent Runs (Live Streaming):**
   - Uses speed multiplier to advance simulation time
   - Formula: `new_sim_time = start_position + (current_time - sim_start_time) × speed_multiplier`
   - Example: 5 min elapsed × 60x = 5 hours of simulation time

3. **Looping Behavior (Infinite Replay):**
   - When replay passes day 89, it wraps to day 0 and continues
   - Internal simulation time remains monotonic (virtual timeline)
   - Emitted `ts` values continue increasing across loops
   - `order_id` is suffixed on looped cycles (`-L1`, `-L2`, ...) to avoid key collisions

### Configuration Parameters

| Parameter | Description | Example Values |
|-----------|-------------|----------------|
| `datasetPath` | Path to canonical_dataset directory | `/dbfs/caspers/canonical_dataset` |
| `simulationStartDay` | Which day to start (0-89) | `70` (day 70 = early March) |
| `speedMultiplier` | Replay speed (1.0 = realtime) | `60.0` (1 real min = 1 sim hour) |

### Speed Multiplier Examples

| Multiplier | Real Time | Simulation Time | Use Case |
|------------|-----------|-----------------|----------|
| `1.0` | 1 second | 1 second | True realtime replay |
| `60.0` | 1 minute | 1 hour | Good default for demos (recommended) |
| `3600.0` | 1 second | 1 hour | Fast replay for testing |
| `86400.0` | 1 second | 1 day | Very fast (process 90 days in 90 seconds) |

### Checkpoint Structure

The replay job stores:
- `_watermark`: last processed **virtual** simulation second (monotonic)
- `_sim_start`: wall-clock start time for replay-speed math

Each run calculates:
```python
elapsed_real = current_time - sim_start_time
elapsed_sim = elapsed_real × speed_multiplier
new_position = start_position + elapsed_sim
```

### Scheduling Patterns

**Works with ANY interval:**
- ✅ Regular (every 5 minutes)
- ✅ Irregular (random intervals)
- ✅ Manual (run on-demand)

Each run automatically processes the correct amount based on elapsed time.

---

## Dataset Generation

### One-shot regen (recommended)

```bash
cd data/canonical
./regenerate.sh
```

This creates a local `.venv`, installs deps, runs `update_locations_parquet.py`,
then `generate_canonical_dataset.py`. Takes **~15–25 min for 8 cities** (longer
than the original ~10 min for 4 because of the EMEA OSM downloads), and needs
internet to fetch OSM road networks via `osmnx`.

When done, commit the updated parquets:

```bash
git add data/canonical/canonical_dataset/locations.parquet \
        data/canonical/canonical_dataset/brand_locations.parquet \
        data/canonical/canonical_dataset/events.parquet
git commit -m "regenerate canonical dataset"
```

### Manual two-step (advanced)

```bash
cd data/canonical
pip install pandas pyarrow osmnx networkx geopy

# 1. Add/edit locations (only updates dim parquets)
python3 update_locations_parquet.py

# 2. Regenerate events for ALL locations present in locations.parquet
python3 generate_canonical_dataset.py
```

### What the generator does

1. Load dimension tables (brands, locations, items)
2. Download/cache OpenStreetMap road networks (one cache file per city, `cache_<code>_graph.pkl`)
3. Generate 90 days of orders with realistic demand patterns per location
4. Calculate real shortest-path routes via OSM + NetworkX
5. Output compact `events.parquet`

### Adding a new location

1. Edit `NEW_LOCATIONS` in `update_locations_parquet.py` (set a fresh `location_id`).
2. Run `./regenerate.sh`.
3. Commit the updated parquet files.

That's the only step required — `stages/canonical_data.ipynb` loads whatever is in `locations.parquet` and the lakeflow pipeline will naturally produce silver/gold rows for every city present in `events.parquet`.

### Data Features

✅ **Real Routes:** Uses OpenStreetMap road networks via `osmnx`
- Actual street coordinates from real road graphs
- Shortest path calculations with realistic drive times
- 30-79 GPS points per route

✅ **Realistic Patterns:**
- Lunch peak (11am-1:30pm, 3x baseline)
- Dinner peak (5pm-8pm, 3.5x baseline)
- Late-night spike in Silicon Valley (9pm-1am, 2x)
- Weekend boost (+20-35% Sat/Sun)
- Day-of-week variation (Monday 1.0x → Saturday 1.35x)

✅ **Growth Models:**
- Exponential growth/decline per location
- Brand launches/exits at specific days
- NootroNourish brand added at day 45 in SV
- Five Gals brand exits at day 60 in Chicago

✅ **Compact Storage:**
- Optimized dtypes (int8, int16, int32, float32)
- Absolute Unix timestamps (no offsets to reconstruct)
- All data embedded in single events.parquet
- JSON stored as strings for routes/items

---

## Architecture Decisions

### Why Pre-Generated Dataset?

**Problem:** Original generator.ipynb notebook runs forever and eventually dies, can't restart mid-simulation.

**Solution:**
1. Generate dataset offline once
2. Ship as static 34.5 MB file
3. Replay on-demand at any speed

### Why Custom Data Source?

**Alternatives considered:**
- ❌ **Read entire file, filter by time:** Doesn't simulate streaming, no state management
- ❌ **Rate limiter on static data:** Complex, doesn't handle restarts well
- ✅ **Custom PySpark DataSource:** Built-in checkpoint, exactly-once semantics, clean API

### Why Checkpoint-Based State?

**Alternatives considered:**
- ❌ **Metadata files:** Complex to manage, prone to sync issues
- ❌ **Watermarks:** Doesn't fit the time-multiplier model
- ✅ **Offset in checkpoint:** Built-in Spark mechanism, reliable, automatic

### Schema Decisions

**Removed:**
- `gk_id` - Was randomly generated UUID with no meaning

**Changed:**
- `location_code` → `location_id` - Use integer ID, resolve to name downstream in demos
- `order_id` - Changed from sequential integers (1, 2, 3...) to random 6-char alphanumeric (A7K2M9, P3X8Q1)

**Why?**
- More realistic (real order IDs aren't sequential)
- Easier to resolve location data in downstream Databricks notebooks
- Cleaner schema for demos

---

## Output Format

### Multi-Line JSON (JSONL)

Output is **newline-delimited JSON** (one event per line):

```json
{"event_id":"7bf5fc...","event_type":"order_created","ts":"2024-01-01 00:01:14.000","location_id":1,"order_id":"BRPOIG","sequence":0,"body":"{...}"}
{"event_id":"30145a...","event_type":"gk_started","ts":"2024-01-01 00:03:00.000","location_id":1,"order_id":"BRPOIG","sequence":1,"body":"{}"}
...
```

**Why JSONL?**
- Easy to parse line-by-line
- Standard format for streaming logs
- Compatible with Spark, Python, most data tools

---

## Testing & Validation

### Local Tests

```bash
# Quick test (3600x speed for fast validation)
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
python3 -c "
from pyspark.sql import SparkSession
from caspers_data_source import CaspersDataSource

spark = SparkSession.builder.appName('Test').master('local[*]').getOrCreate()
spark.dataSource.register(CaspersDataSource)

caspers_stream = spark.readStream \
    .format('caspers') \
    .option('datasetPath', './canonical_dataset') \
    .option('simulationStartDay', '0') \
    .option('speedMultiplier', '3600.0') \
    .load()

query = caspers_stream.writeStream.format('console').option('numRows', 5).start()
query.awaitTermination(timeout=15)
query.stop()
"
```

### Validation Scripts

Additional test scripts in parent directory:
- `test_caspers_stream.py` - Basic 30-second stream test
- `validate_stream.py` - Captures output and validates completeness
- `test_chaotic_schedule.py` - Tests irregular scheduling intervals
- `visualize_routes.py` - Creates interactive map of delivery routes

---

## Performance

### Dataset Size

| File | Size | Rows | Description |
|------|-----:|-----:|-------------|
| events.parquet | 34.5 MB | 1,014,290 | All events (self-contained) |
| locations.parquet | 6 KB | 4 | Ghost kitchen locations |
| brands.parquet | 4 KB | 24 | Food brands |
| items.parquet | 9 KB | 181 | Menu items |
| **Total** | **~35 MB** | | |

### Streaming Performance

- **First run:** Processes 70+ days in ~15 seconds (776K events)
- **Subsequent runs:** <5 seconds per batch (60x speed, 5-min trigger)
- **Memory:** ~100 MB for pandas DataFrame loading
- **CPU:** Minimal (filtering + JSON expansion)

---

## Integration with Existing Demos

This canonical dataset is designed to **replace** the existing generator notebook while maintaining compatibility:

### What Changes

1. **No more long-running generator**
   - Upload static dataset once
   - Schedule replay notebook

2. **Different event format**
   - `location_id` instead of `location_code`
   - Random `order_id` instead of sequential
   - No `gk_id` field

### What Stays the Same

- Same 8 event types
- Same event body structures
- Same 4 cities with same narratives
- Same brands, items, menus

### Migration Path

1. Upload `canonical_dataset/` to DBFS
2. Import `caspers_streaming_notebook.py`
3. Update downstream queries to:
   - Join `location_id` with `locations` table for location names
   - Handle string `order_id` (was int)
   - Remove references to `gk_id`

---

## Troubleshooting

### "Unable to locate a Java Runtime"

```bash
# Install Java (macOS)
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"
```

### "module 'datetime' has no attribute 'timedelta'"

Ensure imports include `timedelta`:
```python
from datetime import datetime, timedelta
```

### "partitions() not implemented"

Use `simpleStreamReader()` not `streamReader()` - already configured.

### First run processes 0 events

Check:
1. `simulationStartDay` is valid (0-89)
2. Dataset path is correct
3. Current time of day - if very early (e.g., 00:05), may have few events

### Data not advancing

Check checkpoint location - might be reusing old checkpoint. Delete checkpoint directory to restart.

---

## Future Enhancements

Possible additions:
- [ ] Support for multiple datasets (different scenarios)
- [ ] Dynamic speed adjustment mid-stream
- [ ] Pause/resume functionality
- [ ] Replay from specific timestamp
- [ ] Generate datasets with different time periods (180 days, 1 year)

---

## Technical Notes

### Why Pandas in Data Source?

Using `pd.read_parquet()` instead of `spark.read.parquet()` avoids circular dependency when loading data inside the custom data source.

### Why Unix Timestamps?

Absolute Unix timestamps simplify comparisons and avoid complex offset arithmetic. Dataset epoch is 2024-01-01 00:00:00.

### Why Single events.parquet?

Self-contained file eliminates need for joins during streaming, simplifying the code and improving performance.

---

## Credits

**Dataset Generation:**
- OpenStreetMap for road networks (via `osmnx`)
- Real GPS coordinates from OSM road graphs
- Realistic demand patterns modeled after food delivery data

**Technology:**
- PySpark Structured Streaming
- Pandas for compact data generation
- NetworkX for shortest path calculations

---

## License

© 2025 Databricks, Inc. Part of the Casper's Kitchens demo platform.
