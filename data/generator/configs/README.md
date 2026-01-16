# Branch Location Configuration Guide

This directory contains JSON configuration files that define bank branch locations for simulation. Each JSON file represents one branch location and its simulation parameters.

## Quick Start

**To add a new branch location:**
1. Copy `sanfrancisco.json` as your template
2. Update the location-specific parameters (see below)
3. Save with a descriptive name (e.g., `chicago.json`, `nyc.json`)

**To select which branch locations to run:**

By default, only San Francisco runs when you execute the Casper's Initializer job. You can control which locations are generated using the `LOCATIONS` parameter in the job. Set this parameter in the **Casper's Initializer job UI** under Parameters when running the job.

Examples:
- `sanfrancisco.json` - Run only San Francisco (default)
- `chicago.json` - Run only Chicago
- `nyc.json` - Run only your custom NYC config
- `sanfrancisco.json,chicago.json` - Run multiple locations (comma-separated)
- `all` - Run all available location configs in this directory

This works for any `.json` files you create in this directory - just specify the filename(s) in the `LOCATIONS` parameter.

**Important:** All `.json` files in this directory are discovered automatically, but only those specified in the `LOCATIONS` parameter will run. Use a different extension (e.g., `.json.template`) if you want example configs that shouldn't be available for selection.

## Configuration Parameters

### Simulation Window

Controls the time range and speed of data generation:

- **`start_days_ago`** (integer): Number of days of historical data to generate
  - Example: `3` starts the simulation 3 days in the past
  - Historical data is generated in one batch at startup

- **`end_days_ahead`** (integer): Number of days into the future to simulate
  - Example: `362` runs for approximately 1 year
  - Set to `0` for historical data only (no ongoing simulation)

- **`speed_up`** (number): Time acceleration multiplier
  - `1` = real-time (1 simulated hour = 1 real hour)
  - `60` = 1 hour of orders generated per real minute
  - `1000` = ~6 weeks compressed into 1 real hour
  - Higher values = faster data generation, useful for creating static datasets quickly

### Transaction Volume

Controls how many transactions are generated over time:

- **`orders_day_1`** (number): Transactions per day at the start of simulation
  - This is the baseline - actual volume varies by day of week and noise

- **`orders_last`** (number): Transactions per day at the end of simulation
  - Volume grows linearly from `orders_day_1` to `orders_last`
  - Set equal to `orders_day_1` for constant volume

- **`noise_pct`** (number): Random daily variation percentage
  - Example: `10` adds ±10% random noise to daily transaction volume
  - Applied via `random.uniform(1-noise, 1+noise)`

**Day of week multipliers** (hardcoded in generator):
- Monday: 1.0x, Tuesday: 1.05x, Wednesday: 1.08x, Thursday: 1.10x
- Friday: 1.25x, Saturday: 1.35x, Sunday: 1.15x

**Intraday patterns** (hardcoded):
- Morning peak: 11:00-13:30 (3x baseline)
- Evening peak: 17:00-20:00 (3.5x baseline)

### Processing Times

The `svc` object defines timing for each stage of transaction processing. Each stage is specified as `[mean, std_dev]` in minutes, using a Gaussian distribution (minimum 0.1 minutes):

- **`cs`**: Transaction initiated → Authorization begins
  - Queue time before authorization starts

- **`sf`**: Authorization starts → Authorization completes
  - Actual authorization processing time

- **`fr`**: Authorization completes → Ready for settlement
  - Validation and staging time

- **`rp`**: Ready for settlement → Processing begins
  - Time between ready and actual settlement processing

Example:
```json
"svc": {
  "cs": [2, 1],   // 2 min average queue, 1 min std dev
  "sf": [10, 3],  // 10 min authorization time, 3 min variation
  "fr": [2, 1],   // 2 min validation
  "rp": [6, 2]    // 6 min settlement wait
}
```

These times generate the following events per transaction:
1. `order_created` (transaction initiated at t=0)
2. `gk_started` (authorization begins after cs delay)
3. `gk_finished` (authorization completes after sf delay)
4. `gk_ready` (ready for settlement after fr delay)
5. `driver_arrived` (processor engaged, sampled between initiation and settlement)
6. `driver_picked_up` (settlement begins after rp delay from ready)
7. `driver_ping` events (during settlement, frequency controlled by `ping_sec`)
8. `delivered` (transaction settled after processing time based on network routing and `driver_mph`)

### Processor Behavior

Controls when payment processors engage and how they route transactions:

- **`driver_arrival`** (object): Timing of processor engagement relative to transaction being ready
  - **`alpha`** and **`beta`**: Shape parameters for Beta distribution
    - Default `[3, 3]` creates a symmetric bell curve
  - **`after_ready_pct`**: Probability processor engages after transaction is ready
    - Example: `0.5` means 50% of processors engage after transaction is ready
    - The other 50% engage between transaction initiation and ready time
  - Processor engagement time is sampled from a Beta distribution within these windows

- **`driver_mph`** (number): Average processing speed for network routing calculations
  - Used to calculate settlement time from network route distance
  - Example: `25` represents relative processing speed through the payment network

### Merchant Dynamics

Controls how merchant/partner transaction volumes change over time:

- **`brand_momentum`** (object): Distribution of merchants across performance categories
  - **`improving`**: Fraction of merchants with growing transaction volumes
  - **`flat`**: Fraction with stable transaction volumes
  - **`declining`**: Fraction with declining transaction volumes
  - Must sum to 1.0
  - Example: `{"improving": 0.1, "flat": 0.2, "declining": 0.7}`
    - 10% of merchants improving, 20% flat, 70% declining

- **`momentum_rates`** (object): Rate of change for non-flat merchants
  - **`growth`**: Daily compound growth rate for improving merchants
    - Example: `0.8` = 0.8% daily growth
  - **`decline`**: Daily compound decline rate for declining merchants
    - Example: `0.2` = 0.2% daily decline
  - Rates are compounded monthly: `(1 ± rate)^(day/30)`

Merchant momentum affects transaction selection - improving merchants are more likely to be chosen for transactions.

### Location Details

Defines the bank branch's geographic location and service area:

- **`gk_location`** (string): Full address or intersection for the bank branch
  - Used by OpenStreetMap/OSMnx to download real road network
  - Examples:
    - `"5th Avenue & 42nd Street, New York, NY"`
    - `"1600 Amphitheatre Parkway, Mountain View, CA"`
  - Must be geocodable by OSMnx (uses Nominatim)

- **`location_name`** (string): Short identifier for this branch location
  - Appears in the `location` field of all events
  - Used for filtering/grouping in downstream analytics
  - Example: `"san_francisco"`, `"chicago_loop"`

- **`radius_mi`** (number): Service area radius in miles
  - Used to download road network around bank branch
  - Customers are sampled from nodes within this radius
  - Converted to meters (× 1609.34) for OSMnx

**Network routing details:**
- Graph is downloaded once at startup and cached
- Only nodes in the same connected component as the bank branch are used
- Routing uses NetworkX shortest path with real road lengths
- Customer addresses are matched to building footprints with real street addresses from OpenStreetMap

### Technical Parameters

Control batch writing and transaction status tracking granularity:

- **`batch_rows`** (integer): Number of events to accumulate before writing to volume
  - Higher values = fewer, larger files
  - Lower values = more frequent writes, more granular streaming

- **`batch_seconds`** (number): Maximum seconds to wait before flushing batch
  - Even if `batch_rows` isn't reached, batch is flushed after this time
  - Prevents delays during low-volume periods

- **`ping_sec`** (integer): Frequency of status updates during settlement (in seconds)
  - Example: `60` = one status ping per minute during settlement
  - Lower values = more granular transaction tracking, more events
  - Number of pings = `floor(settlement_time_minutes * 60 / ping_sec)`

- **`random_seed`** (integer): Seed for random number generation
  - Same seed + same config = identical simulation results
  - Useful for reproducible demos or testing
  - Seeds both Python's `random` and NumPy's `np.random`

### Data Quality Issues

Optional injection of data quality problems for testing:

- **`dq`** (object): Dictionary mapping event types to field corruption rates
  - Currently used by `maybe_corrupt()` function
  - Each field can have a probability of being set to `null`
  - Example: `{"order_created": {"customer_lat": 0.1}}` would null out latitude in 10% of order_created events
  - Usually left as empty object: `{}`

## Example: Creating a Chicago Branch

```json
{
  "start_days_ago": 1,
  "end_days_ahead": 30,
  "speed_up": 60,
  "orders_day_1": 75,
  "orders_last": 150,
  "noise_pct": 12,
  "svc": {
    "cs": [3, 1.5],
    "sf": [12, 4],
    "fr": [2, 1],
    "rp": [8, 3]
  },
  "driver_arrival": {"alpha": 3, "beta": 3, "after_ready_pct": 0.5},
  "brand_momentum": {"improving": 0.15, "flat": 0.25, "declining": 0.6},
  "momentum_rates": {"growth": 0.8, "decline": 0.2},
  "dq": {},
  "gk_location": "Michigan Avenue & Randolph Street, Chicago, IL",
  "location_name": "chicago",
  "radius_mi": 5,
  "driver_mph": 22,
  "batch_rows": 10,
  "batch_seconds": 1,
  "ping_sec": 60,
  "random_seed": 42
}
```

## Output Format

Events are written as JSON files to the configured volume:
- Location: `/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/`
- Format: `YYYYMMDD-HHMMSS.ffffff-{event_id}.json` (for real-time events)
- Format: `YYYYMMDD-HHMMSS.ffffff.json` (for batches)
- Each event has: `event_id`, `event_type`, `ts`, `gk_id`, `location`, `order_id`, `sequence`, `body`
- Body is JSON-encoded string with event-specific payload
