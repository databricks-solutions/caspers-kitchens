"""
Casper's Kitchens Custom Streaming Data Source
Minimal implementation for local testing
"""

from pyspark.sql.datasource import DataSource, DataSourceStreamReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime, timedelta
import pandas as pd
import json


class CaspersDataSource(DataSource):
    """
    Custom streaming data source that replays pre-generated Casper's Kitchens events.

    Options:
    - datasetPath: Path to canonical_dataset directory
    - simulationStartDay: Which day to start simulation (0-89)
    - speedMultiplier: Speed of replay (1.0=realtime, 60.0=60x speed)
    """

    @classmethod
    def name(cls):
        return "caspers"

    def schema(self):
        """Define the output schema for events."""
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("ts", StringType(), False),
            StructField("location_id", IntegerType(), False),
            StructField("order_id", StringType(), False),
            StructField("sequence", IntegerType(), False),
            StructField("body", StringType(), False),
        ])

    def simpleStreamReader(self, schema: StructType):
        """Return a simple stream reader instance (no partitioning needed)."""
        return CaspersStreamReader(schema, self.options)


class CaspersStreamReader(DataSourceStreamReader):
    """
    Stream reader that tracks simulation time as offset.

    Offset = simulation time in seconds (int)
    """

    def __init__(self, schema, options):
        self.schema = schema
        self.options = options

        # Parse options
        self.dataset_path = options.get("datasetPath", "./canonical_dataset")
        self.sim_start_day = int(options.get("simulationStartDay", "70"))
        self.speed_multiplier = float(options.get("speedMultiplier", "1.0"))

        # Load canonical dataset using pandas (NOT spark.read!)
        print(f"📦 Loading dataset from {self.dataset_path}...")
        self.events_df = pd.read_parquet(f"{self.dataset_path}/events.parquet")

        unique_orders = self.events_df['order_id'].nunique()
        print(f"✅ Caspers DataSource initialized")
        print(f"   Orders: {unique_orders:,}")
        print(f"   Events: {len(self.events_df):,}")
        print(f"   Start Day: {self.sim_start_day}")
        print(f"   Speed: {self.speed_multiplier}x")

    def initialOffset(self):
        """
        Return the starting offset for the stream.
        First run outputs all historical data from day 0 to START_DAY + current time.
        Speed multiplier not used for first run - just historical catchup.
        """
        now = datetime.utcnow()
        # Dataset starts at 2024-01-01 00:00:00
        dataset_epoch = datetime(2024, 1, 1).timestamp()

        # Start from beginning of dataset (day 0)
        initial_unix_ts = int(dataset_epoch)

        initial = {
            "simulation_seconds": initial_unix_ts,
            "offset_timestamp": now.isoformat(),
            "is_initial": True  # Flag to indicate this is the first run
        }

        print(f"🎬 Initial offset: day 0 (dataset start)")
        print(f"   First run will output all data from day 0 → day {self.sim_start_day} @ {now.strftime('%H:%M:%S')}")
        print(f"   Speed multiplier ({self.speed_multiplier}x) will be used for subsequent runs")
        return json.dumps(initial)

    def latestOffset(self):
        """
        Return the current offset based on time elapsed since last checkpoint.
        Each run processes: (current_time - last_offset_time) * speed_multiplier
        """
        # This gets called during streaming to determine end offset
        # We need to calculate based on elapsed time, but we don't have access
        # to the checkpoint here. The read() method will handle the actual logic.
        # For latestOffset, we just need to return a reasonable "current" value.

        # Return far future to let read() handle the actual window calculation
        now = datetime.utcnow()
        # End of dataset: 2024-01-01 + 90 days
        dataset_epoch = datetime(2024, 1, 1).timestamp()
        end_unix_ts = int(dataset_epoch + (90 * 86400))

        latest = {
            "simulation_seconds": end_unix_ts,  # End of dataset as Unix timestamp
            "offset_timestamp": now.isoformat()
        }
        return json.dumps(latest)

    def read(self, start_offset):
        """
        Read events from start_offset to current time.
        First run: Output all data from day 0 to START_DAY + current time (no multiplier).
        Subsequent runs: Use speed multiplier to advance simulation time.
        For simpleStreamReader, this returns (iterator, end_offset).
        """
        # Parse start offset
        start = json.loads(start_offset) if start_offset else {"simulation_seconds": 0, "offset_timestamp": datetime.utcnow().isoformat()}
        start_sim_seconds = start["simulation_seconds"]
        start_real_time = datetime.fromisoformat(start["offset_timestamp"])
        is_initial = start.get("is_initial", False)

        # Calculate elapsed real time since last checkpoint
        now = datetime.utcnow()
        dataset_epoch = datetime(2024, 1, 1).timestamp()

        if is_initial:
            # First run: Just output all historical data up to START_DAY + current time
            current_time_of_day = (now.hour * 3600) + (now.minute * 60) + now.second
            end_sim_seconds = int(dataset_epoch + (self.sim_start_day * 86400) + current_time_of_day)
            print(f"📖 Reading events (FIRST RUN - historical catchup):")
            print(f"   Start: day 0 00:00:00")
            print(f"   End:   day {self.sim_start_day} @ {now.strftime('%H:%M:%S')}")
            print(f"   Outputting all historical data (speed multiplier not used)")
        else:
            # Subsequent runs: Use speed multiplier
            elapsed_real_seconds = (now - start_real_time).total_seconds()
            elapsed_sim_seconds = int(elapsed_real_seconds * self.speed_multiplier)
            end_sim_seconds = start_sim_seconds + elapsed_sim_seconds

            start_day = int((start_sim_seconds - dataset_epoch) / 86400)
            end_day = int((end_sim_seconds - dataset_epoch) / 86400)

            print(f"📖 Reading events:")
            print(f"   Start: {start_sim_seconds} Unix timestamp (day {start_day})")
            print(f"   End:   {end_sim_seconds} Unix timestamp (day {end_day})")
            print(f"   Real time elapsed: {elapsed_real_seconds:.1f}s → Sim time: {elapsed_sim_seconds}s ({self.speed_multiplier}x)")

        # Cap at end of dataset (2024-01-01 + 90 days)
        max_sim_seconds = int(dataset_epoch + (90 * 86400))
        end_sim_seconds = min(end_sim_seconds, max_sim_seconds)

        # Filter events to time window (ts_seconds is already absolute)
        windowed_events = self.events_df[
            (self.events_df["ts_seconds"] >= start_sim_seconds) &
            (self.events_df["ts_seconds"] < end_sim_seconds)
        ].copy()

        print(f"   Found {len(windowed_events)} events in window")

        # Expand to full JSON format
        expanded_rows = self._expand_to_json(windowed_events)

        print(f"   ✅ Returning {len(expanded_rows)} events")

        # Convert to Spark Row objects
        from pyspark.sql import Row
        rows = [Row(**row) for row in expanded_rows]

        # Create end offset with current timestamp (remove is_initial flag)
        end_offset_dict = {
            "simulation_seconds": end_sim_seconds,
            "offset_timestamp": now.isoformat()
            # is_initial removed - subsequent runs will use speed multiplier
        }

        # Return iterator and end offset (format for simpleStreamReader)
        return (iter(rows), json.dumps(end_offset_dict))

    def commit(self, end_offset):
        """
        Commit is handled by Spark's checkpoint.
        No-op for our use case.
        """
        end = json.loads(end_offset)
        dataset_epoch = datetime(2024, 1, 1).timestamp()
        day_num = int((end['simulation_seconds'] - dataset_epoch) / 86400)
        print(f"✓ Committed up to {end['simulation_seconds']} Unix timestamp (day {day_num})")

    def _expand_to_json(self, windowed_events_df):
        """
        Expand compact parquet format to full JSON event format.
        Data is already embedded in events, just need to format.
        Uses pandas, returns list of dicts.
        """
        import uuid

        # Event type mapping
        event_types = {
            1: "order_created",
            2: "gk_started",
            3: "gk_finished",
            4: "gk_ready",
            5: "driver_arrived",
            6: "driver_picked_up",
            7: "driver_ping",
            8: "delivered"
        }

        rows = []

        for _, row in windowed_events_df.iterrows():
            event_type = event_types[row["event_type_id"]]

            # Build body based on event type (data already embedded in parquet)
            body = {}
            if event_type == "order_created":
                body = {
                    "customer_lat": float(row["customer_lat"]),
                    "customer_lon": float(row["customer_lon"]),
                    "customer_addr": row["customer_addr"],
                    "items": json.loads(row["items_json"])
                }
            elif event_type == "driver_picked_up":
                if pd.notna(row["route_json"]):
                    body = {
                        "route_points": json.loads(row["route_json"])
                    }
            elif event_type == "driver_ping":
                if pd.notna(row["ping_lat"]):
                    body = {
                        "progress_pct": float(row["ping_progress"]),
                        "loc_lat": float(row["ping_lat"]),
                        "loc_lon": float(row["ping_lon"])
                    }
            elif event_type == "delivered":
                if pd.notna(row["customer_lat"]):
                    body = {
                        "delivered_lat": float(row["customer_lat"]),
                        "delivered_lon": float(row["customer_lon"])
                    }

            # Build event record
            event_record = {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "ts": datetime.fromtimestamp(row["ts_seconds"]).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "location_id": int(row["location_id"]),
                "order_id": str(row["order_id"]),
                "sequence": int(row["sequence"]),
                "body": json.dumps(body)
            }

            rows.append(event_record)

        # Sort by simulation time and sequence
        rows.sort(key=lambda x: (x["ts"], x["sequence"]))

        return rows


if __name__ == "__main__":
    print("Casper's Kitchens Data Source - Test Mode")
    print("=" * 60)

    # Test the reader directly (without Spark streaming)
    print("\n🧪 Testing CaspersStreamReader...")

    options = {
        "datasetPath": "./canonical_dataset",
        "simulationStartDay": "70",
        "speedMultiplier": "60.0"  # 60x speed for testing
    }

    # Create reader
    reader = CaspersStreamReader(None, options)

    # Get initial offset
    initial = reader.initialOffset()
    print(f"\nInitial offset: {initial}")

    # Simulate waiting 1 second (= 60 seconds of sim time at 60x speed)
    import time
    print("\n⏳ Waiting 1 second (= 60 sim seconds at 60x speed)...")
    time.sleep(1)

    # Get latest offset
    latest = reader.latestOffset()
    print(f"Latest offset: {latest}")

    # Read events between offsets
    events = reader.read(initial, latest)

    print(f"\n📊 Retrieved {len(events)} events")
    if events:
        print("\nFirst event:")
        first = events[0].asDict() if hasattr(events[0], 'asDict') else events[0]
        for k, v in first.items():
            if k == "body":
                print(f"  {k}: {v[:100]}..." if len(str(v)) > 100 else f"  {k}: {v}")
            else:
                print(f"  {k}: {v}")

    # Commit
    reader.commit(latest)

    print("\n✅ Test complete!")
