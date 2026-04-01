"""
Canonical Dataset Replay Engine (Business-Agnostic)

Replays a pre-generated parquet dataset as streaming JSON events into a UC volume.
The only business-specific piece is the `transform_fn` that maps compact parquet
columns to the output event format.

Requirements:
- Canonical dataset as parquet with a `ts_seconds` column (Unix timestamp)
- A transform function: (DataFrame, time_shift: int) -> DataFrame
- Databricks runtime (dbutils, spark)

Usage:
    from replay_engine import replay

    def my_transform(df, time_shift):
        # Map compact parquet columns to output JSON format
        return df.withColumn("event_type", ...).withColumn("body", ...).select(...)

    replay(
        canonical_path="./canonical_dataset/events.parquet",
        transform_fn=my_transform,
        catalog="my_catalog",
        schema="simulator",
        volume="events",
        start_day=30,
        speed_multiplier=60.0,
        dataset_days=40,
        dataset_epoch_date=(2024, 1, 1),  # year, month, day the dataset starts
        entity_id_column="order_id",       # column to suffix on loops for uniqueness
    )
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
from typing import Callable, Tuple, Optional


def replay(
    canonical_path: str,
    transform_fn: Callable[[DataFrame, int], DataFrame],
    catalog: str,
    schema: str,
    volume: str,
    start_day: int = 30,
    speed_multiplier: float = 60.0,
    dataset_days: int = 40,
    dataset_epoch_date: Tuple[int, int, int] = (2024, 1, 1),
    entity_id_column: Optional[str] = None,
):
    """
    Replay canonical dataset as streaming JSON events.

    Args:
        canonical_path: Path to the canonical events parquet file (workspace-relative)
        transform_fn: Function that takes (spark_df, time_shift_seconds) and returns
                      the final DataFrame to write as JSON. This is where business-specific
                      column mapping, event_type naming, and body assembly happen.
        catalog: UC catalog name
        schema: UC schema name
        volume: UC volume name for output
        start_day: Which day of the dataset to start simulation from (0-based)
        speed_multiplier: How fast to replay (60.0 = 1 real minute → 60 sim minutes)
        dataset_days: How many days the canonical dataset covers
        dataset_epoch_date: (year, month, day) the canonical dataset starts from
        entity_id_column: Column name to suffix on loops for uniqueness (e.g., "order_id").
                          If None, no loop-suffixing is applied.
    """
    spark = SparkSession.getActiveSession()

    # Paths
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    watermark_path = f"/Volumes/{catalog}/{schema}/misc/_watermark"
    sim_start_path = f"/Volumes/{catalog}/{schema}/misc/_sim_start"

    # Constants
    dataset_epoch = int(datetime(*dataset_epoch_date).timestamp())
    cycle_seconds = dataset_days * 86400
    now = datetime.utcnow()

    print(f"Config: START_DAY={start_day}, SPEED={speed_multiplier}x")
    print(f"Output: {volume_path}")
    print(f"Dataset cycle: {dataset_days} days ({cycle_seconds} seconds)")

    # ── Load canonical dataset ──────────────────────────────────────────
    print("Loading canonical dataset...")
    events_pdf = pd.read_parquet(canonical_path)
    print(f"Loaded {len(events_pdf):,} events")

    # ── Read checkpoint state ───────────────────────────────────────────
    try:
        watermark_data = spark.read.text(watermark_path).first()[0]
        last_sim_seconds = int(watermark_data)
        is_first_run = False
        virtual_day = int((last_sim_seconds - dataset_epoch) / 86400)
        loop_index = int((last_sim_seconds - dataset_epoch) / cycle_seconds)
        print(f"Watermark: {last_sim_seconds} (virtual day {virtual_day}, loop {loop_index})")
    except Exception:
        last_sim_seconds = dataset_epoch
        is_first_run = True
        print("No watermark - first run")

    try:
        sim_start_data = spark.read.text(sim_start_path).first()[0]
        sim_start_time = datetime.fromisoformat(sim_start_data)
        print(f"Sim started: {sim_start_time.isoformat()}")
    except Exception:
        sim_start_time = now
        print(f"Establishing sim start: {sim_start_time.isoformat()}")

    # ── Calculate new position ──────────────────────────────────────────
    if is_first_run:
        current_tod = (now.hour * 3600) + (now.minute * 60) + now.second
        new_end_seconds = int(dataset_epoch + (start_day * 86400) + current_tod)
        print(f"\nFIRST RUN: day 0 -> day {start_day} @ {now.strftime('%H:%M:%S')}")
    else:
        elapsed_real = (now - sim_start_time).total_seconds()
        elapsed_sim = int(elapsed_real * speed_multiplier)

        sim_start_tod = (sim_start_time.hour * 3600) + (sim_start_time.minute * 60) + sim_start_time.second
        start_position = int(dataset_epoch + (start_day * 86400) + sim_start_tod)
        new_end_seconds = start_position + elapsed_sim

        print(f"\nSPEED MODE:")
        print(f"   Real elapsed: {elapsed_real:.0f}s ({elapsed_real/60:.1f} min)")
        print(f"   Sim elapsed: {elapsed_sim}s ({elapsed_sim/3600:.1f} hours)")

    start_virtual_day = int((last_sim_seconds - dataset_epoch) / 86400)
    end_virtual_day = int((new_end_seconds - dataset_epoch) / 86400)

    print(f"   Processing: virtual day {start_virtual_day} -> {end_virtual_day}")

    if new_end_seconds <= last_sim_seconds:
        print("No new data")
        return

    # ── Filter events across loop boundaries ────────────────────────────
    segments = []
    start_loop = int((last_sim_seconds - dataset_epoch) / cycle_seconds)
    end_loop = int((new_end_seconds - dataset_epoch) / cycle_seconds)

    for loop_idx in range(start_loop, end_loop + 1):
        loop_start_virtual = dataset_epoch + (loop_idx * cycle_seconds)
        loop_end_virtual = loop_start_virtual + cycle_seconds

        segment_start_virtual = max(last_sim_seconds, loop_start_virtual)
        segment_end_virtual = min(new_end_seconds, loop_end_virtual)

        if segment_end_virtual <= segment_start_virtual:
            continue

        segment_start_dataset = dataset_epoch + (segment_start_virtual - loop_start_virtual)
        segment_end_dataset = dataset_epoch + (segment_end_virtual - loop_start_virtual)

        segment_pdf = events_pdf[
            (events_pdf["ts_seconds"] > segment_start_dataset)
            & (events_pdf["ts_seconds"] <= segment_end_dataset)
        ].copy()

        if segment_pdf.empty:
            continue

        # Monotonically increasing virtual timestamps across loops
        segment_pdf["virtual_ts_seconds"] = segment_pdf["ts_seconds"] + (loop_idx * cycle_seconds)

        # Suffix entity IDs to avoid collision across loops
        if entity_id_column and loop_idx > 0:
            segment_pdf[entity_id_column] = (
                segment_pdf[entity_id_column].astype(str) + f"-L{loop_idx}"
            )

        segments.append(segment_pdf)

    if not segments:
        print("No events in window")
        return

    new_events_pdf = pd.concat(segments, ignore_index=True)
    event_count = len(new_events_pdf)
    print(f"Processing {event_count:,} events")

    new_events = spark.createDataFrame(new_events_pdf)

    # ── Apply business-specific transform ───────────────────────────────
    today_midnight = datetime(now.year, now.month, now.day)
    dataset_day_0 = today_midnight - timedelta(days=start_day)
    time_shift = int((dataset_day_0 - datetime(*dataset_epoch_date)).total_seconds())

    final_df = transform_fn(new_events, time_shift)
    print("Transformed")

    # ── Write output + update checkpoint ────────────────────────────────
    final_df.write.mode("append").json(volume_path)
    print(f"Wrote {event_count:,} events")

    spark.createDataFrame([(str(new_end_seconds),)], ["value"]).write.mode("overwrite").text(
        watermark_path
    )
    end_loop = int((new_end_seconds - dataset_epoch) / cycle_seconds)
    print(f"Watermark: {new_end_seconds} (virtual day {end_virtual_day}, loop {end_loop})")

    if is_first_run:
        spark.createDataFrame([(sim_start_time.isoformat(),)], ["value"]).write.mode(
            "overwrite"
        ).text(sim_start_path)
        print("Saved sim start time")

    print("\nComplete!")
