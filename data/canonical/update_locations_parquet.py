"""
Add EMEA locations (5-8) to locations.parquet and brand_locations.parquet.

Run this locally once whenever the location set changes:
    cd data/canonical
    python3 update_locations_parquet.py

This only updates the *dimension* parquets. To regenerate the matching
events.parquet (orders + driver pings + routes for these new cities) you
must then re-run `generate_canonical_dataset.py`.

Idempotent — safe to re-run; existing locations are skipped.
"""
import sys
from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:
    sys.exit("ERROR: pandas not installed. Run: pip install pandas pyarrow")

# Resolve paths relative to this script (so it works from any cwd)
HERE = Path(__file__).resolve().parent
LOCATIONS_PATH = HERE / "canonical_dataset" / "locations.parquet"
BRAND_LOCATIONS_PATH = HERE / "canonical_dataset" / "brand_locations.parquet"

# ── New locations to add ────────────────────────────────────────────────────
# Edit this list to add or change locations. `location_id` must be unique.
NEW_LOCATIONS = [
    {"location_id": 5, "name": "London",    "location_code": "lon",
     "lat": 51.5248, "lon": -0.0796,
     "address": "14 Curtain Road, Shoreditch, London EC2A 3NH, UK",
     "narrative": "growing",
     "base_orders_day": 22, "growth_rate_daily": 0.003},
    {"location_id": 6, "name": "Munich",    "location_code": "muc",
     "lat": 48.1601, "lon": 11.5874,
     "address": "Leopoldstrasse 75, 80802 Munich, Germany",
     "narrative": "stable",
     "base_orders_day": 18, "growth_rate_daily": 0.002},
    {"location_id": 7, "name": "Amsterdam", "location_code": "ams",
     "lat": 52.3745, "lon": 4.8979,
     "address": "Damrak 66, 1012 LM Amsterdam, Netherlands",
     "narrative": "growing",
     "base_orders_day": 20, "growth_rate_daily": 0.0025},
    {"location_id": 8, "name": "Vianen",    "location_code": "via",
     "lat": 51.9880, "lon": 5.0895,
     "address": "Voorstraat 78, 4131 LW Vianen, Netherlands",
     "narrative": "growing_fast",
     "base_orders_day": 10, "growth_rate_daily": 0.004},
]

# ── locations.parquet ───────────────────────────────────────────────────────
locs = pd.read_parquet(LOCATIONS_PATH)
print(f"Before: {len(locs)} locations → {sorted(locs['location_id'].tolist())}")
existing_ids = set(locs["location_id"])
to_add = [l for l in NEW_LOCATIONS if l["location_id"] not in existing_ids]
if to_add:
    new_df = pd.DataFrame(to_add)
    for col in locs.columns:
        if col not in new_df.columns:
            new_df[col] = None
    new_df = new_df[locs.columns]
    for col in locs.columns:
        try:
            new_df[col] = new_df[col].astype(locs[col].dtype)
        except Exception:
            pass
    locs = pd.concat([locs, new_df], ignore_index=True)
    locs.to_parquet(LOCATIONS_PATH, index=False)
    print(f"✅ Added {len(to_add)} locations → {[l['name'] for l in to_add]}")
else:
    print("ℹ️  locations already complete")

print()
print(locs[["location_id", "name", "location_code", "lat", "lon"]].to_string(index=False))
print()

# ── brand_locations.parquet ─────────────────────────────────────────────────
# Each new location inherits the brand mix from location 1 (SF) by default.
bl = pd.read_parquet(BRAND_LOCATIONS_PATH)
print(f"brand_locations before: {len(bl)} rows across {bl['location_id'].nunique()} locations")
existing_bl = set(bl["location_id"])
new_loc_ids = [l["location_id"] for l in NEW_LOCATIONS if l["location_id"] not in existing_bl]
if new_loc_ids:
    base = bl[bl["location_id"] == 1].copy()
    frames = []
    for lid in new_loc_ids:
        copy = base.copy()
        copy["location_id"] = lid
        frames.append(copy)
    bl = pd.concat([bl] + frames, ignore_index=True)
    bl.to_parquet(BRAND_LOCATIONS_PATH, index=False)
    print(f"✅ Added brand assignments for location IDs: {new_loc_ids}")
else:
    print("ℹ️  brand_locations already complete")
print(f"brand_locations after:  {len(bl)} rows across {bl['location_id'].nunique()} locations")

print()
print("Done. Next: run `python3 generate_canonical_dataset.py` to regenerate")
print("events.parquet so the new locations have realistic orders + driver pings.")
