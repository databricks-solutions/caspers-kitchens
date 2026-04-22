"""
Add EMEA locations (5-8) to locations.parquet and brand_locations.parquet.
Run this locally once: python3 update_locations_parquet.py
"""
import pandas as pd
import random

random.seed(99)

NEW_LOCATIONS = [
    {"location_id": 5, "name": "London",    "location_code": "lon",
     "lat": 51.5248, "lon": -0.0796,
     "address": "14 Curtain Road, Shoreditch, London EC2A 3NH, UK",
     "base_orders_day": 22, "growth_rate_daily": 0.003},
    {"location_id": 6, "name": "Munich",    "location_code": "muc",
     "lat": 48.1601, "lon": 11.5874,
     "address": "Leopoldstrasse 75, 80802 Munich, Germany",
     "base_orders_day": 18, "growth_rate_daily": 0.002},
    {"location_id": 7, "name": "Amsterdam", "location_code": "ams",
     "lat": 52.3745, "lon": 4.8979,
     "address": "Damrak 66, 1012 LM Amsterdam, Netherlands",
     "base_orders_day": 20, "growth_rate_daily": 0.0025},
    {"location_id": 8, "name": "Vianen",    "location_code": "via",
     "lat": 51.9880, "lon": 5.0895,
     "address": "Voorstraat 78, 4131 LW Vianen, Netherlands",
     "base_orders_day": 10, "growth_rate_daily": 0.004},
]

# ── locations.parquet ───────────────────────────────────────────────────────
locs = pd.read_parquet("canonical_dataset/locations.parquet")
print("Before:", len(locs), "locations")
existing_ids = set(locs["location_id"])
to_add = [l for l in NEW_LOCATIONS if l["location_id"] not in existing_ids]
if to_add:
    new_df = pd.DataFrame(to_add)
    # align columns to existing schema
    for col in locs.columns:
        if col not in new_df.columns:
            new_df[col] = None
    new_df = new_df[locs.columns]
    # cast to match dtypes
    for col in locs.columns:
        try:
            new_df[col] = new_df[col].astype(locs[col].dtype)
        except Exception:
            pass
    locs = pd.concat([locs, new_df], ignore_index=True)
    locs.to_parquet("canonical_dataset/locations.parquet", index=False)
    print(f"✅ Added {len(to_add)} rows. Now: {len(locs)} locations")
else:
    print("ℹ️  Already complete")

print(locs[["location_id", "name", "location_code"]].to_string())

# ── brand_locations.parquet ─────────────────────────────────────────────────
bl = pd.read_parquet("canonical_dataset/brand_locations.parquet")
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
    bl.to_parquet("canonical_dataset/brand_locations.parquet", index=False)
    print(f"\n✅ Added brand assignments for location IDs: {new_loc_ids}")
else:
    print("\nℹ️  brand_locations already complete")

print("Done.")
