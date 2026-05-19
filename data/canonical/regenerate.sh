#!/usr/bin/env bash
# Regenerate the canonical dataset (parquet files in canonical_dataset/).
#
# Usage:
#   cd data/canonical
#   ./regenerate.sh
#
# Requirements (auto-installed into a local .venv):
#   pandas, pyarrow, networkx, osmnx, geopy
#
# Takes ~15–25 min for 8 cities; needs internet to fetch OSM road networks
# from openstreetmap.org via osmnx + geopy/Nominatim.
#
# What it does:
#   1. Create .venv in this directory if not present, install deps
#   2. Add any missing locations to locations.parquet + brand_locations.parquet
#      (via update_locations_parquet.py — edit that file to change which ones)
#   3. Regenerate events.parquet with 90 days of orders for ALL locations
#      now present in locations.parquet (via generate_canonical_dataset.py)
#
# Idempotent — re-run anytime after editing NEW_LOCATIONS in
# update_locations_parquet.py.

set -eo pipefail
cd "$(dirname "$0")"

PIP_INDEX="${PIP_INDEX_URL:-https://pypi-proxy.dev.databricks.com/simple}"

if [ ! -d ".venv" ]; then
    echo "📦 Creating .venv and installing deps (one-time, ~3 min)..."
    python3 -m venv .venv
    .venv/bin/pip install --quiet --upgrade pip --index-url "$PIP_INDEX"
    .venv/bin/pip install --quiet --index-url "$PIP_INDEX" \
        pandas pyarrow networkx osmnx geopy scikit-learn
fi

echo ""
echo "🧭 Step 1/2: update_locations_parquet.py"
.venv/bin/python update_locations_parquet.py

echo ""
echo "🏭 Step 2/2: generate_canonical_dataset.py (this takes a while)"
.venv/bin/python generate_canonical_dataset.py

echo ""
echo "✅ Done. Commit the updated parquet files:"
echo "    git add canonical_dataset/locations.parquet \\"
echo "            canonical_dataset/brand_locations.parquet \\"
echo "            canonical_dataset/events.parquet"
echo "    git commit -m 'regenerate canonical dataset with N locations'"
