#!/bin/bash
# Full data refresh for Every Building: re-downloads PLUTO, DOB demolition and
# new-building permits, landmarks and building footprints from NYC Open Data,
# then rebuilds the dots binary, sidecars, addresses and the vector-tile
# archive. Run from the repo root. Takes on the order of an hour.
set -e
cd "$(dirname "$0")/.."

echo "=== Clearing cached raw downloads ==="
rm -f data/raw/pluto_all.json data/raw/dob_demolitions.json \
      data/raw/dob_new_buildings.json data/raw/lpc_landmarks.json \
      data/raw/building_footprints.geojson

echo "=== Rebuilding dots + sidecars (pipeline.py) ==="
python3 data/pipeline.py

echo "=== Rebuilding footprint tiles (build_pmtiles.py) ==="
python3 data/build_pmtiles.py

echo "REFRESH_ALL_DONE"
