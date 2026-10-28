#!/bin/bash
# Full data refresh for Every Building: re-downloads PLUTO, DOB demolition and
# new-building filings (BIS and DOB NOW), landmarks, building footprints and
# the overlay sources from NYC Open Data, then rebuilds the dots binary,
# sidecars, addresses, overlays and the vector-tile archive. Run from the repo
# root. Takes on the order of an hour.
#
# Not covered here: CommonPlace names (their own monthly GitHub Action) and
# City-owned land (built from the sibling nyc-city-land-map repo; rerun
# data/build_cityland.py after refreshing that project).
#
# After it finishes, the tiles still have to be published: upload
# data/processed/buildings.pmtiles to a NEW release tag, point ARCHIVE_URL in
# api/bt at it and bump the ?v= on the tile URL in index.html.
set -e
cd "$(dirname "$0")/.."

echo "=== Clearing cached raw downloads ==="
rm -f data/raw/pluto_all.json data/raw/dob_demolitions.json \
      data/raw/dob_new_buildings.json data/raw/dobnow_demolitions.json \
      data/raw/dobnow_new_buildings.json data/raw/lpc_landmarks.json \
      data/raw/building_footprints.geojson \
      data/raw/building_footprints_historic.json data/raw/lpc_buildings.json \
      data/raw/zoning_districts.geojson data/raw/parks_properties.geojson

# Footprints first: pipeline.py takes each lot's measured roof height from
# them, and build_pmtiles.py reuses the same download.
echo "=== Downloading building footprints ==="
python3 -c 'import sys; sys.path.insert(0, "data"); import build_pmtiles; build_pmtiles.download_footprints()'

echo "=== Rebuilding dots + sidecars (pipeline.py) ==="
python3 data/pipeline.py

echo "=== Rebuilding overlays ==="
python3 data/build_ghosts.py
python3 data/build_future.py
python3 data/build_architects.py
python3 data/build_zoning.py
python3 data/download_parks.py

echo "=== Rebuilding footprint tiles (build_pmtiles.py) ==="
python3 data/build_pmtiles.py

echo "REFRESH_ALL_DONE"
