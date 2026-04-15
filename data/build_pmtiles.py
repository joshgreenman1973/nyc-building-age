#!/usr/bin/env python3
"""
PMTiles pipeline for Every Building map.

Downloads NYC building footprints (polygons) from Open Data, joins with PLUTO
attributes, and exports GeoJSON for tippecanoe -> PMTiles conversion.

Attributes kept short for tile size:
  yb  = yearbuilt
  nf  = numfloors
  bc  = borocode
  cls = bldgclass (first char)
  ly  = landmark_year
  dy  = demo_year
  ry  = rebuild_year
  hr  = height_roof (from footprints)
  la  = lotarea
"""

import json
import os
import struct
import sys
import time
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).parent
RAW_DIR = SCRIPT_DIR / "raw"
PROCESSED_DIR = SCRIPT_DIR / "processed"

FOOTPRINTS_DATASET = "5zhs-2jue"
FOOTPRINTS_BASE = f"https://data.cityofnewyork.us/resource/{FOOTPRINTS_DATASET}.json"
FOOTPRINTS_FIELDS = "the_geom,mappluto_bbl,height_roof,construction_year,bin"

BATCH_SIZE = 10000  # GeoJSON rows are big, keep batches smaller
FOOTPRINTS_CACHE = RAW_DIR / "building_footprints.geojson"


def download_footprints():
    """Download building footprints in batches from Socrata API."""
    if FOOTPRINTS_CACHE.exists():
        size = FOOTPRINTS_CACHE.stat().st_size
        print(f"  [SKIP] Footprints already downloaded: {size // (1024*1024)}MB")
        return

    print("  Downloading building footprints (1.08M records)...")
    all_features = []
    offset = 0

    while True:
        url = (
            f"{FOOTPRINTS_BASE}?$select={FOOTPRINTS_FIELDS}"
            f"&$limit={BATCH_SIZE}&$offset={offset}&$order=objectid"
        )
        print(f"    Batch at offset {offset}...", end="", flush=True)

        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=120, verify=False)
                resp.raise_for_status()
                break
            except Exception as e:
                if attempt < 2:
                    print(f" retry...", end="", flush=True)
                    time.sleep(5)
                else:
                    raise

        batch = resp.json()
        if not batch:
            print(" done.")
            break

        for rec in batch:
            geom = rec.get("the_geom")
            if not geom:
                continue
            props = {
                "bbl": (rec.get("mappluto_bbl") or "").split(".")[0].strip(),
                "hr": int(float(rec.get("height_roof") or 0)),
                "cy": int(float(rec.get("construction_year") or 0)),
                "bin": str(rec.get("bin") or ""),
            }
            all_features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": props,
            })

        print(f" {len(batch)} rows (total features: {len(all_features)})")

        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    geojson = {"type": "FeatureCollection", "features": all_features}
    print(f"  Writing {len(all_features)} features to cache...")
    with open(FOOTPRINTS_CACHE, "w") as f:
        json.dump(geojson, f)
    size = FOOTPRINTS_CACHE.stat().st_size // (1024 * 1024)
    print(f"  Footprints cached: {size}MB")


def load_pluto_attributes():
    """Load PLUTO attributes from the existing raw data."""
    pluto_path = RAW_DIR / "pluto_all.json"
    print(f"  Loading PLUTO from {pluto_path}...")
    with open(pluto_path) as f:
        data = json.load(f)

    lookup = {}
    for rec in data:
        bbl = str(rec.get("bbl", "")).split(".")[0].strip()
        if not bbl:
            continue
        addr = str(rec.get("address") or "").strip()
        if addr.lower() in ("", "0", "nan", "none"):
            addr = ""
        else:
            addr = " ".join(addr.split())  # collapse whitespace
        lookup[bbl] = {
            "yb": int(float(rec.get("yearbuilt") or 0)),
            "nf": int(float(rec.get("numfloors") or 0)),
            "bc": int(float(rec.get("borocode") or 0)),
            "cls": str(rec.get("bldgclass") or "")[:2],
            "la": int(float(rec.get("lotarea") or 0)),
            "addr": addr,
        }
    print(f"  PLUTO lookup: {len(lookup)} BBLs")
    return lookup


def load_sidecar_lookups():
    """Load landmark, demolition, rebuild lookups from existing processed data."""
    landmarks = {}
    lm_path = PROCESSED_DIR / "landmarks.json"
    if lm_path.exists():
        with open(lm_path) as f:
            raw = json.load(f)
        landmarks = {k: int(v) for k, v in raw.items()}
        print(f"  Landmarks: {len(landmarks)} BBLs")

    demos = {}
    rebuilds = {}
    dm_path = PROCESSED_DIR / "demolitions.json"
    if dm_path.exists():
        with open(dm_path) as f:
            raw = json.load(f)
        for bbl, vals in raw.items():
            if vals.get("d"):
                demos[bbl] = vals["d"]
            if vals.get("r"):
                rebuilds[bbl] = vals["r"]
        print(f"  Demolitions: {len(demos)}, Rebuilds: {len(rebuilds)} BBLs")

    return landmarks, demos, rebuilds


def join_and_export():
    """Join footprints with PLUTO attributes and export line-delimited GeoJSON."""
    print("\n=== Loading data ===")
    pluto = load_pluto_attributes()
    landmarks, demos, rebuilds = load_sidecar_lookups()

    print(f"\n=== Reading footprints ===")
    with open(FOOTPRINTS_CACHE) as f:
        fc = json.load(f)

    features = fc["features"]
    print(f"  {len(features)} footprint features")

    # Output as newline-delimited GeoJSON (tippecanoe's preferred input)
    out_path = PROCESSED_DIR / "buildings_poly.geojson.nl"
    matched = 0
    unmatched = 0

    print(f"  Joining and writing to {out_path.name}...")
    with open(out_path, "w") as f:
        for feat in features:
            bbl = feat["properties"].get("bbl", "")
            hr = feat["properties"].get("hr", 0)
            cy = feat["properties"].get("cy", 0)

            pluto_attrs = pluto.get(bbl, {})

            yb = pluto_attrs.get("yb", 0)
            # Fall back to construction_year from footprints if PLUTO has no year
            if yb == 0 and cy > 0 and 1630 <= cy <= 2026:
                yb = cy

            props = {
                "yb": yb,
                "nf": pluto_attrs.get("nf", 0),
                "bc": pluto_attrs.get("bc", 0),
                "cls": pluto_attrs.get("cls", ""),
                "la": pluto_attrs.get("la", 0),
                "hr": hr,
                "ly": landmarks.get(bbl, 0),
                "dy": demos.get(bbl, 0),
                "ry": rebuilds.get(bbl, 0),
            }

            # Strip zero/empty values to save tile space
            props = {k: v for k, v in props.items() if v}

            out_feat = {
                "type": "Feature",
                "geometry": feat["geometry"],
                "properties": props,
            }
            f.write(json.dumps(out_feat) + "\n")

            if pluto_attrs:
                matched += 1
            else:
                unmatched += 1

    size = out_path.stat().st_size // (1024 * 1024)
    print(f"  Written: {out_path} ({size}MB)")
    print(f"  Matched to PLUTO: {matched}, Unmatched: {unmatched}")

    # Export BBL -> address lookup (for tooltips, loaded separately)
    addr_lookup = {}
    for bbl_key, attrs in pluto.items():
        addr = attrs.get("addr", "")
        if addr:
            addr_lookup[bbl_key] = addr
    addr_path = PROCESSED_DIR / "addresses.json"
    with open(addr_path, "w") as f:
        json.dump(addr_lookup, f, separators=(",", ":"))
    addr_size = addr_path.stat().st_size // (1024 * 1024)
    print(f"  Address lookup: {len(addr_lookup)} entries ({addr_size}MB)")

    return out_path


def main():
    print("=" * 60)
    print("PMTiles Pipeline for Every Building")
    print("=" * 60)

    print("\n=== Step 1: Download building footprints ===")
    download_footprints()

    print("\n=== Step 2: Join and export ===")
    out_path = join_and_export()

    print(f"\n=== Step 3: Generate PMTiles ===")
    tippecanoe = os.path.expanduser("~/.local/bin/tippecanoe")
    pmtiles_path = PROCESSED_DIR / "buildings.pmtiles"

    cmd = (
        f"{tippecanoe}"
        f" -o {pmtiles_path}"
        f" -Z 10 -z 16"
        f" --detect-shared-borders"
        f" --simplification=10"
        f" --drop-densest-as-needed"
        f" -l buildings"
        f" --force"
        f" {out_path}"
    )
    print(f"  Running: {cmd}")
    ret = os.system(cmd)
    if ret != 0:
        print(f"  ERROR: tippecanoe exited with code {ret}")
        sys.exit(1)

    size = pmtiles_path.stat().st_size // (1024 * 1024)
    print(f"\n  PMTiles written: {pmtiles_path} ({size}MB)")
    print("\nDone!")


if __name__ == "__main__":
    main()
