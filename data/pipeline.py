#!/usr/bin/env python3
"""
NYC Building Age Animation — Data Pipeline

Downloads PLUTO (via Socrata API), LPC Landmarks, and DOB Job Filings from NYC Open Data,
joins them by BBL, creates point geometries from lat/lon, and exports to FlatGeobuf + JSON sidecars.

Uses PLUTO tabular data with latitude/longitude rather than MapPLUTO shapefiles,
which avoids needing to find the current shapefile download URL. Points render
efficiently at all zoom levels via Deck.gl ScatterplotLayer.
"""

import json
import math
import os
import struct
import sys
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point

warnings.filterwarnings("ignore", message=".*urllib3.*")
warnings.filterwarnings("ignore", message=".*InsecureRequest.*")

SCRIPT_DIR = Path(__file__).parent
PROCESSED_DIR = SCRIPT_DIR / "processed"
RAW_DIR = SCRIPT_DIR / "raw"

PROCESSED_DIR.mkdir(exist_ok=True)
RAW_DIR.mkdir(exist_ok=True)

# NYC Open Data Socrata API endpoints
PLUTO_DATASET = "64uk-42ks"
PLUTO_BASE = f"https://data.cityofnewyork.us/resource/{PLUTO_DATASET}.json"
PLUTO_FIELDS = "bbl,borough,borocode,block,lot,address,yearbuilt,bldgclass,numfloors,latitude,longitude,lotarea,bldgarea"

LPC_LANDMARKS_URL = "https://data.cityofnewyork.us/resource/buis-pvji.json?$limit=50000&$select=bbl,borough,block,lot,lpc_name,desdate,landmarkty"
DOB_JOBS_DM_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json?$limit=200000&$where=job_type=%27DM%27&$select=borough,block,lot,job_type,pre__filing_date,latest_action_date"
DOB_JOBS_NB_URL = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json?$limit=200000&$where=job_type=%27NB%27&$select=borough,block,lot,job_type,pre__filing_date,latest_action_date"

# Batch size for PLUTO download (API limit is 50000 per request)
BATCH_SIZE = 50000


def download_json(url, dest, description="data"):
    """Download JSON data from NYC Open Data."""
    if dest.exists():
        size = dest.stat().st_size
        print(f"  [SKIP] {description} already downloaded: {dest.name} ({size // 1024}KB)")
        return
    print(f"  Downloading {description}...")
    resp = requests.get(url, timeout=300, verify=False)
    resp.raise_for_status()
    data = resp.json()
    with open(dest, "w") as f:
        json.dump(data, f)
    print(f"    Got {len(data)} records")


def step1_download_pluto():
    """Download PLUTO data in batches via Socrata API."""
    print("\n=== STEP 1: Download PLUTO data ===")

    pluto_path = RAW_DIR / "pluto_all.json"
    if pluto_path.exists():
        size = pluto_path.stat().st_size
        print(f"  [SKIP] PLUTO already downloaded: {size // (1024*1024)}MB")
        return

    all_records = []
    offset = 0

    while True:
        url = f"{PLUTO_BASE}?$select={PLUTO_FIELDS}&$limit={BATCH_SIZE}&$offset={offset}&$order=bbl"
        print(f"  Fetching PLUTO batch at offset {offset}...")
        resp = requests.get(url, timeout=300, verify=False)
        resp.raise_for_status()
        batch = resp.json()

        if not batch:
            break

        all_records.extend(batch)
        print(f"    Got {len(batch)} records (total: {len(all_records)})")

        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    with open(pluto_path, "w") as f:
        json.dump(all_records, f)
    print(f"  Total PLUTO records: {len(all_records)}")


def step2_download_other():
    """Download LPC landmarks and DOB job filings."""
    print("\n=== STEP 2: Download LPC + DOB data ===")

    download_json(LPC_LANDMARKS_URL, RAW_DIR / "lpc_landmarks.json", "LPC Landmarks")
    download_json(DOB_JOBS_DM_URL, RAW_DIR / "dob_demolitions.json", "DOB Demolitions")
    download_json(DOB_JOBS_NB_URL, RAW_DIR / "dob_new_buildings.json", "DOB New Buildings")


def step3_process_pluto():
    """Load and process PLUTO data into a GeoDataFrame."""
    print("\n=== STEP 3: Process PLUTO ===")

    with open(RAW_DIR / "pluto_all.json") as f:
        data = json.load(f)

    print(f"  Records: {len(data)}")

    df = pd.DataFrame(data)

    # Clean BBL - it comes as a decimal string like "4061730023.00000000"
    df["bbl"] = df["bbl"].astype(str).str.split(".").str[0].str.strip()

    # Clean numeric fields
    df["yearbuilt"] = pd.to_numeric(df.get("yearbuilt", 0), errors="coerce").fillna(0).astype(int)
    df["numfloors"] = pd.to_numeric(df.get("numfloors", 0), errors="coerce").fillna(0).astype(int)
    df["borocode"] = pd.to_numeric(df.get("borocode", 0), errors="coerce").fillna(0).astype(int)
    df["latitude"] = pd.to_numeric(df.get("latitude", 0), errors="coerce").fillna(0).astype(float)
    df["longitude"] = pd.to_numeric(df.get("longitude", 0), errors="coerce").fillna(0).astype(float)
    df["lotarea"] = pd.to_numeric(df.get("lotarea", 0), errors="coerce").fillna(0).astype(int)

    # Clamp year to valid range
    df.loc[df["yearbuilt"] < 1630, "yearbuilt"] = 0
    df.loc[df["yearbuilt"] > 2026, "yearbuilt"] = 0

    # Remove records with no valid coordinates
    before = len(df)
    df = df[(df["latitude"] != 0) & (df["longitude"] != 0)].copy()
    print(f"  Removed {before - len(df)} records without coordinates, {len(df)} remaining")

    known = df[df["yearbuilt"] > 0]
    print(f"  Year range: {known['yearbuilt'].min()} - {known['yearbuilt'].max()}")
    print(f"  Unknown year (0): {(df['yearbuilt'] == 0).sum()} lots")

    # Create point geometries
    print("  Creating point geometries...")
    geometry = [Point(lon, lat) for lon, lat in zip(df["longitude"], df["latitude"])]

    gdf = gpd.GeoDataFrame(
        df[["bbl", "yearbuilt", "bldgclass", "numfloors", "borocode", "address", "lotarea"]],
        geometry=geometry,
        crs="EPSG:4326",
    )

    print(f"  GeoDataFrame: {len(gdf)} features")
    return gdf


def step4_load_landmarks():
    """Load and process LPC landmark data."""
    print("\n=== STEP 4: Load LPC Landmarks ===")

    with open(RAW_DIR / "lpc_landmarks.json") as f:
        data = json.load(f)

    print(f"  Raw records: {len(data)}")

    landmark_lookup = {}
    for rec in data:
        bbl = str(rec.get("bbl", "")).strip()
        if "." in bbl:
            bbl = bbl.split(".")[0]
        if not bbl or bbl == "0" or bbl == "":
            continue

        date_str = rec.get("desdate", "")
        if not date_str:
            continue

        try:
            if "T" in date_str:
                year = int(date_str[:4])
            elif "/" in date_str:
                parts = date_str.split("/")
                year = int(parts[-1][:4])
                if year < 100:
                    year += 1900 if year > 50 else 2000
            elif "-" in date_str:
                year = int(date_str[:4])
            else:
                year = int(date_str[:4])

            if 1965 <= year <= 2026:
                if bbl not in landmark_lookup or year < landmark_lookup[bbl]:
                    landmark_lookup[bbl] = year
        except (ValueError, IndexError):
            continue

    print(f"  Landmark BBLs with designation year: {len(landmark_lookup)}")
    return landmark_lookup


def step5_load_dob():
    """Load DOB demolition and new building data."""
    print("\n=== STEP 5: Load DOB Job Filings ===")

    demo_lookup = {}
    rebuild_lookup = {}

    # Borough name to code mapping
    boro_map = {
        "MANHATTAN": "1", "BRONX": "2", "BROOKLYN": "3",
        "QUEENS": "4", "STATEN ISLAND": "5",
    }

    def dob_bbl(rec):
        """Construct BBL from DOB borough/block/lot fields. PLUTO BBL = 10 digits: boro(1) + block(5) + lot(4)."""
        boro = boro_map.get(rec.get("borough", "").upper(), "")
        block_raw = rec.get("block", "").strip()
        lot_raw = rec.get("lot", "").strip()
        if not boro or not block_raw or not lot_raw:
            return ""
        # Convert to integers and back to strip any extra zeros
        try:
            block_int = int(block_raw)
            lot_int = int(lot_raw)
        except ValueError:
            return ""
        return f"{boro}{block_int:05d}{lot_int:04d}"

    def dob_year(rec):
        """Extract year from DOB date fields."""
        for field in ["pre__filing_date", "latest_action_date"]:
            ds = rec.get(field, "")
            if not ds:
                continue
            try:
                # Format: MM/DD/YYYY or YYYY-MM-DD
                if "/" in ds:
                    parts = ds.split("/")
                    y = int(parts[-1][:4])
                elif "-" in ds:
                    y = int(ds[:4])
                elif "T" in ds:
                    y = int(ds[:4])
                else:
                    continue
                if 1900 <= y <= 2026:
                    return y
            except (ValueError, IndexError):
                continue
        return 0

    # Demolitions
    with open(RAW_DIR / "dob_demolitions.json") as f:
        demo_data = json.load(f)
    print(f"  Demolition records: {len(demo_data)}")

    for rec in demo_data:
        bbl = dob_bbl(rec)
        if not bbl:
            continue
        year = dob_year(rec)
        if year > 0:
            if bbl not in demo_lookup or year > demo_lookup[bbl]:
                demo_lookup[bbl] = year

    print(f"  Demolition BBLs: {len(demo_lookup)}")

    # New Buildings
    with open(RAW_DIR / "dob_new_buildings.json") as f:
        nb_data = json.load(f)
    print(f"  New building records: {len(nb_data)}")

    for rec in nb_data:
        bbl = dob_bbl(rec)
        if not bbl:
            continue
        year = dob_year(rec)
        if year > 0:
            if bbl not in rebuild_lookup or year > rebuild_lookup[bbl]:
                rebuild_lookup[bbl] = year

    print(f"  New building BBLs: {len(rebuild_lookup)}")
    return demo_lookup, rebuild_lookup


def step6_join(gdf, landmark_lookup, demo_lookup, rebuild_lookup):
    """Join all data sources."""
    print("\n=== STEP 6: Join datasets ===")

    gdf["landmark_year"] = gdf["bbl"].map(landmark_lookup).fillna(0).astype(int)
    print(f"  Landmarks joined: {(gdf['landmark_year'] > 0).sum()} lots")

    gdf["demo_year"] = gdf["bbl"].map(demo_lookup).fillna(0).astype(int)
    print(f"  Demolitions joined: {(gdf['demo_year'] > 0).sum()} lots")

    gdf["rebuild_year"] = gdf["bbl"].map(rebuild_lookup).fillna(0).astype(int)
    print(f"  Rebuilds joined: {(gdf['rebuild_year'] > 0).sum()} lots")

    return gdf


def step7_export(gdf, landmark_lookup, demo_lookup, rebuild_lookup):
    """Export to FlatGeobuf and JSON sidecars."""
    print("\n=== STEP 7: Export ===")

    # Export columns
    export_cols = ["bbl", "yearbuilt", "landmark_year", "demo_year", "rebuild_year",
                   "bldgclass", "numfloors", "borocode", "address", "lotarea", "geometry"]
    gdf_export = gdf[[c for c in export_cols if c in gdf.columns]].copy()

    # Export to FlatGeobuf
    fgb_path = PROCESSED_DIR / "buildings.fgb"
    print(f"  Writing FlatGeobuf ({len(gdf_export)} features)...")
    gdf_export.to_file(fgb_path, driver="FlatGeobuf", engine="pyogrio")
    fgb_size = fgb_path.stat().st_size / (1024 * 1024)
    print(f"  FlatGeobuf written: {fgb_size:.1f} MB")

    # Also export a compact binary format for faster loading
    # Format: [lon(f32), lat(f32), yearbuilt(u16), borocode(u8), numfloors(u8),
    #          landmark_year(u16), demo_year(u16), rebuild_year(u16), bldgclass(2 bytes), lotarea(u32)]
    # = 22 bytes per record
    print("  Writing compact binary format...")
    bin_path = PROCESSED_DIR / "buildings.bin"
    meta = {"count": len(gdf_export), "bytesPerRecord": 22}

    with open(bin_path, "wb") as f:
        for _, row in gdf_export.iterrows():
            lon = row.geometry.x
            lat = row.geometry.y
            yb = int(row.get("yearbuilt", 0))
            bc = int(row.get("borocode", 0))
            nf = min(int(row.get("numfloors", 0)), 255)
            ly = int(row.get("landmark_year", 0))
            dy = int(row.get("demo_year", 0))
            ry = int(row.get("rebuild_year", 0))
            cls = (str(row.get("bldgclass", "")) + "  ")[:2]
            la = min(int(row.get("lotarea", 0)), 4294967295)

            f.write(struct.pack("<ffHBBHHH2sI",
                                lon, lat, yb, bc, nf, ly, dy, ry,
                                cls.encode("ascii", errors="replace"), la))

    bin_size = bin_path.stat().st_size / (1024 * 1024)
    print(f"  Binary written: {bin_size:.1f} MB")

    # Export addresses as newline-delimited text (index = line number)
    addr_path = PROCESSED_DIR / "addresses.txt"
    print("  Writing addresses sidecar...")
    with open(addr_path, "w") as f:
        for _, row in gdf_export.iterrows():
            addr = str(row.get("address", "")).strip()
            # Clean up address: title case, remove extra whitespace
            if addr and addr.lower() not in ("", "0", "nan", "none"):
                addr = " ".join(addr.split())  # collapse whitespace
            else:
                addr = ""
            f.write(addr + "\n")
    addr_size = addr_path.stat().st_size / (1024 * 1024)
    print(f"  Addresses written: {addr_size:.1f} MB ({len(gdf_export)} lines)")

    # Export landmark lookup as JSON
    landmarks_json = {bbl: year for bbl, year in landmark_lookup.items() if year > 0}
    with open(PROCESSED_DIR / "landmarks.json", "w") as f:
        json.dump(landmarks_json, f)
    print(f"  Landmarks JSON: {len(landmarks_json)} entries")

    # Export demolition/rebuild lookup as JSON
    demo_json = {}
    all_bbls = set(demo_lookup.keys()) | set(rebuild_lookup.keys())
    for bbl in all_bbls:
        demo_json[bbl] = {
            "d": demo_lookup.get(bbl, 0),
            "r": rebuild_lookup.get(bbl, 0),
        }
    with open(PROCESSED_DIR / "demolitions.json", "w") as f:
        json.dump(demo_json, f)
    print(f"  Demolitions JSON: {len(demo_json)} entries")

    # Generate summary stats
    stats = {
        "total_lots": len(gdf_export),
        "known_year": int((gdf_export["yearbuilt"] > 0).sum()),
        "unknown_year": int((gdf_export["yearbuilt"] == 0).sum()),
        "total_landmarks": int((gdf_export["landmark_year"] > 0).sum()),
        "total_demolitions": int((gdf_export["demo_year"] > 0).sum()),
        "total_rebuilds": int((gdf_export["rebuild_year"] > 0).sum()),
        "year_range": [
            int(gdf_export[gdf_export["yearbuilt"] > 0]["yearbuilt"].min()),
            int(gdf_export[gdf_export["yearbuilt"] > 0]["yearbuilt"].max()),
        ],
        "by_decade": {},
        "meta": meta,
    }

    known = gdf_export[gdf_export["yearbuilt"] > 0]
    decades = (known["yearbuilt"] // 10 * 10).value_counts().sort_index()
    stats["by_decade"] = {str(int(k)): int(v) for k, v in decades.items()}

    with open(PROCESSED_DIR / "stats.json", "w") as f:
        json.dump(stats, f, indent=2)
    print(f"  Stats JSON written")

    print(f"\n  === Export complete ===")
    print(f"  FlatGeobuf: {fgb_path} ({fgb_size:.1f} MB)")
    print(f"  Binary: {bin_path} ({bin_size:.1f} MB)")


def main():
    print("=" * 60)
    print("NYC Building Age Animation — Data Pipeline")
    print("=" * 60)

    step1_download_pluto()
    step2_download_other()
    gdf = step3_process_pluto()
    landmark_lookup = step4_load_landmarks()
    demo_lookup, rebuild_lookup = step5_load_dob()
    gdf = step6_join(gdf, landmark_lookup, demo_lookup, rebuild_lookup)
    step7_export(gdf, landmark_lookup, demo_lookup, rebuild_lookup)

    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
