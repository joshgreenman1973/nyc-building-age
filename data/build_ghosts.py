#!/usr/bin/env python3
"""
Build ghosts.bin — footprints of demolished NYC buildings.

Source: Building Footprints Historic (NYC Open Data ipkp-snf6), the city's
companion dataset to Building Footprints holding structures that have been
demolished since digital footprint mapping began (effectively 1996+).

We keep only rows whose last_status_type starts with "Demol" (the dataset
contains the spellings Demolition, Demolitian and Demollition), then dedupe
by BIN keeping the most recently edited row, since a building can appear
multiple times as its footprint was revised before demolition.

Format (little-endian), same vertex scheme as buildings_poly.bin:
  Header (12 bytes):
    uint32 feature_count
    uint32 vertex_pool_size
    uint32 reserved

  Per-feature record (12 bytes each):
    uint16 cy            construction year (0 = unknown)
    uint16 dy            demolition year (falls back to last_edited year)
    uint8  bc            borocode 1..5 from base_bbl (0 = unknown)
    uint8  reserved
    uint32 vertex_offset index into vertex pool (start of outer ring)
    uint16 vertex_count  number of vertices in outer ring

  Vertex pool (4 bytes per vertex):
    uint16 lon_scaled   0..65535 across NYC bbox
    uint16 lat_scaled   0..65535 across NYC bbox

NYC bbox must match build_polygons.py / index.html:
  lon: -74.27 .. -73.69, lat: 40.49 .. 40.92
"""

import json
import struct
import time
from pathlib import Path

import requests

LON_MIN, LON_MAX = -74.27, -73.69
LAT_MIN, LAT_MAX = 40.49, 40.92
LON_RANGE = LON_MAX - LON_MIN
LAT_RANGE = LAT_MAX - LAT_MIN

SCRIPT_DIR = Path(__file__).parent
RAW_DIR = SCRIPT_DIR / "raw"
PROCESSED_DIR = SCRIPT_DIR / "processed"
RAW_CACHE = RAW_DIR / "building_footprints_historic.json"
OUT_PATH = PROCESSED_DIR / "ghosts.bin"

DATASET = "ipkp-snf6"
BASE = f"https://data.cityofnewyork.us/resource/{DATASET}.json"
FIELDS = "the_geom,bin,base_bbl,construction_year,demolition_year,last_edited_date,last_status_type"
BATCH_SIZE = 10000


def scale_lon(lon: float) -> int:
    return max(0, min(65535, int((lon - LON_MIN) / LON_RANGE * 65535)))


def scale_lat(lat: float) -> int:
    return max(0, min(65535, int((lat - LAT_MIN) / LAT_RANGE * 65535)))


def thin_ring(ring, tol=2):
    """Drop consecutive vertices that scale to (nearly) the same uint16 cell."""
    if len(ring) <= 4:
        return ring
    out = [ring[0]]
    last_x, last_y = scale_lon(ring[0][0]), scale_lat(ring[0][1])
    for v in ring[1:-1]:
        x, y = scale_lon(v[0]), scale_lat(v[1])
        if abs(x - last_x) >= tol or abs(y - last_y) >= tol:
            out.append(v)
            last_x, last_y = x, y
    out.append(ring[-1])
    if len(out) < 3:
        return ring
    return out


def download():
    if RAW_CACHE.exists():
        size = RAW_CACHE.stat().st_size
        print(f"  [SKIP] Historic footprints already downloaded: {size // (1024*1024)}MB")
        with open(RAW_CACHE) as f:
            return json.load(f)

    print("  Downloading historic building footprints (~47k demolition records)...")
    rows = []
    offset = 0
    where = "starts_with(last_status_type,'Demol')"
    while True:
        url = (
            f"{BASE}?$select={FIELDS}&$where={where}"
            f"&$limit={BATCH_SIZE}&$offset={offset}&$order=objectid"
        )
        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=120)
                resp.raise_for_status()
                break
            except Exception:
                if attempt < 2:
                    print("    retry...")
                    time.sleep(5)
                else:
                    raise
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        print(f"    {len(rows)} rows...")
        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    RAW_DIR.mkdir(exist_ok=True)
    with open(RAW_CACHE, "w") as f:
        json.dump(rows, f)
    print(f"  Cached {len(rows)} rows")
    return rows


def year_of(datestr):
    try:
        return int(str(datestr)[:4])
    except (ValueError, TypeError):
        return 0


def main():
    rows = download()
    print(f"  {len(rows)} demolition rows")

    # Dedupe by BIN, keeping the most recently edited footprint version.
    # Rows with no BIN are kept as-is.
    by_bin = {}
    no_bin = []
    for r in rows:
        b = str(r.get("bin") or "").strip()
        if not b or b == "0":
            no_bin.append(r)
            continue
        prev = by_bin.get(b)
        if prev is None or str(r.get("last_edited_date") or "") > str(prev.get("last_edited_date") or ""):
            by_bin[b] = r
    deduped = list(by_bin.values()) + no_bin
    print(f"  After BIN dedupe: {len(deduped)} ({len(no_bin)} without BIN)")

    records = []
    skipped_geom = 0
    skipped_year = 0
    for r in deduped:
        geom = r.get("the_geom") or {}
        coords = geom.get("coordinates")
        gtype = geom.get("type")
        if not coords:
            skipped_geom += 1
            continue
        if gtype == "Polygon":
            ring = coords[0]
        elif gtype == "MultiPolygon":
            ring = coords[0][0]
        else:
            skipped_geom += 1
            continue
        if len(ring) < 3:
            skipped_geom += 1
            continue

        dy = int(float(r.get("demolition_year") or 0))
        if not dy:
            dy = year_of(r.get("last_edited_date"))
        if not (1900 <= dy <= 2026):
            skipped_year += 1
            continue

        cy = int(float(r.get("construction_year") or 0))
        if not (1600 <= cy <= 2026):
            cy = 0

        bbl = str(r.get("base_bbl") or "")
        bc = int(bbl[0]) if bbl[:1] in ("1", "2", "3", "4", "5") else 0

        records.append((cy, dy, bc, thin_ring(ring)))

    print(f"  Usable: {len(records)} (skipped {skipped_geom} bad geometry, {skipped_year} no demolition year)")

    feature_count = len(records)
    vertex_count = sum(len(r[3]) for r in records)
    est_mb = (12 + feature_count * 12 + vertex_count * 4) / (1024 * 1024)
    print(f"  Vertices: {vertex_count} (avg {vertex_count / max(1, feature_count):.1f}/feature), est {est_mb:.1f} MB")

    PROCESSED_DIR.mkdir(exist_ok=True)
    with open(OUT_PATH, "wb") as out:
        out.write(struct.pack("<III", feature_count, vertex_count, 0))
        offset = 0
        for cy, dy, bc, ring in records:
            out.write(struct.pack(
                "<HHBBIH",
                cy & 0xFFFF, dy & 0xFFFF, bc & 0xFF, 0,
                offset, len(ring) & 0xFFFF,
            ))
            offset += len(ring)
        for _, _, _, ring in records:
            for x, y in ring:
                out.write(struct.pack("<HH", scale_lon(x), scale_lat(y)))

    print(f"  Wrote {OUT_PATH} ({OUT_PATH.stat().st_size / (1024*1024):.1f} MB)")

    dys = sorted(r[1] for r in records)
    cys = [r[0] for r in records if r[0]]
    print(f"  Demolition years: {dys[0]}-{dys[-1]}; construction year known for {len(cys)}/{len(records)}")


if __name__ == "__main__":
    main()
