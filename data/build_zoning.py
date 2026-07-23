#!/usr/bin/env python3
"""
Build zoning.bin + zoning_meta.json — NYC zoning district polygons.

Source: NYC Department of City Planning's zoning districts layer (nyzd), the
same primary layer behind DCP's ZoLa map, pulled live from DCP's ArcGIS
FeatureServer. The layer is continuous over the entire city: every square foot
of land carries exactly one primary zoning district.

Unlike the other polygon binaries here, this one preserves interior rings
(holes) — 19% of zoning parts have them (a mapped park carved out of a
residential district, say), and filling them would tint the wrong land.

Format (little-endian):
  Header (12 bytes):
    uint32 part_count
    uint32 ring_count
    uint32 vertex_pool_size

  Per-part record (8 bytes, part_count of these):
    uint16 zdIdx         index into meta.zd (zoning district code)
    uint8  cat           0=R 1=C 2=M 3=PARK 4=BPC 5=other (matches the
                         standalone city-land map's numbering)
    uint8  ringCount     rings in this part; ring 0 is the outer ring
    uint32 firstRingIdx  index of this part's first ring record

  Per-ring record (8 bytes, ring_count of these):
    uint32 vertexOffset
    uint16 vertexCount
    uint16 reserved

  Vertex pool (4 bytes per vertex):
    uint16 lon_scaled, uint16 lat_scaled  across the NYC bbox

NYC bbox must match build_polygons.py / index.html:
  lon: -74.27 .. -73.69, lat: 40.49 .. 40.92
"""

import json
import struct
import sys
import time
import urllib.request
from pathlib import Path

LON_MIN, LON_MAX = -74.27, -73.69
LAT_MIN, LAT_MAX = 40.49, 40.92
LON_RANGE = LON_MAX - LON_MIN
LAT_RANGE = LAT_MAX - LAT_MIN

SCRIPT_DIR = Path(__file__).parent
RAW_DIR = SCRIPT_DIR / "raw"
PROCESSED_DIR = SCRIPT_DIR / "processed"
RAW_CACHE = RAW_DIR / "zoning_districts.geojson"
BIN_PATH = PROCESSED_DIR / "zoning.bin"
META_PATH = PROCESSED_DIR / "zoning_meta.json"

SERVICE = ("https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services"
           "/nyzd/FeatureServer/0/query")
PAGE = 2000

CAT_R, CAT_C, CAT_M, CAT_PARK, CAT_BPC, CAT_OTHER = 0, 1, 2, 3, 4, 5


def scale_lon(lon):
    return max(0, min(65535, int((lon - LON_MIN) / LON_RANGE * 65535)))


def scale_lat(lat):
    return max(0, min(65535, int((lat - LAT_MIN) / LAT_RANGE * 65535)))


def thin_ring(ring, tol=2):
    """Drop consecutive vertices that scale to (nearly) the same uint16 cell."""
    if len(ring) <= 5:
        return ring
    out = [ring[0]]
    last_x, last_y = scale_lon(ring[0][0]), scale_lat(ring[0][1])
    for v in ring[1:-1]:
        x, y = scale_lon(v[0]), scale_lat(v[1])
        if abs(x - last_x) >= tol or abs(y - last_y) >= tol:
            out.append(v)
            last_x, last_y = x, y
    out.append(ring[-1])
    return out if len(out) >= 4 else ring


def category(code):
    z = (code or "").strip().upper()
    if z.startswith("PARK"):
        return CAT_PARK
    if z.startswith("BPC"):
        return CAT_BPC
    c = z[:1]
    return {"R": CAT_R, "C": CAT_C, "M": CAT_M}.get(c, CAT_OTHER)


def download():
    if RAW_CACHE.exists():
        print(f"  [SKIP] cached: {RAW_CACHE.stat().st_size // 1024}KB")
        return json.load(open(RAW_CACHE))["features"]

    print("  Downloading zoning districts from DCP ArcGIS...")
    feats, offset = [], 0
    while True:
        url = (f"{SERVICE}?where=1%3D1&outFields=ZONEDIST&outSR=4326&f=geojson"
               f"&resultOffset={offset}&resultRecordCount={PAGE}&geometryPrecision=6")
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=180) as r:
                    data = json.load(r)
                break
            except Exception:
                if attempt == 2:
                    raise
                print("    retry...")
                time.sleep(5)
        batch = data.get("features", [])
        if not batch:
            break
        feats.extend(batch)
        print(f"    {len(feats)} features...")
        if len(batch) < PAGE:
            break
        offset += PAGE

    if not feats:
        print("ERROR: zoning download returned no features", file=sys.stderr)
        sys.exit(1)

    RAW_DIR.mkdir(exist_ok=True)
    json.dump({"type": "FeatureCollection", "features": feats}, open(RAW_CACHE, "w"))
    return feats


def main():
    feats = download()
    print(f"  {len(feats)} zoning district features")

    zd_table, zd_index = [], {}
    parts = []          # (zdIdx, cat, [ring, ring, ...])
    raw_verts = kept_verts = 0
    skipped = 0

    for f in feats:
        geom = f.get("geometry") or {}
        gtype, coords = geom.get("type"), geom.get("coordinates")
        if not coords:
            skipped += 1
            continue
        if gtype == "Polygon":
            polys = [coords]
        elif gtype == "MultiPolygon":
            polys = coords
        else:
            skipped += 1
            continue

        code = (f.get("properties", {}).get("ZONEDIST") or "").strip()
        if code not in zd_index:
            zd_index[code] = len(zd_table)
            zd_table.append(code)
        zi, cat = zd_index[code], category(code)

        for poly in polys:
            rings = []
            for ring in poly:
                raw_verts += len(ring)
                if len(ring) < 4:
                    continue
                thinned = thin_ring(ring)
                if len(thinned) < 4:
                    continue
                kept_verts += len(thinned)
                rings.append(thinned)
            if rings:
                parts.append((zi, cat, rings))

    ring_count = sum(len(p[2]) for p in parts)
    holes = ring_count - len(parts)
    print(f"  Parts: {len(parts)} ({holes} interior rings kept), "
          f"districts: {len(zd_table)}, skipped: {skipped}")
    print(f"  Vertices: {raw_verts} -> {kept_verts} after thinning "
          f"({100 * kept_verts / max(1, raw_verts):.0f}%)")

    if len(zd_table) > 65535:
        print("ERROR: district table overflows uint16", file=sys.stderr)
        sys.exit(1)

    PROCESSED_DIR.mkdir(exist_ok=True)
    with open(BIN_PATH, "wb") as out:
        out.write(struct.pack("<III", len(parts), ring_count, kept_verts))

        ring_idx = 0
        for zi, cat, rings in parts:
            out.write(struct.pack("<HBBI", zi, cat, len(rings), ring_idx))
            ring_idx += len(rings)

        v_off = 0
        for _, _, rings in parts:
            for ring in rings:
                out.write(struct.pack("<IHH", v_off, len(ring), 0))
                v_off += len(ring)

        written = 0
        for _, _, rings in parts:
            for ring in rings:
                for x, y in ring:
                    out.write(struct.pack("<HH", scale_lon(x), scale_lat(y)))
                    written += 1
        assert written == kept_verts, (written, kept_verts)

    json.dump({"zd": zd_table}, open(META_PATH, "w"), separators=(",", ":"))
    print(f"  Wrote {BIN_PATH.name} ({BIN_PATH.stat().st_size / 1e6:.2f} MB), "
          f"{META_PATH.name} ({META_PATH.stat().st_size / 1024:.0f} KB)")

    from collections import Counter
    names = ["Residential", "Commercial", "Manufacturing", "Park", "Battery Park City", "Other"]
    tally = Counter(p[1] for p in parts)
    print("  Parts by category: " + ", ".join(
        f"{names[k]} {tally.get(k, 0)}" for k in range(6) if tally.get(k)))


if __name__ == "__main__":
    main()
