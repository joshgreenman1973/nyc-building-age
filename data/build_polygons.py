#!/usr/bin/env python3
"""
Build a compact, self-contained binary polygon file for NYC building footprints.

This file is INDEPENDENT of buildings.bin (the dots dataset). It carries its
own metadata so that a deck.gl SolidPolygonLayer can render entirely from it.

Format (little-endian):
  Header (12 bytes):
    uint32 feature_count
    uint32 vertex_pool_size
    uint32 reserved

  Per-feature record (24 bytes each, feature_count of these):
    uint16 yb            year built (0 = unknown)
    uint16 ly            landmark year (0 = not landmarked)
    uint16 dy            demolition year
    uint16 ry            rebuild year (zero in source data, kept for shape)
    uint8  bc            borocode 1..5
    uint8  nf            numfloors (clamped 0..255)
    char   cls[2]        building class first 2 chars (PLUTO bldgclass)
    uint32 la            lotarea (clamped 32-bit)
    uint32 vertex_offset index into vertex pool (start of outer ring)
    uint16 vertex_count  number of vertices in this feature's outer ring
    uint16 reserved

  Vertex pool (4 bytes per vertex, vertex_pool_size of these):
    uint16 lon_scaled   0..65535 across NYC bbox
    uint16 lat_scaled   0..65535 across NYC bbox

NYC bounding box (encloses all 5 boroughs):
  lon: -74.27 .. -73.69 (range 0.58)
  lat:  40.49 ..  40.92 (range 0.43)

At ~10 cm precision, this is plenty for buildings.
"""

import json
import struct
import sys
from pathlib import Path

LON_MIN, LON_MAX = -74.27, -73.69
LAT_MIN, LAT_MAX = 40.49, 40.92
LON_RANGE = LON_MAX - LON_MIN
LAT_RANGE = LAT_MAX - LAT_MIN

PROCESSED_DIR = Path(__file__).parent / "processed"
GEOJSON_PATH = PROCESSED_DIR / "buildings_poly.geojson.nl"
OUT_PATH = PROCESSED_DIR / "buildings_poly.bin"


def scale_lon(lon: float) -> int:
    v = int((lon - LON_MIN) / LON_RANGE * 65535)
    return max(0, min(65535, v))


def scale_lat(lat: float) -> int:
    v = int((lat - LAT_MIN) / LAT_RANGE * 65535)
    return max(0, min(65535, v))


def thin_ring(ring, tol=2):
    """Drop consecutive vertices that scale to the same uint16 cell."""
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


def main():
    if not GEOJSON_PATH.exists():
        print(f"ERROR: {GEOJSON_PATH} not found", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {GEOJSON_PATH} ...")

    # First pass: count features and collect (props, ring) pairs.
    # We process line-by-line to avoid loading 507MB into memory at once,
    # but we need to know vertex offsets up-front, so we accumulate first.
    records = []
    skipped = 0

    with open(GEOJSON_PATH) as f:
        for line_no, line in enumerate(f, 1):
            try:
                feat = json.loads(line)
            except Exception:
                skipped += 1
                continue
            geom = feat.get("geometry") or {}
            gtype = geom.get("type")
            coords = geom.get("coordinates")
            if not coords:
                skipped += 1
                continue
            if gtype == "Polygon":
                ring = coords[0]
            elif gtype == "MultiPolygon":
                ring = coords[0][0]
            else:
                skipped += 1
                continue
            if len(ring) < 3:
                skipped += 1
                continue
            ring = thin_ring(ring)
            props = feat.get("properties", {}) or {}
            records.append((props, ring))
            if line_no % 100000 == 0:
                print(f"  {line_no} lines read ...")

    feature_count = len(records)
    vertex_count = sum(len(r) for _, r in records)
    avg_v = vertex_count / max(1, feature_count)
    est_mb = (12 + feature_count * 24 + vertex_count * 4) / (1024 * 1024)
    print(
        f"Features: {feature_count}, total verts: {vertex_count}, "
        f"avg verts/feature: {avg_v:.2f}, skipped: {skipped}"
    )
    print(f"Estimated size: {est_mb:.1f} MB")

    print(f"Writing {OUT_PATH} ...")
    with open(OUT_PATH, "wb") as out:
        # Header
        out.write(struct.pack("<III", feature_count, vertex_count, 0))

        # Per-feature records
        offset = 0
        for props, ring in records:
            yb = int(props.get("yb", 0) or 0)
            ly = int(props.get("ly", 0) or 0)
            dy = int(props.get("dy", 0) or 0)
            ry = int(props.get("ry", 0) or 0)
            bc = int(props.get("bc", 0) or 0)
            nf = min(255, max(0, int(props.get("nf", 0) or 0)))
            cls = (str(props.get("cls", "")) + "  ")[:2]
            cls_bytes = cls.encode("ascii", errors="replace")
            la = min(0xFFFFFFFF, max(0, int(props.get("la", 0) or 0)))
            vc = len(ring)

            out.write(struct.pack(
                "<HHHHBB2sIIHH",
                yb & 0xFFFF,
                ly & 0xFFFF,
                dy & 0xFFFF,
                ry & 0xFFFF,
                bc & 0xFF,
                nf & 0xFF,
                cls_bytes,
                la,
                offset,
                vc & 0xFFFF,
                0,
            ))
            offset += vc

        # Vertex pool
        v_written = 0
        for _, ring in records:
            for x, y in ring:
                out.write(struct.pack("<HH", scale_lon(x), scale_lat(y)))
                v_written += 1

        assert v_written == vertex_count

    actual = OUT_PATH.stat().st_size / (1024 * 1024)
    print(f"Wrote {actual:.1f} MB")


if __name__ == "__main__":
    main()
