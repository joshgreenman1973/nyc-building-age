#!/usr/bin/env python3
"""
Build cityland.bin + cityland_meta.json — city-owned parcel outlines for the
City-owned land overlay.

Source: the standalone nyc-city-land-map project (sibling repo), which joins
DCP's City Owned and Leased Properties (COLP, fn4k-qyk2) to MapPLUTO and
dedupes to one record per tax lot:
  ../../../nyc-city-land-map/lots.geojson   — 14,940 parcel polygons + numerics
  ../../../nyc-city-land-map/colp_map.json  — per-record strings (zoning, use,
                                              parcel name, address), joined on BBL

cityland.bin (little-endian), same vertex scheme as ghosts.bin:
  Header (12 bytes): uint32 feature_count, uint32 vertex_pool_size, uint32 reserved
  Per-feature record (20 bytes):
    uint8  c        readiness category 1-4 (no use / residential / active / parks)
    uint8  flags    bit0 = flagged for possible disposition, bit1 = leased
    uint8  agIdx    index into meta.ag (agency)
    uint8  utIdx    index into meta.ut (current-use text)
    uint16 zIdx     index into meta.z (zoning district text; 0 = none)
    uint16 rf100    residential FAR x 100 (0 = not zoned for housing as-of-right)
    uint32 ar       lot area, sq ft
    uint32 vertex_offset
    uint16 vertex_count
    uint16 reserved
  Vertex pool: uint16 lon/lat scaled across the NYC bbox (matches build_polygons.py)

cityland_meta.json: {"ag": [...], "z": [...], "ut": [...], "labels": [...]}
  labels[i] = parcel name if recorded, else address, else "" (one per feature).
"""

import json
import struct
import sys
from pathlib import Path

LON_MIN, LON_MAX = -74.27, -73.69
LAT_MIN, LAT_MAX = 40.49, 40.92
LON_RANGE = LON_MAX - LON_MIN
LAT_RANGE = LAT_MAX - LAT_MIN

SCRIPT_DIR = Path(__file__).parent
PROCESSED_DIR = SCRIPT_DIR / "processed"
SOURCE_DIR = SCRIPT_DIR / ".." / ".." / ".." / "nyc-city-land-map"
LOTS_PATH = (SOURCE_DIR / "lots.geojson").resolve()
COLP_PATH = (SOURCE_DIR / "colp_map.json").resolve()
BIN_PATH = PROCESSED_DIR / "cityland.bin"
META_PATH = PROCESSED_DIR / "cityland_meta.json"


def scale_lon(lon):
    return max(0, min(65535, int((lon - LON_MIN) / LON_RANGE * 65535)))


def scale_lat(lat):
    return max(0, min(65535, int((lat - LAT_MIN) / LAT_RANGE * 65535)))


def thin_ring(ring, tol=2):
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


class Table:
    """Deduplicating string table; index 0 is always the empty string."""
    def __init__(self):
        self.items = [""]
        self.index = {"": 0}

    def add(self, s):
        s = (s or "").strip()
        if s not in self.index:
            self.index[s] = len(self.items)
            self.items.append(s)
        return self.index[s]


def main():
    for p in (LOTS_PATH, COLP_PATH):
        if not p.exists():
            print(f"ERROR: {p} not found — clone/build nyc-city-land-map first", file=sys.stderr)
            sys.exit(1)

    print(f"Reading {LOTS_PATH.name} + {COLP_PATH.name} ...")
    lots = json.load(open(LOTS_PATH))["features"]
    colp = json.load(open(COLP_PATH))

    # BBL -> string fields; lots.geojson is already deduped to one row per lot,
    # so first record per BBL wins here as well.
    strings_by_bbl = {}
    for r in colp:
        b = r.get("b")
        if b is not None and b not in strings_by_bbl:
            strings_by_bbl[b] = (r.get("z") or "", r.get("ut") or "", r.get("pn") or "", r.get("ad") or "")

    ag_t, z_t, ut_t = Table(), Table(), Table()
    labels = []
    records = []
    skipped = 0

    for f in lots:
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates")
        gtype = geom.get("type")
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

        p = f.get("properties", {})
        z, ut, pn, ad = strings_by_bbl.get(p.get("b"), ("", "", "", ""))
        c = int(p.get("c") or 0)
        if c not in (1, 2, 3, 4):
            skipped += 1
            continue
        flags = (1 if p.get("fc") else 0) | (2 if p.get("le") else 0)
        rf100 = min(0xFFFF, int(round(float(p.get("rf") or 0) * 100)))
        ar = min(0xFFFFFFFF, max(0, int(p.get("ar") or 0)))

        records.append((
            c, flags, ag_t.add(p.get("ag")), ut_t.add(ut), z_t.add(z),
            rf100, ar, thin_ring(ring),
        ))
        labels.append((pn or ad or "").strip())

    print(f"Usable: {len(records)} parcels (skipped {skipped})")
    print(f"Tables: {len(ag_t.items)} agencies, {len(z_t.items)} zones, {len(ut_t.items)} uses")

    if len(ag_t.items) > 255 or len(ut_t.items) > 255 or len(z_t.items) > 65535:
        print("ERROR: string table overflow for field width", file=sys.stderr)
        sys.exit(1)

    vertex_count = sum(len(r[7]) for r in records)
    print(f"Vertices: {vertex_count} (avg {vertex_count / max(1, len(records)):.1f}/parcel)")

    PROCESSED_DIR.mkdir(exist_ok=True)
    with open(BIN_PATH, "wb") as out:
        out.write(struct.pack("<III", len(records), vertex_count, 0))
        offset = 0
        for c, flags, ag_i, ut_i, z_i, rf100, ar, ring in records:
            out.write(struct.pack(
                "<BBBBHHIIHH",
                c, flags, ag_i, ut_i, z_i, rf100, ar,
                offset, len(ring) & 0xFFFF, 0,
            ))
            offset += len(ring)
        for r in records:
            for x, y in r[7]:
                out.write(struct.pack("<HH", scale_lon(x), scale_lat(y)))

    meta = {"ag": ag_t.items, "z": z_t.items, "ut": ut_t.items, "labels": labels}
    with open(META_PATH, "w") as f:
        json.dump(meta, f, separators=(",", ":"), ensure_ascii=False)

    print(f"Wrote {BIN_PATH.name} ({BIN_PATH.stat().st_size / 1e6:.1f} MB), "
          f"{META_PATH.name} ({META_PATH.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
