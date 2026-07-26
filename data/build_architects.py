#!/usr/bin/env python3
"""
Build architects.bin + architects_meta.json — who designed which building.

Source: the Landmarks Preservation Commission's Individual Landmark and
Historic District Building Database (gpmc-yuvp), 38,105 buildings that are
either individual landmarks or contributing buildings inside a historic
district. For each one the LPC recorded the architect or builder, the
architectural style, the primary material, the original use, the historic
district it belongs to, and a construction date.

This is the only citywide dataset that names architects. It covers about 4.5%
of the city's buildings — the designated ones — and roughly a quarter of those
have "Not determined" as the architect, which is preserved as-is rather than
silently dropped.

The LPC ships true building polygons, so the join to this map is exact
point-in-polygon rather than an address or radius guess.

architects.bin (little-endian):
  Header (8 bytes): uint32 record_count, uint32 vertex_pool_size
  Per-record (16 bytes):
    uint16 archIdx, styleIdx, distIdx, useIdx, matIdx  (indices into meta tables)
    uint32 vertexOffset
    uint16 vertexCount
  Vertex pool (4 bytes/vertex): uint16 lon, uint16 lat scaled across the NYC bbox

architects_meta.json: the string tables plus per-record date/name/altered,
which are too high-cardinality to dedupe usefully.
"""

import json
import struct
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

LON_MIN, LON_MAX = -74.27, -73.69
LAT_MIN, LAT_MAX = 40.49, 40.92
LON_RANGE = LON_MAX - LON_MIN
LAT_RANGE = LAT_MAX - LAT_MIN

SCRIPT_DIR = Path(__file__).parent
RAW_DIR = SCRIPT_DIR / "raw"
PROCESSED_DIR = SCRIPT_DIR / "processed"
RAW_CACHE = RAW_DIR / "lpc_buildings.json"
BIN_PATH = PROCESSED_DIR / "architects.bin"
META_PATH = PROCESSED_DIR / "architects_meta.json"

BASE = "https://data.cityofnewyork.us/resource/gpmc-yuvp.json"
FIELDS = ("the_geom,bin,bbl,des_addres,arch_build,alt_arch_1,style_prim,"
          "mat_prim,use_orig,hist_dist,date_combo,build_nme,lm_orig,altered")
PAGE = 5000

# The LPC writes this when the record is blank; keep it visible but don't
# treat it as a name.
UNKNOWN = {"not determined", "not determined,", "unknown", "none", ""}


def scale_lon(v):
    return max(0, min(65535, int((v - LON_MIN) / LON_RANGE * 65535)))


def scale_lat(v):
    return max(0, min(65535, int((v - LAT_MIN) / LAT_RANGE * 65535)))


def thin_ring(ring, tol=2):
    if len(ring) <= 5:
        return ring
    out = [ring[0]]
    lx, ly = scale_lon(ring[0][0]), scale_lat(ring[0][1])
    for v in ring[1:-1]:
        x, y = scale_lon(v[0]), scale_lat(v[1])
        if abs(x - lx) >= tol or abs(y - ly) >= tol:
            out.append(v)
            lx, ly = x, y
    out.append(ring[-1])
    return out if len(out) >= 4 else ring


def clean(v):
    v = (v or "").strip()
    return "" if v.lower() in UNKNOWN else v


def download():
    if RAW_CACHE.exists():
        print(f"  [SKIP] cached: {RAW_CACHE.stat().st_size // 1024}KB")
        return json.load(open(RAW_CACHE))

    print("  Downloading LPC building database...")
    rows, offset = [], 0
    while True:
        q = urllib.parse.urlencode({
            "$select": FIELDS, "$limit": PAGE, "$offset": offset, "$order": ":id",
        })
        for attempt in range(3):
            try:
                with urllib.request.urlopen(f"{BASE}?{q}", timeout=180) as r:
                    batch = json.load(r)
                break
            except Exception:
                if attempt == 2:
                    raise
                print("    retry...")
                time.sleep(5)
        if not batch:
            break
        rows.extend(batch)
        print(f"    {len(rows)} rows...")
        if len(batch) < PAGE:
            break
        offset += PAGE

    if not rows:
        print("ERROR: LPC download returned nothing", file=sys.stderr)
        sys.exit(1)
    RAW_DIR.mkdir(exist_ok=True)
    json.dump(rows, open(RAW_CACHE, "w"))
    return rows


class Table:
    def __init__(self):
        self.items, self.index = [""], {"": 0}

    def add(self, s):
        s = (s or "").strip()
        if s not in self.index:
            self.index[s] = len(self.items)
            self.items.append(s)
        return self.index[s]


def outer_ring(geom):
    if not geom:
        return None
    t, c = geom.get("type"), geom.get("coordinates")
    if not c:
        return None
    ring = c[0] if t == "Polygon" else (c[0][0] if t == "MultiPolygon" else None)
    return ring if ring and len(ring) >= 4 else None


def main():
    rows = download()
    print(f"  {len(rows)} LPC building records")

    arch_t, style_t, dist_t, use_t, mat_t = Table(), Table(), Table(), Table(), Table()
    records, dates, names, alts, skipped = [], [], [], [], 0

    for r in rows:
        ring = outer_ring(r.get("the_geom"))
        if not ring:
            skipped += 1
            continue
        ring = thin_ring(ring)
        records.append((
            arch_t.add(clean(r.get("arch_build"))),
            style_t.add(clean(r.get("style_prim"))),
            dist_t.add(clean(r.get("hist_dist"))),
            use_t.add(clean(r.get("use_orig"))),
            mat_t.add(clean(r.get("mat_prim"))),
            ring,
        ))
        dates.append(clean(r.get("date_combo")))
        names.append(clean(r.get("build_nme")))
        alts.append(clean(r.get("alt_arch_1")))

    vertex_count = sum(len(x[5]) for x in records)
    print(f"  Usable: {len(records)} (skipped {skipped} without geometry)")
    print(f"  Vertices: {vertex_count} | architects: {len(arch_t.items) - 1}, "
          f"styles: {len(style_t.items) - 1}, districts: {len(dist_t.items) - 1}")

    for name, t in (("architect", arch_t), ("style", style_t), ("district", dist_t),
                    ("use", use_t), ("material", mat_t)):
        if len(t.items) > 65535:
            print(f"ERROR: {name} table overflows uint16", file=sys.stderr)
            sys.exit(1)

    PROCESSED_DIR.mkdir(exist_ok=True)
    with open(BIN_PATH, "wb") as out:
        out.write(struct.pack("<II", len(records), vertex_count))
        off = 0
        for a, s, d, u, m, ring in records:
            out.write(struct.pack("<HHHHHIH", a, s, d, u, m, off, len(ring)))
            off += len(ring)
        written = 0
        for *_, ring in records:
            for x, y in ring:
                out.write(struct.pack("<HH", scale_lon(x), scale_lat(y)))
                written += 1
        assert written == vertex_count

    json.dump({
        "arch": arch_t.items, "style": style_t.items, "dist": dist_t.items,
        "use": use_t.items, "mat": mat_t.items,
        "dates": dates, "names": names, "alts": alts,
    }, open(META_PATH, "w"), separators=(",", ":"), ensure_ascii=False)

    named = sum(1 for a, *_ in records if a != 0)
    in_dist = sum(1 for r in records if r[2] != 0)
    print(f"  With a named architect: {named} ({100 * named / len(records):.0f}%)")
    print(f"  In a historic district: {in_dist}")
    print(f"  Wrote {BIN_PATH.name} ({BIN_PATH.stat().st_size / 1e6:.2f} MB), "
          f"{META_PATH.name} ({META_PATH.stat().st_size / 1e6:.2f} MB)")


if __name__ == "__main__":
    main()
