#!/usr/bin/env python3
"""
Build future.json — buildings that do not exist yet, and buildings the city
expects to lose.

Source: the live Building Footprints layer (5zhs-2jue). Most of its 1.08M rows
describe standing structures, but a few hundred describe the near future:

  Rising      feature_code 5100 (under construction) or 1003 (a placeholder
              triangle drawn for a permitted-but-unbuilt building), or
              last_status_type 'Marked for Construction'.
  Coming down last_status_type 'Marked for Demolition'.

The city also marks ~50 rows 'Investigate Construction' / 'Investigate
Demolition', meaning it is not certain the change happened. Those are excluded
here rather than presented as fact; the count is reported so the omission is
visible.

This is the mirror image of the ghost buildings overlay, which draws what has
already been demolished. Output is a small JSON file (a few hundred polygons),
loaded on demand by the Future spotlight.
"""

import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
RAW_DIR = SCRIPT_DIR / "raw"
PROCESSED_DIR = SCRIPT_DIR / "processed"
OUT_PATH = PROCESSED_DIR / "future.json"

BASE = "https://data.cityofnewyork.us/resource/5zhs-2jue.json"
FIELDS = ("the_geom,bin,base_bbl,mappluto_bbl,construction_year,"
          "feature_code,last_status_type,height_roof")

RISING_WHERE = ("feature_code IN (5100,1003) OR "
                "last_status_type = 'Marked for Construction'")
FALLING_WHERE = "last_status_type = 'Marked for Demolition'"
INVESTIGATE_WHERE = ("last_status_type IN "
                     "('Investigate Construction','Investigate Demolition')")

CAT_RISING, CAT_FALLING = 0, 1


def fetch(where, label):
    url = f"{BASE}?$select={urllib.parse.quote(FIELDS)}&$where={urllib.parse.quote(where)}&$limit=5000"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=180) as r:
                rows = json.load(r)
            print(f"  {label}: {len(rows)} rows")
            return rows
        except Exception:
            if attempt == 2:
                raise
            print("    retry...")
            time.sleep(5)


def count(where):
    q = urllib.parse.urlencode({"$select": "count(*) AS n", "$where": where})
    with urllib.request.urlopen(f"{BASE}?{q}", timeout=60) as r:
        return int(float(json.load(r)[0]["n"]))


def load_addresses():
    """BBL -> street address, from the PLUTO snapshot already on disk."""
    path = RAW_DIR / "pluto_all.json"
    if not path.exists():
        print("  [warn] pluto_all.json missing; addresses will be blank")
        return {}
    lookup = {}
    for rec in json.load(open(path)):
        bbl = str(rec.get("bbl", "")).split(".")[0].strip()
        addr = str(rec.get("address") or "").strip()
        if bbl and addr and addr.lower() not in ("0", "nan", "none"):
            lookup[bbl] = " ".join(addr.split())
    print(f"  Address lookup: {len(lookup)} BBLs")
    return lookup


def outer_ring(geom):
    """Outer ring of the first part, rounded to ~1m, or None."""
    if not geom:
        return None
    gtype, coords = geom.get("type"), geom.get("coordinates")
    if not coords:
        return None
    if gtype == "Polygon":
        ring = coords[0]
    elif gtype == "MultiPolygon":
        ring = coords[0][0]
    else:
        return None
    if len(ring) < 4:
        return None
    return [[round(x, 5), round(y, 5)] for x, y in ring]


def main():
    PROCESSED_DIR.mkdir(exist_ok=True)
    print("=== Future buildings ===")
    addresses = load_addresses()

    rising = fetch(RISING_WHERE, "rising")
    falling = fetch(FALLING_WHERE, "coming down")
    try:
        investigating = count(INVESTIGATE_WHERE)
    except Exception:
        investigating = 0

    out, skipped = [], 0
    for rows, cat in ((rising, CAT_RISING), (falling, CAT_FALLING)):
        for r in rows:
            ring = outer_ring(r.get("the_geom"))
            if not ring:
                skipped += 1
                continue
            bbl = (r.get("mappluto_bbl") or r.get("base_bbl") or "").split(".")[0].strip()
            cy = int(float(r.get("construction_year") or 0))
            hr = int(float(r.get("height_roof") or 0))
            out.append({
                "p": ring,
                "c": cat,
                "cy": cy if 1600 <= cy <= 2030 else 0,
                "hr": hr,
                "st": (r.get("last_status_type") or "").strip(),
                "ft": int(float(r.get("feature_code") or 0)),
                "addr": addresses.get(bbl, ""),
            })

    if not out:
        print("ERROR: no future-building features returned", file=sys.stderr)
        sys.exit(1)

    json.dump({"features": out, "excludedInvestigate": investigating},
              open(OUT_PATH, "w"), separators=(",", ":"))

    n_rise = sum(1 for f in out if f["c"] == CAT_RISING)
    n_fall = sum(1 for f in out if f["c"] == CAT_FALLING)
    with_addr = sum(1 for f in out if f["addr"])
    print(f"  Rising: {n_rise} | Coming down: {n_fall} | skipped (no geometry): {skipped}")
    print(f"  Excluded as 'investigate' (city unsure): {investigating}")
    print(f"  With address: {with_addr}/{len(out)}")
    print(f"  Wrote {OUT_PATH.name} ({OUT_PATH.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
