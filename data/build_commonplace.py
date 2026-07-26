#!/usr/bin/env python3
"""
Build data/processed/commonplace.json — the city's own name for a building.

Source: CommonPlace (NYC Open Data / Socrata t95h-5fsr), published by the
Office of Technology and Innovation. It is the gazetteer behind Geosupport,
the city's geocoder: the file that resolves a name ("PS 61", "Rikers Island")
to a building, a street segment and a coordinate. Its stated purpose, per the
agency's own data dictionary, is to aid dispatch, so coverage follows what
agencies get called out to rather than what is notable. Roughly 1 percent of
the city's buildings carry a name here — but they are disproportionately the
ones someone would hover over.

Two rules enforced below, both from that data dictionary (PointOfInterest.pdf,
attached to the dataset):

1. NYPD-sourced rows are dropped unless they are public-safety facilities.
   The metadata states: "NYPD common places, with the exception of precinct
   locations, cannot be distributed." Most are ordinary places that entered
   through the Sprint/PCAD dispatch systems and are carried by another source
   anyway.

2. Only FACILITY_TYPE is decoded, never FACILITY_DOMAINS. The published table
   has 13 categories and FACILITY_TYPE matches it exactly; the column named
   FACILITY_DOMAINS carries 18 distinct values the table does not describe.

Note this is a *spatial* layer, not a join. PLUTO tax lots carry a BBL, not a
BIN, so there is no key to join CommonPlace on — names are matched to whatever
the cursor is over by proximity at runtime.

Standard library only. Run: python3 data/build_commonplace.py
"""
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROCESSED_DIR = Path(__file__).parent / "processed"
OUT_PATH = PROCESSED_DIR / "commonplace.json"

BASE = "https://data.cityofnewyork.us/resource/t95h-5fsr.json"
PAGE = 20000
NYC = (40.40, 41.00, -74.30, -73.60)  # lat_min, lat_max, lng_min, lng_max
UA = "nyc-building-age/commonplace (github.com/joshgreenman1973)"

# FACILITY TYPE, verbatim from the dataset's data dictionary.
TYPES = {
    1: "Residential", 2: "Education", 3: "Cultural", 4: "Recreation",
    5: "Social services", 6: "Transportation", 7: "Commercial",
    8: "Government", 9: "Religious", 10: "Health", 11: "Public safety",
    12: "Water", 13: "Miscellaneous",
}
TYPE_ORDER = [TYPES[i] for i in sorted(TYPES)]

# Names arrive shouting in all caps.
KEEP_UPPER = {
    "NYC", "NYPD", "FDNY", "EMS", "NYCHA", "MTA", "DOT", "DEP", "DOE", "DSNY",
    "HRA", "OCME", "DHS", "DOB", "DOF", "DCP", "OEM", "USA", "US", "UN", "NY",
    "PS", "IS", "MS", "JHS", "HS", "PK", "RC", "JFK", "LGA", "PATH", "BQE",
    "YMCA", "YMHA", "YWCA", "CUNY", "SUNY", "NYU", "PAL", "VA", "TV", "AME",
    "BMT", "IRT", "IND", "LIRR", "FDR", "SI", "II", "III", "IV", "VI", "VII",
    "VIII", "NE", "NW", "SE", "SW", "PL", "SQ", "BLVD", "AVE",
}
SMALL = {"of", "the", "and", "at", "on", "for", "in", "de", "la", "el", "von", "van"}
ABBREV = {"FT": "Ft", "MT": "Mt", "DR": "Dr", "JR": "Jr", "SR": "Sr", "RD": "Rd"}


def fetch(params):
    url = BASE + "?" + urllib.parse.urlencode(params)
    last = None
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.load(r)
        except Exception as e:  # noqa: BLE001 - transient network/HTTP
            last = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Socrata request failed after retries: {last}\n{url}")


def paginate(params):
    offset = 0
    while True:
        page = fetch({**params, "$limit": PAGE, "$offset": offset})
        if not page:
            return
        yield from page
        if len(page) < PAGE:
            return
        offset += PAGE


def titlecase(name):
    """ALL CAPS -> readable, preserving acronyms and school designations."""
    out = []
    toks = name.split()
    for i, tok in enumerate(toks):
        lead = re.match(r"^\W*", tok).group(0)
        trail = re.search(r"\W*$", tok).group(0)
        core = tok[len(lead):len(tok) - len(trail)] if trail else tok[len(lead):]
        if not core:
            out.append(tok)
            continue
        up = core.upper()
        prev = toks[i - 1] if i else ""
        if up == "ST":
            # Street when it trails a number or ordinal, Saint otherwise.
            new = "ST" if any(ch.isdigit() for ch in prev) else "St"
        elif up in ABBREV:
            new = ABBREV[up]
        elif up in KEEP_UPPER:
            new = up
        elif any(ch.isdigit() for ch in core):
            new = up
        elif core.lower() in SMALL and i > 0:
            new = core.lower()
        elif len(core) <= 2 and core.isalpha():
            new = up
        else:
            new = core.capitalize()
            new = re.sub(r"^(O')(\w)", lambda m: m.group(1) + m.group(2).upper(), new)
            new = re.sub(r"^(Mc)([a-z])", lambda m: m.group(1) + m.group(2).upper(), new)
        out.append(lead + new + trail)
    return " ".join(out)


def main():
    print("Fetching CommonPlace (t95h-5fsr)...")
    rows = list(paginate({
        "$select": "feature_name,facility_type,source,saftype,the_geom,modified_date",
    }))
    print(f"  Rows: {len(rows)}")
    if not rows:
        raise RuntimeError("CommonPlace returned zero rows — refusing to write an empty file.")
    if len(rows) < 15000:
        raise RuntimeError(
            f"CommonPlace returned only {len(rows)} rows; the file has carried "
            "roughly 20,000 for years. Refusing to write a suspiciously short build."
        )

    pts, counts = [], {}
    dropped = {"nypd_restricted": 0, "outside_nyc": 0, "no_geom": 0, "no_type": 0}
    newest = ""

    for r in rows:
        src = (r.get("source") or "").strip()
        try:
            ti = int(float(r.get("facility_type")))
        except (TypeError, ValueError):
            dropped["no_type"] += 1
            continue
        if ti not in TYPES:
            dropped["no_type"] += 1
            continue
        # The data dictionary forbids redistributing NYPD common places other
        # than precinct/public-safety locations.
        if src == "NYPD" and ti != 11:
            dropped["nypd_restricted"] += 1
            continue

        coords = (r.get("the_geom") or {}).get("coordinates") or []
        if len(coords) != 2:
            dropped["no_geom"] += 1
            continue
        lng, lat = float(coords[0]), float(coords[1])
        if not (NYC[0] <= lat <= NYC[1] and NYC[2] <= lng <= NYC[3]):
            dropped["outside_nyc"] += 1
            continue

        name = titlecase((r.get("feature_name") or "").strip())
        if not name:
            continue

        # SAFTYPE, per the data dictionary: G = "Complex NAP" (the whole
        # campus or terminal), X = "Constituent NAP" (a part inside it).
        # Ranking the parent above its parts stops a hover over Grand Central
        # from returning the gift shop that happens to sit two meters closer.
        saf = (r.get("saftype") or "").strip().upper()
        rank = 0 if saf == "G" else (2 if saf == "X" else 1)

        label = TYPES[ti]
        counts[label] = counts.get(label, 0) + 1
        pts.append([round(lat, 5), round(lng, 5), name, TYPE_ORDER.index(label), rank])

        md = r.get("modified_date") or ""
        if md > newest:
            newest = md

    if len(pts) < 12000:
        raise RuntimeError(f"Only {len(pts)} points survived filtering — expected well over 12,000.")

    payload = {
        "types": TYPE_ORDER,
        "pts": sorted(pts, key=lambda p: (p[0], p[1])),
        "counts": counts,
        "dropped": dropped,
        "built": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "modified": newest[:10],
    }
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)

    kb = os.path.getsize(OUT_PATH) / 1024
    print(f"Wrote {OUT_PATH} — {len(pts)} named places, {kb:.0f} KB")
    print(f"  dropped: {dropped}")
    print(f"  newest modified_date: {newest[:10]}")


if __name__ == "__main__":
    sys.exit(main())
