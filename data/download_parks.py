#!/usr/bin/env python3
"""
Download NYC Parks Properties and export simplified polygons for web rendering.
Output: parks.json with coordinates and acquisition years.
"""

import json
import warnings
from pathlib import Path

import requests

warnings.filterwarnings("ignore", message=".*urllib3.*")
warnings.filterwarnings("ignore", message=".*InsecureRequest.*")

PROCESSED_DIR = Path(__file__).parent / "processed"
RAW_DIR = Path(__file__).parent / "raw"
PROCESSED_DIR.mkdir(exist_ok=True)
RAW_DIR.mkdir(exist_ok=True)

# Parks Properties dataset - GeoJSON resource endpoint (paginated)
PARKS_BASE = "https://data.cityofnewyork.us/resource/enfh-gkve.geojson"
BATCH_SIZE = 1000


def simplify_coords(coords, tolerance=0.00005):
    """Simplify a coordinate ring using Douglas-Peucker-like reduction.
    Keep every Nth point to reduce size while maintaining shape."""
    if len(coords) <= 8:
        return coords
    # Keep roughly 1 in N points, always keeping first and last
    n = max(2, len(coords) // 20)
    simplified = [coords[0]]
    for i in range(n, len(coords) - 1, n):
        simplified.append(coords[i])
    simplified.append(coords[-1])
    return simplified


def extract_year(date_str):
    """Extract year from acquisition date string."""
    if not date_str:
        return 0
    try:
        # Format: YYYY-MM-DDTHH:MM:SS or similar
        year = int(str(date_str)[:4])
        if 1600 <= year <= 2026:
            return year
    except (ValueError, IndexError):
        pass
    return 0


def main():
    print("=== Downloading NYC Parks Properties ===")

    raw_path = RAW_DIR / "parks_properties.geojson"

    if not raw_path.exists():
        print("  Downloading GeoJSON in batches...")
        all_features = []
        offset = 0
        while True:
            url = f"{PARKS_BASE}?$limit={BATCH_SIZE}&$offset={offset}"
            resp = requests.get(url, timeout=300, verify=False)
            resp.raise_for_status()
            data = resp.json()
            features = data.get("features", [])
            if not features:
                break
            all_features.extend(features)
            print(f"    Batch at offset {offset}: {len(features)} features (total: {len(all_features)})")
            if len(features) < BATCH_SIZE:
                break
            offset += BATCH_SIZE

        geojson = {"type": "FeatureCollection", "features": all_features}
        with open(raw_path, "w") as f:
            json.dump(geojson, f)
        print(f"  Downloaded: {raw_path.stat().st_size // 1024}KB")
    else:
        print(f"  [SKIP] Already downloaded: {raw_path.stat().st_size // 1024}KB")

    print("  Processing parks...")
    with open(raw_path) as f:
        geojson = json.load(f)

    parks = []
    for feat in geojson.get("features", []):
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        geom_type = geom.get("type", "")

        name = props.get("signname", "") or props.get("location", "") or ""
        acq_date = props.get("acquisitiondate", "")
        year = extract_year(acq_date)
        acres = 0
        try:
            acres = round(float(props.get("acres", 0) or 0), 2)
        except (ValueError, TypeError):
            pass

        borough = props.get("borough", "")

        # Extract polygon coordinates
        # Handle MultiPolygon and Polygon
        polys = []
        if geom_type == "MultiPolygon":
            for poly in geom.get("coordinates", []):
                # Each poly is a list of rings; take outer ring
                if poly and poly[0]:
                    simplified = simplify_coords(poly[0])
                    if len(simplified) >= 3:
                        # Round coordinates to 5 decimal places (~1m precision)
                        polys.append([[round(c[0], 5), round(c[1], 5)] for c in simplified])
        elif geom_type == "Polygon":
            coords = geom.get("coordinates", [])
            if coords and coords[0]:
                simplified = simplify_coords(coords[0])
                if len(simplified) >= 3:
                    polys.append([[round(c[0], 5), round(c[1], 5)] for c in simplified])

        if not polys:
            continue

        parks.append({
            "n": name,
            "y": year,
            "a": acres,
            "b": borough[0] if borough else "",
            "p": polys,
        })

    # Sort by year (0s last, then chronological)
    parks.sort(key=lambda p: (p["y"] == 0, p["y"]))

    out_path = PROCESSED_DIR / "parks.json"
    with open(out_path, "w") as f:
        json.dump(parks, f, separators=(",", ":"))

    size_kb = out_path.stat().st_size / 1024
    with_year = sum(1 for p in parks if p["y"] > 0)
    print(f"  Parks: {len(parks)} total, {with_year} with acquisition year")
    print(f"  Output: {out_path} ({size_kb:.0f}KB)")
    print(f"  Year range: {min(p['y'] for p in parks if p['y'] > 0)} - {max(p['y'] for p in parks if p['y'] > 0)}")


if __name__ == "__main__":
    main()
