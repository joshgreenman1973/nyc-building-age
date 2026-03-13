#!/usr/bin/env python3
"""
Extract addresses from cached PLUTO data in the same order as buildings.bin.
Must replicate the same filtering/ordering as pipeline.py step3 + step7.
"""
import json
import pandas as pd
from pathlib import Path

RAW_DIR = Path(__file__).parent / "raw"
PROCESSED_DIR = Path(__file__).parent / "processed"

print("Loading PLUTO data...")
with open(RAW_DIR / "pluto_all.json") as f:
    data = json.load(f)

print(f"  Records: {len(data)}")
df = pd.DataFrame(data)

# Replicate pipeline.py step3 processing
df["bbl"] = df["bbl"].astype(str).str.split(".").str[0].str.strip()
df["yearbuilt"] = pd.to_numeric(df.get("yearbuilt", 0), errors="coerce").fillna(0).astype(int)
df["numfloors"] = pd.to_numeric(df.get("numfloors", 0), errors="coerce").fillna(0).astype(int)
df["borocode"] = pd.to_numeric(df.get("borocode", 0), errors="coerce").fillna(0).astype(int)
df["latitude"] = pd.to_numeric(df.get("latitude", 0), errors="coerce").fillna(0).astype(float)
df["longitude"] = pd.to_numeric(df.get("longitude", 0), errors="coerce").fillna(0).astype(float)
df["lotarea"] = pd.to_numeric(df.get("lotarea", 0), errors="coerce").fillna(0).astype(int)

df.loc[df["yearbuilt"] < 1630, "yearbuilt"] = 0
df.loc[df["yearbuilt"] > 2026, "yearbuilt"] = 0

before = len(df)
df = df[(df["latitude"] != 0) & (df["longitude"] != 0)].copy()
print(f"  Filtered to {len(df)} records (removed {before - len(df)} without coords)")

# Write addresses in same order
addr_path = PROCESSED_DIR / "addresses.txt"
print("Writing addresses...")
with open(addr_path, "w") as f:
    for addr in df["address"]:
        addr = str(addr).strip() if pd.notna(addr) else ""
        if addr.lower() in ("", "0", "nan", "none"):
            addr = ""
        else:
            addr = " ".join(addr.split())
        f.write(addr + "\n")

size_mb = addr_path.stat().st_size / (1024 * 1024)
print(f"  Written: {addr_path} ({size_mb:.1f} MB, {len(df)} lines)")

# Verify count matches binary
bin_path = PROCESSED_DIR / "buildings.bin"
bin_count = bin_path.stat().st_size // 24  # BYTES_PER_RECORD = 24
print(f"  Binary has {bin_count} records")
if bin_count == len(df):
    print("  ✓ Counts match!")
else:
    print(f"  ✗ MISMATCH! addresses={len(df)}, binary={bin_count}")
