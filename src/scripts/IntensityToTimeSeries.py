#!/usr/bin/env python3
"""Bulk convert grid carbon intensity CSV files in data/intensity into
TimeSeries CSVs (timestamp,value) stored under data/intensity_timeseries.

Input formats observed:
  date,start,actual                       (hourly or 5-min / other cadence)
  date,start,end,forecast,actual,index    (UK carbon intensity style, 30-min)

We detect the sampling period automatically by computing the delta between the
first two chronological timestamps (seconds). All rows must share that period.

Output naming uses TimeSeries.to_csv pattern:
  {base_name}_{ts_type}_p{period}.csv
All outputs now use ts_type 'ci' (no distinction for marginal vs average).

Base name is the source filename (without extension) with spaces replaced by '_'.

Usage:
  PYTHONPATH=src python3 scripts/convert_intensity_to_timeseries.py
Optional flags:
  --force   Overwrite existing generated files.
"""
from __future__ import annotations
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple
import argparse
from datetime import datetime

# Import TimeSeries
from models.TimeSeries import TimeSeries  # PYTHONPATH should include src/

INTENSITY_DIR = Path('data/intensity')
OUTPUT_DIR = Path('data/intensity_timeseries')

@dataclass
class ParsedSeries:
    ts: TimeSeries
    source_path: Path
    output_path: Path


def parse_intensity_file(path: Path) -> TimeSeries:
    with open(path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        raise ValueError(f"No data rows in {path}")

    # Determine which column holds the numeric value; prefer 'actual', else raise.
    value_col = None
    for candidate in ('actual', 'value'):
        if candidate in reader.fieldnames:  # type: ignore[arg-type]
            value_col = candidate
            break
    if value_col is None:
        raise ValueError(f"Could not find intensity value column in {path}")

    # Build list of (timestamp_seconds, value)
    stamps: List[Tuple[int, float]] = []
    for r in rows:
        date_part = r.get('date') or r.get('Date')
        start_part = r.get('start') or r.get('Start')
        if not (date_part and start_part):
            raise ValueError(f"Missing date/start in row for {path}: {r}")
        # Combine date + time
        dt_str = f"{date_part} {start_part}"  # expected 'YYYY-MM-DD HH:MM'
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        except ValueError as e:
            raise ValueError(f"Failed to parse datetime '{dt_str}' in {path}: {e}")
        ts_sec = int(dt.timestamp())
        try:
            val = float(r[value_col]) if r[value_col] not in (None, '') else float('nan')
        except ValueError:
            val = float('nan')
        stamps.append((ts_sec, val))

    # Sort (should already be chronological)
    stamps.sort(key=lambda x: x[0])

    if len(stamps) < 2:
        raise ValueError(f"Not enough rows to determine period in {path}")

    # Determine period as smallest positive delta
    deltas = [b - a for (a, _), (b, _) in zip(stamps, stamps[1:])]
    deltas_pos = [d for d in deltas if d > 0]
    if not deltas_pos:
        raise ValueError(f"Could not compute positive time delta in {path}")
    period = min(deltas_pos)

    # Validate uniformity (allow occasional duplicates or missing intervals but warn)
    unique_deltas = sorted(set(deltas_pos))
    if len(unique_deltas) > 1:
        # Simple heuristic: if all deltas are multiples of the smallest, accept.
        multiples_ok = all(d % period == 0 for d in unique_deltas)
        if not multiples_ok:
            raise ValueError(f"Non-uniform sampling in {path}: deltas={unique_deltas}")

    start_timestamp = stamps[0][0]
    # Reconstruct values at uniform period steps; if gaps, we'll fill with NaN.
    values: List[float] = []
    ts_index = {ts: v for ts, v in stamps}
    # Determine expected length using last timestamp
    last_ts = stamps[-1][0]
    steps = ((last_ts - start_timestamp) // period) + 1
    for i in range(steps):
        ts = start_timestamp + i * period
        values.append(ts_index.get(ts, float('nan')))

    # Derive ts_type from filename
    name_lower = path.name.lower()
    # Unified naming: every intensity variant (including marginal) becomes 'ci'
    ts_type = 'ci'

    base_name = path.stem.replace(' ', '_')
    return TimeSeries(period=period, start_timestamp=start_timestamp, values=values, ts_type=ts_type)


def convert_all(force: bool = False, rename_existing: bool = False) -> List[ParsedSeries]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if rename_existing:
        renamed = rename_existing_outputs()
        if renamed:
            print(f"[rename] adjusted {len(renamed)} existing file names")
    parsed: List[ParsedSeries] = []
    for path in sorted(INTENSITY_DIR.glob('*.csv')):
        try:
            ts = parse_intensity_file(path)
            base_name = path.stem.replace(' ', '_')
            out_path = OUTPUT_DIR / f"{base_name}_{ts.ts_type}_p{ts.period}.csv"
            if out_path.exists() and not force:
                print(f"[skip] {out_path.name} (exists)")
                continue
            written = ts.to_csv(base_name, OUTPUT_DIR)
            parsed.append(ParsedSeries(ts=ts, source_path=path, output_path=written))
            print(f"[ok] {path.name} -> {written.name} ({len(ts)} points, period={ts.period}s)")
        except Exception as e:
            print(f"[err] {path.name}: {e}")
    return parsed


def rename_existing_outputs() -> List[Tuple[Path, Path]]:
    """Rename legacy outputs to unified *_ci_* naming.

    Patterns handled:
      *_intensity_p -> *_ci_p
      *_marg_intensity_p -> *_ci_p
      *_marg_ci_p -> *_ci_p
    Returns list of (old_path, new_path).
    """
    renamed: List[Tuple[Path, Path]] = []
    patterns = [
        ('_marg_intensity_', '_ci_'),
        ('_intensity_', '_ci_'),
        ('_marg_ci_', '_ci_'),
    ]
    for old in OUTPUT_DIR.glob('*.csv'):
        for src, dst in patterns:
            if src in old.name:
                new_name = old.name.replace(src, dst)
                new = old.with_name(new_name)
                if new == old:
                    continue
                if new.exists():
                    # If target exists, skip to avoid data loss.
                    break
                old.rename(new)
                renamed.append((old, new))
                break
    return renamed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--force', action='store_true', help='Overwrite existing outputs')
    ap.add_argument('--rename-existing', action='store_true', help='Rename legacy *_intensity_* files to *_ci_* variant before converting')
    args = ap.parse_args()
    convert_all(force=args.force, rename_existing=args.rename_existing)

if __name__ == '__main__':
    main()
