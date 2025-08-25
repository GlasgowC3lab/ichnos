#!/usr/bin/env python3
"""Batch convert all Nextflow trace CSV files under data/trace into UniversalTrace CSVs.

For every *.csv in data/trace, we parse it using UniversalTrace.from_nextflow_trace_csv
and output a corresponding CSV with the same filename into data/universal_traces.
Existing files are overwritten. Empty outputs still contain a header.
"""
from pathlib import Path
from models.UniversalTrace import UniversalTrace
import sys

def convert_all(trace_dir: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_files = sorted([p for p in trace_dir.glob('*.csv')])
    if not csv_files:
        print(f"No trace CSV files found in {trace_dir}")
        return 0
    count_total = 0
    for src in csv_files:
        try:
            traces = UniversalTrace.from_nextflow_trace_csv(str(src))
        except Exception as e:
            print(f"[WARN] Failed to parse {src.name}: {e}")
            continue
        dest = out_dir / src.name
        UniversalTrace.to_csv(traces, str(dest))
        print(f"Converted {src.name}: {len(traces)} rows -> {dest.relative_to(Path.cwd())}")
        count_total += len(traces)
    return count_total

if __name__ == '__main__':
    repo_root = Path(__file__).resolve().parents[1]
    trace_dir = repo_root / 'data' / 'trace'
    out_dir = repo_root / 'data' / 'universal_traces'
    total = convert_all(trace_dir, out_dir)
    print(f"Done. Total UniversalTrace rows written: {total}")
