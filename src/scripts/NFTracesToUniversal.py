#!/usr/bin/env python3
"""Batch convert all Nextflow trace CSV files under data/trace into UniversalTrace CSVs.

For every *.csv in data/trace, we parse it using UniversalTrace.from_nextflow_trace_csv
and output a corresponding CSV with the same filename into data/universal_traces.
Existing files are overwritten. Empty outputs still contain a header.
"""
import os
import glob
from src.models.UniversalTrace import UniversalTrace
import sys

def convert_all(trace_dir: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    csv_files = sorted(glob.glob(os.path.join(trace_dir, '*.csv')))
    if not csv_files:
        print(f"No trace CSV files found in {trace_dir}")
        return 0
    count_total = 0
    for src in csv_files:
        try:
            traces = UniversalTrace.from_nextflow_trace_csv(src)
        except Exception as e:
            print(f"[WARN] Failed to parse {os.path.basename(src)}: {e}")
            continue
        dest = os.path.join(out_dir, os.path.basename(src))
        UniversalTrace.to_csv(traces, dest)
        rel_dest = os.path.relpath(dest, os.getcwd())
        print(f"Converted {os.path.basename(src)}: {len(traces)} rows -> {rel_dest}")
        count_total += len(traces)
    return count_total

if __name__ == '__main__':
    trace_dir = 'data/trace'
    out_dir = 'data/universal_traces'
    total = convert_all(trace_dir, out_dir)
    print(f"Done. Total UniversalTrace rows written: {total}")
