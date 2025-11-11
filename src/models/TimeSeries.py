from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List
import csv

@dataclass
class TimeSeries:
    """Generic time series representation used for auxiliary telemetry (RAPL, CPU usage, CI, etc.).

    Attributes:
      period: Sampling period (same time unit as start_timestamp; e.g. ms). Must be > 0.
      start_timestamp: Start timestamp (epoch in the chosen unit) for the first value.
      values: Ordered list of numeric measurements.
      ts_type: String label describing the series type (e.g. 'rapl', 'cpu_usage', 'ci').

    Export:
      to_csv(base_name, output_dir) -> Path
        Writes a CSV with columns: timestamp,value
        Each row i has timestamp = start_timestamp + i * period
        Filename pattern: {base_name}_{ts_type}_p{period}.csv
    """
    period: int
    start_timestamp: int
    values: List[float]
    ts_type: str

    def __post_init__(self):
        if self.period <= 0:
            raise ValueError("period must be positive")

    def __len__(self):
        return len(self.values)

    def to_csv(self, base_name: str, output_dir: Path | str) -> Path:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{base_name}_{self.ts_type}_p{self.period}.csv"
        out_path = output_dir / filename
        with open(out_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "value"])
            ts = self.start_timestamp
            for i, v in enumerate(self.values):
                writer.writerow([ts + i * self.period, v])
        return out_path

    @staticmethod
    def from_iterable(period: int, start_timestamp: int, values: Iterable[float], ts_type: str) -> 'TimeSeries':
        return TimeSeries(period=period, start_timestamp=start_timestamp, values=list(values), ts_type=ts_type)
