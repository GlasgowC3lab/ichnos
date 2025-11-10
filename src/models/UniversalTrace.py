import csv
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class UniversalTrace:
    """A minimal, universal trace record for workflow tasks. The idea is to provide a consistent schema for representing task execution data across different systems. Any kind of workload (Nextflow, Spark, Airflow, other) that can be broken down into a set of tasks can be represented by this trace format.

    Required fields:
      - id: unique task identifier
      - start: start timestamp (epoch ms or ns – upstream convention)
      - end: completion timestamp (same unit as start)
      - cpu_count: number of CPUs requested/used
      - avg_cpu_usage: average CPU utilisation (0-100 or 0-1 depending on source) – preserved as given
      - cpu_model: CPU model string (may be empty if unknown)
      - memory: memory usage/request (bytes unless upstream converts)
      - hostname: hostname/node where the task executed (may be empty if unknown)

    Optional fields (still emitted as CSV columns; empty string when missing):
      - rapl_timeseries: filename containing RAPL (energy/power) time series
      - cpu_usage_timeseries: filename containing detailed CPU usage time series
    """
    id: str
    name: str  # human-readable task / process name
    start: int
    end: int
    cpu_count: int
    avg_cpu_usage: float
    cpu_model: str
    memory: float
    hostname: str
    rapl_timeseries: Optional[str] = None
    cpu_usage_timeseries: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'name': self.name,
            'start': self.start,
            'end': self.end,
            'cpu_count': self.cpu_count,
            'avg_cpu_usage': self.avg_cpu_usage,
            'cpu_model': self.cpu_model,
            'memory': self.memory,
            'hostname': self.hostname,
            'rapl_timeseries': self.rapl_timeseries or '',
            'cpu_usage_timeseries': self.cpu_usage_timeseries or ''
        }

    @staticmethod
    def fieldnames() -> List[str]:
        return [
            'id', 'name', 'start', 'end', 'cpu_count', 'avg_cpu_usage', 'cpu_model', 'memory',
            'hostname', 'rapl_timeseries', 'cpu_usage_timeseries'
        ]

    @staticmethod
    def to_csv(traces: List['UniversalTrace'], filepath: str):
        if not traces:
            # still create an empty file with header for schema visibility
            with open(filepath, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=UniversalTrace.fieldnames())
                writer.writeheader()
            return
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=UniversalTrace.fieldnames())
            writer.writeheader()
            for t in traces:
                writer.writerow(t.to_dict())

    @staticmethod
    def from_csv(filepath: str) -> List['UniversalTrace']:
        traces: List[UniversalTrace] = []
        with open(filepath, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if not row.get('id'):
                    continue
                traces.append(UniversalTrace(
                    id=row['id'],
                    name=row.get('name') or '',
                    start=int(row.get('start') or 0),
                    end=int(row.get('end') or 0),
                    cpu_count=int(row.get('cpu_count') or 0),
                    avg_cpu_usage=float(row.get('avg_cpu_usage') or 0.0),
                    cpu_model=row.get('cpu_model') or '',
                    memory=float(row.get('memory') or 0.0),
                    hostname=row.get('hostname') or '',
                    rapl_timeseries=(row.get('rapl_timeseries') or None) or None,
                    cpu_usage_timeseries=(row.get('cpu_usage_timeseries') or None) or None
                ))
        return traces

    # --- Adapters ---
    @staticmethod
    def from_nextflow_trace_csv(filepath: str) -> List['UniversalTrace']:
        """Parse a Nextflow trace CSV file and return UniversalTrace records.

        Expected Nextflow columns (lenient): id, start, complete, cpus|cpu, %cpu|cpu_usage, cpu_model, memory|rss
        We normalise column names to UniversalTrace schema.
        Missing numeric fields default to 0; missing strings to ''.
        """
        traces: List[UniversalTrace] = []
        with open(filepath, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                task_id = row.get('id') or row.get('task_id') or row.get('hash')
                if not task_id:
                    continue
                # Prefer explicit process/task header as name; fallback to id
                name = row.get('process') or row.get('task') or row.get('name') or task_id
                start = int(row.get('start') or 0)
                end = int(row.get('complete') or row.get('end') or 0)
                cpus_raw = row.get('cpus') or row.get('cpu') or 0
                try:
                    cpu_count = int(float(cpus_raw))
                except ValueError:
                    cpu_count = 0
                cpu_usage_raw = row.get('%cpu') or row.get('cpu_usage') or row.get('cpuUsage') or 0.0
                try:
                    avg_cpu_usage = float(cpu_usage_raw)
                except ValueError:
                    avg_cpu_usage = 0.0
                cpu_model = row.get('cpu_model') or row.get('cpuModel') or ''
                memory_raw = row.get('memory') or row.get('rss') or 0.0
                # Nextflow memory may be strings like '123 MB' - attempt to parse numeric prefix
                if isinstance(memory_raw, str):
                    mem_tokens = memory_raw.strip().split()
                    try:
                        memory_val = float(mem_tokens[0])
                    except (ValueError, IndexError):
                        memory_val = 0.0
                else:
                    try:
                        memory_val = float(memory_raw)
                    except ValueError:
                        memory_val = 0.0
                hostname = row.get('hostname') or row.get('host') or row.get('node') or ''
                traces.append(UniversalTrace(
                    id=task_id,
                    name=name or '',
                    start=start,
                    end=end,
                    cpu_count=cpu_count,
                    avg_cpu_usage=avg_cpu_usage,
                    cpu_model=cpu_model,
                    memory=memory_val,
                    hostname=hostname
                ))
        return traces
