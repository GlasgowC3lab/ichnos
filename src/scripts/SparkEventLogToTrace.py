# Script to Convert Spark Event Log to Standardized Task Trace CSV Format

# Imports
from src.utils.Usage import print_usage_exit_SparkEventLogToTrace as print_usage_exit

import json
import csv
import sys
from typing import Dict

# Constants
INPUT_FILE_NAME = "input-file-name"
OUTPUT_FILE = "output-file"
CSV_HEADERS = [
    "id", "name", "start", "end", "cpu_count", "avg_cpu_usage",
    "cpu_model", "memory", "hostname", "rapl_timeseries", "cpu_usage_timeseries"
]


# Functions
def load_hostname_mapping(input_file_name: str) -> Dict[str, str]:
    """
    Load the IP-to-hostname mapping from the _hosts.csv file.

    Args:
        input_file_name (str): Name of the Spark event log file.

    Returns:
        Dict[str, str]: Dictionary mapping IP addresses to hostnames.
    """
    # Construct the mapping file path
    base_name = input_file_name.rsplit('.', 1)[0] if '.' in input_file_name else input_file_name
    mapping_file_path = f"data/spark_event_logs/{base_name}_hosts.csv"

    ip_to_hostname = {}

    try:
        with open(mapping_file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ip = row.get('ip', '').strip()
                hostname = row.get('hostname', '').strip()
                if ip and hostname:
                    ip_to_hostname[ip] = hostname
        print(f"[SparkEventLogToTrace] Loaded {len(ip_to_hostname)} hostname mappings from [{mapping_file_path}]")
    except FileNotFoundError:
        print(f"[SparkEventLogToTrace] No hostname mapping file found at [{mapping_file_path}], using IP addresses")
    except Exception as e:
        print(f"[SparkEventLogToTrace] Error reading hostname mapping: {e}, using IP addresses")

    return ip_to_hostname


def parse_spark_event_log(input_file_name: str, output_path: str) -> None:
    """
    Parse a Spark event log JSON file from the data/spark_event_logs directory and convert it to a CSV trace file.

    Args:
        input_file_name (str): Name of the Spark event log file.
        output_path (str): Path for the output CSV file.
    """
    # Load hostname mapping
    ip_to_hostname = load_hostname_mapping(input_file_name)

    input_path = f"data/spark_event_logs/{input_file_name}"
    with open(input_path, 'r') as infile, open(output_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(CSV_HEADERS)
        
        for line in infile:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            if event.get("Event") == "SparkListenerTaskEnd":
                task_info = event.get("Task Info", {})
                task_metrics = event.get("Task Metrics", {})

                stage_id = event.get("Stage ID")
                task_id = task_info.get("Task ID")
                host = task_info.get("Host", "")
                launch_ms = task_info.get("Launch Time")
                finish_ms = task_info.get("Finish Time")

                if None in (stage_id, task_id, launch_ms, finish_ms):
                    continue

                duration_ms = finish_ms - launch_ms
                duration_s = duration_ms / 1000.0
                cpu_time_ns = task_metrics.get("Executor CPU Time", 0)
                peak_mem = task_metrics.get("Peak Execution Memory", 0)

                if duration_ms > 0:
                    cpu_util_fraction = (cpu_time_ns / 1e6) / duration_ms
                    avg_cpu_percent = round(cpu_util_fraction * 100, 2)
                else:
                    avg_cpu_percent = 0.0

                start_unix = int(launch_ms / 1000)
                end_unix = int(finish_ms / 1000)

                trace_id = f"{stage_id}_{task_id}"
                name = f"stage_{stage_id}_task_{task_id}"

                # Map IP address to hostname if available
                hostname = ip_to_hostname.get(host, host)

                writer.writerow([
                    trace_id,
                    name,
                    start_unix,
                    end_unix,
                    1,                    # cpu_count
                    avg_cpu_percent,
                    "",                   # cpu_model
                    peak_mem,
                    hostname,
                    "",                   # rapl_timeseries
                    ""                    # cpu_usage_timeseries
                ])
    
    print(f"[SparkEventLogToTrace] Successfully converted event log to trace file [{output_path}]")


def validate_arguments(args: list[str]) -> Dict[str, str]:
    """
    Validate and parse command-line arguments for the SparkEventLogToTrace script.

    Args:
        args (list[str]): List of command-line arguments.
    
    Returns:
        Dict[str, str]: Parsed and validated settings dictionary.
    """
    if len(args) != 2:
        print_usage_exit()
    
    return {
        INPUT_FILE_NAME: args[0],
        OUTPUT_FILE: args[1]
    }


def convert_spark_log(input_file_name: str, output_file: str) -> None:
    """
    Main conversion function that processes the Spark event log.

    Args:
        input_file_name (str): Name of the Spark event log file in data/spark_event_logs.
        output_file (str): Path to the output CSV file.
    """
    parse_spark_event_log(input_file_name, output_file)


# Main
if __name__ == "__main__":
    arguments = sys.argv[1:]
    settings = validate_arguments(arguments)
    convert_spark_log(settings[INPUT_FILE_NAME], settings[OUTPUT_FILE])
