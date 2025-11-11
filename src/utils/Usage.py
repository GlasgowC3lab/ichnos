"""
Module: Usage
This module provides utility functions to print usage instructions and exit
for various scripts within the Ichnos project.
"""

import logging
from src.Constants import ELECTRICITY_MAPS, NATIONAL_GRID

def print_usage_exit_TemporalInterrupt() -> None:
    """
    Print usage information for the TemporalInterrupt script and exit.
    """
    usage = "$ python -m src.scripts.TemporalInterrupt <trace> <ci-file-name> <power_model> <? interval=60> <? pue=1.0> <? memory-coeff=0.392>"
    example = "$ python -m src.scripts.TemporalInterrupt nanoseq ci gpg14_performance_minmax 60 1.0 0.392"
    logging.error(usage)
    logging.error(example)
    exit(-1)


def print_usage_exit_FetchCarbonIntensity() -> None:
    """
    Print usage information for the FetchCarbonIntensity script and exit.
    """
    usage = "[FetchCarbonIntensity] Usage: py FetchCarbonIntensity.py <source> <YYYY-MM-DD:HH-MM> <YYYY-MM-DD:HH-MM>"
    example1 = f"[FetchCarbonIntensity] $ py FetchCarbonIntensity.py {ELECTRICITY_MAPS} 2024-03-01:09-00 2024-03-03:17-00"
    example2 = f"[FetchCarbonIntensity] $ py FetchCarbonIntensity.py {NATIONAL_GRID} 2024-03-01:09-00 2024-03-01:17-00"
    logging.error(usage)
    logging.error(example1)
    logging.error(example2)
    exit(-1)


def print_usage_exit_ExtractTimeline() -> None:
    """
    Print usage information for the ExtractTimeline script and exit.
    """
    usage = "carbon-footprint $ python -m src.scripts.ExtractTimeline <trace-file-name>"
    example = "carbon-footprint $ python -m src.scripts.ExtractTimeline test"
    logging.error(usage)
    logging.error(example)
    exit(-1)


def print_usage_exit_ExportCarbonIntensity() -> None:
    """
    Print usage information for the ExportCarbonIntensity script and exit.
    """
    usage = "[ExportCarbonIntensity] Expected Usage: py ExportCarbonIntensity.py <YYYY-MM-DD> <YYYY-MM-DD> <region>"
    example = "[ExportCarbonIntensity] Example Use: py ExportCarbonIntensity.py 2023-11-26 2023-11-28 de"
    logging.error(usage)
    logging.error(example)
    exit(-1)


def print_usage_exit_Explorer() -> None:
    """
    Print usage information for the Explorer script and exit.
    """
    usage = "[Explorer] Expected Usage: py explorer.py <trace-file> <ci-file> <shift> <model-name> <interval> <pue> <memory-coefficient>"
    example = "[Explorer] Example Use: py explorer.py test ci-20240218 6 gpg14_performance 60 1.0 0.392"
    logging.error(usage)
    logging.error(example)
    exit(-1)


def print_usage_exit_Convertor() -> None:
    """
    Print usage information for the Convertor script and exit.
    """
    usage = ("[Convertor] $ Expected Use - Arguments Format: <change-command> <trace-file-name.end> "
             "<delimiter> <direction|new-start> <shift|original-start> <output-name>")
    example_time = "[Convertor] $ adjust file by days-hours-minutes: change-time test.csv , + 00-06-30 changed"
    example_stamp = "[Convertor] $ adjust file by ms: change-ms test.csv , + 23400000 changed"
    example_start = "[Convertor] $ adjust file start time using date: change-start test.csv , 2024-03-12:09-00 2024-01-01:10-00 changed"
    example_start_ms = "[Convertor] $ adjust file start time using ms: change-start-ms test.csv , 2024-03-12:09-00 1701083201729 changed"
    logging.error(usage)
    logging.error(example_time)
    logging.error(example_stamp)
    logging.error(example_start)
    logging.error(example_start_ms)
    exit(-1)

def print_usage_exit_SparkEventLogToTrace() -> None:
    """
    Print usage information for the SparkEventLogToTrace script and exit.
    """
    usage = "[SparkEventLogToTrace] Usage: python -m src.scripts.SparkEventLogToTrace <input-file-name> <output-file>"
    example = "[SparkEventLogToTrace] Example: python -m src.scripts.SparkEventLogToTrace spark-event-log output/tasks_trace.csv"
    logging.error(usage)
    logging.error(example)
    exit(-1)
