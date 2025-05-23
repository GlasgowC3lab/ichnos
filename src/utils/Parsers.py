"""
Module: Parsers
This module contains functions to parse input files and command-line arguments
for carbon footprint calculations. It provides utilities for parsing CI files,
trace files, and for validating input values.
"""

import logging
from typing import Tuple, List, Dict, Union
from src.Constants import *
from src.models.TraceRecord import TraceRecord

"""
This parses arguments for the CarbonFootprint script (getting deprecated).
"""
def parse_arguments_CarbonFootprint(args: List[str]) -> Dict[str, Union[float, int, str]]:
    """
    Parse command-line arguments for the CarbonFootprint script.

    Expected arguments:
      - [0]: trace file name
      - [1]: carbon intensity value or file name
      - [2]: minimum watts (float)
      - [3]: maximum watts (float)
      Optionally:
      - [4]: PUE (float)
      - [5]: memory coefficient (float)
      For 8 arguments, additional reserved memory (float) and number of nodes (int) are expected.
    
    :param args: List of argument strings.
    :return: Dictionary mapping argument names to their parsed values.
    """
    if len(args) != 4 and len(args) != 6 and len(args) != 8:
        _print_usage_exit_CarbonFootprint()

    arguments: Dict[str, Union[float, int, str]] = {}
    arguments[TRACE] = args[0]
    
    if _check_if_float(args[1]):
        arguments[CI] = float(args[1])
    else:
        arguments[CI] = args[1]
    
    arguments[MIN_WATTS] = float(args[2])
    arguments[MAX_WATTS] = float(args[3])
    
    if len(args) == 6:
        arguments[PUE] = float(args[4])
        arguments[MEMORY_COEFFICIENT] = float(args[5])
    elif len(args) == 8:
        arguments[PUE] = float(args[4])
        arguments[MEMORY_COEFFICIENT] = float(args[5])
        arguments[RESERVED_MEMORY] = float(args[6])
        arguments[NUM_OF_NODES] = int(args[7])
    else:
        arguments[PUE] = DEFAULT_PUE_VALUE
        arguments[MEMORY_COEFFICIENT] = DEFAULT_MEMORY_POWER_DRAW

    return arguments

"""
This parses arguments for the IchnosCF script.
"""
def parse_arguments(args: List[str]) -> Dict[str, Union[float, int, str]]:
    """
    Parse command-line arguments for the IchnosCF script.

    Expected arguments:
      - [0]: trace file name
      - [1]: carbon intensity value or file name
      - [2]: power model name
      Optionally:
      - [3]: interval (int)
      - Given 6 arguments, [4] is PUE and [5] is memory coefficient.
      - With 8 arguments, additional reserved memory (float) and number of nodes (int) are provided.
      Defaults are used when only 3 arguments are provided.
    
    :param args: List of argument strings.
    :return: Dictionary mapping argument names to their parsed values.
    """
    if len(args) != 3 and len(args) != 4 and len(args) != 6 and len(args) != 8:
        _print_usage_exit_IchnosCF()

    arguments: Dict[str, Union[float, int, str]] = {}
    arguments[TRACE] = args[0]
    
    if _check_if_float(args[1]):
        arguments[CI] = float(args[1])
    else:
        arguments[CI] = args[1]
    
    arguments[MODEL_NAME] = args[2]
    
    if len(args) == 4:
        arguments[INTERVAL] = int(args[3])
        arguments[PUE] = DEFAULT_PUE_VALUE
        arguments[MEMORY_COEFFICIENT] = DEFAULT_MEMORY_POWER_DRAW
    elif len(args) == 6:
        arguments[INTERVAL] = int(args[3])
        arguments[PUE] = float(args[4])
        arguments[MEMORY_COEFFICIENT] = float(args[5])
    elif len(args) == 8:
        arguments[INTERVAL] = int(args[3])
        arguments[PUE] = float(args[4])
        arguments[MEMORY_COEFFICIENT] = float(args[5])
        arguments[RESERVED_MEMORY] = float(args[6])
        arguments[NUM_OF_NODES] = int(args[7])
    else:
        arguments[INTERVAL] = 60
        arguments[PUE] = DEFAULT_PUE_VALUE
        arguments[MEMORY_COEFFICIENT] = DEFAULT_MEMORY_POWER_DRAW

    return arguments

def parse_ci_intervals(filename: str) -> Dict[str, float]:
    """
    Parse a carbon intensity intervals file.
    
    :param filename: Path to the CI file.
    :return: Dictionary mapping time keys to carbon intensity values.
    """
    (header, data) = _get_ci_file_data(filename)

    date_i = header.index("date")
    start_i = header.index("start")
    value_i = header.index("actual")

    ci_map: Dict[str, float] = {}

    for row in data:
        parts = row.split(",")
        date = parts[date_i]
        month_day = '/'.join([val.zfill(2) for val in date.split('-')[-2:]])
        key = month_day + '-' + parts[start_i]
        try:
            value = float(parts[value_i])
        except ValueError:
            logging.error("Invalid carbon intensity value in file %s", filename)
            continue
        ci_map[key] = value

    return ci_map


def parse_trace_file(filepath: str) -> List[TraceRecord]:
    """
    Parse a trace file into a list of TraceRecord objects.
    
    :param filepath: Path to the trace file.
    :return: List of TraceRecord objects.
    """
    try:
        with open(filepath, 'r') as file:
            lines = [line.rstrip() for line in file]
    except Exception as e:
        logging.error("Error opening trace file %s: %s", filepath, e)
        raise
    header = lines[0]
    records: List[TraceRecord] = []
    for line in lines[1:]:
        try:
            trace_record = TraceRecord(header, line, DELIMITER)
            records.append(trace_record)
        except Exception as e:
            logging.error("Error parsing line in trace file %s: %s", filepath, e)
    return records

##################################
# MARK: Private functions
##################################

def _get_ci_file_data(filename: str) -> Tuple[List[str], List[str]]:
    """
    Get raw carbon intensity file data.
    
    :param filename: Path to the CI file.
    :return: Tuple containing the header list and a list of data lines.
    """
    try:
        with open(filename, 'r') as file:
            raw = file.readlines()
    except Exception as e:
        logging.error("Error reading file %s: %s", filename, e)
        raise
    header = [val.strip() for val in raw[0].split(",")]
    data = raw[1:]
    return (header, data)


def _print_usage_exit_CarbonFootprint() -> None:
    """
    Print usage information for the CarbonFootprint script and exit.
    """
    usage = ("Ichnos: python -m src.scripts.CarbonFootprint <trace-name> "
             "<ci-value|ci-file-name> <min-watts> <max-watts> "
             "<? pue=1.0> <? memory-coeff=0.392>")
    logging.error(usage)
    exit(-1)


def _print_usage_exit_IchnosCF() -> None:
    """
    Print usage information for the IchnosCF script and exit.
    """
    usage = ("Ichnos: python -m src.scripts.IchnosCF <trace-name> "
             "<ci-value|ci-file-name> <power_model> <? interval=60> "
             "<? pue=1.0> <? memory-coeff=0.392>")
    logging.error(usage)
    exit(-1)


def _check_if_float(value: str) -> bool:
    """
    Check if the provided string value represents a float.
    
    :param value: The string to verify.
    :return: True if the string represents a float, otherwise False.
    """
    return value.replace('.', '').isnumeric()