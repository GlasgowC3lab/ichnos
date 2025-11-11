"""
Module: Parsers
This module contains functions to parse input files and command-line arguments
for carbon footprint calculations. It provides utilities for parsing CI files,
trace files, and for validating input values.
"""

import logging
from typing import Tuple, List, Dict, Union
import yaml
from src.Constants import *
from src.models.UniversalTrace import UniversalTrace
from src.utils.Usage import print_usage_exit_TemporalInterrupt

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

def parse_arguments_with_config(args: List[str]) -> Dict[str, Union[float, int, str]]:
    """
    Parse command-line arguments for IchnosCF/OperationalCarbon, supporting a -c <config.yaml> flag.
    If -c is present, load arguments from the YAML config file and override with any additional CLI args.
    """
    if '-c' in args:
        c_idx = args.index('-c')
        if c_idx + 1 >= len(args):
            raise ValueError('Config file path must follow -c flag')
        config_path = args[c_idx + 1]
        with open(config_path, 'r') as f:
            config_args = yaml.safe_load(f)
        if config_args is None:
            config_args = {}
        # Remove -c and config path from args
        args = args[:c_idx] + args[c_idx+2:]
        # If any args remain, treat them as positional overrides (trace, ci, model, ...)
        positional_keys = [TRACE, CI, MODEL_NAME, INTERVAL, PUE, MEMORY_COEFFICIENT, RESERVED_MEMORY, NUM_OF_NODES]
        for i, val in enumerate(args):
            if i < len(positional_keys):
                config_args[positional_keys[i]] = val
        config_args = _set_defaults_for_missing_args(config_args)
        return config_args
    else:
        return parse_arguments(args)

"""
This parses arguments for the TemporalInterrupt script.
"""
def parse_arguments_TemporalInterrupt(args: List[str]) -> Dict[str, Union[float, int, str]]:
    """
    Parse command-line arguments for the TemporalInterrupt script.

    Expected arguments:
      - [0]: workflow file name
      - [1]: carbon intensity value or file name
      - [2]: power model name
      Optionally:
      - [3]: interval (int)
      - Given 6 arguments, [4] is PUE and [5] is memory coefficient.
      Defaults are used when only 3 arguments are provided.
    
    :param args: List of argument strings.
    :return: Dictionary mapping argument names to their parsed values.
    """
    if len(args) != 3 and len(args) != 6:
        print_usage_exit_TemporalInterrupt()

    arguments: Dict[str, Union[float, int, str]] = {}
    arguments[TRACE] = args[0]
    arguments[CI] = args[1]
    arguments[MODEL_NAME] = args[2]
    
    if len(args) == 6:
        arguments[INTERVAL] = int(args[3])
        arguments[PUE] = float(args[4])
        arguments[MEMORY_COEFFICIENT] = float(args[5])
    else: 
        arguments[INTERVAL] = DEFAULT_INTERVAL_VALUE
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


def parse_universal_trace_file(filepath: str) -> List[UniversalTrace]:
    """Parse a universal trace CSV (produced by UniversalTrace.to_csv)."""
    try:
        traces = UniversalTrace.from_csv(filepath)
    except Exception as e:
        logging.error("Error parsing universal trace file %s: %s", filepath, e)
        raise
    return traces

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

def _set_defaults_for_missing_args(args_dict: Dict[str, Union[float, int, str]]) -> Dict[str, Union[float, int, str]]:
    """
    Set default values for optional arguments if they are missing from the config dict.
    """
    if INTERVAL not in args_dict:
        args_dict[INTERVAL] = 60
    if PUE not in args_dict:
        args_dict[PUE] = DEFAULT_PUE_VALUE
    if MEMORY_COEFFICIENT not in args_dict:
        args_dict[MEMORY_COEFFICIENT] = DEFAULT_MEMORY_POWER_DRAW
    return args_dict

