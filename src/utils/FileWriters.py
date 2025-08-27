"""
Module: FileWriters
This module contains functions to write output files such as summary reports, 
detailed trace reports, and any other file-based outputs required by the project.
"""

import os
import logging
from typing import Iterable, Any
from src.models.CarbonRecord import HEADERS as LEGACY_HEADERS, CarbonRecord  # legacy
from src.models.ProcessedTrace import ProcessedTrace
from src.models.UniversalTrace import UniversalTrace

def write_trace_file(folder: str, trace_file: str, records: Iterable[Any]) -> None:
    """
    Write trace records to a CSV file.
    
    :param folder: Directory where the file will be saved.
    :param trace_file: Base name for the trace file.
    :param records: Iterable of trace record objects.
    """
    _create_folder(folder)
    output_file_name = f"{folder}/{trace_file}-trace.csv"
    try:
        with open(output_file_name, "w") as file:
            # Determine header & row writer based on record type
            first = None
            records = list(records)
            if records:
                first = records[0]
            if isinstance(first, ProcessedTrace):
                fns = ProcessedTrace.fieldnames()
                file.write(','.join(fns) + '\n')
                for r in records:
                    row = ','.join(str(r.to_dict()[h]) for h in fns)
                    file.write(row + '\n')
            elif isinstance(first, UniversalTrace):
                header = ','.join(UniversalTrace.fieldnames())
                file.write(f"{header}\n")
                for r in records:
                    row = ','.join(str(r.to_dict()[h]) for h in UniversalTrace.fieldnames())
                    file.write(row + "\n")
            else:  # CarbonRecord fallback
                file.write(f"{LEGACY_HEADERS}\n")
                for record in records:
                    file.write(f"{record}\n")
    except Exception as e:
        logging.error("Failed to write trace file %s: %s", output_file_name, e)
        raise

def write_summary_file(folder: str, trace_file: str, content: str) -> None:
    """
    Write a summary report to a text file.
    
    :param folder: Directory where the file will be saved.
    :param trace_file: Base name for the summary file.
    :param content: Text content of the summary.
    """
    _create_folder(folder)
    output_file_name = f"{folder}/{trace_file}-summary.txt"
    try:
        with open(output_file_name, "w") as file:
            file.write(content)
    except Exception as e:
        logging.error("Failed to write summary file %s: %s", output_file_name, e)
        raise

def write_trace_and_detailed_report(folder: str, trace_file: str, records: Iterable[CarbonRecord], content: str) -> None:  # legacy path
    """
    Write both detailed trace and summary reports.
    
    :param folder: Directory where files will be saved.
    :param trace_file: Base name to use for output files.
    :param records: Iterable of trace record objects.
    :param content: Summary content to include in the detailed report.
    """
    output_file_name = f"{folder}/{trace_file}-detailed-summary.txt"
    whole_tasks = {}
    for record in records:
        curr_id = getattr(record, 'id', getattr(getattr(record, 'universal', None), 'id', None))
        if curr_id is None:
            continue
        if curr_id in whole_tasks:
            present = whole_tasks[curr_id]
            # Only aggregate for legacy CarbonRecord
            if isinstance(present, CarbonRecord) and isinstance(record, CarbonRecord):
                if present.co2e is not None and record.co2e is not None:
                    present.co2e += record.co2e
                if present.energy is not None and record.energy is not None:
                    present.energy += record.energy
        else:
            whole_tasks[curr_id] = record
    records = list(whole_tasks.values())
    try:
        write_trace_file(folder, trace_file, records)
    except Exception as e:
        logging.error("Error writing trace file from detailed report: %s", e)
        raise
    sorted_records = sorted(records, key=lambda r: (-r.co2e, -r.energy, -r.realtime))
    sorted_records_par = sorted(records, key=lambda r: (-r.energy, -r.realtime))
    try:
        with open(output_file_name, "w") as file:
            file.write(f'Detailed Report for {trace_file}\n')
            file.write('\nTop 10 Tasks - ranked by footprint, energy and realtime:\n')
            for record in sorted_records[:10]:
                file.write(record.name + ':' + record.id + '\n')
            file.write('\nTop 10 Tasks - ranked by energy and realtime:\n')
            for record in sorted_records_par[:10]:
                file.write(record.name + ':' + record.id + '\n')
            diff = set(sorted_records[:10]).difference(set(sorted_records_par[:10]))
            if len(diff) == 0:
                file.write('\nThe top 10 tasks with the largest energy and realtime have the largest footprint.\n')
            else:
                file.write('\nThe following tasks have one of the top 10 largest footprints, but not the highest energy or realtime...\n')
                file.write(', '.join([record.name + ':' + record.id for record in diff]))
    except Exception as e:
        logging.error("Failed to write detailed report file %s: %s", output_file_name, e)
        raise

def write_task_trace_and_rank_report(folder: str, trace_file: str, records: Iterable[Any]) -> None:
    """
    Write detailed and ranked task reports.
    
    :param folder: Directory where files will be saved.
    :param trace_file: Base name for the output files.
    :param records: Iterable of trace record objects.
    """
    _create_folder(folder)
    output_file_name = f"{folder}/{trace_file}-detailed-summary.txt"
    technical_output_file_name = f"{folder}/{trace_file}-task-ranked.csv"
    whole_tasks = {}
    for record in records:
        curr_id = getattr(record, 'id', getattr(getattr(record, 'universal', None), 'id', None))
        if curr_id is None:
            continue
        if curr_id in whole_tasks:
            present = whole_tasks[curr_id]
            if isinstance(present, CarbonRecord) and isinstance(record, CarbonRecord):
                if present.co2e is not None and record.co2e is not None:
                    present.co2e += record.co2e
                if present.energy is not None and record.energy is not None:
                    present.energy += record.energy
        else:
            whole_tasks[curr_id] = record
    records = list(whole_tasks.values())
    try:
        write_trace_file(folder, trace_file, records)
    except Exception as e:
        logging.error("Error writing trace file for task rank report: %s", e)
        raise
    # sorted_records = sorted(records, key=lambda r: (-r.co2e, -r.energy, -r.realtime))
    # sorted_records_par = sorted(records, key=lambda r: (-r.energy, -r.realtime))
    # try:
    #     with open(output_file_name, "w") as report_file:
    #         with open(technical_output_file_name, "w") as task_rank_file:
    #             report_file.write(f'Detailed Report for {trace_file}\n')
    #             task_rank_file.write(f'{HEADERS}\nBREAK\n')
    #             report_file.write('\nTop 10 Tasks - ranked by footprint, energy and realtime:\n')
    #             task_rank_file.write('TOP|FOOTPRINT-ENERGY-REALTIME\n')
    #             report_file.write(f'\n{HEADERS}\n')
    #             for record in sorted_records[:10]:
    #                 report_file.write(f"{record}\n")
    #                 task_rank_file.write(f"{record}\n")
    #             task_rank_file.write('BREAK\n')
    #             report_file.write('\nTop 10 Tasks - ranked by energy and realtime:\n')
    #             report_file.write(f'\n{HEADERS}\n')
    #             task_rank_file.write('TOP|ENERGY-REALTIME\n')
    #             for record in sorted_records[:10]:
    #                 report_file.write(f"{record}\n")
    #                 task_rank_file.write(f"{record}\n")
    #             diff = set(sorted_records[:10]).difference(set(sorted_records_par[:10]))
    #             if len(diff) == 0:
    #                 report_file.write('\nThe top 10 tasks with the largest energy and realtime have the largest footprint.\n')
    #                 task_rank_file.write('BREAK\nSAME\nEND\n')
    #             else:
    #                 report_file.write('\nThe following tasks have one of the top 10 largest footprints, but not the highest energy or realtime...\n')
    #                 report_file.write(', '.join([str(task) for task in diff]))
    #                 task_rank_file.write('BREAK\nDIFF\nEND\n')
    # except Exception as e:
    #     logging.error("Failed to write task trace and rank report files: %s", e)
    #     raise

##################################
# MARK: Private Functions
##################################
def _create_folder(folder: str) -> None:
    """
    Create the folder if it does not already exist.
    
    :param folder: Directory path to create.
    """
    try:
        if not os.path.exists(folder):
            os.makedirs(folder)
    except Exception as e:
        logging.error("Failed to create folder %s: %s", folder, e)
        raise