"""
Module: FileWriters
This module contains functions to write output files such as summary reports, 
detailed trace reports, and any other file-based outputs required by the project.
"""

import os
import logging
from typing import Iterable, List
from src.models.ProcessedTrace import ProcessedTrace

def write_trace_file(folder: str, trace_file: str, records: Iterable[ProcessedTrace]) -> None:
    """Write processed trace records (ProcessedTrace) to CSV.

    Always writes the ProcessedTrace header schema.
    """
    _create_folder(folder)
    output_file_name = f"{folder}/{trace_file}-trace.csv"
    rec_list: List[ProcessedTrace] = list(records)
    try:
        with open(output_file_name, "w") as file:
            fns = ProcessedTrace.fieldnames()
            file.write(','.join(fns) + '\n')
            for r in rec_list:
                row = ','.join(str(r.to_dict()[h]) for h in fns)
                file.write(row + '\n')
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

def write_trace_and_detailed_report(folder: str, trace_file: str, records: Iterable[ProcessedTrace], content: str) -> None:
    """Write processed traces plus a human-readable ranking summary.

    Aggregates duplicate task ids (summing footprint metrics) then ranks tasks
    by average_co2e (primary) and runtime (secondary). A second list ranks by
    marginal_co2e. Embodied emissions included only for informational tie-breaks.
    """
    output_file_name = f"{folder}/{trace_file}-detailed-summary.txt"
    # Aggregate duplicates
    aggregated: dict[str, ProcessedTrace] = {}
    for r in records:
        tid = r.universal.id
        if tid in aggregated:
            existing = aggregated[tid]
            existing.average_co2e += r.average_co2e
            existing.marginal_co2e += r.marginal_co2e
            existing.embodied_co2e += r.embodied_co2e
            # avg_ci: simple mean of the two (no energy weighting available)
            existing.avg_ci = (existing.avg_ci + r.avg_ci) / 2 if existing.avg_ci and r.avg_ci else (existing.avg_ci or r.avg_ci)
        else:
            aggregated[tid] = r
    rec_list: List[ProcessedTrace] = list(aggregated.values())
    # Persist CSV
    write_trace_file(folder, trace_file, rec_list)
    # Sorting helpers
    def runtime(pr: ProcessedTrace) -> float:
        return (pr.universal.end - pr.universal.start) / 1000.0
    rec_sorted_footprint = sorted(rec_list, key=lambda x: (-x.average_co2e, -runtime(x)))
    rec_sorted_marginal = sorted(rec_list, key=lambda x: (-x.marginal_co2e, -runtime(x)))
    try:
        with open(output_file_name, "w") as file:
            file.write(f"Detailed Report for {trace_file}\n")
            file.write(f"{content}\n\n" if content else "")
            file.write("Top 10 Tasks - ranked by average CO2e then runtime:\n")
            for r in rec_sorted_footprint[:10]:
                file.write(f"{r.universal.name}:{r.universal.id} average_co2e={r.average_co2e:.4f} runtime_s={runtime(r):.2f}\n")
            file.write("\nTop 10 Tasks - ranked by marginal CO2e then runtime:\n")
            for r in rec_sorted_marginal[:10]:
                file.write(f"{r.universal.name}:{r.universal.id} marginal_co2e={r.marginal_co2e:.4f} runtime_s={runtime(r):.2f}\n")
            foot_top = rec_sorted_footprint[:10]
            marg_top = rec_sorted_marginal[:10]
            marg_ids = {r.universal.id for r in marg_top}
            diff_records = [r for r in foot_top if r.universal.id not in marg_ids]
            if not diff_records:
                file.write("\nThe top 10 marginal CO2e tasks coincide with the top 10 average CO2e tasks.\n")
            else:
                file.write("\nTasks in average CO2e top 10 but not marginal top 10:\n")
                file.write(', '.join(f"{r.universal.name}:{r.universal.id}" for r in diff_records))
    except Exception as e:
        logging.error("Failed to write detailed report file %s: %s", output_file_name, e)
        raise

def write_task_trace_and_rank_report(folder: str, trace_file: str, records: Iterable[ProcessedTrace]) -> None:
    """Deprecated: prefer write_trace_and_detailed_report. Kept for API stability."""
    write_trace_and_detailed_report(folder, trace_file, records, content="")

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