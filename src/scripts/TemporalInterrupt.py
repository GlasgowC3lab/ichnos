"""Module for calculating carbon footprints and exploring temporal shifting of workflows.

This module provides functions to compute energy consumption from tasks, 
apply temporal shifting based on carbon intensity, and generate corresponding reports.
"""

# New models
from src.models.UniversalTrace import UniversalTrace
from src.models.ProcessedTrace import ProcessedTrace
from src.models.TaskExtractionResult import TaskExtractionResult
from src.models.TempShiftResult import TempShiftResult
from src.WorkflowNameConstants import *
from src.Constants import CI, TRACE, PUE, MODEL_NAME, MEMORY_COEFFICIENT, INTERVAL
from src.utils.TimeUtils import to_timestamp, get_intervals, extract_tasks_by_interval
from src.scripts.OperationalCarbon import calculate_carbon_footprint_ccf
from src.scripts.EmbodiedCarbon import calculate_cpu_embodied_carbon
from src.utils.Parsers import parse_ci_intervals
from src.utils.Parsers import parse_arguments_TemporalInterrupt
from src.utils.NodeConfigModelReader import get_cpu_model

import sys
import numpy as np
from typing import Dict, List, Tuple, Callable, Union

def explore_temporal_shifting_for_workflow(workflow: str, task_extraction_result: TaskExtractionResult, ci: Dict[str, float], model_name: str, pue: float, memory_coefficient: float) -> TempShiftResult:
    """
    Explore shifting of workflow execution times based on minimum carbon intensity.

    Parameters:
        workflow (str): The workflow identifier.
        task_extraction_result (TaskExtractionResult): The result of task extraction.
        ci (Dict[str, float]): Carbon intensity values keyed by time.
        model_name (str): Model name for utilised resources.
        pue (float): Power usage effectiveness multiplier.
        memory_coefficient (float): Coefficient for memory consumption.

    Returns:
        str: A comma-separated string output with original and shifted carbon footprint details.
    """
    tasks_by_interval = task_extraction_result.tasks_by_interval
    overhead_intervals = task_extraction_result.overhead_intervals

    # Calculate total time of workflow from trace records
    trace_records = task_extraction_result.all_tasks  # List[UniversalTrace]
    cpu_model = trace_records[0].cpu_model if trace_records else ''
    # Check if all trace records have the same CPU model
    if trace_records:
        if not all(record.cpu_model == cpu_model for record in trace_records):
            raise ValueError("All trace records must have the same CPU model for consistent calculations.")
    if not cpu_model:
        cpu_model = get_cpu_model()

    # Identify Hours in Order
    intervals_by_key = {}

    for interval, tasks in tasks_by_interval.items():  # tasks: List[UniversalTrace]
        if len(tasks) > 0:
            hour_ts = to_timestamp(interval)
            month = str(hour_ts.month).zfill(2)
            day = str(hour_ts.day).zfill(2)
            hh = str(hour_ts.hour).zfill(2)
            mm = str(hour_ts.minute).zfill(2)
            key = f'{month}/{day}-{hh}:{mm}'
            intervals_by_key[key] = tasks

    # Calculate Original Carbon Footprint
    orig_carbon_result = calculate_carbon_footprint_ccf(tasks_by_interval, ci, pue, model_name, memory_coefficient, False)
    orig_carbon_emissions = orig_carbon_result.carbon_emissions


    # Calculate Original Workflow Makespan in seconds
    wf_start = task_extraction_result.workflow_start
    wf_end = task_extraction_result.workflow_end
    makespan = (wf_end - wf_start) / 1000

    embodied_carbon_orig = calculate_cpu_embodied_carbon(cpu_model, makespan / 3600) # Convert makespan to hours for embodied carbon calculation
    
    # Prepare Script Output
    op_carb_output = [workflow, str(orig_carbon_emissions), str(makespan)]
    emb_carb_output = [workflow, str(embodied_carbon_orig), str(makespan)]
    
    # SHIFTING LOGIC
    for shift in [6, 12, 24, 48, 96]:  # flexibility to run over windows 'shift' hours after the workflow executed
        keys = list(intervals_by_key.keys())  # keys that the workflow executes over
        wf_intervals = len(keys)  # hours of workflow execution
        ci_keys = list(ci.keys())  # all windows that have ci values, as keys
        start = keys[0]  # workflow start key
        end = keys[-1]  # workflow end key
        start_i = ci_keys.index(start)  # workflow start index
        end_i = ci_keys.index(end)  # workflow end index
        shift_keys = ci_keys[start_i:end_i + shift + 1]  # all keys within the shift (only forwards)
        # shift_keys = ci_keys[start_i - shift:end_i + shift + 1]

        dat = np.array([ci[key] for key in shift_keys])  # store corresponding ci values for the potential shifts
        ind = sorted(np.argpartition(dat, wf_intervals)[:wf_intervals])  # indices of the minimum ci values
        # the indices are sorted to retain chronological order over time
        min_keys = [shift_keys[i] for i in ind]  # matching keys for the minimum ci values

        ci_for_shifted_trace = {}
        for i in range(0, len(min_keys)):
            ci_for_shifted_trace[keys[i]] = ci[min_keys[i]]

        # Report Optimal CI Temporal Shifting Carbon Footprint
        shifted_carbon_result = calculate_carbon_footprint_ccf(tasks_by_interval, ci_for_shifted_trace, pue, model_name, memory_coefficient, False)
        operational_carbon = shifted_carbon_result.carbon_emissions

        # Report Overhead of Interrupting Temporal Shifting
        oh_interval_inds = get_intervals(ind)
        overhead_ms = 0

        if len(overhead_intervals) > 0:
            for oh_interval_ind in oh_interval_inds:
                overhead_ms += overhead_intervals[oh_interval_ind]

        op_saving = ((orig_carbon_emissions - operational_carbon) / orig_carbon_emissions) * 100

        overhead_s = overhead_ms / 1000
        overhead_perc = (overhead_s / makespan) * 100
        
        total_with_overhead_hrs = (makespan + overhead_s) / 3600  # convert seconds to hours
        embodied_carbon = calculate_cpu_embodied_carbon(cpu_model, total_with_overhead_hrs)
        emb_saving = ((embodied_carbon_orig - embodied_carbon) / embodied_carbon_orig) * 100

        op_carb_output.append(f'{op_saving:.1f}%:{operational_carbon}:{overhead_s}|{overhead_perc:.1f}%')  # reports overhead in seconds
        emb_carb_output.append(f'{emb_saving:.1f}%:{embodied_carbon}:{overhead_s}|{overhead_perc:.1f}%')  

    return TempShiftResult(
        op_carbon_results=','.join(op_carb_output),
        emb_carbon_results=','.join(emb_carb_output)
    )

def main(workflows: List[str], ci: Dict[str, float], arguments: Dict[str, Union[str, float, int]], outfilename: str) -> None:
    """
    Main function to process workflows and write the temporal shifting report.

    Parameters:
        workflows (List[str]): List of workflow identifiers.
        ci (Dict[str, float]): Carbon intensity values.
        arguments (Dict[str, Union[str, float, int]]): Argument dictionary parsed from command line.
        outfilename (str): Filename for results

    Returns:
        None
    """
    # Data
    pue: float = arguments[PUE]
    interval: int = arguments[INTERVAL]
    model_name: str = arguments[MODEL_NAME]
    memory_coefficient: float = arguments[MEMORY_COEFFICIENT]
    results = []

    emb_outfilename = outfilename.replace('.csv', '-emb.csv')

    for workflow in workflows:
        task_extraction_result = extract_tasks_by_interval(workflow, interval)

        result = explore_temporal_shifting_for_workflow(workflow, task_extraction_result, ci, model_name, pue, memory_coefficient)
        results.append(result)

    with open(outfilename, 'w') as op_file, open(emb_outfilename, 'w') as emb_file:
        op_file.write('workflow,footprint,makespan,flexible-6h,flexible-12h,flexible-24h,flexible-48h,flexible-96h\n')
        emb_file.write('workflow,embodied-carbon,makespan,flexible-6h,flexible-12h,flexible-24h,flexible-48h,flexible-96h\n')
        
        for result in results:
            op_file.write(f'{result.op_carbon_results}\n')
            emb_file.write(f'{result.emb_carbon_results}\n')

# Main Script
if __name__ == '__main__':
    arguments = sys.argv[1:]
    settings = parse_arguments_TemporalInterrupt(arguments)
    workflow = settings[TRACE]
    ci_source_file = f"data/intensity/{settings[CI]}.csv"
    ci = parse_ci_intervals(ci_source_file)

    workflows = []
    for i in range(1, 4):
        workflows.append(f'{workflow}-{i}')

    if 'marg' in settings[CI]:
        outfilename = f'output/{workflow}-marg-ts.csv'
    else:
        outfilename = f'output/{workflow}-avg-ts.csv'

    main(workflows, ci, settings, outfilename)
