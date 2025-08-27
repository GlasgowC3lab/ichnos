import logging
from typing import Dict, List, Tuple, Union
from src.utils.TimeUtils import to_timestamp, extract_tasks_by_interval
from src.utils.Parsers import parse_ci_intervals, parse_arguments_with_config, parse_universal_trace_file
from src.utils.FileWriters import write_summary_file, write_task_trace_and_rank_report
from src.utils.NodeConfigModelReader import get_cpu_model
from src.Constants import *
from src.scripts.OperationalCarbon import calculate_carbon_footprint_ccf
from src.scripts.EmbodiedCarbon import calculate_cpu_embodied_carbon
from src.models.UniversalTrace import UniversalTrace
from src.models.IchnosResult import IchnosResult
from src.models.OperationalCarbonResult import OperationalCarbonResult
from src.models.TaskExtractionResult import TaskExtractionResult

import sys
import yaml

def main(arguments: Dict[str, Union[str, float, int]]) -> IchnosResult:
    """
    Main function to compute and report the carbon footprint.
    
    :param arguments: Argument dictionary parsed from command line.
    :return: An IchnosResult object containing the summary and emissions.
    """

    # Data
    workflow: str = arguments[TRACE]
    pue: float = arguments[PUE]
    interval: int = arguments[INTERVAL]
    model_name: str = arguments[MODEL_NAME]
    memory_coefficient: float = arguments[MEMORY_COEFFICIENT]

    if memory_coefficient is None:
        memory_coefficient = DEFAULT_MEMORY_POWER_DRAW

    task_extraction_result: TaskExtractionResult = extract_tasks_by_interval(workflow, interval)
    tasks_by_interval = task_extraction_result.tasks_by_interval

    ## Get raw UniversalTrace records for computing embodied carbon
    filename: str = workflow
    if len(filename.split(".")) > 1:
        filename = filename.split(".")[-2]
    try:
        trace_records: List[UniversalTrace] = parse_universal_trace_file(f"data/universal_traces/{filename}.{FILE}")
    except Exception as e:
        logging.error("Failed to parse universal trace file %s: %s", f"data/universal_traces/{filename}.{FILE}", e)
        trace_records = []
    #################

    # for curr_interval, records_list in tasks_by_interval.items():
    #     print(f'interval: {to_timestamp(curr_interval)}')
    #     if records_list:
    #         print(f'tasks: {", ".join([record.id for record in records_list])}')

    summary: str = ""
    summary += "Carbon Footprint Trace:\n"
    summary += f"- carbon-intensity: {arguments[CI]}\n"
    summary += f"- power-usage-effectiveness: {pue}\n"
    summary += f"- power model selected: {model_name}\n"
    summary += f"- memory-power-draw: {memory_coefficient}\n"

    if isinstance(arguments[CI], float):
        ci = arguments[CI]
    else:
        ci_filename: str = f"data/intensity/{arguments[CI]}.{FILE}"
        ci = parse_ci_intervals(ci_filename)

    check_reserved_memory_flag: bool = RESERVED_MEMORY in arguments

    op_carbon_result = calculate_carbon_footprint_ccf(tasks_by_interval, ci, pue, model_name, memory_coefficient, check_reserved_memory_flag)
    cpu_energy = op_carbon_result.cpu_energy
    cpu_energy_pue = op_carbon_result.cpu_energy_pue
    mem_energy = op_carbon_result.memory_energy
    mem_energy_pue = op_carbon_result.memory_energy_pue
    op_carbon_emissions = op_carbon_result.carbon_emissions
    node_memory_usage = op_carbon_result.node_memory_usage
    records_res = op_carbon_result.records

    fallback_cpu_model: str = get_cpu_model(model_name)
    # Compute embodied carbon directly from UniversalTrace list (per-task allocation)
    emb_carbon_emissions = 0.0
    for ut in trace_records:
        cpu_model = ut.cpu_model if (ut.cpu_model and ut.cpu_model != 'None') else fallback_cpu_model
        duration_h = max(0.0, (ut.end - ut.start)) / 1000 / 3600
        emb_carbon_emissions += calculate_cpu_embodied_carbon(cpu_model, duration_h, cpu_usage=1.0)
    total_carbon_emissions = op_carbon_emissions + emb_carbon_emissions

    summary += "\nCloud Carbon Footprint Method:\n"
    summary += f"- Energy Consumption (exc. PUE): {cpu_energy}kWh\n"
    summary += f"- Energy Consumption (inc. PUE): {cpu_energy_pue}kWh\n"
    summary += f"- Memory Energy Consumption (exc. PUE): {mem_energy}kWh\n"
    summary += f"- Memory Energy Consumption (inc. PUE): {mem_energy_pue}kWh\n"
    summary += f"- Operational Carbon Emissions: {op_carbon_emissions}gCO2e\n"
    summary += f"- Embodied Carbon Emissions: {emb_carbon_emissions}gCO2e\n"
    summary += f"- Total Carbon Emissions: {total_carbon_emissions}gCO2e\n"

    print(f"Operational Carbon Emissions: {op_carbon_emissions}gCO2e")
    print(f"Embodied Carbon Emissions: {emb_carbon_emissions}gCO2e")
    print(f"Total Carbon Emissions: {total_carbon_emissions}gCO2e")
    
    if check_reserved_memory_flag:
        total_res_mem_energy: float = 0.0
        total_res_mem_emissions: float = 0.0

        for realtime, ci_val in node_memory_usage:
            res_mem_energy: float = (arguments[RESERVED_MEMORY] * memory_coefficient * realtime * 0.001) * arguments[NUM_OF_NODES]  # convert from W to kW
            total_res_mem_energy += res_mem_energy
            total_res_mem_emissions += res_mem_energy * ci_val

        total_energy: float = total_res_mem_energy + cpu_energy + mem_energy
        res_report: str = f"Reserved Memory Energy Consumption: {total_res_mem_energy}kWh"
        res_ems_report: str = f"Reserved Memory Carbon Emissions: {total_res_mem_emissions}gCO2e"
        energy_split_report: str = f"% CPU [{((cpu_energy / total_energy) * 100):.2f}%] | % Memory [{(((total_res_mem_energy + mem_energy) / total_energy) * 100):.2f}%]"
        summary += f"\n{res_report}\n"
        summary += f"{res_ems_report}\n"
        summary += f"{energy_split_report}\n"
        print(res_report)
        print(energy_split_report)

    # if TASK_FLAG:
    #     total_time: float = 0.0

    #     for _, tasks_list in tasks_by_interval.items():
    #         for task in tasks_list:
    #             total_time += task.realtime

    #     summary += f"\nTask Runtime: {total_time}ms\n"

    # Report Summary
    if isinstance(ci, float):
        ci = str(int(ci))
    else:
        ci = arguments[CI]

    write_summary_file("output", workflow + "-" + ci + "-" + model_name, summary)
    write_task_trace_and_rank_report("output", workflow + "-" + ci + "-" + model_name, records_res)

    return IchnosResult(
        summary=summary,
        operational_emissions=op_carbon_result.carbon_emissions,
        embodied_emissions=emb_carbon_emissions
    )


def get_carbon_footprint(command: str) -> IchnosResult:
    """
    Parse the command and compute the carbon footprint.
    
    :param command: Command string.
    :return: A tuple of (summary string, carbon emissions).
    """
    arguments = parse_arguments_with_config(command.split(' '))
    return main(arguments)


# Main Script
if __name__ == '__main__':
    # Parse Arguments
    args: List[str] = sys.argv[1:]
    arguments = parse_arguments_with_config(args)
    main(arguments)
