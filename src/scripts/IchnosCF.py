import logging
from typing import Dict, List, Tuple, Union
from src.utils.TimeUtils import to_timestamp, extract_tasks_by_interval
from src.utils.Parsers import parse_ci_intervals, parse_arguments_with_config, parse_trace_file
from src.utils.FileWriters import write_summary_file, write_task_trace_and_rank_report
from src.utils.NodeConfigModelReader import get_memory_draw
from src.Constants import *
from src.scripts.OperationalCarbon import calculate_carbon_footprint_ccf
from src.scripts.EmbodiedCarbon import embodied_carbon_for_trace_records
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
    unique_nodes = list({task.hostname for task in task_extraction_result.all_tasks})

    ## Get raw TraceRecords for computing embodied carbon
    filename: str = workflow
    if len(filename.split(".")) > 1:
        filename = filename.split(".")[-2]
    try:
        trace_records = parse_trace_file(f"data/trace/{filename}.{FILE}")
    except Exception as e:
        logging.error("Failed to parse trace file %s: %s", f"data/trace/{filename}.{FILE}", e)
        raise
    #################

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

    op_carbon_result = calculate_carbon_footprint_ccf(tasks_by_interval, ci, pue, model_name, memory_coefficient, unique_nodes)
    cpu_energy = op_carbon_result.cpu_energy
    cpu_energy_pue = op_carbon_result.cpu_energy_pue
    mem_energy = op_carbon_result.memory_energy
    mem_energy_pue = op_carbon_result.memory_energy_pue
    op_carbon_emissions = op_carbon_result.carbon_emissions
    static_energy_per_host = op_carbon_result.static_cpu_energy_per_host
    static_memory_energy = op_carbon_result.static_mem_energy
    records_res = op_carbon_result.records

    static_energy = 0.0
    for host in static_energy_per_host.keys():
        static_energy += static_energy_per_host[host]

    #fallback_cpu_model: str = get_cpu_model(model_name)
    #emb_carbon_emissions = embodied_carbon_for_trace_records(trace_records, use_cpu_usage=False, fallback_cpu_model=fallback_cpu_model)
    emb_carbon_emissions = 0.0
    total_carbon_emissions = op_carbon_emissions #+ emb_carbon_emissions

    summary += "\nCloud Carbon Footprint Method:\n"
    summary += f"- Energy Consumption (exc. PUE): {cpu_energy + static_energy}kWh\n"
    summary += f"- Energy Consumption (inc. PUE): {cpu_energy_pue + (static_energy * pue)}kWh\n"
    summary += f"- Memory Energy Consumption (exc. PUE): {mem_energy}kWh\n"
    summary += f"- Memory Energy Consumption (inc. PUE): {mem_energy_pue}kWh\n"
    summary += f"- Operational Carbon Emissions: {op_carbon_emissions}gCO2e\n"
    summary += f"- Embodied Carbon Emissions: {emb_carbon_emissions}gCO2e\n"
    summary += f"- Total Carbon Emissions: {total_carbon_emissions}gCO2e\n"

    print(f"Operational Carbon Emissions: {op_carbon_emissions}gCO2e")
    print(f"Embodied Carbon Emissions: {emb_carbon_emissions}gCO2e")
    print(f"Total Carbon Emissions: {total_carbon_emissions}gCO2e")

    if check_reserved_memory_flag:
        total_energy: float = static_memory_energy + cpu_energy + mem_energy
        res_report: str = f"Reserved Memory Energy Consumption: {static_memory_energy}kWh"
        energy_split_report: str = f"% CPU [{((cpu_energy / total_energy) * 100):.2f}%] | % Memory [{(((static_memory_energy + mem_energy) / total_energy) * 100):.2f}%]"
        summary += f"\n{res_report}\n"
        summary += f"{energy_split_report}\n"
        print(res_report)
        print(energy_split_report)

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
