import json
import logging
from typing import Dict, List, Tuple, Union
from src.utils.TimeUtils import to_timestamp, extract_tasks_by_interval
from src.utils.Parsers import parse_ci_intervals, parse_arguments_with_config, parse_ichnos_trace_file
from src.utils.FileWriters import write_summary_file, write_task_trace_and_rank_report
from src.utils.NodeConfigModelReader import get_memory_draw, get_cpu_model
from src.Constants import *
from src.scripts.OperationalCarbon import calculate_carbon_footprint_ccf
from src.scripts.EmbodiedCarbon import calculate_cpu_embodied_carbon
from src.models.IchnosTrace import IchnosTrace
from src.models.IchnosResult import IchnosResult
from src.models.OperationalCarbonResult import OperationalCarbonResult
from src.models.TaskExtractionResult import TaskExtractionResult

import sys


logger = logging.getLogger(__name__)


def normalize_node_governors(value) -> Dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            with open(value, "r", encoding="utf-8") as handle:
                value = json.load(handle)
        except Exception as exc:
            logger.warning("Ignoring node-governors from %s: %s", value, exc)
            return {}
    if not isinstance(value, dict):
        logger.warning("Ignoring node-governors: expected a mapping or JSON file path, got %s", type(value).__name__)
        return {}
    normalized: Dict[str, str] = {}
    for node, governor in value.items():
        if isinstance(governor, dict):
            governor = governor.get("dominant_governor") or governor.get("governor")
        node_text = str(node).strip()
        governor_text = str(governor).strip() if governor is not None else ""
        if node_text and governor_text:
            normalized[node_text] = governor_text
        else:
            logger.warning("Ignoring empty node governor entry for node %r", node)
    return normalized

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
    node_governors: Dict[str, str] = normalize_node_governors(arguments.get(NODE_GOVERNORS))

    if memory_coefficient is None:
        memory_coefficient = DEFAULT_MEMORY_POWER_DRAW

    task_extraction_result: TaskExtractionResult = extract_tasks_by_interval(workflow, interval)
    tasks_by_interval = task_extraction_result.tasks_by_interval
    unique_nodes = list({task.hostname for task in task_extraction_result.all_tasks})

    ## Get raw IchnosTrace records for computing embodied carbon
    filename: str = workflow
    if len(filename.split(".")) > 1:
        filename = filename.split(".")[-2]
    try:
        trace_records: List[IchnosTrace] = parse_ichnos_trace_file(f"data/ichnos_traces/{filename}.{FILE}")
    except Exception as e:
        logging.error("Failed to parse ichnos trace file %s: %s", f"data/ichnos_traces/{filename}.{FILE}", e)
        trace_records = []
    #################

    summary: str = ""
    summary += "Carbon Footprint Trace:\n"
    summary += f"- carbon-intensity: {arguments[CI]}\n"
    summary += f"- power-usage-effectiveness: {pue}\n"
    summary += f"- power model selected: {model_name}\n"
    if node_governors:
        summary += f"- node governors: {json.dumps(node_governors, sort_keys=True)}\n"
    summary += f"- memory-power-draw: {memory_coefficient}\n"

    if isinstance(arguments[CI], float):
        ci = arguments[CI]
    else:
        ci_filename: str = f"data/intensity/{arguments[CI]}.{FILE}"
        ci = parse_ci_intervals(ci_filename)

    ###################
    # Water footprint input parameters
    wue = arguments[WUE] if WUE in arguments else None
    ewif = None
    if wue and arguments[EWIF]:
        if isinstance(arguments[EWIF], float):
            ewif = arguments[EWIF]
        else:
            ewif_filename: str = f"data/intensity/{arguments[EWIF]}.{FILE}"
            ewif = parse_ci_intervals(ewif_filename)
    # Land use footprint input parameters
    lue = arguments[LUE] if LUE in arguments else None
    elif_ = None
    if lue and arguments[ELIF]:
        if isinstance(arguments[ELIF], float):
            elif_ = arguments[ELIF]
        else:
            elif_filename: str = f"data/intensity/{arguments[ELIF]}.{FILE}"
            elif_ = parse_ci_intervals(elif_filename)

    ###################


    check_reserved_memory_flag: bool = RESERVED_MEMORY in arguments

    op_carbon_result = calculate_carbon_footprint_ccf(
        tasks_grouped_by_interval=tasks_by_interval, 
        ci=ci, 
        pue=pue, 
        model_name=model_name, 
        memory_coefficient=memory_coefficient, 
        unique_nodes=unique_nodes,
        ewif=ewif, 
        wue=wue, 
        elif_=elif_, 
        lue=lue,
        node_governors=node_governors
    )
    cpu_energy = op_carbon_result.cpu_energy
    cpu_energy_pue = op_carbon_result.cpu_energy_pue
    mem_energy = op_carbon_result.memory_energy
    mem_energy_pue = op_carbon_result.memory_energy_pue
    op_carbon_emissions = op_carbon_result.carbon_emissions
    static_energy_per_host = op_carbon_result.static_cpu_energy_per_host
    static_memory_energy = op_carbon_result.static_mem_energy
    static_memory_emissions = op_carbon_result.static_mem_emissions
    records_res = op_carbon_result.records

    op_water_emissions = op_carbon_result.water_emissions
    op_land_emissions = op_carbon_result.land_emissions

    ######### Compute embodied carbon with per-node CPU models #########
    node_cpu_models: Dict[str, str] = {}
    for node in unique_nodes:
        node_cpu_models[node] = get_cpu_model(node)

    emb_carbon_emissions = 0.0
    for ut in trace_records:
        host = ut.hostname
        # Use the task's cpu_model if available, otherwise fall back to node-specific CPU model
        cpu_model = ut.cpu_model if (ut.cpu_model and ut.cpu_model != 'None') else node_cpu_models.get(host, None)
        if cpu_model:
            duration_h = (ut.end - ut.start) / 1000 / 3600
            emb_carbon_emissions += calculate_cpu_embodied_carbon(cpu_model, duration_h, cpu_usage=1.0)
    ###################################################################
    
    static_energy = 0.0
    for host in static_energy_per_host.keys():
        static_energy += static_energy_per_host[host]

    total_carbon_emissions = op_carbon_emissions + emb_carbon_emissions

    summary += "\nIchnos Output:\n"
    summary += f"- Static Energy Consumption (exc. PUE): {static_energy}kWh\n"
    summary += f"- Energy Consumption (exc. PUE): {cpu_energy + static_energy}kWh\n"
    summary += f"- Energy Consumption (inc. PUE): {cpu_energy_pue + (static_energy * pue)}kWh\n"
    summary += f"- Task Memory Energy Consumption (exc. PUE): {mem_energy}kWh\n"
    summary += f"- Task Memory Energy Consumption (inc. PUE): {mem_energy_pue}kWh\n"
    summary += f"- Operational Carbon Emissions: {op_carbon_emissions}gCO2e\n"
    summary += f"- Embodied Carbon Emissions: {emb_carbon_emissions}gCO2e\n"
    summary += f"- Total Carbon Emissions: {total_carbon_emissions}gCO2e\n"
    
    print(f"Energy Consumption (exc. PUE): {cpu_energy + static_energy}kWh")
    print(f"Energy Consumption (inc. PUE): {cpu_energy_pue + (static_energy * pue)}kWh")
    print(f"Task Memory Energy Consumption (exc. PUE): {mem_energy}kWh")
    print(f"Task Memory Energy Consumption (inc. PUE): {mem_energy_pue}kWh")

    print(f"Operational Carbon Emissions: {op_carbon_emissions}gCO2e")
    print(f"Embodied Carbon Emissions: {emb_carbon_emissions}gCO2e")
    print(f"Total Carbon Emissions: {total_carbon_emissions}gCO2e")

    if wue:
        summary += f"- Total Water Footprint: {op_water_emissions}l\n"
        print(f"Total Water Footprint: {op_water_emissions} Liters")

    if lue:
        summary += f"- Total Land Use Footprint: {op_land_emissions}sqm\n"
        print(f"Total Land Use Footprint: {op_land_emissions} square meters")

    if check_reserved_memory_flag:
        total_energy: float = cpu_energy + mem_energy
        res_report: str = f"Node Memory Energy Consumption: {static_memory_energy}kWh\n"
        res_report += f"Node Memory Carbon Emissions: {static_memory_emissions}gCO2e"
        # energy_split_report: str = f"% CPU [{((cpu_energy / total_energy) * 100):.2f}%] | % Memory [{((mem_energy / total_energy) * 100):.2f}%]"
        summary += f"\n{res_report}\n"
        # summary += f"{energy_split_report}\n"
        print(res_report)
        # print(energy_split_report)

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
