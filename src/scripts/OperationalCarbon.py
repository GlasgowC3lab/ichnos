from typing import Callable, Dict, List, Tuple, Union
from src.models.UniversalTrace import UniversalTrace
from src.models.ProcessedTrace import ProcessedTrace
from src.models.TaskEnergyResult import TaskEnergyResult
from src.models.OperationalCarbonResult import OperationalCarbonResult
from src.models.TaskExtractionResult import TaskExtractionResult
from src.utils.TimeUtils import to_timestamp, extract_tasks_by_interval
from src.utils.PowerModel import get_power_model_for_node
from src.utils.NodeConfigModelReader import get_memory_draw, get_system_cores, get_system_memory
from src.utils.Parsers import parse_ci_intervals, parse_arguments_with_config
from src.Constants import *
from datetime import datetime

import sys

# find time when tasks are actively running
def compute_active_time_per_host(tasks: List[UniversalTrace]) -> Dict[str, float]:
    tasks_by_host: Dict[str, List[Tuple[float, float]]] = {}

    for task in tasks:
        host: str = task.hostname
        start_ts: float = float(task.start)
        end_ts: float = float(task.end)
        if host in tasks_by_host:
            tasks_by_host[host].append((start_ts, end_ts))
        else:
            tasks_by_host[host] = [(start_ts, end_ts)]

    active_time_by_host: Dict[str, float] = {}

    for host, intervals in tasks_by_host.items():
        if not intervals:
            continue

        intervals.sort(key=lambda x: x[0])  # sort by start timestamp
        merged: List[Tuple[float, float]] = []
        current_start, current_end = intervals[0]

        for start, end in intervals[1:]:
            if start <= current_end:  # overlap or contiguous
                current_end = max(current_end, end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))

        total_ms: float = sum(end - start for start, end in merged)
        active_time_by_host[host] = total_ms / 1000 / 3600  # return time in hours

    return active_time_by_host


# Estimate Energy Consumption (accept UniversalTrace)
def estimate_task_energy_consumption_ccf(task: UniversalTrace, model: Callable[[float], float], model_name: str, memory_coefficient: float, system_cores: int) -> TaskEnergyResult:
    """
    Estimate the energy consumptions for a task.
    
    :param task: UniversalTrace task record.
    :param model: Power model function.
    :param model_name: Name of the power model.
    :param memory_coefficient: Coefficient for memory power draw.
    :param system_cores: no. of cores on the system utilised. 
    :return: TaskEnergyResult object containing core and memory energy consumption in kWh.
    """
    if not system_cores:
        system_cores = 32

    # Time (h)
    time_h: float = (task.end - task.start) / 1000 / 3600  # convert from ms to hours
    # CPU Usage (%)
    cpu_usage: float = task.avg_cpu_usage / system_cores  # nextflow reports as overall utilisation
    # Memory (GB)
    memory: float = (task.memory or 0.0) / 1073741824  # memory reported in bytes 
    # Core Energy Consumption (without PUE)
    core_consumption: float = time_h * model(cpu_usage) * 0.001  # convert from W to kW
    if 'baseline' in model_name:
        # model = baseline, model = TDP
        # https://github.com/nextflow-io/nf-co2footprint/blob/master/src/main/nextflow/co2footprint/CO2FootprintComputer.groovy
        core_consumption: float = time_h * model(task.avg_cpu_usage) * 0.001 
    # Memory Power Consumption (without PUE)
    memory_consumption: float = memory * memory_coefficient * time_h * 0.001  # convert from W to kW
    # Overall and Memory Consumption (kWh) (without PUE)
    return TaskEnergyResult(core_consumption=core_consumption, memory_consumption=memory_consumption)


# Estimate Carbon Footprint 
def calculate_carbon_footprint_ccf(tasks_grouped_by_interval: Dict[datetime, List[UniversalTrace]], ci: Union[float, Dict[str, float]], pue: float, model_name: str, memory_coefficient: float, unique_nodes: List[str], check_node_memory: bool = False, ewif: Union[float, Dict[str, float]]= None, wue: float = None, elif_: Union[float, Dict[str, float]] = None, lue: float = None ) -> OperationalCarbonResult:
    """
    Calculate the carbon footprint using the CCF methodology.
    
    :param tasks_grouped_by_interval: Dict mapping interval to list of tasks.
    :param ci: Carbon intensity as a float or dict.
    :param pue: Power usage effectiveness.
    :param model_name: Power model name.
    :param memory_coefficient: Memory power draw coefficient.
    :param unique_nodes: List of unique nodes used to execute workflow tasks.
    :return: Tuple containing aggregated metrics and a list of processed tasks.
    """
    # Ensure wue and ewif are both provided together or both None
    if (wue is None) != (ewif is None):
        raise ValueError("Both wue and ewif must be provided together.")
    # Ensure lue and elif_ are both provided together or both None
    if (lue is None) != (elif_ is None):
        raise ValueError("Both lue and elif_ must be provided together.")
    total_energy: float = 0.0
    total_energy_pue: float = 0.0
    total_memory_energy: float = 0.0
    total_memory_energy_pue: float = 0.0
    total_carbon_emissions: float = 0.0
    # adding water and land use footprint when available
    total_water_emissions: float = 0.0 if (wue and ewif) else None
    total_land_emissions: float = 0.0 if (lue and elif_) else None

    records: List[ProcessedTrace] = []
    node_power_models: Dict[str, Callable[[float], float]] = {}
    node_memory_coeffs: Dict[str, float] = {}
    node_system_cores: Dict[str, int] = {}
    node_memory: Dict[str, float] = {}

    for node in unique_nodes: 
        node_power_models[node] = get_power_model_for_node(node, model_name)
        node_memory_coeffs[node] = get_memory_draw(node, model_name)
        node_system_cores[node] = get_system_cores(node)
        node_memory[node] = get_system_memory(node)

    static_energy = {}
    static_memory_energy = 0.0
    total_static_cpu_emissions: float = 0.0

    for group_interval, tasks in tasks_grouped_by_interval.items():
        if tasks:
            # determine the intensity key
            interval_static_energy: float = 0.0 
            
            hour_ts = to_timestamp(group_interval)
            hh: str = str(hour_ts.hour).zfill(2)
            month: str = str(hour_ts.month).zfill(2)
            day: str = str(hour_ts.day).zfill(2)
            mm: str = str(hour_ts.minute).zfill(2)
            intensity_key: str = f'{month}/{day}-{hh}:{mm}'

            # fetching ci value
            if isinstance(ci, float):
                ci_val: float = ci
            else:
                #hour_ts = to_timestamp(group_interval)
                #hh: str = str(hour_ts.hour).zfill(2)
                #month: str = str(hour_ts.month).zfill(2)
                #day: str = str(hour_ts.day).zfill(2)
                #mm: str = str(hour_ts.minute).zfill(2)
                #ci_key: str = f'{month}/{day}-{hh}:{mm}'
                ci_val = ci[intensity_key] 
            
            ###################
            # fetching ewif value
            
            ewif_val = None
            if ewif:
                if isinstance(ewif, float):
                    ewif_val: float = ewif
                else:
                    ewif_val = ewif[intensity_key]

            # fetching elif value
            elif_val = None
            if elif_ :
                if isinstance(elif_, float):
                    elif_val: float = elif_
                else:
                    elif_val = elif_[intensity_key]
            ###################

            for task in tasks:
                host = task.hostname
                power_model = node_power_models[host][0]
                memory_coefficient = node_memory_coeffs[host]
                system_cores = node_system_cores[host]
                energy_result = estimate_task_energy_consumption_ccf(task, power_model, model_name, memory_coefficient, system_cores)
                energy_core, energy_mem = energy_result.core_consumption, energy_result.memory_consumption
                energy_core_pue: float = energy_core * pue
                energy_mem_pue: float = energy_mem * pue
                task_footprint: float = (energy_core_pue + energy_mem_pue) * ci_val
                total_energy += energy_core
                total_energy_pue += energy_core_pue
                total_memory_energy += energy_mem
                total_memory_energy_pue += energy_mem_pue
                total_carbon_emissions += task_footprint

                ###################
                # adding water footprint when available
                task_water_footprint = None
                if wue and ewif:
                    task_water_footprint_onsite: float = (energy_core + energy_mem) * wue # kWh * (L/kWh) = L
                    task_water_footprint_offsite: float = (energy_core_pue + energy_mem_pue) * ewif_val # kWh * (L/kWh) = L
                    task_water_footprint: float = task_water_footprint_onsite + task_water_footprint_offsite # kWh * (L/kWh) = L
                    total_water_emissions += task_water_footprint
                # adding land use footprint when available
                task_land_footprint = None
                if lue and elif_:
                    task_land_footprint_onsite: float = (energy_core + energy_mem) * lue  # kWh * (m2/kWh) = m2
                    task_land_footprint_offsite: float = (energy_core_pue + energy_mem_pue) * elif_val # kWh * (m2/kWh) = m2
                    task_land_footprint: float = task_land_footprint_onsite + task_land_footprint_offsite # kWh * (m2/kWh) = m2
                    total_land_emissions += task_land_footprint
                ###################

                records.append(ProcessedTrace(
                    universal=task,
                    average_co2e=task_footprint,
                    marginal_co2e=task_footprint,
                    embodied_co2e=0.0,
                    avg_ci=ci_val, 
                    average_water=task_water_footprint, # in Liters
                    avg_ewif=ewif_val,
                    average_land=task_land_footprint, # in square meters
                    avg_elif=elif_val,
                ))

            # static power consumption over active periods
            active_time_per_host = compute_active_time_per_host(tasks)

            # compute static cpu power consumption for each host over workflow execution
            for host in active_time_per_host.keys():
                energy = active_time_per_host[host] * node_power_models[host][1] * 0.001  # convert from W to kWh
                interval_static_energy += energy

                if host in static_energy:
                    static_energy[host] += energy
                else:
                    static_energy[host] = energy

                static_memory_energy += active_time_per_host[host] * node_memory_coeffs[host] * node_memory[host] * 0.001  # convert from Wh to kWh

            # static energy --> attribute to carbon emissions:
            total_static_cpu_emissions += interval_static_energy * ci_val

    return OperationalCarbonResult(
        cpu_energy=total_energy,
        cpu_energy_pue=total_energy_pue,
        memory_energy=total_memory_energy,
        memory_energy_pue=total_memory_energy_pue,
        carbon_emissions=total_carbon_emissions, 
        water_emissions=total_water_emissions, # in Liters
        land_emissions=total_land_emissions, # in square meters
        static_cpu_energy_per_host=static_energy,
        static_mem_energy=static_memory_energy,
        records=records
    )


if __name__ == "__main__":
    # Parse Arguments
    args: List[str] = sys.argv[1:]
    arguments = parse_arguments_with_config(args)

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

    if isinstance(arguments[CI], float):
        ci = arguments[CI]
    else:
        ci_filename: str = f"data/intensity/{arguments[CI]}.{FILE}"
        ci = parse_ci_intervals(ci_filename)

    check_reserved_memory_flag: bool = RESERVED_MEMORY in arguments

    cf, records_res = calculate_carbon_footprint_ccf(tasks_by_interval, ci, pue, model_name, memory_coefficient, unique_nodes, check_reserved_memory_flag)
    cpu_energy, cpu_energy_pue, mem_energy, mem_energy_pue, carbon_emissions, node_memory_usage = cf

    print(f"Carbon Emissions: {carbon_emissions}gCO2e")
