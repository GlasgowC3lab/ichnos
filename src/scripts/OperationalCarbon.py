from typing import Callable, Dict, List, Union
from src.models.CarbonRecord import CarbonRecord
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
def compute_active_time_per_host(tasks):
    tasks_by_host = {}

    for task in tasks:
        host = task.hostname
        if host in tasks_by_host:
            tasks_by_host[host].append((float(task.start), float(task.complete)))
        else:
            tasks_by_host[host] = [(float(task.start), float(task.complete))]

    active_time_by_host = {}

    for host, intervals in tasks_by_host.items():
        if not intervals:
            continue

        intervals.sort(key=lambda x: x[0])  # sort by start timestamp
        merged = []
        current_start, current_end = intervals[0]

        for start, end in intervals[1:]:
            if start <= current_end:  # overlap or contiguous
                current_end = max(current_end, end)
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))

        total_ms = sum(end - start for start, end in merged)
        active_time_by_host[host] = total_ms / 1000 / 3600  # return time in hours

    return active_time_by_host


def estimate_task_energy_consumption_ccf(task: CarbonRecord, model: Callable[[float], float], model_name: str, memory_coefficient: float, system_cores: int) -> TaskEnergyResult:
    """
    Estimate the energy consumptions for a task.
    
    :param task: CarbonRecord of the task.
    :param model: Power model function.
    :param model_name: Name of the power model.
    :param memory_coefficient: Coefficient for memory power draw.
    :param system_cores: no. of cores on the system utilised. 
    :return: TaskEnergyResult object containing core and memory energy consumption in kWh.
    """
    if not system_cores:
        system_cores = 32

    # Time (h)
    time_h: float = task.realtime / 1000 / 3600  # convert from ms to hours
    # CPU Usage (%)
    cpu_usage: float = task.cpu_usage / system_cores  # nextflow reports as overall utilisation
    # Memory (GB)
    memory: float = task.memory / 1073741824  # memory reported in bytes  https://www.nextflow.io/docs/latest/metrics.html 
    if 'baseline' in model_name:
        # model = baseline, model = TDP
        core_consumption: float = time_h * ((task.cpu_usage / 100) * model) * 0.001 
# https://github.com/nextflow-io/nf-co2footprint/blob/master/src/main/nextflow/co2footprint/CO2FootprintComputer.groovy
    else:
        # Core Energy Consumption (without PUE)
        core_consumption: float = time_h * model(cpu_usage) * 0.001  # convert from W to kW
    # Memory Power Consumption (without PUE)
    memory_consumption: float = memory * memory_coefficient * time_h * 0.001  # convert from W to kW
    # Overall and Memory Consumption (kWh) (without PUE)
    return TaskEnergyResult(core_consumption=core_consumption, memory_consumption=memory_consumption)


def calculate_carbon_footprint_ccf(tasks_grouped_by_interval: Dict[datetime, List[CarbonRecord]], ci: Union[float, Dict[str, float]], pue: float, model_name: str, memory_coefficient: float, nodes: List[str]) -> OperationalCarbonResult:
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
    total_energy: float = 0.0
    total_energy_pue: float = 0.0
    total_memory_energy: float = 0.0
    total_memory_energy_pue: float = 0.0
    total_carbon_emissions: float = 0.0
    records: List[CarbonRecord] = []
    node_power_models: dict = {}
    node_memory_coeffs: dict = {}
    node_system_cores: dict = {}
    node_memory: dict = {}

    for node in nodes: 
        node_power_models[node] = get_power_model_for_node(node, model_name)
        node_memory_coeffs[node] = get_memory_draw(node, model_name)
        node_system_cores[node] = get_system_cores(node, model_name)
        node_memory[node] = get_system_memory(node)

    static_energy = {}
    static_memory_energy = 0.0
    total_static_cpu_emissions: float = 0.0

    for group_interval, tasks in tasks_grouped_by_interval.items():
        if tasks:
            interval_static_energy: float = 0.0 

            if isinstance(ci, float):
                ci_val: float = ci
            else:
                hour_ts = to_timestamp(group_interval)
                hh: str = str(hour_ts.hour).zfill(2)
                month: str = str(hour_ts.month).zfill(2)
                day: str = str(hour_ts.day).zfill(2)
                mm: str = str(hour_ts.minute).zfill(2)
                ci_key: str = f'{month}/{day}-{hh}:{mm}'
                ci_val = ci[ci_key] 

            for task in tasks:
                host = task.hostname
                power_model = node_power_models[host][0]
                memory_coefficient = node_memory_coeffs[host]
                system_cores = node_system_cores[host]
                energy_result = estimate_task_energy_consumption_ccf(task, power_model, model_name, memory_coefficient, system_cores)
                energy, memory = energy_result.core_consumption, energy_result.memory_consumption
                energy_pue: float = energy * pue
                memory_pue: float = memory * pue
                task_footprint: float = (energy_pue + memory_pue) * ci_val
                task.energy = energy_pue
                task.co2e = task_footprint
                task.avg_ci = ci_val
                total_energy += energy
                total_energy_pue += energy_pue
                total_memory_energy += memory
                total_memory_energy_pue += memory_pue
                total_carbon_emissions += task_footprint
                records.append(task)

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
        carbon_emissions=total_carbon_emissions + total_static_cpu_emissions,
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
