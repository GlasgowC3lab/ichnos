from typing import Callable, Dict, List, Tuple, Union
from src.models.UniversalTrace import UniversalTrace
from src.models.ProcessedTrace import ProcessedTrace
from src.models.TaskEnergyResult import TaskEnergyResult
from src.models.OperationalCarbonResult import OperationalCarbonResult
from src.utils.TimeUtils import to_timestamp, extract_tasks_by_interval
from src.utils.PowerModel import get_power_model
from src.utils.NodeConfigModelReader import get_memory_draw, get_system_cores
from src.utils.Parsers import parse_ci_intervals, parse_arguments_with_config
from src.Constants import *
from datetime import datetime

import sys
import yaml

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
    # Number of Cores (int)
    no_cores: int = task.cpu_count # TODO: we need to revise the use of cpu_count vs system_cores
    # CPU Usage (%)
    cpu_usage: float = task.avg_cpu_usage / system_cores  # nextflow reports as overall utilisation
    # Memory (GB)
    memory: float = (task.memory or 0.0) / 1073741824  # memory reported in bytes 
    # Core Energy Consumption (without PUE)
    core_consumption: float = time_h * model(cpu_usage) * 0.001  # convert from W to kW
    if 'baseline' in model_name:
        # model = baseline, model = TDP
        # https://github.com/nextflow-io/nf-co2footprint/blob/master/src/main/nextflow/co2footprint/CO2FootprintComputer.groovy
        core_consumption: float = time_h * model(task.cpu_usage) * 0.001 
    # Memory Power Consumption (without PUE)
    memory_consumption: float = memory * memory_coefficient * time_h * 0.001  # convert from W to kW
    # Overall and Memory Consumption (kWh) (without PUE)
    return TaskEnergyResult(core_consumption=core_consumption, memory_consumption=memory_consumption)


# Estimate Carbon Footprint 
def calculate_carbon_footprint_ccf(tasks_grouped_by_interval: Dict[datetime, List[UniversalTrace]], ci: Union[float, Dict[str, float]], pue: float, model_name: str, memory_coefficient: float, check_node_memory: bool = False, ewif: Union[float, Dict[str, float]]= None, wue: float = None, elif_: Union[float, Dict[str, float]] = None, lue: float = None ) -> OperationalCarbonResult:
    """
    Calculate the carbon footprint using the CCF methodology.
    
    :param tasks_grouped_by_interval: Dict mapping interval to list of tasks.
    :param ci: Carbon intensity as a float or dict.
    :param pue: Power usage effectiveness.
    :param model_name: Power model name.
    :param memory_coefficient: Memory power draw coefficient.
    :param check_node_memory: Flag to check reserved memory.
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
    node_memory_used: List[Tuple[float, float]] = []
    power_model = get_power_model(model_name)
    system_cores = get_system_cores(model_name)
    memory_coefficient = get_memory_draw(model_name)

    for group_interval, tasks in tasks_grouped_by_interval.items():
        if tasks:
            # determine the intensity key
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

            if check_node_memory:
                starts: List[int] = [int(task.start) for task in tasks]
                ends: List[int] = [int(task.end) for task in tasks]

                earliest: int = min(starts)
                latest: int = max(ends)
                realtime: float = (latest - earliest) / 1000 / 3600  # convert from ms to h 
                node_memory_used.append((realtime, ci_val))

            for task in tasks:
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

    return OperationalCarbonResult(
        cpu_energy=total_energy,
        cpu_energy_pue=total_energy_pue,
        memory_energy=total_memory_energy,
        memory_energy_pue=total_memory_energy_pue,
        carbon_emissions=total_carbon_emissions, 
        water_emissions=total_water_emissions, # in Liters
        land_emissions=total_land_emissions, # in square meters
        node_memory_usage=node_memory_used,
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

    ((tasks_by_interval, _), _) = extract_tasks_by_interval(workflow, interval)
    
    # for curr_interval, records_list in tasks_by_interval.items():
        # print(f'interval: {to_timestamp(curr_interval)}')
        # if records_list:
        #     print(f'tasks: {", ".join([record.id for record in records_list])}')
    
    if isinstance(arguments[CI], float):
        ci = arguments[CI]
    else:
        ci_filename: str = f"data/intensity/{arguments[CI]}.{FILE}"
        ci = parse_ci_intervals(ci_filename)

    check_reserved_memory_flag: bool = RESERVED_MEMORY in arguments
    
    cf, records_res = calculate_carbon_footprint_ccf(tasks_by_interval, ci, pue, model_name, memory_coefficient, check_reserved_memory_flag)
    cpu_energy, cpu_energy_pue, mem_energy, mem_energy_pue, carbon_emissions, node_memory_usage = cf

    print(f"Carbon Emissions: {carbon_emissions}gCO2e")
