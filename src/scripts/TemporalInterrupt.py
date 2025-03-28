from src.models.TraceRecord import TraceRecord
from src.models.CarbonRecord import CarbonRecord
from src.WorkflowNameConstants import *
from src.Constants import FILE, DELIMITER
from src.utils.TimeUtils import to_timestamp, get_hours, extract_tasks_by_hour

import sys
import numpy as np

def get_ci_file_data(filename):
    with open(filename, 'r') as file:
        raw = file.readlines()
        header = [val.strip() for val in raw[0].split(",")]
        data = raw[1:]

    return (header, data)


def parse_ci_intervals(filename):
    (header, data) = get_ci_file_data(filename)

    date_i = header.index("date")
    start_i = header.index("start")
    value_i = header.index("actual")

    ci_map = {}

    for row in data:
        parts = row.split(",")
        date = parts[date_i]
        month_day = '/'.join([val.zfill(2) for val in date.split('-')[-2:]])
        key = month_day + '-' + parts[start_i]
        value = float(parts[value_i])
        ci_map[key] = value

    return ci_map


def parse_trace_file(filepath):
    with open(filepath, 'r') as file:
        lines = [line.rstrip() for line in file]

    header = lines[0]
    records = []

    for line in lines[1:]:
        trace_record = TraceRecord(header, line, DELIMITER)
        records.append(trace_record)

    return records


def linear_power_model(cpu_usage, min_watts, max_watts):
    return min_watts + cpu_usage * (max_watts - min_watts)


def print_usage_exit():
    usage = "$ python -m src.scripts.TemporalInterrupt <ci-file-name> <pue> <memory_coefficient> <min-watts> <max-watts>"
    example = "$ python -m src.scripts.TemporalInterrupt ci 1.0 0.392 65 219"

    print(usage)
    print(example)
    exit(-1)


def get_carbon_record(record: TraceRecord):
    return record.make_carbon_record()



def calculate_carbon_footprint_for_task(task: CarbonRecord, min_watts, max_watts, memory_coefficient):
    # Time (h)
    time = task.get_realtime() / 1000 / 3600  # convert from ms to h
    # Number of Cores (int)
    no_cores = task.get_core_count()
    # CPU Usage (%)
    cpu_usage = task.get_cpu_usage() / (100.0 * no_cores)
    # Memory (GB)
    memory = task.get_memory() / 1073741824  # bytes to GB
    # Core Energy Consumption (without PUE)
    core_consumption = time * linear_power_model(cpu_usage, min_watts, max_watts) * 0.001  # convert from W to kW
    # Memory Power Consumption (without PUE)
    memory_consumption = memory * memory_coefficient * time * 0.001  # convert from W to kW
    # Overall and Memory Consumption (kW) (without PUE)
    return (core_consumption, memory_consumption)


def calculate_carbon_footprint(tasks_by_hour, ci, pue: float, min_watts, max_watts, memory_coefficient):
    total_energy = 0.0
    total_energy_pue = 0.0
    total_memory_energy = 0.0
    total_memory_energy_pue = 0.0
    total_carbon_emissions = 0.0

    for hour, tasks in tasks_by_hour.items():
        if len(tasks) > 0:
            hour_ts = to_timestamp(hour)
            month = str(hour_ts.month).zfill(2)
            day = str(hour_ts.day).zfill(2)
            hh = str(hour_ts.hour).zfill(2)
            mm = str(hour_ts.minute).zfill(2)
            ci_key = f'{month}/{day}-{hh}:{mm}'
            ci_val = ci[ci_key] 

            for task in tasks:
                (energy, memory) = calculate_carbon_footprint_for_task(task, min_watts, max_watts, memory_coefficient)
                energy_pue = energy * pue
                memory_pue = memory * pue
                task_footprint = (energy_pue + memory_pue) * ci_val
                task.set_energy(energy_pue)
                task.set_co2e(task_footprint)
                task.set_avg_ci(ci_val)
                total_energy += energy
                total_energy_pue += energy_pue
                total_memory_energy += memory
                total_memory_energy_pue += memory_pue
                total_carbon_emissions += task_footprint

    return (total_energy, total_energy_pue, total_memory_energy, total_memory_energy_pue, total_carbon_emissions)

def explore_temporal_shifting_for_workflow(workflow, tasks_by_hour, ci, min_watts, max_watts, overhead_hours, pue, memory_coefficient):
    # Identify Hours in Order
    hours_by_key = {}

    for hour, tasks in tasks_by_hour.items():
        if len(tasks) > 0:
            hour_ts = to_timestamp(hour)
            month = str(hour_ts.month).zfill(2)
            day = str(hour_ts.day).zfill(2)
            hh = str(hour_ts.hour).zfill(2)
            mm = str(hour_ts.minute).zfill(2)
            key = f'{month}/{day}-{hh}:{mm}'
            hours_by_key[key] = tasks

    # Calculate Original Carbon Footprint
    (_, _, _, _, orig_carbon_emissions) = calculate_carbon_footprint(tasks_by_hour, ci, pue, min_watts, max_watts, memory_coefficient)

    # Prepare Script Output
    output = [workflow, str(orig_carbon_emissions)]

    # SHIFTING LOGIC
    for shift in [6, 12, 24, 48, 96]:  # flexibility to run over windows 'shift' hours before and after the workflow executed
        keys = list(hours_by_key.keys())  # keys that the workflow executes over
        wf_hours = len(keys)  # hours of workflow execution
        ci_keys = list(ci.keys())  # all windows that have ci values, as keys
        start = keys[0]  # workflow start key
        end = keys[-1]  # workflow end key
        start_i = ci_keys.index(start)  # workflow start index
        end_i = ci_keys.index(end)  # workflow end index
        shift_keys = ci_keys[start_i - shift:end_i + shift + 1]  # all keys within the shift
        # reliant on ci data provided being long enough for the shift window, if not this will error

        dat = np.array([ci[key] for key in shift_keys])  # store corresponding ci values for the potential shifts
        ind = sorted(np.argpartition(dat, wf_hours)[:wf_hours])  # indices of the minimum ci values
        # the indices are sorted to retain chronological order over time
        min_keys = [shift_keys[i] for i in ind]  # matching keys for the minimum ci values

        ci_for_shifted_trace = {}
        for i in range(0, len(min_keys)):
            ci_for_shifted_trace[keys[i]] = ci[min_keys[i]]

        # Report Optimal CI Temporal Shifting Carbon Footprint
        (_, _, _, _, carbon_emissions) = calculate_carbon_footprint(tasks_by_hour, ci_for_shifted_trace, pue, min_watts, max_watts, memory_coefficient)

        # Report Overhead of Interrupting Temporal Shifting
        oh_hour_inds = get_hours(ind)
        overhead = 0

        if len(overhead_hours) > 0:
            for oh_hour_ind in oh_hour_inds:
                overhead += overhead_hours[oh_hour_ind]

        saving = ((orig_carbon_emissions - carbon_emissions) / orig_carbon_emissions) * 100

        output.append(f'{saving:.1f}%:{carbon_emissions}:{overhead / 1000}')  # reports overhead in seconds

    return ','.join(output)


def main(workflows, ci, min_watts, max_watts, pue, memory_coefficient):
    results = []

    for workflow in workflows:
        (tasks_by_hour, overhead_hours) = extract_tasks_by_hour(workflow)
        result = explore_temporal_shifting_for_workflow(workflow, tasks_by_hour, ci, min_watts, max_watts, overhead_hours, pue, memory_coefficient)
        results.append(result)

    with open('output/workflows-temp-shift-interrupt.csv', 'w') as f:
        f.write('workflow,footprint,flexible-6h,flexible-12h,flexible-24h,flexible-48h,flexible-96h\n')

        for result in results:
            f.write(f'{result}\n')


# Main Script
if __name__ == '__main__':
    # Parse Arguments
    arguments = sys.argv[1:]

    if len(arguments) != 5:
        print_usage_exit()

    filename = arguments[0]  # list of workflow traces
    ci_filename = f"data/intensity/{arguments[0]}.{FILE}"
    pue = float(arguments[1])
    memory_coefficient = float(arguments[2])
    min_watts = int(arguments[3])
    max_watts = int(arguments[4])
    ci = parse_ci_intervals(ci_filename)

    main(WORKFLOWS_TEST, ci, min_watts, max_watts, pue, memory_coefficient)
