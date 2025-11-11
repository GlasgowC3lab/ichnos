from src.models.UniversalTrace import UniversalTrace
from src.utils.Usage import print_usage_exit_ExtractTimeline as print_usage_exit

import sys
import configparser
import datetime as time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# Default Values
DEFAULT = "default"
FILE = "csv"
DELIMITER = ","
MEMORY_COEFFICIENT = 0.392  # CCF Average (See Website)


# Functions
def parse_universal_trace(filepath):
    return UniversalTrace.from_csv(filepath)



def get_timeline_data(record: UniversalTrace):
    data = {}

    data["process"] = record.name
    data["realtime"] = record.end - record.start
    data["start"] = record.start
    data["complete"] = record.end
    data["cpu_count"] = record.cpu_count
    data["cpu_usage"] = record.avg_cpu_usage
    data["cpu_model"] = record.cpu_model
    data["memory"] = record.memory
    
    return data


def get_tasks_by_hour(start_hour, end_hour, tasks):
    tasks_by_hour = {}

    step = 60 * 60 * 1000  # 60 minutes in ms
    i = start_hour - step  # start an hour before to be safe

    while i <= end_hour:
        data = [] 

        for task in tasks: 
            # full task is within this hour
            if int(task["start"]) >= i and int(task["complete"]) <= i + step:
                data.append(task)
            # task ends within this hour (but starts in a previous hour)
            elif int(task["complete"]) > i and int(task["complete"]) <= i + step and int(task["start"]) < i:
                # add task from start of this hour until end of hour
                partial_task = task.copy()
                partial_task["start"] = i
                data.append(partial_task)
            # task starts within this hour (but ends in a later hour)
            elif int(task["start"]) > i and int(task["start"]) <= i + step and int(task["complete"]) > i + step: 
                # add task from start to end of this hour
                partial_task = task.copy()
                partial_task["complete"] = i + step
                data.append(partial_task)
            # task starts before hour and ends after this hour
            elif int(task["start"]) < i and int(task["complete"]) > i + step:
                partial_task = task.copy()
                partial_task["start"] = i
                partial_task["end"] = i + step
                data.append(partial_task)

        tasks_by_hour[i] = data
        i += step

    return tasks_by_hour


def plot_task_timeline(tasks):
    # y_pos = np.arange(len(tasks))
    x1_pos = []
    ends = []
    widths = []
    labels = []

    for task in tasks:
        x1_pos.append(int(task["start"]))
        width = int(task["complete"]) - int(task["start"])
        ends.append(int(task["complete"]))
        widths.append(width)
        labels.append(task["process"].split(":")[-1:])

    earliest = min(x1_pos)
    latest = max(ends)
    earliest_hh = int(pd.to_datetime(earliest, unit="ms").round('60min').timestamp() * 1000)  # closest hour in ms
    latest_hh = int(pd.to_datetime(latest, unit="ms").round('60min').timestamp() * 1000)  # closest hour in ms
    diff = 60 * 60 * 1000  # 15 minutes in ms
    ticks = []
    ticklabels = []

    i = earliest_hh - diff
    while i <= latest_hh + diff:
        ticks.append(i)
        ticklabels.append(pd.to_datetime(i, unit="ms").strftime("%H:%M"))
        i += diff 

    fig, ax = plt.subplots()
    ax.invert_yaxis()

    for i in range(len(tasks)):
        ax.barh(labels[i], widths[i], left=x1_pos[i], label=labels[i], alpha=0.7)

    i = earliest_hh
    while i <= latest_hh:
        ax.axvline(i)
        i += diff

    tasks_by_hour = get_tasks_by_hour(earliest_hh, latest_hh, tasks)

    ax.set_xticks(ticks)
    ax.set_xticklabels(ticklabels)

    # report workflow length in seconds
    print(f"workflow {round((latest - earliest) / 1000)} seconds")

    plt.show()

    return tasks_by_hour


def extract_timeline(filename):
    if len(filename.split(".")) > 1:
        filename = filename.split(".")[-2]

    records = parse_universal_trace(f"data/universal_traces/{filename}.{FILE}")
    data_records = []

    for record in records:
        data = get_timeline_data(record)
        data_records.append(data)

    return plot_task_timeline(data_records)


def get_ci_for_interval(start, end):
    return []


# Main Script
if __name__ == '__main__':
    # Parse Arguments
    arguments = sys.argv[1:]

    if len(arguments) != 1:
        print_usage_exit()

    filename = arguments[0]
    extract_timeline(filename)
