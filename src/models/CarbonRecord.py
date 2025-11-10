"""
Module: CarbonRecord
This module defines the CarbonRecord dataclass which encapsulates energy consumption,
carbon emissions, and performance metrics for a given computational task.
"""

from dataclasses import dataclass, field
from typing import Optional

HEADERS = "name,id,co2e,energy,avg_ci,realtime,cpu_model,cpu_count,cpu_powerdraw,cpu_usage,memory,memory_powerdraw,hostname"

@dataclass(unsafe_hash=True)
class CarbonRecord:
    """
    Dataclass representing a record of carbon footprint and energy consumption.
    
    Attributes:
        energy (Optional[float]): Energy consumption.
        co2e (Optional[float]): Carbon emissions.
        id (str): Identifier for the record.
        realtime (float): Real time duration.
        start (Optional[int]): Start time of the task.
        complete (Optional[int]): Completion time of the task.
        core_count (int): Number of CPU cores used.
        cpu_powerdraw (Optional[float]): CPU power draw metric.
        cpu_usage (float): CPU usage percentage.
        cpu_model (str): CPU model information.
        memory (Optional[float]): Memory consumption.
        name (str): Task name.
        avg_ci (Optional[float]): Average carbon intensity.
        memory_powerdraw (Optional[float]): Memory power draw metric.
        hostname (str): Hostname.
    """
    energy: Optional[float]
    co2e: Optional[float]
    id: str
    realtime: float
    start: Optional[int] = None
    complete: Optional[int] = None
    core_count: int = 0
    cpu_powerdraw: Optional[float] = None
    cpu_usage: float = 0.0
    cpu_model: str = ""
    memory: Optional[float] = None
    name: str = ""
    avg_ci: Optional[float] = None
    memory_powerdraw: Optional[float] = field(default=None, repr=False)
    hostname: str = ""
    
    def __str__(self) -> str:
        """
        Return the CSV-like string representation of the CarbonRecord.
        """
        return f"{self.name},{self.id},{self.co2e},{self.energy},{self.avg_ci},{self.realtime},"\
               f"{self.cpu_model},{self.core_count},{self.cpu_powerdraw},{self.cpu_usage},{self.memory},{self.memory_powerdraw},{self.hostname}"
