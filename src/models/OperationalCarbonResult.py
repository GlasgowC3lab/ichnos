from dataclasses import dataclass
from typing import List, Tuple
from src.models.ProcessedTrace import ProcessedTrace

@dataclass
class OperationalCarbonResult:
    """
    Represents the result of an operational carbon calculation.
    """
    cpu_energy: float
    cpu_energy_pue: float
    memory_energy: float
    memory_energy_pue: float
    carbon_emissions: float
    water_emissions: float # in Liters
    land_emissions: float # in square meters
    node_memory_usage: List[Tuple[float, float]]
    # List of processed trace (per interval or per original task instance)
    records: List[ProcessedTrace]
