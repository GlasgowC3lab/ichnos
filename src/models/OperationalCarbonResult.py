from dataclasses import dataclass
from typing import List, Tuple, Dict
from src.models.CarbonRecord import CarbonRecord

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
    static_cpu_energy_per_host: Dict[str, float]
    static_mem_energy: float
    records: List[CarbonRecord]
