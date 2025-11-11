from src.external_apis.Boavizta import get_cpu_impact
from src.models.ProcessedTrace import ProcessedTrace

from typing import List
import sys

DEFAULT_CPU_LIFETIME = 4.0 * 365.25 * 24  # 4 years in hours

def calculate_cpu_embodied_carbon(cpu_model: str, duration_used: float, lifetime: float = None, cpu_usage: float = 1.0) -> float:
    """
    Calculate the embodied carbon for a CPU model based on its usage duration and expected lifetime. Currently assumes 100% CPU utilisation of all cores.
    
    Parameters:
    cpu_model (str): The model of the CPU.
    duration_used (float): The duration the CPU has been used in time units; only used as a ratio so units don't matter, unless no lifetime is provided, then the unit has to be hours.
    lifetime (float): The expected lifetime of the CPU in time units. If None, defaults to 5 years (in hours).
    cpu_usage (float): The percentage of CPU usage (0.0 to 1.0). Defaults to 1.0 (100% usage).
    
    Returns:
    float: The estimated embodied carbon in grams.
    """
    
    cpu_lca = get_cpu_impact(cpu_model)
    if lifetime is None:
        lifetime = DEFAULT_CPU_LIFETIME
    embodied_carbon_kg = cpu_lca * (duration_used / lifetime)
    embodied_carbon_kg *= cpu_usage  # Adjust for CPU usage
    return embodied_carbon_kg * 1000  # Convert to grams

def embodied_carbon_for_processed_traces(records: List[ProcessedTrace], use_cpu_usage: bool = False, fallback_cpu_model: str = None) -> float:
    """Calculate embodied carbon for a list of ProcessedTrace objects.

    Each ProcessedTrace carries a UniversalTrace (processed.universal) supplying time & hardware info.
    """
    total = 0.0
    for rec in records:
        u = rec.universal
        cpu_model = u.cpu_model if (u.cpu_model and u.cpu_model != 'None') else fallback_cpu_model
        duration_ms = (u.end - u.start)
        duration_used_h = duration_ms / 1000 / 3600 if duration_ms else 0.0
        adjusted_cpu_usage = 1.0
        if use_cpu_usage and u.cpu_count:
            usage_val = u.avg_cpu_usage
            if usage_val > 1.0:
                usage_val = usage_val / 100.0
            adjusted_cpu_usage = min(1.0, max(0.0, usage_val))
        total += calculate_cpu_embodied_carbon(cpu_model, duration_used_h, cpu_usage=adjusted_cpu_usage)
    return total


if __name__ == "__main__":
    # Parse args
    args = sys.argv[1:]
    if len(args) < 2:
        print("Usage: python calculate_cpu_embodied_carbon.py <cpu_model> <duration_used> [lifetime]")
        sys.exit(1)
  
    cpu_model = args[0]
    duration_used = float(args[1])
    lifetime = float(args[2]) if len(args) > 2 else None
    
    embodied_carbon = calculate_cpu_embodied_carbon(cpu_model, duration_used, lifetime)
    print(f"Estimated embodied carbon for {cpu_model} used for {duration_used} hours: {embodied_carbon:.2f} g CO2e")
    