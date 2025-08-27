from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List
from src.models.UniversalTrace import UniversalTrace
from src.models.ProcessedTrace import ProcessedTrace

@dataclass
class TaskExtractionResult:
    """
    Represents the result of extracting tasks by interval.
    """
    # Mapping of interval bucket (epoch ms) -> list of UniversalTrace tasks (possibly sliced)
    tasks_by_interval: Dict[datetime, List[UniversalTrace]]
    # All original universal trace records (unsliced)
    all_tasks: List[UniversalTrace]
    overhead_intervals: List[int]
    workflow_start: int
    workflow_end: int
