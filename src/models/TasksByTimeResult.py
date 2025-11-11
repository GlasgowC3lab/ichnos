from dataclasses import dataclass
from typing import Dict, List
from src.models.UniversalTrace import UniversalTrace

@dataclass
class TasksByTimeResult:
    """
    Represents the result of grouping tasks by a time interval, including overheads.
    """
    tasks_by_time: Dict[int, List[UniversalTrace]]
    overheads: List[int]
