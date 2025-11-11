# Default Values
DEFAULT = "default"
FILE = "csv"
DELIMITER = ","
TRACE = "trace"
CI = "ci"
PUE = "pue"

###################
# adding water and land use input parameters
WUE = "wue"
EWIF = "ewif"
LUE = "lue"
ELIF = "elif"
###################

INTERVAL = "interval"
CORE_POWER_DRAW = "core-power-draw"
MEMORY_COEFFICIENT = "memory-coefficient"
MIN_WATTS = "min-watts"
MAX_WATTS = "max-watts"
GA = "GA"
CCF = "CCF"
BOTH = "BOTH"
DEFAULT_PUE_VALUE = 1.0  # Disregard PUE if 1.0
DEFAULT_MEMORY_POWER_DRAW = 0.392  # W/GB
DEFAULT_INTERVAL_VALUE = 60
RESERVED_MEMORY = "reserved-memory"
NUM_OF_NODES = "num-of-nodes"
TASK_FLAG = True
MODEL_NAME = 'model-name' 

# FetchCarbonIntensity Constants
NG_BASE_URL = "https://api.carbonintensity.org.uk/"
NG_ENDPOINT_INTENSITY = "intensity"
NG_ENDPOINT_INTENSITY_DATE = NG_ENDPOINT_INTENSITY + "/date"
HEADERS = {"Accept": "application/json"}
ELECTRICITY_MAPS = "electricity-maps"
NATIONAL_GRID = "national-grid"
SOURCE = "source"
START = "start"
END = "end"
YEAR = "year"
MONTH = "month"
DAY = "day"
HOUR = "hour"
MINS = "mins"