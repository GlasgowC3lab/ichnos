
from datetime import datetime


from src.external_apis.energy_mix.neso import get_energy_mix_last, get_energy_mix_pt24, get_energy_mix

test = ["last_half_hour", "24h", "range"][-1]
# 1) download the data from the NESO API   
if test == "last_half_hour":
    data = get_energy_mix_last() 
elif test == "24h":
    # 24h ending at a given time 
    from_time = datetime.now().strftime('%Y-%m-%dT%H:%MZ')
    data = get_energy_mix_pt24(from_time) 
else:
    assert test == "range"
    from_time = "2024-06-01T00:00Z"
    to_time = "2024-06-25T00:00Z"
    data = get_energy_mix(from_time, to_time)

if type(data) != list: data = [data]

# 2) convert it into EnergyMixRecord objects
from src.external_apis.energy_mix.energy_mix_record import EnergyMixRecord


mix_cache = "date,start,end,wind,solar,hydro,geothermal,biomass,nuclear,coal,gas,oil,unknown\n"
ci_cache = "date,start,end,actual\n"
ewif_cache = "date,start,end,actual\n"
elif_cache = "date,start,end,actual\n"

for start_t, end_t, mix in data:
    emr = EnergyMixRecord(start_t, end_t, mix)
    mix_cache += emr.__str__() + "\n"
    date, start, end, actual = emr.get_avg_ci()
    ci_cache += f"{date},{start},{end},{actual}\n"
    date, start, end, actual = emr.get_avg_ewif()
    ewif_cache += f"{date},{start},{end},{actual}\n"
    date, start, end, actual = emr.get_avg_elif()
    elif_cache += f"{date},{start},{end},{actual}\n"


# 3) store them in a local cache (a file)
cache_path = "src/energy_mix/cache"
import os
if not os.path.exists(cache_path):
    os.makedirs(cache_path)
with open(f"{cache_path}/energy_mix.csv", "w") as f:
    f.write(mix_cache)

with open(f"{cache_path}/ci.csv", "w") as f:
    f.write(ci_cache)

with open(f"{cache_path}/ewif.csv", "w") as f:
    f.write(ewif_cache)

with open(f"{cache_path}/elif.csv", "w") as f:
    f.write(elif_cache)

with open(f"{cache_path}/elif.csv", "w") as f:
    f.write(elif_cache)
