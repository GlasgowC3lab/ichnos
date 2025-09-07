
from datetime import datetime


from src.external_apis.energy_mix.electricity_maps import get_energy_mix_last, get_energy_mix_pt24


test = ["last_hour", "24h"][-1]
zone="DE"
# 1) download the data from the NESO API
if test == "last_hour":
    data = get_energy_mix_last(zone) 
elif test == "24h":
    data = get_energy_mix_pt24(zone) 

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
cache_path = "data/intensity"
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

