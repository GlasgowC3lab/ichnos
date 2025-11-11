from src.utils.APIRequests import make_json_get_request
from datetime import datetime, timedelta

def cast(generationmix):
    mix = {}
    sum = 0.0
    for source, value in generationmix.items():
        if source == 'hydro discharge' or source == 'battery discharge':
            source = "unknown"
        mix[source] = value
        sum += value
    #sum_perc = 0.0
    for source, value in mix.items():
        mix[source] = value / sum if sum > 0 else 0
    #    sum_perc += mix[source]
    #print(f"sum_perc = {sum_perc}")

    return mix

api_key = "oxXFjh6cXPnPd"
def get_energy_mix_last(zone, mix_type="powerConsumptionBreakdown"):
    # This function fetches the last energy mix data from the Electricity Maps API (past hour)
    data = make_json_get_request(url=f'https://api.electricitymaps.com/v3/power-breakdown/latest?zone={zone}', my_api_key=api_key)
    start_dt = datetime.strptime(data["datetime"], '%Y-%m-%dT%H:%M:%S.%fZ')
    end_dt = start_dt + timedelta(hours=1)
    mix = cast(data[mix_type])
    return start_dt, end_dt, mix

def get_energy_mix_pt24(zone, mix_type="powerConsumptionBreakdown"):
    # This function fetches the past 24h energy mix data from the Electricity Maps API
    data_24h = make_json_get_request(url=f'https://api.electricitymaps.com/v3/power-breakdown/history?zone={zone}', my_api_key=api_key)['history']
    hours = []
    for entry in data_24h:
        start_dt = datetime.strptime(entry["datetime"], '%Y-%m-%dT%H:%M:%S.%fZ')
        end_dt = start_dt + timedelta(hours=1)
        mix = cast(entry[mix_type])
        hours.append((start_dt, end_dt, mix))
    return hours
