from src.utils.APIRequests import make_json_get_request
from datetime import datetime, timedelta

def cast(generationmix):
    mix = {}
    sum = 0.0
    print(f"Raw generation mix: {generationmix}")

    for source, value in generationmix.items():
        if source == 'flows': continue
        if source in ['hydro discharge', 'battery discharge']: # v3
            source = "unknown"
        if source in [ 'hydro storage', 'battery storage']: # v4
            source = "unknown"
            value = value["discharge"] # - value["charge"]
        if value is None: 
            value = 0.0

        print(f"Processing source: {source}, value: {value}")
        if source in mix.keys():
            mix[source] += value
        else:   
            mix[source] = value

        #assert value is not None, f"Value for source {source} is None"
        
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
    data = make_json_get_request(url=f'https://api.electricitymaps.com/v3/power-breakdown/latest?zone={zone}', api_key=api_key)
    start_dt = datetime.strptime(data["datetime"], '%Y-%m-%dT%H:%M:%S.%fZ')
    end_dt = start_dt + timedelta(hours=1)
    mix = cast(data[mix_type])
    return start_dt, end_dt, mix

def get_energy_mix_pt24(zone, mix_type="powerConsumptionBreakdown"):
    # This function fetches the past 24h energy mix data from the Electricity Maps API
    data_24h = make_json_get_request(url=f'https://api.electricitymaps.com/v3/power-breakdown/history?zone={zone}', api_key=api_key)['history']
    hours = []
    for entry in data_24h:
        start_dt = datetime.strptime(entry["datetime"], '%Y-%m-%dT%H:%M:%S.%fZ')
        end_dt = start_dt + timedelta(hours=1)
        mix = cast(entry[mix_type])
        hours.append((start_dt, end_dt, mix))
    return hours



def get_energy_mix(zone, start_str=None, end_str=None, mix_type="mix"):
    # This function fetches the past 24h energy mix data from the Electricity Maps API

    print(f"Fetching data for zone {zone} from {start_str} to {end_str}...")
    print(type(start_str), type(end_str))
    start_dt = datetime.strptime(start_str, '%Y-%m-%dT%H:%MZ')
    end_dt = datetime.strptime(end_str, '%Y-%m-%dT%H:%MZ')

    api_key = "replace_api_key"
 
    data_period = make_json_get_request(url=f"https://api.electricitymaps.com/v4/electricity-mix/past-range?zone={zone}&start={start_dt.strftime('%Y-%m-%d+%H%%3A%M')}&end={end_dt.strftime('%Y-%m-%d+%H%%3A%M')}", api_key=api_key)["data"]#['history']

    hours = []
    for entry in data_period:
        start_dt = datetime.strptime(entry["datetime"], '%Y-%m-%dT%H:%M:%S.%fZ')
        end_dt = start_dt + timedelta(hours=1)
        mix = cast(entry[mix_type])
        hours.append((start_dt, end_dt, mix))
    return hours