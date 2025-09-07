from src.utils.APIRequests import make_json_get_request
from datetime import datetime

def cast(generationmix):
    mix = {}
    for entry in generationmix:
        mix[entry["fuel"].lower()] = entry["perc"] / 100
    return mix

def get_energy_mix_last():
    # This function fetches the last energy mix data from the UK NESO API (past half hour)
    data = make_json_get_request(url='https://api.carbonintensity.org.uk/generation')["data"]
    return datetime.strptime(data["from"], '%Y-%m-%dT%H:%MZ'), datetime.strptime(data["to"], '%Y-%m-%dT%H:%MZ'), cast(data["generationmix"])

def get_energy_mix_pt24(from_time):
    data_24h = make_json_get_request(url=f'https://api.carbonintensity.org.uk/generation/{from_time}/pt24h')["data"]
    return [(datetime.strptime(data["from"], '%Y-%m-%dT%H:%MZ'), datetime.strptime(data["to"], '%Y-%m-%dT%H:%MZ'), cast(data["generationmix"])) for data in data_24h]

def get_energy_mix(from_time, to_time):
    data_range = make_json_get_request(url=f'https://api.carbonintensity.org.uk/generation/{from_time}/{to_time}')["data"]
    return [(datetime.strptime(data["from"], '%Y-%m-%dT%H:%MZ'), datetime.strptime(data["to"], '%Y-%m-%dT%H:%MZ'), cast(data["generationmix"])) for data in data_range]

