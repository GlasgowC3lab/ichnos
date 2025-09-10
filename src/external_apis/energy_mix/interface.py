
from src.external_apis.energy_mix.energy_mix_record import EnergyMixRecord

def fetch_data(func, *args, **kwargs):
    data = func(*args, **kwargs)
    if type(data) != list:
        data = [data]
    return data


def format_data(data: list, factors: list):
    mix = "date,start,end,wind,solar,hydro,geothermal,biomass,nuclear,coal,gas,oil,unknown\n"
    ci = "date,start,end,actual\n"
    ewif = "date,start,end,actual\n"
    elif_ = "date,start,end,actual\n"

    for start_t, end_t, mix_data in data:
        print(f"Processing data from {start_t} to {end_t}")

        emr = EnergyMixRecord(start_t, end_t, mix_data)
        print(emr.__str__())
        mix += emr.__str__() + "\n"
        date, start, end, actual = emr.get_avg_ci()
        ci += f"{date},{start},{end},{actual}\n"
        date, start, end, actual = emr.get_avg_ewif()
        ewif += f"{date},{start},{end},{actual}\n"
        date, start, end, actual = emr.get_avg_elif()
        elif_ += f"{date},{start},{end},{actual}\n"
    intensities = {
        "mix": mix,
        "ci": ci,
        "ewif": ewif,
        "elif": elif_
    }
    print(factors)
    return {
        factor: intensities[factor] for factor in factors
    }

def store_intensity_data(to_store: dict, path="data/intensity", suffix=""):
    # 3) store them in a local cache (a file)
    import os
    if not os.path.exists(path):
        os.makedirs(path)

    for factor, data in to_store.items():
        if factor == "mix":
            filename = "energy_mix.csv"
        elif factor == "ci":
            filename = "ci.csv"
        elif factor == "ewif":
            filename = "ewif.csv"
        elif factor == "elif":
            filename = "elif.csv"
        else:
            continue
        with open(f"{path}/{suffix}{filename}", "w") as f:
            f.write(data)



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch energy mix data from external APIs")
    parser.add_argument('--api', type=str, choices=['neso', 'electricity_maps'], required=True, help='The API to fetch data from')
    parser.add_argument('--factors', nargs='+', type=str, help='The intensity factors to compute', default=['ewif', 'elif'], choices=['mix', 'ci', 'ewif', 'elif'])
    parser.add_argument('--mode', type=str, choices=['last', '24h', 'range'], required=True, help='The mode of data fetching')
    parser.add_argument('--zone', type=str, help='The zone for electricity_maps API')
    parser.add_argument('--from_time', type=str, help='The start time for neso range or 24h mode (format: YYYY-MM-DDTHH:MMZ)')
    parser.add_argument('--to_time', type=str, help='The end time for range mode (format: YYYY-MM-DDTHH:MMZ)')
    parser.add_argument('--output_suffix', type=str, help='The output suffix for the files', default='')
    args = parser.parse_args()

    if args.api == 'neso':
        from src.external_apis.energy_mix.neso import get_energy_mix_last, get_energy_mix_pt24, get_energy_mix 
        if args.mode == 'last':
            data = fetch_data(get_energy_mix_last)
        elif args.mode == '24h':
            if not args.from_time:
                raise ValueError("from_time is required for 24h mode")
            data = fetch_data(get_energy_mix_pt24, args.from_time)
        elif args.mode == 'range':
            if not args.from_time or not args.to_time:
                raise ValueError("from_time and to_time are required for range mode")
            data = fetch_data(get_energy_mix, args.from_time, args.to_time)
        else:
            raise ValueError("Invalid mode for neso API")
    elif args.api == 'electricity_maps':
        from src.external_apis.energy_mix.electricity_maps import get_energy_mix_last, get_energy_mix_pt24
        if not args.zone:
            raise ValueError("zone is required for electricity_maps API")
        if args.mode == 'last':
            data = fetch_data(get_energy_mix_last, args.zone)
        elif args.mode == '24h':
            data = fetch_data(get_energy_mix_pt24, args.zone)
        else:
            raise ValueError("Invalid mode for electricity_maps API")


    intensities = format_data(data, args.factors)
    print(intensities.keys())
    store_intensity_data(intensities, "data/intensity", args.output_suffix)


# Example usage:
# python -m src.external_apis.energy_mix.interface --api neso --mode range --from_time 2023-11-15T00:00Z --to_time 2023-12-08T23:00Z --factors mix ci ewif elif 

