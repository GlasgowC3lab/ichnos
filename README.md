# Ichnos+
A project with scripts to methodically calculate the energy consumption, carbon emissions, embodied carbon emissions, land and water use of workflow executions, originally focusing on Nextflow trace files. 

# Setup
To collect node measurements for CPU and Memory energy consumption, refer to the instructions and scripts available in the [node measurement](node_measurement) directory.

## Usage
> [!IMPORTANT]  
> The trace file must use raw data values (e.g. duration recorded in ms) in a CSV format. Please set `trace.raw = true` and `trace.sep = ','` in your `nextflow.config`. In addition to the standard fields, `start`, `complete`, `cpus`, `memory`, `process` and `hostname` must be added to the trace file.
>
> Example entry in `nextflow.config`: 
> ```config
>  trace {
>      raw = true
>      sep = ','
>      fields = ['task_id', 'hash', 'native_id', 'name', 'status', 'exit', 'submit', 'duration', 'realtime', '%cpu', 'peak_rss', 'peak_vmem', 'rchar', 'wchar', 'start', 'complete', 'cpus', 'memory', 'process', 'hostname']
>  }
> ```
Now move the trace files to [data trace](data/trace/). You can run the carbon footprint estimation through config file defined values run 
```
uv run python -m src.scripts.IchnosCF -c config.yaml
```

# Output
The script will produce two files. If the trace file name was 'test', then 'test-trace.csv' would produce a csv file of Carbon Records with energy consumption (inc. PUE) and carbon footprint for each task in the trace file. The 'test-summary.txt' file will contain details around the provided parameters (e.g. CI, PUE) and the overall energy, memory and carbon footprint.     
See the [test-summary](output/test-summary.txt) and [test-trace](output/test-trace.csv). 

# Extras
Using the [convertor](src/scripts/Convertor.py) we can create updated trace files with an updated timestamp (only altering for the same day by a number of hours and minutes at present) to show how Carbon Intensity affects the Carbon Footprint over a day (or some given period in the future).   
See example output:
```
westkath@misool:~/code/carbon-footprint$ python3 -m src.scripts.Convertor nf-rangeland-21-1.txt - 6 0 \;
westkath@misool:~/code/carbon-footprint$ python3 -m src.scripts.Convertor nf-rangeland-21-1.txt + 6 0 \;
```

Results and evaluation repository is available [ichnos-results-evaluation](https://github.com/GlasgowC3lab/ichnos-results-evaluation).

## Credits
- [Carbon Footprint](src/scripts/CarbonFootprint.py) is adapted from the [nf-co2footprint](https://github.com/nextflow-io/nf-co2footprint) plugin which was based on the carbon footprint computation method developed in the [Green Algorithms](https://www.green-algorithms.org/) project. 
  > **Green Algorithms: Quantifying the Carbon Footprint of Computation.**
  > Lannelongue, L., Grealey, J., Inouye, M.,
  > Adv. Sci. 2021, 2100707. https://doi.org/10.1002/advs.202100707
- [Carbon Intensity](src/scripts/CarbonIntensity.py) makes use of the [Carbon Intensity API](https://carbonintensity.org.uk/).
- [Nextflow Trace Files](data/trace/) are generated from [Nextflow]() workflow executions. 
  > **Nextflow enables reproducible computational workflows**
  > P. Di Tommaso, M. Chatzou, E. W. Floden, P. P. Barja, E. Palumbo, and C. Notredame,
  > Nature Biotechnology, vol. 35, no. 4, pp. 316–319, Apr. 2017, https://doi.org/10.1038/nbt.3820
- [Carbon Footprint](src/scripts/CarbonFootprint.py) also features an adaptation of the calculation for variable compute energy usage from the [Cloud Carbon Footprint Methodology](https://www.cloudcarbonfootprint.org/docs/methodology/).
- [Electricity Maps (2025)](https://www.electricitymaps.com). 2023/4 Hourly Carbon Intensity Data (Version January 27, 2025). Electricity Maps. 

