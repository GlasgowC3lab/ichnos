# Carbon-Footprint
A project with scripts to methodically calculate the Carbon Footprint of Workflow Executions from Nextflow trace files.

# Usage
For the current version, replicating the previous calculation approach noted in the Credits section, example usage has been provided with default values:
```
$ python -m src.scripts.IchnosCF <trace-name> <ci-value|ci-file-name> <power_model> <? interval=60> <? pue=1.0> <? memory-coeff=0.392>
$ python3 -m src.scripts.IchnosCF ampliseq-1 uk-marg-010125-110225 gpg_15_powersave_linear 5 1.0 0.392
```      

> **Note**  
> The trace file name must be the file name only, and traces should be csv files stored in the [data trace](data/trace/) directory!

> **Note**  
> The trace file must use raw data values, e.g. duration recorded in ms, this is possible by using the trace.raw flag when executing a nextflow workflow. 

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

## 📖 Publications

If you use Ichnos in your research, please cite our paper:

Kathleen West, Magnus Reid, Yehia Elkhatib and Lauritz Thamsen. "[Ichnos: A Carbon Footprint Estimator for Scientific Workflows](https://arxiv.org/abs/2411.12456)" Full Workshop Paper for *1st International Workshop on Low Carbon Computing (LOCO24)*. 2024

Bibtex:
```
@misc{west2025ichnoscarbonfootprintestimator,
      title={Ichnos: A Carbon Footprint Estimator for Scientific Workflows}, 
      author={Kathleen West and Magnus Reid and Yehia Elkhatib and Lauritz Thamsen},
      year={2025},
      eprint={2411.12456},
      archivePrefix={arXiv},
      primaryClass={cs.DC},
      url={https://arxiv.org/abs/2411.12456}, 
}
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

