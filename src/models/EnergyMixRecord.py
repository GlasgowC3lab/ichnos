from  datetime import datetime
import numpy as np
from dataclasses import dataclass

@dataclass(frozen=True)
class CarbonIntensityFactors:
    # SOURCE: IPCC https://www.ipcc.ch/site/assets/uploads/2018/02/ipcc_wg3_ar5_annex-iii.pdf median values lifecycle emissions column table A.III.2 page 1335
    nuclear: float = 12 # gCO2eq/kWh 
    geothermal: float = 38
    biomass: float = (740 + 230) / 2
    coal: float = 820
    wind: float = (11 + 12) / 2
    solar: float = (27 + 41 + 48) / 3
    hydro: float = 24
    gas: float = 490
    oil: float = 720 # SOURCE:https://ourworldindata.org/safest-sources-of-energy

    @property
    def unknown(self) -> float:
        values = [
            self.nuclear, self.geothermal, self.biomass, self.coal,
            self.wind, self.solar, self.hydro, self.gas, self.oil
        ]
        return float(np.mean(values))
@dataclass(frozen=True)
class WaterIntensityFactors:
    # SOURCE: https://www.nrel.gov/docs/fy11osti/50900.pdf median from table 1 and 2 (water consumption)
    nuclear: float = 0.00378541*((672 + 269 + 610)/3)  # l/kWh   # tower + once-through + pond (cooling) 
    geothermal: float = 0.00378541*((1796+10+2583+3600+4784+0+135+859+221+1406)/10) # tower(dry steam, flash freshwater, flash geothermal fluid, binay, EGS), Dry (flash, binary, EGS), hybrid (binary, EGS)
    biomass: float = 0.00378541*((553+235+300+390+35)/5) # tower (steam, biogas), once-through steam, pond stram, dry biogas
    coal: float = 0.00378541*((687+471+493+372+942+846+540+250+113+103+545+779+42)/13) # tower (generic, subcritical, supercritical, IGCC, subcritical with CCS, supercritical with CCS), once-through (generic, subcritical, supercritical), pond (generic, subcritical, supercritical)
    wind: float = 0.00378541*(0)
    solar: float = 0.00378541*((26+865+786+1000+78+26+338+170+5)/9) # PV, CSP (tower (trough, power tower, fresnel), dry (trough, power tower), hybrid (trough, power tower), n/a (stirling))
    hydro: float =  0.00378541*(4491)
    gas: float =  0.00378541*((198+826+378+100+240+240+2+340)/8) # tower (combined cylce, stram, comined cycle with CCS), once-through (combined cycle, steam), pond combined cycle, dry combined cycle, inlet steam

    @property
    def unknown(self) -> float:
        values = [
            self.nuclear, self.geothermal, self.biomass, self.coal,
            self.wind, self.solar, self.hydro, self.gas
        ]
        return float(np.mean(values))

    @property
    def oil(self) -> float:
        return float(np.mean([
            self.nuclear, self.geothermal, self.biomass, self.coal, self.wind, self.solar, self.hydro, self.gas
        ]))
@dataclass(frozen=True)
class LandUseIntensityFactors:
    # SOURCE: https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0270155 LUIE_total https://thebreakthrough.org/blog/whats-the-land-use-intensity-of-different-energy-sources  
    nuclear: float = 7.1*10**-5  # m2/kWh 
    geothermal: float = 45*10**-5 
    biomass: float = ((130 + 58000)/2)*10**-5 # residue + dedicated 
    coal: float = 1000*10**-5
    wind: float = ((130 + 12000)/2)*10**-5 # footprint + spacing 
    solar: float = ((1300 + 2000)/2)*10**-5 # CSP + ground mounted PV
    hydro: float = 650*10**-5 
    gas: float = ((410 + 1900)/2)*10**-5 # footprint + spacing 
    oil: float = ((410 + 1900)/2)*10**-5 # from the paperElectricity generation from oil combustion was included in some scenarios in very small quantities; we used the footprint LUIE from a natural gas plant for this figure, as estimates in the literature are not available.

    @property
    def unknown(self) -> float:
        values = [
            self.nuclear, self.geothermal, self.biomass, self.coal,
            self.wind, self.solar, self.hydro, self.gas, self.oil
        ]
        return float(np.mean(values))

class EnergyMixRecord:
    def __init__(self, start_t: datetime, end_t: datetime, mix: dict):
        self.start_t = start_t
        self.date = start_t.date().isoformat()
        self.start = start_t.time().strftime('%H:%M')
        self.end = end_t.time().strftime('%H:%M')
        self.sources = ["wind", "solar", "hydro", "geothermal", "biomass", "nuclear", "coal", "gas", "oil", "unknown"]

        self.wind = float(mix.get("wind", 0.0))
        self.solar = float(mix.get("solar", 0.0))
        self.hydro = float(mix.get("hydro", 0.0))
        self.geothermal = float(mix.get("geothermal", 0.0))
        self.biomass = float(mix.get("biomass", 0.0))
        self.nuclear = float(mix.get("nuclear", 0.0))
        self.coal = float(mix.get("coal", 0.0))
        self.gas = float(mix.get("gas", 0.0))
        self.oil = float(mix.get("oil", 0.0))
        self.unknown = float(mix.get("unknown", 0.0))

        self.ci_factors = CarbonIntensityFactors()
        self.ewif_factors = WaterIntensityFactors()
        self.elif_factors = LandUseIntensityFactors()

    def __str__(self):
        #start_str = self.start_t.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
        #end_str = self.end_t.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'
        return f"{self.date},{self.start},{self.end},{self.gas},{self.coal},{self.biomass},{self.nuclear},{self.hydro},{self.unknown},{self.wind},{self.solar}"

    def _avg_intensity(self, factors):
        avg_intensity = sum([
            getattr(self, source) * getattr(factors, source)
            for source in self.sources
        ])
        return avg_intensity

    def get_avg_ci(self):
        return self.date, self.start, self.end, self._avg_intensity(self.ci_factors) # gCO2eq/kWh
    def get_avg_ewif(self):
        return self.date, self.start, self.end, self._avg_intensity(self.ewif_factors) # l/kWh
    def get_avg_elif(self):
        return self.date, self.start, self.end, self._avg_intensity(self.elif_factors) # m2/kWh