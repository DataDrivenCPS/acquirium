import math
from datetime import datetime

import numpy as np

def generate_new_values(ts: datetime, rng: np.random.RandomState) -> dict:
    """Realistic, deterministic feed drivers for a given timestamp.

    All signals combine a physical seasonal/diurnal component with a small
    seeded stochastic perturbation.  Returned in SI units used by the feed.
    """
    doy = ts.timetuple().tm_yday  # 1..366
    hour = ts.hour + ts.minute / 60.0

    # Seawater temperature [K]: warmest in late summer (~day 210), ~17-31 C.
    season = -math.cos(2 * math.pi * (doy - 20) / 365.25)  # min ~Jan, max ~Jul
    temperature = 297.5 + 7.0 * season + rng.normal(0.0, 0.3)

    # TDS salinity [kg/m3]: mild seasonal rise with evaporation, in phase w/ temp.
    conc_tds = 35.0 + 1.5 * season + rng.normal(0.0, 0.3)

    # TSS turbidity [kg/m3]: low baseline, lognormal noise, occasional storm spikes.
    tss = 0.03 * float(np.exp(rng.normal(0.0, 0.25)))
    if rng.random_sample() < 0.03:  # ~3% of steps see a turbidity event
        tss *= rng.uniform(3.0, 6.0)

    # Intake flow [m3/s]: demand-following diurnal pattern (peak mid-day) +/- noise.
    diurnal = 1.0 + 0.08 * math.sin(2 * math.pi * (hour - 9) / 24.0)
    flow_vol = 0.3092 * diurnal * (1.0 + rng.normal(0.0, 0.02))

    # TOC [kg/m3]: ~3 mg/L baseline, higher in the warm season (algal
    # productivity), lognormal noise, occasional bloom spikes.
    toc = 0.003 * (1.0 + 0.4 * max(season, 0.0)) * float(np.exp(rng.normal(0.0, 0.2)))
    if rng.random_sample() < 0.03:  # ~3% of steps see an algal bloom
        toc *= rng.uniform(1.5, 3.0)

    # Physical clamps.
    return {
        "temperature": max(temperature, 274.0),
        "conc_tds": max(conc_tds, 1.0),
        "conc_tss": min(max(tss, 1e-4), 1.0),
        "conc_toc": min(max(toc, 1e-4), 0.1),
        "flow_vol": max(flow_vol, 0.05),
    }