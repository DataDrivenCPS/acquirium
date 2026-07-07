import math
from datetime import datetime, timezone

import numpy as np

# --------------------------------------------------------------------------- #
# Membrane fouling trajectory
# --------------------------------------------------------------------------- #
# Fouling (cake layer / biofilm build-up) adds resistance to water transport,
# so the membrane's water permeability coefficient A declines over time. We
# model a slow exponential decline from a pristine A0 toward an asymptotic,
# heavily-fouled floor A_min = A_MIN_FRAC * A0, with time constant
# FOULING_TAU_DAYS, plus small multiplicative noise for short-term cake-layer
# variability. A CIP (clean-in-place) resets the membrane back near A0, which
# is out of scope here -- this trajectory models a single fouling run between
# cleanings.
A0 = 4.2e-12  # pristine membrane water permeability [m/s-Pa], matches flowsheet.py
A_MIN_FRAC = 0.4  # asymptotic floor as a fraction of A0 (severe fouling)
FOULING_TAU_DAYS = 45.0  # time constant of the exponential decline

# Elapsed time is measured from this epoch so the trajectory is reproducible
# for the batch data-generator (whose default start is 2025-01-01) and still
# well-defined for the live simulation driver (which passes real "now" values).
FOULING_EPOCH = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _elapsed_days(ts: datetime) -> float:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max((ts - FOULING_EPOCH).total_seconds() / 86400.0, 0.0)


def fouling_A_comp(elapsed_days: float, rng: np.random.RandomState) -> float:
    """Membrane water permeability [m/s-Pa] at ``elapsed_days`` into a fouling run."""
    a_min = A_MIN_FRAC * A0
    trend = a_min + (A0 - a_min) * math.exp(-elapsed_days / FOULING_TAU_DAYS)
    noisy = trend * float(np.exp(rng.normal(0.0, 0.01)))
    return min(max(noisy, a_min * 0.9), A0)


def generate_new_values(ts: datetime, rng: np.random.RandomState) -> dict:
    """Realistic, deterministic feed drivers for a given timestamp, plus the
    current point on the membrane fouling trajectory.

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

    # Membrane fouling: exponential decline in water permeability A.
    a_comp = fouling_A_comp(_elapsed_days(ts), rng)

    # Physical clamps.
    return {
        "temperature": max(temperature, 274.0),
        "conc_tds": max(conc_tds, 1.0),
        "conc_tss": min(max(tss, 1e-4), 1.0),
        "conc_toc": min(max(toc, 1e-4), 0.1),
        "flow_vol": max(flow_vol, 0.05),
        "A_comp": a_comp,
    }