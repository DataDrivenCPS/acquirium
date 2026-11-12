"""Synthetic data for the seawater RO plant.

Three feed signals (temperature, salinity, flow) and the feed turbidity are
built from a seasonal or daily cycle plus noise. Every other column follows
from them: by mass balance where the plant fixes the relation, and by a
polynomial fitted to a WaterTAP simulation of the same flowsheet where it
does not (permeate flow and salinity, retentate pressure, pump power).
Setpoints, efficiencies and chemical doses are constant.

The value at a timestamp depends only on the timestamp and the seed, so any
window of the same series can be generated on its own.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import polars as pl

DEFAULT_START = datetime(2025, 1, 1, tzinfo=timezone.utc)
DEFAULT_END = datetime(2026, 1, 1, tzinfo=timezone.utc)
DEFAULT_STEP = timedelta(minutes=10)

# Columns that never change. Units are those of the model points.
CONSTANTS: dict[str, float] = {
    "conn-cartridge-filtration-to-S1-pressure-pa": 100_000.0,
    "P1-efficiency": 0.8,
    "P1-out-pressure-pa": 7_000_000.0,
    "P2-efficiency": 0.8,
    "PXR-efficiency": 0.95,
    "RO-membrane-area": 13_914.0,
    "RO-in-pressure-pa": 7_000_000.0,
    "RO-out-pressure-pa": 101_325.0,
    "PXR-brine-out-pressure-pa": 100_000.0,
    "ferric-chloride-addition-ferric-chloride-dose": 20.0,
    "anti-scalant-addition-anti-scalant-dose": 5.0,
    "lime-addition-lime-dose": 2.3,
}

# Product water volume per kg of permeate [m3/kg].
_PRODUCT_VOLUME_PER_KG = 0.0010002

# Fits to the WaterTAP run, as coefficients of (1, q, c, t, q*q) with
# q = flow - 0.31 m3/s, c = TDS - 33.5 kg/m3, t = temperature - 290 K.
_FITS: dict[str, tuple[float, float, float, float, float]] = {
    "water_density": (989.7632, 0.0, -0.9985323, 0.002665051, 0.0),
    "RO-out-flow-mass-water": (141.2452, 188.7371, -3.322305, -0.3325982, -461.3887),
    "RO-out-flow-mass-tds": (0.02951288, -0.02394979, 0.0003597183, -6.164516e-05, 0.0),
    "RO-out-retentate-pressure-pa": (6756776.0, -1291154.0, -1958.503, 2895.116, 0.0),
    "P1-mechanical-power": (1218707.0, 1630373.0, -28717.79, -2650.224, -3984187.0),
    "P2-mechanical-power": (121220.0, 842173.0, 2618.864, -313.7068, 1931626.0),
}


def _uniform(micros: np.ndarray, seed: int, channel: int) -> np.ndarray:
    """A uniform number in (0, 1) per timestamp, from a hash of the timestamp."""
    with np.errstate(over="ignore"):
        x = micros.astype(np.uint64)
        x = x + np.uint64(seed) * np.uint64(0x9E3779B97F4A7C15)
        x = x + np.uint64(channel + 1) * np.uint64(0xD1B54A32D192ED03)
        x = (x ^ (x >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        x = (x ^ (x >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        x = x ^ (x >> np.uint64(31))
    return ((x >> np.uint64(11)).astype(np.float64) + 0.5) / float(1 << 53)


def _normal(micros: np.ndarray, seed: int, channel: int) -> np.ndarray:
    """A standard normal number per timestamp."""
    u1 = _uniform(micros, seed, 2 * channel)
    u2 = _uniform(micros, seed, 2 * channel + 1)
    return np.sqrt(-2.0 * np.log(u1)) * np.cos(2.0 * np.pi * u2)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def generate(
    start: datetime = DEFAULT_START,
    end: datetime = DEFAULT_END,
    *,
    step: timedelta = DEFAULT_STEP,
    seed: int = 42,
) -> pl.DataFrame:
    """Plant data from ``start`` up to, not including, ``end``.

    Returns the narrow layout ``insert_timeseries_arrow`` takes: one row per
    step and model point, with the columns ``ts`` (UTC), ``ref_name`` (the
    point's local name) and ``value`` (in the point's unit). Rows are ordered
    by time. Naive datetimes are read as UTC. The default is the year 2025 in
    10 minute steps.
    """
    start, end = _utc(start), _utc(end)
    if step <= timedelta(0):
        raise ValueError("step must be positive")
    if end < start:
        raise ValueError("end must not be before start")

    step_us = step // timedelta(microseconds=1)
    start_us = (start - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)
    end_us = (end - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)
    micros = np.arange(start_us, end_us, step_us, dtype=np.int64)

    days = micros / 86_400e6
    year = days.astype("datetime64[D]").astype("datetime64[Y]")
    day_of_year = days - year.astype("datetime64[D]").astype(np.float64) + 1.0
    hour = (days % 1.0) * 24.0

    # Feed. Seawater is warmest and saltiest in late July, flow peaks mid-afternoon.
    season = -np.cos(2.0 * np.pi * (day_of_year - 20.0) / 365.25)
    temperature = 297.5 + 7.0 * season + 0.3 * _normal(micros, seed, 0)
    tds = 35.0 + 1.5 * season + 0.3 * _normal(micros, seed, 1)
    daily = 1.0 + 0.08 * np.sin(2.0 * np.pi * (hour - 9.0) / 24.0)
    flow = 0.3092 * daily * (1.0 + 0.02 * _normal(micros, seed, 2))

    # Turbidity: low baseline with a storm spike in about 3% of the steps.
    tss = 0.03 * np.exp(0.25 * _normal(micros, seed, 3))
    storm = _uniform(micros, seed, 100) < 0.03
    tss = np.where(storm, tss * (3.0 + 3.0 * _uniform(micros, seed, 101)), tss)

    q, c, t = flow - 0.31, tds - 33.5, temperature - 290.0

    def fitted(name: str) -> np.ndarray:
        k0, kq, kc, kt, kqq = _FITS[name]
        return k0 + kq * q + kc * c + kt * t + kqq * q * q

    feed_water = flow * fitted("water_density")
    feed_tds = flow * tds
    permeate_water = fitted("RO-out-flow-mass-water")
    permeate_tds = fitted("RO-out-flow-mass-tds")
    brine_water = feed_water - permeate_water
    brine_tds = feed_tds - permeate_tds
    product_flow = permeate_water * _PRODUCT_VOLUME_PER_KG

    columns: dict[str, np.ndarray] = {
        "intake-in-flow-rate-m3s": flow,
        "intake-in-tds-concentration": tds,
        "intake-in-tss-concentration-kgm3": tss,
        "conn-cartridge-filtration-to-S1-temperature-k": temperature,
        "conn-cartridge-filtration-to-S1-flow-mass-water": feed_water,
        "conn-cartridge-filtration-to-S1-flow-mass-tds": feed_tds,
        "P1-mechanical-power": fitted("P1-mechanical-power"),
        "P2-mechanical-power": fitted("P2-mechanical-power"),
        "RO-in-temperature-k": temperature,
        "RO-in-flow-mass-water": feed_water,
        "RO-in-flow-mass-tds": feed_tds,
        "RO-out-flow-mass-water": permeate_water,
        "RO-out-flow-mass-tds": permeate_tds,
        "RO-out-retentate-pressure-pa": fitted("RO-out-retentate-pressure-pa"),
        "RO-out-retentate-flow-mass-water": brine_water,
        "RO-out-retentate-flow-mass-tds": brine_tds,
        "PXR-brine-out-flow-mass-water": brine_water,
        "PXR-brine-out-flow-mass-tds": brine_tds,
        "storage-tank-3-out-flow-rate-m3s": product_flow,
        "storage-tank-3-out-tds-concentration": permeate_tds / product_flow,
    }
    for name, value in CONSTANTS.items():
        columns[name] = np.full(len(micros), value)

    wide = pl.DataFrame(columns).insert_column(
        0, pl.Series("ts", micros, dtype=pl.Datetime("us", "UTC"))
    )
    narrow = wide.unpivot(index="ts", variable_name="ref_name", value_name="value")
    return narrow.sort("ts", maintain_order=True)
