"""Optimize a pump schedule with SciPy and record the run with Acquirium.

Run from this directory with:

    uv run python record_experiment.py
"""
import json
from datetime import datetime, timedelta
from pathlib import Path

import acquirium as aq
import numpy as np
import polars as pl
from scipy.optimize import linprog

HERE = Path(__file__).resolve().parent
INPUTS = HERE / "inputs"
INITIAL_VOLUME = 400.0  # m3
PUMP_CAPACITY = 120.0  # m3/h

# --- Acquirium: describe what each experiment records -----------------------
ac = aq.init(data_dir=HERE / ".data")
study = ac.study.define("pump-schedule-script")

# File input: save the site settings and hourly profiles used by each run.
input_files = study.input("input files").file()
# Scalar inputs: the conditions this run was solved for.
initial_volume = study.input("initial tank volume").scalar(unit="M3")
pump_capacity = study.input("pump capacity").scalar(unit="M3-PER-HR")
# Scalar output: the cost the optimizer found.
operating_cost = study.output("operating cost").scalar(unit="USD")
# Table output: the full hourly schedule, saved as one Parquet file.
schedule_table = study.output("pump schedule").table()
# Time-series output: tank volume, linked to the tank's property in the plant model.
tank_volume = study.output("tank volume").timeseries(
    observed="urn:example:pump-tank:tank-volume",
    unit="M3",
)
# Log: solver status saved with the run.
solver_events = study.log("solver events")

# --- Acquirium: start this experiment and snapshot its inputs ----------------
experiment = study.start(metadata={"scenario": "baseline"})
try:
    input_files.record(INPUTS / "site.json")
    input_files.record(INPUTS / "hourly.csv")
    initial_volume.record(INITIAL_VOLUME)
    pump_capacity.record(PUMP_CAPACITY)

    # --- SciPy: choose hourly pump flows that meet demand at least cost ------
    site = json.loads((INPUTS / "site.json").read_text())
    hourly = pl.read_csv(INPUTS / "hourly.csv")
    demand = hourly["demand_m3_per_hour"].to_numpy()
    price = hourly["price_usd_per_kwh"].to_numpy()
    hours = len(demand)
    # Tank volume after each hour: the start, plus pumped, minus drawn so far.
    cumulative = np.tril(np.ones((hours, hours)))
    volume_without_pumping = INITIAL_VOLUME - np.cumsum(demand)
    # Stay between the minimum and the capacity; end at least as full as at the start.
    result = linprog(
        price * site["energy_intensity_kwh_per_m3"],
        A_ub=np.vstack([cumulative, -cumulative, -cumulative[-1:]]),
        b_ub=np.concatenate([
            site["tank_capacity_m3"] - volume_without_pumping,
            volume_without_pumping - site["minimum_volume_m3"],
            [volume_without_pumping[-1] - INITIAL_VOLUME],
        ]),
        bounds=[(0, PUMP_CAPACITY)] * hours,
        method="highs",
    )
    if not result.success:
        raise RuntimeError(result.message)
    start = datetime.fromisoformat(site["start"])
    schedule = pl.DataFrame({
        "time": [start + timedelta(hours=hour) for hour in range(hours)],
        "pump_flow_m3_per_hour": result.x,
        "tank_volume_m3": volume_without_pumping + cumulative @ result.x,
    })

    # --- Acquirium: save the results and complete the experiment -------------
    operating_cost.record(result.fun)
    schedule_table.record(schedule)
    recorded_volume = tank_volume.record(
        schedule.select("time", pl.col("tank_volume_m3").alias("value"))
    )
    solver_events.record({"status": result.status, "message": result.message, "iterations": result.nit})
    experiment.finish()
except Exception as error:
    experiment.fail(error)
    raise

# --- Acquirium: read back what this run saved --------------------------------
saved_schedule = experiment.observations(schedule_table).latest().dataframe()
baseline_results = study.experiments.where(scenario="baseline").scalars()

print(f"experiment: {experiment.run_id}")
print(f"operating cost: {result.fun:.2f} USD")
print(f"tank volume stream: {recorded_volume.ref_uri}")
print(saved_schedule.head())
print(baseline_results)

aq.shutdown()
