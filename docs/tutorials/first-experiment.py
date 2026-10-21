"""A runnable marimo tutorial for Acquirium's Experiment interface.

From examples/pump-tank, run:

    uv run marimo edit ../../docs/tutorials/first-experiment.py
"""

import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
    from datetime import datetime, timedelta
    from pathlib import Path
    from uuid import uuid4

    import acquirium as aq
    import marimo as mo
    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl
    from scipy.optimize import linprog

    return Path, aq, datetime, json, linprog, mo, np, pl, plt, timedelta, uuid4


@app.cell
def _(mo):
    mo.md(r"""
    # Your first experiment

    A pump fills a storage tank, and the tank supplies a water demand that
    changes through the day. Electricity costs more in the late afternoon.
    Choose the pump's hourly flow so the tank never runs low, at the lowest
    electricity cost.

    You will solve this schedule for several scenarios with SciPy, save each
    run's inputs and results as an Experiment, compare the scenarios, and
    compare a saved result with site measurements.

    Run from the example directory:

    ```bash
    cd examples/pump-tank
    uv sync
    uv run marimo edit ../../docs/tutorials/first-experiment.py
    ```
    """)
    return


@app.cell
def _(Path):
    REPOSITORY = Path(__file__).resolve().parents[2]
    EXAMPLE = REPOSITORY / "examples" / "pump-tank"
    INPUTS = EXAMPLE / "inputs"
    PLANT_MODEL = EXAMPLE / "pump-tank-model.ttl"
    return EXAMPLE, INPUTS, PLANT_MODEL


@app.cell
def _(EXAMPLE, PLANT_MODEL, aq, uuid4):
    # Keep data in examples/pump-tank/.data. Tag runs from this notebook session.
    ac = aq.init(data_dir=EXAMPLE / ".data")
    ac.insert_graph_file(PLANT_MODEL, source_id="plant")
    notebook_session = str(uuid4())
    return ac, notebook_session


@app.cell
def _(mo):
    mo.md(r"""
    ## The model

    The inputs are the site's settings and an hourly profile of demand and
    electricity price for two days. Nothing in this section uses Acquirium:
    it is the calculation that the experiment records.
    """)
    return


@app.cell
def _(INPUTS, json, pl):
    site = json.loads((INPUTS / "site.json").read_text())
    hourly = pl.read_csv(INPUTS / "hourly.csv")
    hourly
    return hourly, site


@app.cell
def _(mo):
    mo.md(r"""
    `solve_schedule()` is a linear program. Each hour's pump flow is a
    decision variable between zero and the pump's capacity. The tank volume
    after each hour is the starting volume plus everything pumped minus
    everything drawn so far. It must stay between the minimum and the tank's
    capacity, and end at least as full as it started. The cost of each hour's
    pumping is its flow times the energy per cubic metre times that hour's
    price.

    The function returns SciPy's result and, if a schedule exists, a table
    of hourly values.
    """)
    return


@app.cell
def _(datetime, hourly, linprog, np, pl, site, timedelta):
    def solve_schedule(initial_volume_m3, pump_capacity_m3_per_hour):
        demand = hourly["demand_m3_per_hour"].to_numpy()
        price = hourly["price_usd_per_kwh"].to_numpy()
        hours = len(demand)
        energy_per_m3 = site["energy_intensity_kwh_per_m3"]

        # Tank volume after each hour: the start, plus pumped, minus drawn so far.
        cumulative = np.tril(np.ones((hours, hours)))
        volume_without_pumping = initial_volume_m3 - np.cumsum(demand)

        result = linprog(
            price * energy_per_m3,
            A_ub=np.vstack([cumulative, -cumulative, -cumulative[-1:]]),
            b_ub=np.concatenate([
                site["tank_capacity_m3"] - volume_without_pumping,
                volume_without_pumping - site["minimum_volume_m3"],
                [volume_without_pumping[-1] - initial_volume_m3],
            ]),
            bounds=[(0, pump_capacity_m3_per_hour)] * hours,
            method="highs",
        )
        if not result.success:
            return result, None

        start = datetime.fromisoformat(site["start"])
        schedule = pl.DataFrame({
            "time": [start + timedelta(hours=hour) for hour in range(hours)],
            "pump_flow_m3_per_hour": result.x,
            "tank_volume_m3": volume_without_pumping + cumulative @ result.x,
            "pump_power_kw": result.x * energy_per_m3,
            "price_usd_per_kwh": price,
        })
        return result, schedule

    return (solve_schedule,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Declare variables

    A **Study** declares the variables shared by its runs. Each variable has
    a role, input or output, and a type that selects its storage:

    - `.file()` saves a copy of a file.
    - `.scalar()` records a number or boolean, with an optional unit.
    - `.table()` saves a table, read back as Polars.
    - `.timeseries()` records samples in an Acquirium stream.
    - `study.log()` records events.

    `observed` links a variable to the plant property it describes. The plant
    model loaded above defines the tank and pump properties used here.
    """)
    return


@app.cell
def _(ac):
    # Plant URIs link the recorded values to the facility model.
    # Assume we know these for now; we'll discuss how to find these later.
    TANK_URI = "urn:example:pump-tank:tank"
    TANK_VOLUME_URI = "urn:example:pump-tank:tank-volume"
    PUMP_POWER_URI = "urn:example:pump-tank:pump-power"

    # Declare reusable recording handles; record() supplies their values.
    study = ac.study.define("pump-schedule")

    # File input: keep a copy of the input files used by each run.
    input_files = study.input("input files").file()
    # Scalar inputs: the conditions each run is solved for.
    initial_volume = study.input("initial tank volume").scalar(
        unit="M3",
        observed=TANK_VOLUME_URI,
    )
    pump_capacity = study.input("pump capacity").scalar(unit="M3-PER-HR")

    # Scalar outputs: one value per run.
    feasible = study.output("feasible").scalar()
    operating_cost = study.output("operating cost").scalar(unit="USD")
    peak_power = study.output("peak pump power").scalar(unit="KiloW")
    # Table output: the hourly schedule, saved as one file.
    pump_schedule = study.output("pump schedule").table()
    # Time-series outputs: a value at each hour, linked to plant properties.
    tank_volume = study.output("tank volume").timeseries(
        observed=TANK_VOLUME_URI,
        unit="M3",
    )
    pump_power = study.output("pump power").timeseries(
        observed=PUMP_POWER_URI,
        unit="KiloW",
    )
    # Log: solver status and messages for each run.
    solver_events = study.log("solver events")
    return (
        TANK_URI,
        TANK_VOLUME_URI,
        feasible,
        initial_volume,
        input_files,
        operating_cost,
        peak_power,
        pump_capacity,
        pump_power,
        pump_schedule,
        solver_events,
        study,
        tank_volume,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## Run the baseline

    Start an Experiment and record its inputs before solving. Metadata such
    as the scenario name makes the run easy to find later.
    """)
    return


@app.cell
def _():
    baseline_initial_volume_m3 = 400.0
    baseline_pump_capacity_m3_per_hour = 120.0
    return baseline_initial_volume_m3, baseline_pump_capacity_m3_per_hour


@app.cell
def _(
    INPUTS,
    baseline_initial_volume_m3,
    baseline_pump_capacity_m3_per_hour,
    initial_volume,
    input_files,
    notebook_session,
    pump_capacity,
    study,
):
    baseline_metadata = {"scenario": "baseline", "notebook_session": notebook_session}
    # The declared handles now record into this Experiment.
    baseline_experiment = study.start(metadata=baseline_metadata)
    # Snapshot the files before solving; later edits leave these copies intact.
    input_files.record(INPUTS / "site.json")
    input_files.record(INPUTS / "hourly.csv")
    initial_volume.record(baseline_initial_volume_m3)
    pump_capacity.record(baseline_pump_capacity_m3_per_hour)
    baseline_experiment.as_dict()
    return (baseline_experiment,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Solve the schedule

    The `except` block marks the Experiment failed if solving raises an
    error, then raises it again.
    """)
    return


@app.cell
def _(
    baseline_experiment,
    baseline_initial_volume_m3,
    baseline_pump_capacity_m3_per_hour,
    solve_schedule,
):
    try:
        baseline_result, baseline_schedule = solve_schedule(
            baseline_initial_volume_m3, baseline_pump_capacity_m3_per_hour
        )
        if baseline_schedule is None:
            raise RuntimeError(baseline_result.message)
    except Exception as solve_error:
        # Acquirium: save the failure, then raise the original exception.
        baseline_experiment.fail(solve_error)
        raise
    baseline_schedule
    return baseline_result, baseline_schedule


@app.cell
def _(mo):
    mo.md(r"""
    ### Record the outputs and finish the Experiment

    `record()` takes each value in the form its type stores: a number, a
    Polars table, or rows with `time` and `value` columns. NumPy numbers are
    converted for you. Finished runs reject further writes.
    """)
    return


@app.cell
def _(
    baseline_experiment,
    baseline_result,
    baseline_schedule,
    feasible,
    operating_cost,
    peak_power,
    pl,
    pump_power,
    pump_schedule,
    solver_events,
    tank_volume,
):
    feasible.record(True)
    operating_cost.record(baseline_result.fun)
    peak_power.record(baseline_schedule["pump_power_kw"].max())
    pump_schedule.record(baseline_schedule)
    tank_volume.record(
        baseline_schedule.select("time", pl.col("tank_volume_m3").alias("value"))
    )
    pump_power.record(
        baseline_schedule.select("time", pl.col("pump_power_kw").alias("value"))
    )
    solver_events.record({
        "status": baseline_result.status,
        "message": baseline_result.message,
        "iterations": baseline_result.nit,
    })
    # Values are already saved; finish() marks the run successful.
    baseline_record = baseline_experiment.finish()
    baseline_record
    return (baseline_record,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Repeat the calculation for more scenarios

    `record_scenario()` runs the same steps for one scenario. All runs share
    the Study's declarations.

    A scenario with no feasible schedule is still a result: it records
    `feasible = False` and the solver's message, then finishes. `fail()` is
    for errors in the calculation itself.
    """)
    return


@app.cell
def _(
    INPUTS,
    feasible,
    initial_volume,
    input_files,
    notebook_session,
    operating_cost,
    peak_power,
    pl,
    pump_capacity,
    pump_power,
    pump_schedule,
    solve_schedule,
    solver_events,
    study,
    tank_volume,
):
    def record_scenario(scenario, initial_volume_m3, pump_capacity_m3_per_hour):
        experiment = study.start(
            metadata={"scenario": scenario, "notebook_session": notebook_session}
        )
        try:
            input_files.record(INPUTS / "site.json")
            input_files.record(INPUTS / "hourly.csv")
            initial_volume.record(initial_volume_m3)
            pump_capacity.record(pump_capacity_m3_per_hour)

            result, schedule = solve_schedule(initial_volume_m3, pump_capacity_m3_per_hour)
            feasible.record(schedule is not None)
            solver_events.record(
                {"status": result.status, "message": result.message, "iterations": result.nit}
            )
            if schedule is not None:
                operating_cost.record(result.fun)
                peak_power.record(schedule["pump_power_kw"].max())
                pump_schedule.record(schedule)
                tank_volume.record(schedule.select("time", pl.col("tank_volume_m3").alias("value")))
                pump_power.record(schedule.select("time", pl.col("pump_power_kw").alias("value")))
            experiment.finish()
        except Exception as error:
            experiment.fail(error)
            raise
        return experiment

    return (record_scenario,)


@app.cell
def _(baseline_record, pl, record_scenario, study):
    completed_experiments = [study.experiments.get(baseline_record["run_id"])]
    scenarios = [
        ("low starting volume", 250.0, 120.0),
        ("larger pump", 400.0, 160.0),
        ("smaller pump", 400.0, 90.0),
        ("undersized pump", 400.0, 60.0),
    ]
    for _scenario, _volume, _capacity in scenarios:
        completed_experiments.append(record_scenario(_scenario, _volume, _capacity))
    pl.from_dicts([_experiment.as_dict() for _experiment in completed_experiments])
    return (completed_experiments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Read saved experiments

    Reopen the Study by name to load its saved variables, units, and
    metadata. These reads also work from a new client session.

    Use `.where()` to filter runs and variables by metadata. The syntax is
    the same as plant queries; these selections stay within this Study.
    """)
    return


@app.cell
def _(ac, completed_experiments, mo):
    # Load saved variable declarations for analysis.
    recorded_study = ac.study.get("pump-schedule")
    mo.vstack([
        mo.md(f"{len(completed_experiments)} runs completed. Saved variables:"),
        recorded_study.variables.frame().select("label", "role", "kind", "unit"),
    ])
    return (recorded_study,)


@app.cell
def _(notebook_session, recorded_study):
    # Select the successful runs from this notebook session.
    current_experiments = recorded_study.experiments.where(
        status="succeeded",
        notebook_session=notebook_session,
    )
    current_experiments.frame()
    return (current_experiments,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Compare scenarios

    `.scalars()` reads one row per run and one column per scalar variable.
    Name the metadata keys to include as columns. The undersized pump has no
    schedule, so its cost is null.
    """)
    return


@app.cell
def _(current_experiments):
    results = current_experiments.scalars(metadata=["scenario"])
    results
    return (results,)


@app.cell
def _(mo):
    mo.md(r"""
    Formulas are ordinary Polars expressions. Compare each scenario's cost
    with the baseline's. See
    [Compare runs with formulas](../how-to/compare-experiment-runs.md) for
    more.
    """)
    return


@app.cell
def _(pl, results):
    baseline_cost = results.filter(pl.col("scenario") == "baseline")["operating cost"][0]
    comparison = results.with_columns(
        (baseline_cost - pl.col("operating cost")).alias("savings vs baseline")
    ).sort("operating cost")
    comparison.select("scenario", "feasible", "operating cost", "savings vs baseline")
    return (comparison,)


@app.cell
def _(comparison, pl, plt):
    cost_figure, cost_axis = plt.subplots()
    _feasible_runs = comparison.filter(pl.col("feasible"))
    cost_axis.barh(_feasible_runs["scenario"], _feasible_runs["operating cost"])
    cost_axis.set(xlabel="Operating cost (USD)", title="Optimized cost by scenario")
    cost_axis.grid(alpha=0.25, axis="x")
    cost_figure.tight_layout()
    cost_figure
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Read a saved table

    Select the cheapest feasible run and read back its schedule. The table
    output returns the Polars table that was recorded.
    """)
    return


@app.cell
def _(comparison, mo, pl, recorded_study):
    cheapest = comparison.filter(pl.col("feasible")).row(0, named=True)
    cheapest_experiment = recorded_study.experiments.get(cheapest["experiment_id"])
    cheapest_schedule = (
        cheapest_experiment.observations("pump schedule").latest().dataframe()
    )
    mo.vstack([
        mo.md(
            f"The **{cheapest['scenario']}** scenario costs "
            f"**${cheapest['operating cost']:.2f}**."
        ),
        cheapest_schedule.head(),
    ])
    return cheapest, cheapest_schedule


@app.cell
def _(cheapest, cheapest_schedule, plt):
    schedule_figure, flow_axis = plt.subplots()
    flow_axis.step(
        cheapest_schedule["time"],
        cheapest_schedule["pump_flow_m3_per_hour"],
        where="post",
        label="pump flow",
    )
    flow_axis.set(ylabel="Pump flow (m³/h)", title=f"Schedule: {cheapest['scenario']}")
    price_axis = flow_axis.twinx()
    price_axis.step(
        cheapest_schedule["time"],
        cheapest_schedule["price_usd_per_kwh"],
        where="post",
        color="tab:orange",
        label="price",
    )
    price_axis.set(ylabel="Price (USD/kWh)")
    schedule_figure.legend(loc="upper right")
    schedule_figure.autofmt_xdate()
    schedule_figure
    return


@app.cell
def _(mo):
    mo.md(r"""
    The pump runs mostly while power is cheap, filling the tank ahead of the
    evening price peak, and stops during the peak.

    ### Find outputs by unit

    Select time-series outputs with `unit="kW"`. It matches the declaration's
    `"KiloW"` because both resolve to the same unit. The saved-variable table
    keeps the unit string used in the declaration.
    """)
    return


@app.cell
def _(recorded_study):
    power_variables = recorded_study.variables.where(
        role="output", kind="timeseries", unit="kW"
    )
    power_variables.frame().select("label", "role", "kind", "unit")
    return (power_variables,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Read time-series outputs

    A run's time-series recording holds a stream reference and a time range.
    `.frame()` lists them; `.dataframe()` fetches the samples.
    """)
    return


@app.cell
def _(current_experiments, power_variables):
    saved_power = power_variables["pump power"]
    power_recordings = current_experiments.observations(saved_power)
    power_recordings.frame().select("experiment_id", "ref_uri", "range_start", "range_end")
    return


@app.cell
def _(comparison, pl, plt, recorded_study):
    volume_figure, volume_axis = plt.subplots()
    for _row in comparison.filter(pl.col("feasible")).iter_rows(named=True):
        _run = recorded_study.experiments.get(_row["experiment_id"])
        # Each run saved one tank-volume batch. Read that batch's samples.
        _samples = _run.observations("tank volume").latest().dataframe()
        volume_axis.plot(_samples["ts"], _samples["value"], label=_row["scenario"])
    volume_axis.set(xlabel="Time", ylabel="Tank volume (m³)", title="Saved tank volume by scenario")
    volume_axis.legend()
    volume_axis.grid(alpha=0.25)
    volume_figure.autofmt_xdate()
    volume_figure
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Inspect declaration metadata

    Use `.query().include(...).metadata()` to choose graph metadata columns.
    That table shows the resolved unit URI. Use `.metadata_objects()` for
    Python dictionaries; include `unit.raw` to read the original unit string.
    """)
    return


@app.cell
def _(power_variables):
    power_variable_query = power_variables.query()
    power_variable_query.include("unit").metadata()
    return (power_variable_query,)


@app.cell
def _(power_variable_query):
    power_variable_query.include("unit", "unit.raw").metadata_objects()
    return


@app.cell
def _(TANK_VOLUME_URI, recorded_study):
    # Find variables linked to the tank-volume property.
    tank_variables = recorded_study.variables.where(observed=TANK_VOLUME_URI)
    tank_variables.frame().select("label", "role", "kind", "observed", "unit")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Compare a saved run with site data

    Once Acquirium is collecting site data, you can compare a measured
    stream with an experiment result. Find the stream, read the same time
    range, and compare the values.

    This notebook adds a synthetic site stream so you can try these steps.
    At your site, use the stream already collected by your driver and skip
    the example setup.
    """)
    return


@app.cell
def _(current_experiments):
    # Retrieve the baseline's tank-volume output and its time range.
    baseline_runs = current_experiments.where(scenario="baseline")
    baseline_volume_observation = baseline_runs.observations("tank volume").latest()
    comparison_start = baseline_volume_observation.range_start.isoformat()
    comparison_end = baseline_volume_observation.range_end.isoformat()
    simulated_volume = baseline_volume_observation.dataframe()
    simulated_volume.head()
    return comparison_end, comparison_start, simulated_volume


@app.cell(hide_code=True)
def _(TANK_VOLUME_URI, ac, mo, np, simulated_volume):
    # Example setup only: emulate measurements arriving from a site driver.
    _hour = np.arange(simulated_volume.height)
    _measured = simulated_volume["value"].to_numpy() + 15.0 * np.sin(2.0 * np.pi * _hour / 24.0) + 0.6 * _hour

    # A real driver's configuration supplies its source ID and stream name.
    site_source = "synthetic-pump-station"
    site_stream_name = "tank-level-sensor"
    ac.register_streams([{
        "source_id": site_source,
        "ref_name": site_stream_name,
        "point_uri": TANK_VOLUME_URI,
        "label": "tank volume",
        "unit": "http://qudt.org/vocab/unit/M3",
        "value_kind": "numeric",
    }])
    ac.insert_timeseries(
        site_source,
        site_stream_name,
        list(zip(simulated_volume["ts"].to_list(), _measured.tolist(), strict=True)),
        point_uri=TANK_VOLUME_URI,
        replace=True,
    )
    mo.md("**Example setup:** synthetic site measurements are ready.")
    return site_source, site_stream_name


@app.cell
def _(mo):
    mo.md(r"""
    ### Find the site stream

    Query the tank's measurements. For your site, replace `TANK_URI` with
    your tank's URI in the plant model. Experiment results do not appear
    here: each run's stream has its own point, so plant queries return site
    data only. `ref_uri` identifies each stream.

    If you know the source ID and stream name from the driver configuration,
    `ac.reference_uri(source_id, stream_name)` gives its reference URI.
    """)
    return


@app.cell
def _(TANK_URI, ac, site_source, site_stream_name):
    tank_measurements = ac.query().entity(uri=TANK_URI, alias="tank").measurement(
        alias="measurement",
        include_connection_points=False,
    )
    # This example knows the source and name from its setup cell.
    site_ref_uri = str(ac.reference_uri(site_source, site_stream_name))
    tank_measurements.data().metadata(include_ref_uris=True)
    return (site_ref_uri,)


@app.cell
def _(mo):
    mo.md(r"""
    To list the runs that simulated the same property instead, select stream
    points by their `observed` attribute:
    """)
    return


@app.cell
def _(TANK_VOLUME_URI, ac):
    simulated_tank_volumes = ac.query().measurement(alias="simulated", observed=TANK_VOLUME_URI)
    simulated_tank_volumes.include("run_id", "study_id").metadata_objects()
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Match timestamps and compare values

    Read the site stream over the simulation's time range, give the value
    columns distinct names, and join on timestamp. Both streams use cubic
    metres; convert units first if yours differ. If your site samples arrive
    at different times, resample both streams to a common interval first.
    """)
    return


@app.cell
def _(ac, comparison_end, comparison_start, pl, simulated_volume, site_ref_uri):
    site_volume = ac.client.timeseries_df(site_ref_uri, start=comparison_start, end=comparison_end)
    tank_comparison = simulated_volume.select(
        "ts", pl.col("value").alias("simulated_volume_m3")
    ).join(
        site_volume.select("ts", pl.col("value").alias("site_volume_m3")),
        on="ts",
        how="inner",
    ).with_columns(
        # A positive residual means the site reading is above the simulation.
        (pl.col("site_volume_m3") - pl.col("simulated_volume_m3")).alias("residual_m3")
    )
    tank_comparison.head()
    return (tank_comparison,)


@app.cell
def _(pl, tank_comparison):
    # Summarize average bias, average absolute error, and largest error.
    tank_comparison.select(
        pl.col("residual_m3").mean().alias("mean_bias_m3"),
        pl.col("residual_m3").abs().mean().alias("mean_absolute_error_m3"),
        pl.col("residual_m3").abs().max().alias("maximum_absolute_error_m3"),
    )
    return


@app.cell
def _(plt, tank_comparison):
    comparison_figure, comparison_axis = plt.subplots()
    comparison_axis.plot(tank_comparison["ts"], tank_comparison["simulated_volume_m3"], label="Baseline schedule")
    comparison_axis.plot(
        tank_comparison["ts"],
        tank_comparison["site_volume_m3"],
        label="Site measurements (synthetic)",
        alpha=0.8,
    )
    comparison_axis.set(xlabel="Time", ylabel="Tank volume (m³)", title="Simulation and site tank volume")
    comparison_axis.legend()
    comparison_axis.grid(alpha=0.25)
    comparison_figure.autofmt_xdate()
    comparison_figure
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Check what the Study stores

    `usage()` counts runs, recordings, samples, and files, with their sizes.
    Delete runs you no longer need with `.delete()`; see
    [Record experiments](../how-to/record-experiments.md#check-storage-and-delete-runs).
    """)
    return


@app.cell
def _(recorded_study):
    recorded_study.usage()
    return


if __name__ == "__main__":
    app.run()
