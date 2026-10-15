"""A runnable marimo tutorial for Acquirium's Experiment interface.

From examples/flexpse, run:

    uv run marimo edit ../../docs/tutorials/first-experiment.py
"""

import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell
def _():
    from contextlib import chdir
    from pathlib import Path
    from uuid import uuid4

    import acquirium as aq
    import marimo as mo
    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl
    import pyomo.environ as pyo
    from pyomo.opt import assert_optimal_termination

    from flexcore.config.io import load_model_config
    from flexcore.solvers import get_solver
    from flexops import build_model

    return (
        Path,
        aq,
        assert_optimal_termination,
        build_model,
        chdir,
        get_solver,
        load_model_config,
        mo,
        np,
        pl,
        plt,
        pyo,
        uuid4,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Your first experiment

    Run three scenarios from FlexPSE's
    [`api_freeze`](https://github.com/flex-pse/flex-pse/tree/main/src/flexops/tests/fixtures/api_freeze)
    model. Save each run's inputs and results, then compare scenarios and
    explore the saved data.

    Run from the example directory:

    ```bash
    cd examples/flexpse
    uv sync
    uv run marimo edit ../../docs/tutorials/first-experiment.py
    ```
    """)
    return


@app.cell
def _(Path):
    REPOSITORY = Path(__file__).resolve().parents[2]
    EXAMPLE = REPOSITORY / "examples" / "flexpse"
    INPUTS = EXAMPLE / "inputs"
    PLANT_MODEL = EXAMPLE / "flexpse-watr-model.ttl"
    return EXAMPLE, INPUTS, PLANT_MODEL


@app.cell
def _(EXAMPLE, PLANT_MODEL, aq, uuid4):
    # Keep data in examples/flexpse/.data. Tag runs from this notebook session.
    ac = aq.init(data_dir=EXAMPLE / ".data")
    ac.insert_graph_file(PLANT_MODEL, source_id="plant")
    notebook_session = str(uuid4())
    return ac, notebook_session


@app.cell
def _(mo):
    mo.md(r"""
    ## Declare variables

    A **Study** declares the variables shared by its runs.

    - `.scalar()` records a number and optional unit.
    - `.file()` saves a file copy.
    - `.timeseries()` records samples in an Acquirium stream.
    - `study.log()` records events.

    `observed` identifies the plant property the values describe.
    """)
    return


@app.cell
def _(ac):
    # Plant URIs link the recorded values to the facility model.
    # Assume we know these for now; we'll discuss how to find these later
    TANK_URI = "urn:flex-pse:api-freeze:tank"
    TANK_VOLUME_URI = "urn:flex-pse:api-freeze:tank-volume"
    TREATMENT_FLOW_URI = "urn:flex-pse:api-freeze:treatment-flow"
    ELECTRICAL_POWER_URI = (
        "urn:flex-pse:api-freeze:aggregate-electrical-power"
    )

    # Declare reusable recording handles; record() supplies their values.
    # input/output describe the role; file/scalar/timeseries select storage.
    study = ac.study.define("flexpse-watr-model")

    # File input: keep a copy of each configuration used by the model.
    model_inputs = study.input("model input files").file(
        media_type="application/json"
    )
    # Scalar inputs: one chosen flow and starting volume per run.
    # unit resolves to the shared vocabulary; observed identifies a plant property.
    treatment_flow = study.input("treatment flow").scalar(
        unit="M3-PER-HR",
        observed=TREATMENT_FLOW_URI,
    )
    initial_tank_volume = study.input("initial tank volume").scalar(
        unit="M3",
        observed=TANK_VOLUME_URI,
    )

    # Scalar output: one operating cost computed by the solver.
    operating_cost = study.output("aggregate operating cost").scalar(unit="USD")
    # Time-series outputs: results at each simulation timestamp.
    tank_volume = study.output("tank volume").timeseries(
        observed=TANK_VOLUME_URI,
        unit="M3",
    )
    electrical_power = study.output("aggregate electrical power").timeseries(
        observed=ELECTRICAL_POWER_URI,
        unit="KiloW",
    )
    # Log: solver status and diagnostic events for each run.
    solver_events = study.log("solver events")
    return (
        TANK_URI,
        TANK_VOLUME_URI,
        electrical_power,
        initial_tank_volume,
        model_inputs,
        operating_cost,
        solver_events,
        study,
        tank_volume,
        treatment_flow,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## Run the baseline

    Set the treatment flow and initial tank volume. Start an Experiment
    and record these inputs before building the model.
    """)
    return


@app.cell
def _():
    baseline_flow_m3_per_hour = 10.0
    baseline_tank_volume_m3 = 500.0
    return baseline_flow_m3_per_hour, baseline_tank_volume_m3


@app.cell
def _(
    INPUTS,
    baseline_flow_m3_per_hour,
    baseline_tank_volume_m3,
    initial_tank_volume,
    model_inputs,
    notebook_session,
    study,
    treatment_flow,
):
    # Metadata labels help us find this run later.
    baseline_metadata = {
        "model": "api-freeze",
        "scenario": "baseline",
        "notebook_session": notebook_session,
    }
    # The declared handles now record into this Experiment.
    baseline_experiment = study.start(metadata=baseline_metadata)
    # Snapshot the files before solving; later edits leave these copies intact.
    model_inputs.record(INPUTS / "model.json")
    model_inputs.record(INPUTS / "tariff.json")
    model_inputs.record(INPUTS / "dr_events.json")
    treatment_flow.record(baseline_flow_m3_per_hour)
    initial_tank_volume.record(baseline_tank_volume_m3)
    baseline_experiment.as_dict()
    return (baseline_experiment,)


@app.cell
def _(mo):
    mo.md("""
    ### Build the model and set operating conditions

    Equal inflow and treatment throughput keep the tank volume constant.
    Build from the inputs directory to resolve the tariff and demand-response
    filenames.

    The setup below uses FlexPSE and Pyomo. The `except` block uses Acquirium
    to mark the Experiment failed if setup raises an error.
    """)
    return


@app.cell
def _(
    INPUTS,
    baseline_experiment,
    baseline_flow_m3_per_hour,
    baseline_tank_volume_m3,
    build_model,
    chdir,
    load_model_config,
):
    try:
        # FlexPSE: load the configuration and build the model.
        with chdir(INPUTS):
            baseline_model = build_model(load_model_config(INPUTS / "model.json"))

        # Pyomo: set the tank's starting volume.
        baseline_model.waterfacility.tank.initial_volume.set_value(
            baseline_tank_volume_m3
        )

        # Pyomo: fix inflow and treatment flow at every time step.
        for baseline_time in baseline_model.time_block.time_index:
            baseline_model.waterfacility.tank.flow_in[baseline_time].fix(
                baseline_flow_m3_per_hour
            )
            baseline_model.waterfacility.plant.flow_out[baseline_time].fix(
                baseline_flow_m3_per_hour
            )
    except Exception as setup_error:
        # Acquirium: save the failure, then raise the original exception.
        baseline_experiment.fail(setup_error)
        raise
    return (baseline_model,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Solve the model

    Expand the connections between model units into equations, then solve
    with HiGHS. Check that the solver found an optimal solution before
    extracting results.
    """)
    return


@app.cell
def _(
    assert_optimal_termination,
    baseline_experiment,
    baseline_model,
    get_solver,
    pyo,
):
    try:
        # Pyomo: turn network arcs into constraints between connected units.
        pyo.TransformationFactory("network.expand_arcs").apply_to(baseline_model)

        # FlexPSE: select HiGHS and solve the model.
        baseline_solver = get_solver(model=baseline_model, prefer="highs")
        baseline_solver_results = baseline_solver.solve(baseline_model)

        # Pyomo: require an optimal solution.
        assert_optimal_termination(baseline_solver_results)
        solved_baseline_model = baseline_model
    except Exception as solve_error:
        # Acquirium: mark the Experiment failed if solving fails.
        baseline_experiment.fail(solve_error)
        raise
    return baseline_solver_results, solved_baseline_model


@app.cell
def _(mo):
    mo.md(r"""
    ### Inspect the results

    Extract the objective as a number and the trajectories as a Polars table.
    """)
    return


@app.cell
def _(pl, pyo, solved_baseline_model):
    # Extract numbers from the solved Pyomo expressions.
    baseline_cost_usd = float(pyo.value(solved_baseline_model.objective))
    baseline_timestamps = list(solved_baseline_model.time_block.datetime_index)
    baseline_power_values = []
    baseline_volume_values = []
    baseline_power_output = solved_baseline_model.costing.aggregate_electrical_power
    baseline_tank_output = solved_baseline_model.waterfacility.tank.volume

    for baseline_result_time in solved_baseline_model.time_block.time_index:
        # Convert power to the unit declared for electrical_power.
        baseline_power_expression = baseline_power_output[baseline_result_time]
        baseline_power_kw = pyo.units.convert(
            baseline_power_expression, to_units=pyo.units.kW
        )
        baseline_power_values.append(pyo.value(baseline_power_kw))

        baseline_volume_expression = baseline_tank_output[baseline_result_time]
        baseline_volume_values.append(pyo.value(baseline_volume_expression))

    baseline_trajectories = pl.DataFrame({
        "ts": baseline_timestamps,
        "power_kw": baseline_power_values,
        "volume_m3": baseline_volume_values,
    })
    baseline_trajectories
    return baseline_cost_usd, baseline_trajectories


@app.cell
def _(mo):
    mo.md(r"""
    ### Record the outputs and finish the Experiment

    Record the cost, trajectories, and solver event, then call `finish()`.
    The handles write to the active baseline Experiment.

    To repeat the baseline, rerun from its `study.start()` cell or change its
    inputs. Finished runs reject further writes.
    """)
    return


@app.cell
def _(
    baseline_cost_usd,
    baseline_experiment,
    baseline_solver_results,
    baseline_trajectories,
    electrical_power,
    operating_cost,
    solver_events,
    tank_volume,
):
    operating_cost.record(baseline_cost_usd)
    # record() accepts tables with ts and value columns.
    electrical_power.record(
        baseline_trajectories.select("ts", "power_kw").rename({"power_kw": "value"})
    )
    tank_volume.record(
        baseline_trajectories.select("ts", "volume_m3").rename({"volume_m3": "value"})
    )
    solver_events.record({
        "event": "solve-complete",
        "status": str(baseline_solver_results.solver.status),
        "termination_condition": str(
            baseline_solver_results.solver.termination_condition
        ),
    })
    # Values are already saved; finish() marks the run successful.
    baseline_record = baseline_experiment.finish()
    baseline_record
    return (baseline_record,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Repeat the calculation for two more scenarios

    Put the model setup, solve, and extraction in functions for reuse.
    These functions contain only model code.

    The loop starts one Experiment per scenario and records its inputs and
    results. All runs share the Study declarations.
    """)
    return


@app.cell
def _(
    INPUTS,
    assert_optimal_termination,
    build_model,
    chdir,
    get_solver,
    load_model_config,
    pyo,
):
    def solve_scenario(flow_m3_per_hour, tank_volume_m3):
        with chdir(INPUTS):
            model = build_model(load_model_config(INPUTS / "model.json"))
        model.waterfacility.tank.initial_volume.set_value(tank_volume_m3)
        for time in model.time_block.time_index:
            model.waterfacility.tank.flow_in[time].fix(flow_m3_per_hour)
            model.waterfacility.plant.flow_out[time].fix(flow_m3_per_hour)
        pyo.TransformationFactory("network.expand_arcs").apply_to(model)
        results = get_solver(model=model, prefer="highs").solve(model)
        assert_optimal_termination(results)
        return model, results

    return (solve_scenario,)


@app.cell
def _(pl, pyo):
    def extract_trajectories(model):
        timestamps = list(model.time_block.datetime_index)
        power_values = []
        volume_values = []

        for time in model.time_block.time_index:
            power_expression = model.costing.aggregate_electrical_power[time]
            power_kw = pyo.units.convert(power_expression, to_units=pyo.units.kW)
            power_values.append(pyo.value(power_kw))

            volume_expression = model.waterfacility.tank.volume[time]
            volume_values.append(pyo.value(volume_expression))

        trajectory_columns = {
            "ts": timestamps,
            "power_kw": power_values,
            "volume_m3": volume_values,
        }
        return pl.DataFrame(trajectory_columns)

    return (extract_trajectories,)


@app.cell
def _(
    INPUTS,
    baseline_record,
    electrical_power,
    extract_trajectories,
    initial_tank_volume,
    model_inputs,
    notebook_session,
    operating_cost,
    pl,
    pyo,
    solve_scenario,
    solver_events,
    study,
    tank_volume,
    treatment_flow,
):
    completed_experiments = [study.experiments.get(baseline_record["run_id"])]
    scenarios = [
        ("low throughput / low initial volume", 8.0, 250.0),
        ("high throughput / high initial volume", 12.0, 750.0),
    ]
    for scenario, flow_m3_per_hour, tank_volume_m3 in scenarios:
        scenario_metadata = {
            "model": "api-freeze",
            "scenario": scenario,
            "notebook_session": notebook_session,
        }
        scenario_experiment = study.start(metadata=scenario_metadata)
        try:
            model_inputs.record(INPUTS / "model.json")
            model_inputs.record(INPUTS / "tariff.json")
            model_inputs.record(INPUTS / "dr_events.json")
            treatment_flow.record(flow_m3_per_hour)
            initial_tank_volume.record(tank_volume_m3)

            scenario_model, scenario_solver_results = solve_scenario(
                flow_m3_per_hour, tank_volume_m3
            )
            trajectories = extract_trajectories(scenario_model)

            operating_cost.record(float(pyo.value(scenario_model.objective)))
            electrical_power.record(
                trajectories.select("ts", "power_kw").rename({"power_kw": "value"})
            )
            tank_volume.record(
                trajectories.select("ts", "volume_m3").rename({"volume_m3": "value"})
            )
            solver_events.record({
                "event": "solve-complete",
                "status": str(scenario_solver_results.solver.status),
                "termination_condition": str(
                    scenario_solver_results.solver.termination_condition
                ),
            })
            scenario_experiment.finish()
        except Exception as scenario_error:
            scenario_experiment.fail(scenario_error)
            raise
        completed_experiments.append(scenario_experiment)
    run_summaries = []
    for completed_run in completed_experiments:
        run_summaries.append(completed_run.as_dict())
    completed_runs_table = pl.from_dicts(run_summaries)
    completed_runs_table
    return (completed_experiments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Read saved experiments

    Reopen the Study by name to load its saved variables, units, and metadata.
    These queries also work from a new client session.

    Use `.where()` to filter runs and variables by metadata. The syntax is
    the same as plant queries; these selections stay within this Study.
    """)
    return


@app.cell
def _(ac, completed_experiments, mo):
    # Load saved variable declarations for analysis.
    recorded_study = ac.study.get("flexpse-watr-model")
    variable_catalog = recorded_study.variables.frame()
    mo.vstack([
        mo.md(
            f"{len(completed_experiments)} runs completed. Saved variables:"
        ),
        variable_catalog,
    ])
    return (recorded_study,)


@app.cell
def _(notebook_session, recorded_study):
    # Select successful runs from this notebook execution.
    current_experiments = recorded_study.experiments.where(
        status="succeeded",
        notebook_session=notebook_session,
    )
    experiment_history = current_experiments.frame()
    experiment_history
    return (current_experiments,)


@app.cell
def _(mo):
    mo.md("""
    ### Find outputs by unit

    Select the power output with `unit="kW"`. It matches our declaration's
    `"KiloW"` because both resolve to the same unit. The saved-variable table
    keeps the unit string used in the declaration.
    """)
    return


@app.cell
def _(recorded_study):
    # Find output declarations by their semantic unit.
    power_variables = recorded_study.variables.where(role="output", unit="kW")
    power_variable_table = power_variables.frame()
    power_variable_table.select("label", "role", "kind", "unit")
    return (power_variables,)


@app.cell
def _(mo):
    mo.md("""
    The table above lists variable declarations. Select the baseline run,
    then its power recordings. Their `.frame()` shows stream references and
    recorded intervals. `.dataframe()` fetches the samples for one recording.

    This example records one power batch per run. `.latest()` selects that
    recording; with several batches it would select only the last one.
    """)
    return


@app.cell
def _(current_experiments, power_variables):
    # Choose a variable handle from the filtered declarations.
    saved_power = power_variables["aggregate electrical power"]

    # Select this notebook's baseline run.
    baseline_power_runs = current_experiments.where(scenario="baseline")
    baseline_power_recordings = baseline_power_runs.observations(saved_power)
    baseline_power_recordings.frame().select(
        "experiment_id", "ref_uri", "range_start", "range_end"
    )
    return (baseline_power_recordings,)


@app.cell
def _(baseline_power_recordings):
    # Fetch samples within the recording's time range.
    baseline_power_recording = baseline_power_recordings.latest()
    baseline_power_samples = baseline_power_recording.dataframe()
    baseline_power_samples.head()
    return


@app.cell
def _(mo):
    mo.md("""
    ### Inspect declaration metadata

    Use `.query().include(...).metadata()` to choose graph metadata columns.
    That table shows the resolved unit URI. Use `.metadata_objects()` for Python
    dictionaries; include `unit.raw` to read the original unit string too.
    """)
    return


@app.cell
def _(power_variables):
    # A graph metadata table displays the resolved unit URI.
    power_variable_query = power_variables.query()
    power_unit_table = power_variable_query.include("unit").metadata()
    power_unit_table
    return (power_variable_query,)


@app.cell
def _(power_variable_query):
    # Read the resolved unit and original string as Python objects.
    power_unit_objects = power_variable_query.include("unit", "unit.raw").metadata_objects()
    power_unit_objects
    return


@app.cell
def _(current_experiments):
    # Choose descriptive metadata columns from the same run selection.
    selected_run_query = current_experiments.query()
    selected_run_metadata = selected_run_query.include("scenario", "model").metadata()
    selected_run_metadata
    return


@app.cell
def _(current_experiments, pl, recorded_study):
    # Load variable handles from the saved Study.
    stored_cost = recorded_study.variables["aggregate operating cost"]
    stored_flow = recorded_study.variables["treatment flow"]
    stored_initial_volume = recorded_study.variables["initial tank volume"]

    costs = current_experiments.observations(stored_cost).frame().select(
        "experiment_id",
        pl.col("value").alias("operating_cost_usd"),
    )
    flows = current_experiments.observations(stored_flow).frame().select(
        "experiment_id",
        pl.col("value").alias("treatment_flow_m3_per_hour"),
    )
    initial_volumes = current_experiments.observations(
        stored_initial_volume
    ).frame().select(
        "experiment_id",
        pl.col("value").alias("initial_tank_volume_m3"),
    )

    # Pair each cost with the inputs recorded under the same run ID.
    result_history = (
        costs.join(flows, on="experiment_id")
        .join(initial_volumes, on="experiment_id")
        .sort("treatment_flow_m3_per_hour")
    )
    result_history
    return (result_history,)


@app.cell
def _(plt, result_history):
    cost_figure, cost_axis = plt.subplots()
    cost_axis.plot(
        result_history["treatment_flow_m3_per_hour"],
        result_history["operating_cost_usd"],
        marker="o",
    )
    cost_axis.set(
        xlabel="Treatment flow (m³/h)",
        ylabel="Optimized operating cost (USD)",
        title="Objective history across Experiments",
    )
    cost_axis.grid(alpha=0.25)
    cost_figure
    return


@app.cell
def _(mo, recorded_study, result_history):
    # Find the highest cost with Polars.
    highest_result = result_history.sort(
        "operating_cost_usd", descending=True
    ).row(0, named=True)
    highest_experiment = recorded_study.experiments.get(
        highest_result["experiment_id"]
    )

    mo.md(
        f"""
        ### Highest objective value

        Experiment `{highest_experiment.experiment_id}` had the highest
        optimized operating cost: **${highest_result['operating_cost_usd']:.2f}**
        at **{highest_result['treatment_flow_m3_per_hour']:.1f} m³/h**.
        """
    )
    return (highest_experiment,)


@app.cell
def _(highest_experiment):
    # Retrieve every recorded input, output, and event for one Experiment.
    highest_experiment_record = highest_experiment.observations().frame()
    highest_experiment_record
    return


@app.cell
def _(current_experiments, plt, recorded_study):
    stored_power = recorded_study.variables["aggregate electrical power"]
    power_figure, power_axis = plt.subplots()

    experiments_to_plot = current_experiments.all()
    for experiment in experiments_to_plot:
        power_recording = experiment.observations(stored_power).latest()

        # Each run saved one power batch. Read that batch's samples.
        samples = power_recording.dataframe()
        power_axis.plot(
            samples["ts"],
            samples["value"],
            label=experiment.metadata["scenario"],
        )

    power_axis.set(
        xlabel="Time",
        ylabel="Aggregate electrical power (kW)",
        title="Stored time-series outputs",
    )
    power_axis.legend()
    power_axis.grid(alpha=0.25)
    power_figure.autofmt_xdate()
    power_figure
    return


@app.cell
def _(TANK_VOLUME_URI, recorded_study):
    # Find variables linked to the tank-volume property.
    tank_variables = recorded_study.variables.where(observed=TANK_VOLUME_URI)
    tank_variable_table = tank_variables.frame()
    tank_variable_links = tank_variable_table.select(
        "label", "role", "kind", "observed", "unit"
    )
    tank_variable_links
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Compare a saved run with site data

    Once Acquirium is collecting site data, you can compare a measured
    stream with an experiment result. Find the stream, read the same time
    range, and compare the values.

    This notebook adds a synthetic site stream automatically so you can
    try these steps. At your site, use the stream already collected by
    your driver and skip the example setup.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Read the saved simulation

    Select the baseline run, then find its recorded tank-volume output.
    The observation contains the result's stream URI and time range.
    """)
    return


@app.cell
def _(current_experiments):
    # Find the baseline among this session's successful runs.
    baseline_runs = current_experiments.where(scenario="baseline")

    # Retrieve the tank-volume output recorded for that run.
    baseline_volume_observation = baseline_runs.observations("tank volume").latest()
    baseline_volume_observation.as_dict()
    return (baseline_volume_observation,)


@app.cell
def _(baseline_volume_observation):
    # Use this same interval when reading the site measurements.
    comparison_start = baseline_volume_observation.range_start.isoformat()
    comparison_end = baseline_volume_observation.range_end.isoformat()

    # Fetch samples from the simulation's saved stream.
    simulated_volume = baseline_volume_observation.dataframe()
    simulated_volume.head()
    return comparison_end, comparison_start, simulated_volume


@app.cell(hide_code=True)
def _(TANK_VOLUME_URI, ac, mo, np, simulated_volume):
    # Example setup only: emulate measurements arriving from a site driver.
    sample_number = np.arange(simulated_volume.height)
    daily_cycle = 22.0 * np.sin(2.0 * np.pi * sample_number / 96.0)
    slow_drift = 0.015 * sample_number
    example_values = simulated_volume["value"].to_numpy() + daily_cycle + slow_drift
    example_timestamps = simulated_volume["ts"].to_list()
    example_measurements = example_values.tolist()
    example_pairs = zip(example_timestamps, example_measurements, strict=True)
    example_rows = list(example_pairs)

    # A real driver's configuration supplies its source ID and stream name.
    site_source = "synthetic-flexpse-deployment"
    site_stream_name = "tank-volume-sensor"
    example_stream = {
        "source_id": site_source,
        "ref_name": site_stream_name,
        "point_uri": TANK_VOLUME_URI,
        "label": "tank volume",
        "unit": "http://qudt.org/vocab/unit/M3",
        "value_kind": "numeric",
    }
    ac.register_streams([example_stream])
    ac.insert_timeseries(
        site_source,
        site_stream_name,
        example_rows,
        point_uri=TANK_VOLUME_URI,
        replace=True,
    )
    mo.md("**Example setup:** synthetic site measurements are ready.")
    return site_source, site_stream_name


@app.cell
def _(mo):
    mo.md(r"""
    ### Find the site stream

    Query the tank's measurements and display their stream references.
    For your site, replace `TANK_URI` with your tank's URI in the plant model.
    The `point_label` column gives the measurement label; `ref_uri`
    identifies a specific stream. Several streams can share a label or
    plant property.

    If you know the source ID and stream name from the driver configuration,
    `ac.reference_uri(source_id, stream_name)` gives its reference URI.
    Otherwise, inspect the table and copy the `ref_uri` you want to read.
    """)
    return


@app.cell
def _(TANK_URI, ac, site_source, site_stream_name):
    # Start at the tank in the plant model.
    tank_query = ac.query().entity(uri=TANK_URI, alias="tank")

    # Find measurements on the tank itself.
    tank_measurements = tank_query.measurement(
        alias="measurement",
        include_connection_points=False,
    )

    # List each stream separately, including its reference URI.
    tank_data = tank_measurements.data()
    tank_stream_catalog = tank_data.metadata(include_ref_uris=True)

    # This example knows the source and name from its setup cell.
    site_ref_uri = str(ac.reference_uri(site_source, site_stream_name))
    tank_stream_catalog
    return site_ref_uri, tank_stream_catalog


@app.cell
def _(pl, site_ref_uri, tank_stream_catalog):
    # Select the site stream from the catalog before reading its samples.
    site_stream = tank_stream_catalog.filter(pl.col("ref_uri") == site_ref_uri)
    site_stream
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Read the site measurements

    Pass the selected stream URI and the simulation's time range to the
    time-series client. Both streams in this example use cubic metres.
    Convert units first if your site stream uses a different unit.
    """)
    return


@app.cell
def _(ac, comparison_end, comparison_start, site_ref_uri):
    # Read only the interval covered by the experiment result.
    site_volume = ac.client.timeseries_df(
        site_ref_uri,
        start=comparison_start,
        end=comparison_end,
    )
    site_volume.head()
    return (site_volume,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Match timestamps and compare values

    Give the two value columns distinct names, then join on timestamp.
    This keeps timestamps present in both streams. If your site samples
    arrive at different times, resample both streams to a common interval
    before joining.
    """)
    return


@app.cell
def _(simulated_volume, site_volume):
    # Keep the timestamp and give the simulation values a clear name.
    simulation_values = simulated_volume.select("ts", "value")
    simulation_values = simulation_values.rename({"value": "simulated_volume_m3"})

    # Do the same for the site measurements.
    site_values = site_volume.select("ts", "value")
    site_values = site_values.rename({"value": "site_volume_m3"})

    # Compare values measured at the same timestamp.
    tank_comparison = simulation_values.join(site_values, on="ts", how="inner")
    tank_comparison.head()
    return (tank_comparison,)


@app.cell
def _(pl, tank_comparison):
    # A positive residual means the site reading is above the simulation.
    residual = pl.col("site_volume_m3") - pl.col("simulated_volume_m3")
    tank_residuals = tank_comparison.with_columns(residual.alias("residual_m3"))
    tank_residuals.head()
    return (tank_residuals,)


@app.cell
def _(pl, tank_residuals):
    # Summarize average bias, average absolute error, and largest error.
    comparison_summary = tank_residuals.select(
        pl.col("residual_m3").mean().alias("mean_bias_m3"),
        pl.col("residual_m3").abs().mean().alias("mean_absolute_error_m3"),
        pl.col("residual_m3").abs().max().alias("maximum_absolute_error_m3"),
    )
    comparison_summary
    return


@app.cell
def _(plt, tank_comparison):
    # These samples are 15 minutes apart. Show the first week.
    samples_per_day = 24 * 4
    comparison_window = tank_comparison.head(7 * samples_per_day)
    comparison_figure, comparison_axis = plt.subplots()

    # Plot the saved simulation.
    comparison_axis.plot(
        comparison_window["ts"],
        comparison_window["simulated_volume_m3"],
        label="FlexPSE simulation",
    )

    # Plot the site stream. This notebook uses synthetic measurements.
    comparison_axis.plot(
        comparison_window["ts"],
        comparison_window["site_volume_m3"],
        label="Site measurements (synthetic)",
        alpha=0.8,
    )
    comparison_axis.set(
        xlabel="Time",
        ylabel="Tank volume (m³)",
        title="Simulation and site tank volume",
    )
    comparison_axis.legend()
    comparison_axis.grid(alpha=0.25)
    comparison_figure.autofmt_xdate()
    comparison_figure
    return


@app.cell
def _(mo):
    mo.md(r"""
    For your own experiment, select its saved output and your driver's
    stream, then use the same read, join, and comparison steps. The synthetic
    setup is only needed to supply example data for this notebook.
    """)
    return


if __name__ == "__main__":
    app.run()
