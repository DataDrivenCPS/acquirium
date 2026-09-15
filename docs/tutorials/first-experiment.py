"""A runnable marimo tutorial for Acquirium's Experiment interface.

From examples/flexpse, run:

    uv run marimo edit ../../docs/tutorials/first-experiment.py
"""

import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
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
        get_solver,
        load_model_config,
        mo,
        np,
        os,
        pl,
        plt,
        pyo,
        uuid4,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Your first experiment

    This notebook runs several variants of FlexPSE's
    [`api_freeze`](https://github.com/flex-pse/flex-pse/tree/main/src/flexops/tests/fixtures/api_freeze)
    model and records them as Acquirium Experiments.

    The FlexPSE calculation remains an ordinary model build and solve. The
    Experiment interface adds a durable account of its inputs, outputs,
    events, and their meaning. After producing several runs, we will reopen
    the Study from SQL, discover its variables, compare the results, and
    retrieve one Experiment without relying on the declaration cells.

    Run this notebook from its derived environment:

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
    # aq.init starts an embedded server and keeps its SQL/time-series data in
    # examples/flexpse/.data. A unique session tag lets the analysis below
    # select only the Experiments created by this execution of the notebook.
    ac = aq.init(data_dir=EXAMPLE / ".data")
    ac.insert_graph_file(PLANT_MODEL, source_id="plant")
    notebook_session = str(uuid4())
    return ac, notebook_session


@app.cell
def _(mo):
    mo.md(r"""
    ## Describe the Study before recording values

    A **Study** describes a reusable family of comparable calculations.
    Variables declare what will be recorded and how it should be stored;
    they do not contain results themselves.

    - `.scalar()` stores a single value with metadata such as its unit.
    - `.file()` snapshots the bytes of an input file.
    - `.timeseries()` creates a normal Acquirium stream and records its URI.
    - `study.log()` stores ordered events from a run.

    The `observed` URI on tank volume and electrical power is the durable
    connection to the plant model. It says what physical property the
    simulated values describe, rather than merely attaching a display
    label to a column.
    """)
    return


@app.cell
def _(ac):
    TANK_URI = "urn:flex-pse:api-freeze:tank"
    TANK_VOLUME_URI = "urn:flex-pse:api-freeze:tank-volume"
    TREATMENT_FLOW_URI = "urn:flex-pse:api-freeze:treatment-flow"
    ELECTRICAL_POWER_URI = (
        "urn:flex-pse:api-freeze:aggregate-electrical-power"
    )

    study = ac.study.define("flexpse-watr-model")

    model_inputs = study.input("model input files").file(
        media_type="application/json"
    )
    treatment_flow = study.input("treatment flow").scalar(
        unit="M3-PER-HR",
        observed=TREATMENT_FLOW_URI,
    )
    initial_tank_volume = study.input("initial tank volume").scalar(
        unit="M3",
        observed=TANK_VOLUME_URI,
    )

    operating_cost = study.output("aggregate operating cost").scalar(unit="USD")
    tank_volume = study.output("tank volume").timeseries(
        observed=TANK_VOLUME_URI,
        unit="M3",
    )
    electrical_power = study.output("aggregate electrical power").timeseries(
        observed=ELECTRICAL_POWER_URI,
        unit="KiloW",
    )
    solver_events = study.log("solver events")
    return (
        electrical_power,
        ELECTRICAL_POWER_URI,
        initial_tank_volume,
        model_inputs,
        operating_cost,
        solver_events,
        study,
        TANK_URI,
        TANK_VOLUME_URI,
        tank_volume,
        treatment_flow,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ## Keep the model calculation and the experiment record distinct

    The function below repeats one calculation for each set of initial
    conditions. Its middle block is the FlexPSE model code: build the model,
    set its operating conditions, solve it, and turn results into ordinary
    Python values.

    The Acquirium blocks around it have a narrower job. Before the solve,
    they record the inputs that the model consumes. Afterwards, they record
    the resulting scalar and trajectories. `record()` always targets the
    currently active Experiment and dispatches according to the variable's
    declaration above.
    """)
    return


@app.cell
def _(
    INPUTS,
    assert_optimal_termination,
    build_model,
    electrical_power,
    get_solver,
    initial_tank_volume,
    load_model_config,
    model_inputs,
    notebook_session,
    operating_cost,
    os,
    pyo,
    solver_events,
    study,
    tank_volume,
    treatment_flow,
):
    def solve_and_record(*, scenario, flow_m3_per_hour, tank_volume_m3):
        # --- Acquirium: identify this run and record the model inputs --------
        experiment = study.start(
            metadata={
                "model": "api-freeze",
                "scenario": scenario,
                "notebook_session": notebook_session,
            }
        )
        try:
            for path in (
                INPUTS / "model.json",
                INPUTS / "tariff.json",
                INPUTS / "dr_events.json",
            ):
                model_inputs.record(path)
            treatment_flow.record(flow_m3_per_hour)
            initial_tank_volume.record(tank_volume_m3)

            # --- FlexPSE: the model calculation -----------------------------
            # The config refers to its other inputs by bare filename, matching
            # the upstream fixture, so build it from the inputs directory.
            os.chdir(INPUTS)
            model = build_model(load_model_config(INPUTS / "model.json"))
            model.waterfacility.tank.initial_volume.set_value(tank_volume_m3)

            # Equal inflow and treatment throughput preserve the chosen tank
            # volume while changing the facility load between scenarios.
            for time in model.time_block.time_index:
                model.waterfacility.tank.flow_in[time].fix(flow_m3_per_hour)
                model.waterfacility.plant.flow_out[time].fix(flow_m3_per_hour)

            pyo.TransformationFactory("network.expand_arcs").apply_to(model)
            results = get_solver(model=model, prefer="highs").solve(model)
            assert_optimal_termination(results)

            objective = float(pyo.value(model.objective))
            power_rows = [
                (
                    timestamp,
                    pyo.value(
                        pyo.units.convert(
                            model.costing.aggregate_electrical_power[time],
                            to_units=pyo.units.kW,
                        )
                    ),
                )
                for timestamp, time in zip(
                    model.time_block.datetime_index,
                    model.time_block.time_index,
                    strict=True,
                )
            ]
            volume_rows = [
                (timestamp, pyo.value(model.waterfacility.tank.volume[time]))
                for timestamp, time in zip(
                    model.time_block.datetime_index,
                    model.time_block.time_index,
                    strict=True,
                )
            ]

            # --- Acquirium: associate outputs with the same Experiment ------
            operating_cost.record(objective)
            electrical_power.record(power_rows)
            tank_volume.record(volume_rows)
            solver_events.record(
                {
                    "event": "solve-complete",
                    "status": str(results.solver.status),
                    "termination_condition": str(
                        results.solver.termination_condition
                    ),
                }
            )
            experiment.finish()
        except Exception as error:
            experiment.fail(error)
            raise

        return {
            "experiment_id": experiment.experiment_id,
            "scenario": scenario,
            "treatment_flow_m3_per_hour": flow_m3_per_hour,
            "initial_tank_volume_m3": tank_volume_m3,
            "operating_cost_usd": objective,
        }

    return (solve_and_record,)


@app.cell
def _(mo):
    mo.md("""
    ## Try several initial settings

    Each call starts a separate Experiment but reuses the Study's variable
    definitions. The variations are intentionally ordinary function
    arguments: Acquirium does not require a configuration framework around
    the simulation.
    """)
    return


@app.cell
def _(solve_and_record):
    low_throughput = solve_and_record(
        scenario="low throughput / low initial volume",
        flow_m3_per_hour=8.0,
        tank_volume_m3=250.0,
    )
    low_throughput
    return


@app.cell
def _(solve_and_record):
    baseline = solve_and_record(
        scenario="baseline",
        flow_m3_per_hour=10.0,
        tank_volume_m3=500.0,
    )
    baseline
    return


@app.cell
def _(solve_and_record):
    high_throughput = solve_and_record(
        scenario="high throughput / high initial volume",
        flow_m3_per_hour=12.0,
        tank_volume_m3=750.0,
    )
    high_throughput
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Reopen and discover the Study post-hoc

    From here on, pretend this is a new analysis session. We reopen the
    Study by its persistent name. No variable is redeclared: Acquirium
    loads the stored schema, including roles, kinds, units, and plant URIs.
    That is important for analyses written by someone other than the model
    author—or simply written six months later.
    """)
    return


@app.cell
def _(ac):
    recorded_study = ac.study.get("flexpse-watr-model")
    variable_catalog = recorded_study.variables.frame()
    variable_catalog
    return (recorded_study,)


@app.cell
def _(notebook_session, recorded_study):
    current_experiments = recorded_study.experiments.where(
        status="succeeded",
        metadata={"notebook_session": notebook_session},
    )
    experiment_history = current_experiments.frame()
    experiment_history
    return (current_experiments,)


@app.cell
def _(current_experiments, pl, recorded_study):
    # These handles came from persisted declarations, not the recording cells.
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
    # Argmax is ordinary dataframe analysis, not a special Experiment method.
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

        The same table could just as easily be grouped, filtered, joined to
        another dataset, or sorted ascending to find the minimum. Acquirium's
        job here is to provide a trustworthy history, not prescribe the
        analysis.
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
def _(ac, current_experiments, plt, recorded_study):
    stored_power = recorded_study.variables["aggregate electrical power"]
    power_figure, power_axis = plt.subplots()

    for experiment in current_experiments.all():
        observation = experiment.observations(stored_power).latest()

        # Experiment storage tells us which ordinary Acquirium stream and
        # interval belong to the result. The normal time-series interface does
        # the actual read; there is no experiment-specific sample store.
        samples = ac.client.timeseries_df(
            observation.ref_uri,
            start=observation.range_start.isoformat(),
            end=observation.range_end.isoformat(),
        )
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
def _(TANK_VOLUME_URI, pl, recorded_study):
    # A scalar initial condition and a trajectory can describe the same plant
    # property. Their different kinds remain explicit in the Study schema.
    tank_variable_links = pl.DataFrame(
        [
            {
                "variable": variable.label,
                "role": variable.role,
                "kind": variable.kind,
                "plant_property": variable.metadata.get("observed"),
                "unit": variable.metadata.get("unit"),
            }
            for variable in recorded_study.variables
            if variable.metadata.get("observed") == TANK_VOLUME_URI
        ]
    )
    tank_variable_links
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Compare a simulation with synthetic deployment history

    We do not have measurements from a real installation, so the next cell
    creates a deterministic, visibly labeled **synthetic deployment stream**.
    It stands in for rows that would normally arrive through a driver. This is
    deliberately outside the Experiment: it has deployment provenance, not
    simulation provenance.

    Both streams are registered against the model's same `tank-volume`
    property. The graph can therefore tell us they describe the same physical
    quantity even though their source systems, storage references, and
    provenance differ.
    """)
    return


@app.cell
def _(ac, np, recorded_study, result_history):
    baseline_id = result_history.filter(
        result_history["treatment_flow_m3_per_hour"] == 10.0
    )["experiment_id"][0]
    baseline_experiment = recorded_study.experiments.get(baseline_id)
    stored_tank_volume = recorded_study.variables["tank volume"]
    baseline_volume_observation = baseline_experiment.observations(
        stored_tank_volume
    ).latest()

    simulated_volume = ac.client.timeseries_df(
        baseline_volume_observation.ref_uri,
        start=baseline_volume_observation.range_start.isoformat(),
        end=baseline_volume_observation.range_end.isoformat(),
    )

    sample_number = np.arange(simulated_volume.height)
    synthetic_volume = (
        simulated_volume["value"].to_numpy()
        + 22.0 * np.sin(2.0 * np.pi * sample_number / 96.0)
        + 0.015 * sample_number
    )
    synthetic_deployment_rows = list(
        zip(
            simulated_volume["ts"].to_list(),
            synthetic_volume.tolist(),
            strict=True,
        )
    )

    return baseline_volume_observation, simulated_volume, synthetic_deployment_rows


@app.cell
def _(TANK_VOLUME_URI, ac, synthetic_deployment_rows):
    deployment_source = "synthetic-flexpse-deployment"
    deployment_ref_name = "tank-volume-sensor"

    # This is the same registration + insertion path a real driver uses.
    ac.register_streams(
        [
            {
                "source_id": deployment_source,
                "ref_name": deployment_ref_name,
                "point_uri": TANK_VOLUME_URI,
                "label": "tank volume",
                "unit": "http://qudt.org/vocab/unit/M3",
                "value_kind": "numeric",
            }
        ]
    )
    ac.insert_timeseries(
        deployment_source,
        deployment_ref_name,
        synthetic_deployment_rows,
        point_uri=TANK_VOLUME_URI,
        replace=True,
    )
    deployment_ref_uri = str(
        ac.reference_uri(deployment_source, deployment_ref_name)
    )
    return (deployment_ref_uri,)


@app.cell
def _(TANK_URI, ac):
    # Start from the tank in the plant model, then discover measurements
    # attached to it. include_ref_uris=True keeps simulation and deployment
    # streams separate instead of folding them into one property row.
    tank_measurements = (
        ac.query()
        .entity(uri=TANK_URI, alias="tank")
        .measurement(alias="measurement", include_connection_points=False)
    )
    tank_stream_catalog = tank_measurements.data().metadata(
        include_ref_uris=True
    )
    tank_stream_catalog
    return


@app.cell
def _(
    ac,
    baseline_volume_observation,
    deployment_ref_uri,
    pl,
    simulated_volume,
):
    deployed_volume = ac.client.timeseries_df(
        deployment_ref_uri,
        start=baseline_volume_observation.range_start.isoformat(),
        end=baseline_volume_observation.range_end.isoformat(),
    )

    tank_comparison = (
        simulated_volume.select(
            "ts",
            pl.col("value").alias("simulated_volume_m3"),
        )
        .join(
            deployed_volume.select(
                "ts",
                pl.col("value").alias("synthetic_deployment_volume_m3"),
            ),
            on="ts",
        )
        .with_columns(
            (
                pl.col("synthetic_deployment_volume_m3")
                - pl.col("simulated_volume_m3")
            ).alias("residual_m3")
        )
    )

    comparison_summary = tank_comparison.select(
        pl.col("residual_m3").mean().alias("mean_bias_m3"),
        pl.col("residual_m3").abs().mean().alias("mean_absolute_error_m3"),
        pl.col("residual_m3").abs().max().alias("maximum_absolute_error_m3"),
    )
    comparison_summary
    return (tank_comparison,)


@app.cell
def _(plt, tank_comparison):
    # A week is enough to see the invented daily cycle without hiding the
    # comparison in a month of dense points.
    comparison_window = tank_comparison.head(7 * 24 * 4)
    comparison_figure, comparison_axis = plt.subplots()
    comparison_axis.plot(
        comparison_window["ts"],
        comparison_window["simulated_volume_m3"],
        label="FlexPSE simulation",
    )
    comparison_axis.plot(
        comparison_window["ts"],
        comparison_window["synthetic_deployment_volume_m3"],
        label="Synthetic deployment",
        alpha=0.8,
    )
    comparison_axis.set(
        xlabel="Time",
        ylabel="Tank volume (m³)",
        title="Same plant property, different provenance",
    )
    comparison_axis.legend()
    comparison_axis.grid(alpha=0.25)
    comparison_figure.autofmt_xdate()
    comparison_figure
    return


@app.cell
def _(mo):
    mo.md(r"""
    The comparison does not assert that synthetic measurements are part of an
    Experiment. Instead, it composes three existing modules:

    - the **Experiment ledger** selects the baseline run and identifies its
      simulated tank-volume stream;
    - the **plant graph** establishes that both streams describe the tank's
      volume property;
    - the normal **time-series interface** retrieves both sets of samples.

    In a real deployment, a driver would replace the synthetic insertion cell.
    The discovery and comparison cells would remain the same. Scalar Study
    values work the same way: the baseline's `initial tank volume` observation
    links to the tank-volume property, while its numeric value remains scoped
    to that particular Experiment rather than becoming a global fact about the
    plant.
    """)
    return


if __name__ == "__main__":
    app.run()
