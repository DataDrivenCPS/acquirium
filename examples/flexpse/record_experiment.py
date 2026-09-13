"""Solve a FlexPSE model and record the run with Acquirium.

Run from this directory with:

    uv run python record_experiment.py
"""
import os
from pathlib import Path

import acquirium as aq
import pyomo.environ as pyo
from pyomo.opt import assert_optimal_termination

from flexcore.config.io import load_model_config
from flexcore.solvers import get_solver
from flexops import build_model


HERE = Path(__file__).resolve().parent
INPUTS = HERE / "inputs"
TREATMENT_FLOW = 10.0  # m^3/h

ac = aq.init(data_dir=HERE / ".data")
study = ac.study.define("flexpse-api-freeze")

model_inputs = study.input("model input files").file(
    media_type="application/json"
)
treatment_flow = study.input("treatment flow").scalar(unit="M3-PER-HR")
operating_cost = study.output("aggregate operating cost").scalar(unit="USD")
electrical_power = study.output("aggregate electrical power").timeseries(
    observed="urn:flex-pse:api-freeze:aggregate-electrical-power",
    unit="KiloW",
)
solver_events = study.log("solver events")

experiment = study.start(
    metadata={"model": "api-freeze", "scenario": "baseline"}
)
try:
    for path in (
        INPUTS / "model.json",
        INPUTS / "tariff.json",
        INPUTS / "dr_events.json",
    ):
        model_inputs.record(path)
    treatment_flow.record(TREATMENT_FLOW)

    # The model config refers to tariff.json and dr_events.json by bare
    # filename, matching the upstream api_freeze fixture.
    os.chdir(INPUTS)
    model = build_model(load_model_config(INPUTS / "model.json"))

    # The frozen fixture leaves operating conditions free. Add one recorded
    # condition so this example has a nonzero load: equal tank inflow and
    # treatment throughput preserve inventory.
    for t in model.time_block.time_index:
        model.waterfacility.tank.flow_in[t].fix(TREATMENT_FLOW)
        model.waterfacility.plant.flow_out[t].fix(TREATMENT_FLOW)
    solver_events.record({"event": "model-built"})

    pyo.TransformationFactory("network.expand_arcs").apply_to(model)
    results = get_solver(model=model, prefer="highs").solve(model)
    assert_optimal_termination(results)

    objective = float(pyo.value(model.objective))
    operating_cost.record(objective)

    power_rows = [
        (
            timestamp,
            pyo.value(
                pyo.units.convert(
                    model.costing.aggregate_electrical_power[t],
                    to_units=pyo.units.kW,
                )
            ),
        )
        for timestamp, t in zip(
            model.time_block.datetime_index,
            model.time_block.time_index,
            strict=True,
        )
    ]
    electrical_power.record(power_rows)

    solver_events.record(
        {
            "event": "solve-complete",
            "status": str(results.solver.status),
            "termination_condition": str(results.solver.termination_condition),
        }
    )
    experiment.finish()
except Exception as error:
    experiment.fail(error)
    raise

power_ref = ac.reference_uri(
    f"experiment/{experiment.run_id}",
    electrical_power.label,
)
stored_power = ac.client.timeseries_df(str(power_ref))

print(f"experiment: {experiment.run_id}")
print(f"aggregate operating cost: {objective:.2f} USD")
print(f"electrical power: {power_ref}")
print(stored_power.head())

aq.shutdown()
