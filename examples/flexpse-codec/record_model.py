"""Save a flex-pse model with an Acquirium experiment and build it back.

Run from this directory with:

    uv run python record_model.py
"""
from pathlib import Path

import acquirium as aq
import flexops as fo
import pyomo.environ as pyo
from flexops import logic
from flexops.core.compare import assert_models_equivalent
from flexops.costing import currency_units
from pyomo.environ import units as pyunits
from pyomo.network import Arc

from flexpse_codec import FlexModelCodec
from plant_extensions import end_full

HERE = Path(__file__).resolve().parent


def build_plant():
    """The tank and pump from flex-pse's "Round-trip a model" guide."""
    m = pyo.ConcreteModel(name="site")
    m.time_block = fo.TimeBlock(start_date="2025-01-01", end_date="2025-01-02", time_step=1 * pyunits.hr)
    m.properties = fo.SimpleAqueousFlow()
    m.costing = fo.FlexCosting(
        time_block=m.time_block,
        energy_prices={"electrical": 0.1 * currency_units("USD") / pyunits.kWh},
    )
    m.site = fo.PlantBlock(time_block=m.time_block)
    m.site.tank = fo.Tank(property_package=m.properties)
    m.site.pump = fo.ConstantEnergyIntensityModel(
        property_package=m.properties,
        energy_intensity=0.5 * pyunits.kWh / pyunits.m**3,
        costing_package=m.costing,
    )
    m.site.tank_to_pump = Arc(source=m.site.tank.outlet, destination=m.site.pump.inlet)
    pyo.TransformationFactory("network.expand_arcs").apply_to(m)

    flow = pyunits.m**3 / pyunits.hr
    logic.add_status(m.site.pump, m.site.pump.flow_in, 0 * flow, 10 * flow)
    end_full(m, tank="site.tank", fraction=0.5)
    m.site.pump.status[0].fix(0)

    m.costing.cost_process()
    m.objective = pyo.Objective(expr=m.costing.aggregate_operating_cost)
    return m


# --- Acquirium: describe what each experiment records -----------------------
ac = aq.init(data_dir=HERE / ".data")
study = ac.study.define("flexpse-model-state")
# Object output: the whole model, saved through flex-pse's spec. Nothing
# inside it is searchable; it is kept to be loaded back.
model_state = study.output("model").object(codec=FlexModelCodec)
# Scalar input: one fact about the model worth filtering runs on.
horizon = study.input("horizon steps").scalar()

m = build_plant()
experiment = study.start(metadata={"scenario": "pump off in the first hour"})
try:
    horizon.record(len(m.time_block.time_index))
    saved = model_state.record(m)
    experiment.finish()
except Exception as error:
    experiment.fail(error)
    raise

# --- Acquirium: load the saved model back and check it ----------------------
restored = experiment.observations(model_state).latest().load()
assert_models_equivalent(m, restored)

print(f"experiment: {experiment.run_id}")
print(f"saved {saved['filename']}: {saved['byte_length']} bytes")
print(f"restored model {restored.name!r}; pump status[0] fixed at {restored.site.pump.status[0].value:g}")

aq.shutdown()
