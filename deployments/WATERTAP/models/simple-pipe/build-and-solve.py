
from idaes.core import FlowsheetBlock
from pyomo.environ import (
    ConcreteModel,
)
from watertap.property_models.seawater_prop_pack import SeawaterParameterBlock
from watertap.unit_models.pressure_changer import Pump
from watertap.core.solvers import get_solver

from idaes.core.util.scaling import calculate_scaling_factors


def change_inputs(m, d: dict) -> None:
    """Re-fix the varying feed inputs on the flowsheet."""
    m.fs.pump.control_volume.properties_in[0].pressure.fix(d["pressure"])  # Pa
    m.fs.pump.control_volume.properties_in[0].temperature.fix(d["temperature"])  # K
    m.fs.pump.control_volume.properties_in[0].flow_mass_phase_comp["Liq", "H2O"].fix(
        d["flow_rate"]
    )  # kg/s
    m.fs.pump.control_volume.properties_in[0].flow_mass_phase_comp["Liq", "TDS"].fix(
        d["saltwater_flow_rate"]
    )  # kg/s

    m.fs.pump.deltaP.fix(500_000)  # Pa
    # pump efficiency
    m.fs.pump.efficiency_pump.fix(0.8)

def build() -> ConcreteModel:
    m = ConcreteModel()
    # FlowsheetBlock is the base for all system models
    # Here we connect our unit models and to model the overall process
    # The FlowsheetBlock manages time (if dynamic) and connects units.
    # dynamic=False means this is "Steady State" (a snapshot in time).
    m.fs = FlowsheetBlock(dynamic=False)


    # Add property package
    # Here we assume we will use seawater
    # All units on this flowsheet can now refer to 'm.fs.properties' to know how to calculate density, enthalpy, etc. for Seawater.
    m.fs.properties = SeawaterParameterBlock()


    # Add a pump unit model
    m.fs.pump = Pump(
        property_package=m.fs.properties,
    )

    return m

def solve(m: ConcreteModel) -> None:
    """Solve the flowsheet model."""
    # scale variables properly to help with solver convergence
    calculate_scaling_factors(m)
    # initialize from the (now fixed) inlet state before the full solve
    m.fs.pump.initialize()
    solver = get_solver()
    solver.solve(m)