"""
This is the build-and-solve script for the seawater RO flowsheet model.  It is used by the
watertap driver, data generator and the simulator """

from __future__ import annotations
from pyomo.environ import units as pyunits , ConcreteModel
from watertap.flowsheets.seawater_RO_desalination.seawater_RO_desalination import (
    build_flowsheet,
    initialize_system,
    solve as _solve_flowsheet,
)

def change_inputs(m: ConcreteModel, d: dict) -> None:
    """Re-fix the varying feed inputs on the flowsheet."""
    m.fs.feed.flow_vol[0].fix(d["flow_vol"] * pyunits.m**3 / pyunits.s)
    m.fs.feed.conc_mass_comp[0, "tds"].fix(d["conc_tds"] * pyunits.kg / pyunits.m**3)
    m.fs.feed.conc_mass_comp[0, "tss"].fix(d["conc_tss"] * pyunits.kg / pyunits.m**3)
    m.fs.tb_prtrt_desal.properties_out[0].temperature.fix(
        d["temperature"] * pyunits.K
    )

def build() -> ConcreteModel:
    """Build the flowsheet model."""
    # erd_type must be set ("pressure_exchanger" or "pump_as_turbine"); the
    # mapping covers the pressure-exchanger train (S1 / P2 / PXR units).
    m = build_flowsheet(erd_type="pressure_exchanger")
    initialize_system(m)
    return m

def solve(m: ConcreteModel) -> None:
    """Solve the flowsheet model."""
    _solve_flowsheet(m)