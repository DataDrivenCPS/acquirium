"""
This is the build-and-solve script for the fouled-membrane variant of the seawater RO
flowsheet model. It is identical to the ``seawater-ro`` model except ``change_inputs``
also accepts an ``A_comp`` key (membrane water permeability [m/s-Pa]) so a caller can
re-fix the RO membrane's permeability at every re-solve, driving it down a fouling
trajectory (see ``generate-values.py``). It is used by the watertap driver, data
generator and the simulator."""

from __future__ import annotations
from pathlib import Path
import sys

from pyomo.environ import units as pyunits, ConcreteModel

# Use the vendored flowsheet (a local copy of WaterTAP's seawater_RO_desalination
# extended with a TOC constituent through pretreatment).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flowsheet import (  # noqa: E402
    build_flowsheet,
    initialize_system,
    solve as _solve_flowsheet,
)

def change_inputs(m: ConcreteModel, d: dict) -> None:
    """Re-fix the varying feed inputs on the flowsheet."""
    m.fs.feed.flow_vol[0].fix(d["flow_vol"] * pyunits.m**3 / pyunits.s)
    m.fs.feed.conc_mass_comp[0, "tds"].fix(d["conc_tds"] * pyunits.kg / pyunits.m**3)
    m.fs.feed.conc_mass_comp[0, "tss"].fix(d["conc_tss"] * pyunits.kg / pyunits.m**3)
    if "conc_toc" in d:
        m.fs.feed.conc_mass_comp[0, "nonvolatile_toc"].fix(
            d["conc_toc"] * pyunits.kg / pyunits.m**3
        )
    m.fs.tb_prtrt_desal.properties_out[0].temperature.fix(
        d["temperature"] * pyunits.K
    )
    if "A_comp" in d:
        # Membrane water permeability [m/s-Pa]; declines over time as fouling
        # (cake layer / biofilm) adds resistance to water transport.
        m.fs.desalination.RO.A_comp.fix(d["A_comp"])

def build() -> ConcreteModel:
    """Build the flowsheet model."""
    # erd_type must be set ("pressure_exchanger" or "pump_as_turbine"); the
    # mapping covers the pressure-exchanger train (S1 / P2 / PXR units).
    m = build_flowsheet(erd_type="pressure_exchanger")
    # Touch the on-demand brine TDS concentration so the var and its constraint
    # exist before solve and the mapping can read it (Ocean Plan III.M.3).
    m.fs.disposal.properties[0].conc_mass_phase_comp
    initialize_system(m)
    return m

def solve(m: ConcreteModel) -> None:
    """Solve the flowsheet model."""
    _solve_flowsheet(m)