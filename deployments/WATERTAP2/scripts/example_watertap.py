
from pyomo.environ import (
    ConcreteModel,
    value,
    Objective,
    Param,
)
from pyomo.network import Arc
from idaes.core import FlowsheetBlock
from watertap.core.solvers import get_solver
from idaes.core.util.initialization import  propagate_state
from idaes.models.unit_models.mixer import MomentumMixingType
import idaes.core.util.scaling as iscale
from idaes.core.util.model_statistics import degrees_of_freedom
from pyomo.environ import TransformationFactory


from watertap.unit_models.pressure_changer import Pump
import watertap.property_models.NaCl_prop_pack as props
from watertap.unit_models.reverse_osmosis_0D import (
    ReverseOsmosis0D,
    ConcentrationPolarizationType,
    MassTransferCoefficient,
    PressureChangeType,
)


def build_and_solve(
    flow_vol = 1e-3,
    salt_mass_conc = 35e-3,
    operating_pressure = 5e6,
    flow_mass_liq = 985e-3,
    flow_mass_salt = 15e-3,
):
    # flowsheet set up
    m = ConcreteModel()
    m.fs = FlowsheetBlock(dynamic=False)
    m.fs.properties = props.NaClParameterBlock()
    m.fs.P1 = Pump(property_package=m.fs.properties)
    m.fs.RO = ReverseOsmosis0D(
        property_package=m.fs.properties,
        has_pressure_change=False,
        pressure_change_type=PressureChangeType.fixed_per_stage,
        mass_transfer_coefficient=MassTransferCoefficient.calculated,
        concentration_polarization_type=ConcentrationPolarizationType.calculated,
    )
    m.fs.P2 = Pump(property_package=m.fs.properties)
    m.fs.s01 = Arc(source=m.fs.P1.outlet, destination=m.fs.RO.inlet)
    m.fs.s02 = Arc(source=m.fs.RO.permeate, destination=m.fs.P2.inlet)
    TransformationFactory("network.expand_arcs").apply_to(m)

    # set unit model values
    iscale.set_scaling_factor(m.fs.P1.control_volume.work, 1e-3)
    iscale.set_scaling_factor(m.fs.RO.area, 1e-2)
    iscale.set_scaling_factor(m.fs.P2.control_volume.work, 1e-3)

    

    m.fs.properties.set_default_scaling(
        "flow_mass_phase_comp", 1000 * flow_vol, index=("Liq", "H2O")
    )
    m.fs.properties.set_default_scaling(
        "flow_mass_phase_comp", 1e-3 / flow_vol / salt_mass_conc, index=("Liq", "NaCl")
    )
    iscale.set_scaling_factor(
        m.fs.P1.control_volume.properties_out[0].flow_vol_phase["Liq"], 1
    )
    iscale.set_scaling_factor(m.fs.P1.work_fluid[0], 1)
    iscale.set_scaling_factor(m.fs.RO.mass_transfer_phase_comp[0, "Liq", "NaCl"], 1e4)
    iscale.set_scaling_factor(
        m.fs.RO.feed_side.mass_transfer_term[0, "Liq", "NaCl"], 1e4
    )

    iscale.calculate_scaling_factors(m)

    # pump 1, high pressure pump, 2 degrees of freedom (efficiency and outlet pressure)
    m.fs.P1.efficiency_pump.fix(0.80)  # pump efficiency [-]
      # Pa
    # m.fs.P1.control_volume.properties_out[0].pressure.fix(operating_pressure)

    # RO unit
    m.fs.RO.A_comp.fix(4.2e-12)  # membrane water permeability coefficient [m/s-Pa]
    m.fs.RO.B_comp.fix(3.5e-8)  # membrane salt permeability coefficient [m/s]
    m.fs.RO.feed_side.channel_height.fix(1e-3)  # channel height in membrane stage [m]
    m.fs.RO.feed_side.spacer_porosity.fix(0.85)  # spacer porosity in membrane stage [-]
    m.fs.RO.permeate.pressure[0].fix(101325)  # atmospheric pressure [Pa]
    m.fs.RO.width.fix(5)  # stage width [m]

    # initialize RO
    m.fs.RO.feed_side.properties_in[0].flow_mass_phase_comp["Liq", "H2O"].fix(flow_mass_liq)
    m.fs.RO.feed_side.properties_in[0].flow_mass_phase_comp["Liq", "NaCl"].fix(flow_mass_salt)
    m.fs.RO.feed_side.properties_in[0].temperature.fix(298.15)  # K
    m.fs.RO.feed_side.properties_in[0].pressure.fix(operating_pressure)  # Pa
    # m.fs.RO.feed_side.properties_out[0].pressure = 101325  # Pa

    m.fs.RO.area.fix(50)  # guess area for RO initialization
    m.fs.P2.efficiency_pump.fix(0.80)
    
    m.fs.RO.recovery_mass_phase_comp[0, "Liq", "H2O"].unfix()
    target_recovery = m.fs.RO.recovery_mass_phase_comp[0, "Liq", "H2O"].value
    m.fs.RO.area_objective = Objective(
        expr=(m.fs.RO.recovery_mass_phase_comp[0, "Liq", "H2O"] - target_recovery)
        ** 2
    )
    # m.fs.RO.feed_side.properties_in[0].flow_vol_phase.fix(1)
    # m.fs.RO.feed_side.properties_in[0].mass_frac_phase_comp["Liq", "H2O"].fix(1-salt_mass_conc)



    solver = get_solver()
    optarg = solver.options
    # m.display(filename="before_init.txt")
    # print("Degrees of freedom before initialization: ", degrees_of_freedom(m))
    m.fs.RO.initialize(
            optarg=optarg
        )
    m.fs.RO.recovery_mass_phase_comp[0, "Liq", "H2O"].fix(target_recovery)
    
    m.fs.RO.del_component(m.fs.RO.area_objective)
    m.fs.P1.initialize(optarg=optarg)
    m.fs.P2.control_volume.properties_out[0].pressure.fix(
        value(m.fs.P2.control_volume.properties_out[0].pressure)
    )
    m.fs.P2.initialize(optarg=optarg)
    m.fs.P2.control_volume.properties_out[0].pressure.unfix()
    propagate_state(m.fs.s01)
    propagate_state(m.fs.s02)
    # print("Degrees of freedom after initialization: ", degrees_of_freedom(m))
    results = solver.solve(m, tee=False)
    results = None
    return m, results


if __name__ == "__main__":
    m, results = build_and_solve()