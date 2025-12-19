# Pyomo imports
from pyomo.environ import (
    ConcreteModel,
    check_optimal_termination,
    assert_optimal_termination,
    value,
)

# IDAES imports
from idaes.core import FlowsheetBlock
from idaes.core.util.model_statistics import degrees_of_freedom
from idaes.core.util.scaling import calculate_scaling_factors

# WaterTAP imports
from watertap.core.solvers import get_solver
from watertap.property_models.seawater_prop_pack import SeawaterParameterBlock
from watertap.unit_models.pressure_changer import Pump



# Create a Model 
# This is the basis of all waterTap models
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

# Here we can learn about the degrees of freedom of the model
# If we want to optimize we need >0 degrees of freedom
# For a simulation we need 0 degrees of freedom

# print("Degrees of freedom =", degrees_of_freedom(m))

#Here we can check what variables need to be fixed
# m.fs.pump.display()


# This is how we fix variables

# 'control_volume' is the physics block inside the pump.
# 'properties_in[0]' represents the water state at time t=0.
# inlet pressure
m.fs.pump.control_volume.properties_in[0].pressure.fix(101325)  # Pa
# inlet temperature
m.fs.pump.control_volume.properties_in[0].temperature.fix(298.15)  # K
# We fix the Mass Flow of each component individually.
# "Liq" is the Liquid Phase. "H2O" is water, "TDS" is Total Dissolved Solids (Salt).
m.fs.pump.control_volume.properties_in[0].flow_mass_phase_comp["Liq", "H2O"].fix(
    1
)  # kg/s
m.fs.pump.control_volume.properties_in[0].flow_mass_phase_comp["Liq", "TDS"].fix(
    0.1
)  # kg/s
# pump pressure increase
m.fs.pump.deltaP.fix(500_000)  # Pa
# pump efficiency
m.fs.pump.efficiency_pump.fix(0.8)

# For a simulation, we should now have 0 degrees of freedom
# print("Degrees of freedom =", degrees_of_freedom(m))


flow_inputs = [1.0, 1.2, 1.5, 1.2, 1.0]


# scale variables properly to help with solver convergence
calculate_scaling_factors(m)

m.fs.pump.initialize()

solver = get_solver()
results = solver.solve(m)

pump_work = value(m.fs.pump.work_mechanical[0])
print(f"Pump work: {pump_work:.2f} W")