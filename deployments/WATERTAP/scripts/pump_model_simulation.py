# Pyomo imports
from pyomo.environ import (
    ConcreteModel,
    # check_optimal_termination,
    # assert_optimal_termination,
    value,
)

# IDAES imports
from idaes.core import FlowsheetBlock
# from idaes.core.util.model_statistics import degrees_of_freedom
from idaes.core.util.scaling import calculate_scaling_factors

# WaterTAP imports
from watertap.core.solvers import get_solver
from watertap.property_models.seawater_prop_pack import SeawaterParameterBlock
from watertap.unit_models.pressure_changer import Pump

# Stream imports for simulation
import numpy as np
import pandas as pd
import paho.mqtt.client as mqtt
BROKER = "mosquitto"
PORT = 1883  # Default MQTT port

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

TOPICS = [
    ("pump_inlet_temperature", 0),
    ("water_flow_mass_rate", 0),
    ("saltwater_flow_mass_rate", 0),
    ("pump_inlet_pressure", 0),
]

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(TOPICS)


new_values = pd.DataFrame(columns=["Time", "pump_inlet_temperature", "water_flow_mass_rate", "saltwater_flow_mass_rate", "pump_inlet_pressure"])

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    # Store the new value
    try:
        data = eval(payload)  # Convert string back to dict
        timestamp = data["Timestamp"]
        value = data["Value"]
        global new_values
        if timestamp not in new_values["Time"].values:
            new_row = {
                "Time": timestamp,
                topic: value
            }
            new_values = pd.concat([new_values, pd.DataFrame([new_row])], ignore_index=True)
        else:
            new_values.loc[new_values["Time"] == timestamp, topic] = value
        simulate()
        
    except Exception as e:
        print(f"Error processing message: {e}")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)

client2 = mqtt.Client(client_id="pump_work")
client2.connect(BROKER, PORT)

def find_complete_row():
    #check if we have all required inputs
    required_columns = ["pump_inlet_temperature", "water_flow_mass_rate", "saltwater_flow_mass_rate", "pump_inlet_pressure"]
    global new_values
    for index, row in new_values.iterrows():
        if all(col in row and pd.notna(row[col]) for col in required_columns):
            #remove this row from new_values and return it
            new_values = new_values.drop(index)
            return row


def simulate():
    complete_row = find_complete_row()
    if complete_row is None:
        return
    # 'control_volume' is the physics block inside the pump.
    # 'properties_in[0]' represents the water state at time t=0.
    # inlet pressure
    m.fs.pump.control_volume.properties_in[0].pressure.fix(complete_row["pump_inlet_pressure"])  # Pa
    # inlet temperature
    m.fs.pump.control_volume.properties_in[0].temperature.fix(complete_row["pump_inlet_temperature"])  # K
    # We fix the Mass Flow of each component individually.
    # "Liq" is the Liquid Phase. "H2O" is water, "TDS" is Total Dissolved Solids (Salt).
    m.fs.pump.control_volume.properties_in[0].flow_mass_phase_comp["Liq", "H2O"].fix(
        complete_row["water_flow_mass_rate"]
    )  # kg/s
    m.fs.pump.control_volume.properties_in[0].flow_mass_phase_comp["Liq", "TDS"].fix(
        complete_row["saltwater_flow_mass_rate"]
    )  # kg/s
    # pump pressure increase
    m.fs.pump.deltaP.fix(500_000)  # Pa
    # pump efficiency
    m.fs.pump.efficiency_pump.fix(0.8)

    # For a simulation, we should now have 0 degrees of freedom
    # print("Degrees of freedom =", degrees_of_freedom(m))


    # scale variables properly to help with solver convergence
    calculate_scaling_factors(m)

    m.fs.pump.initialize()

    solver = get_solver()
    results = solver.solve(m)

    pump_work = value(m.fs.pump.work_mechanical[0])
    
    data_work = {
        "Timestamp": complete_row["Time"],
        "Value": float(pump_work)
    }   
    message_work = str(data_work)  # Convert dict to string
    client2.publish("pump_work", message_work)

client.loop_forever()
