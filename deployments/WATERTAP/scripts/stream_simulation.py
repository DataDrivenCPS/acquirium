import numpy as np
import paho.mqtt.client as mqtt
import datetime
import time

BROKER = "mosquitto"
PORT = 1883  # Default MQTT port

time_start = datetime.datetime.strptime("2023-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
measurements = ['pump_inlet_temperature', 'water_flow_mass_rate', "saltwater_flow_mass_rate","pump_inlet_pressure"]

print("Starting stream simulation...")
client1 = mqtt.Client(client_id="pump_inlet_temperature")
client1.connect(BROKER, PORT)
client2 = mqtt.Client(client_id="water_flow_mass_rate")
client2.connect(BROKER, PORT)   
client3 = mqtt.Client(client_id="saltwater_flow_mass_rate")
client3.connect(BROKER, PORT)   
client4 = mqtt.Client(client_id="pump_inlet_pressure")
client4.connect(BROKER, PORT)

def generate_water_temperature(time):
    '''
    Simulate daily water temperature variations using a sine wave pattern.
    Warmer during the day, cooler at night.
    Warmer during summer months, cooler during winter months.

    Deterministic function for reproducibility.

    Input:
    time: datetime object
    Output:
    temperature in Kelvin
    '''
    day_of_year = time.timetuple().tm_yday
    hour_of_day = time.hour + time.minute / 60.0 
    # Seasonal variation: peak at day 200 (mid-July), trough at day 20 (late January)
    seasonal_variation = 10 * np.sin(2 * np.pi * (day_of_year - 200) / 365)
    # Daily variation: peak at 15:00, trough at 3:00
    daily_variation = 5 * np.sin(2 * np.pi * (hour_of_day - 15) / 24)
    base_temp = 288  # Base temperature in Kelvin (15°C)
    temperature = base_temp + seasonal_variation + daily_variation
    return temperature

def generate_water_flow_rate(time):
    '''
    Simulate water flow rate variations based on time of day.
    Higher flow rates during typical usage hours (6 AM - 10 AM, 4 PM - 9 PM),
    Medium flow rates during midday (10 AM - 4 PM, 9 PM - 11 PM),
    Lower flow rates during off-peak hours (10 PM - 6 AM).

    Deterministic function for reproducibility.

    Input:
    time: datetime object
    Output:
    flow rate in kg/s
    '''
    hour_of_day = time.hour + time.minute / 60.0 
    if 6 <= hour_of_day < 10 or 16 <= hour_of_day < 21:
        return 2.0  # High flow rate in kg/s
    elif 10 <= hour_of_day < 16 or 21 <= hour_of_day < 23:
        return 1.0  # Medium flow rate in kg/s
    else:
        return 0.5  # Low flow rate in kg/s 
    
def simulate_stream(time):
    temperature = generate_water_temperature(time)
    flow_rate = generate_water_flow_rate(time)
    saltwater_flow_rate = 0.1 
    pressure = 101325 

    data_temp = {
        "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "Value": float(temperature)
    }
    data_flow = {
        "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "Value": float(flow_rate)   
    }
    data_salt = {
        "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "Value": float(saltwater_flow_rate)
    }
    data_pressure = {
        "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "Value": float(pressure)
    }
    message_temp = str(data_temp)  # Convert dict to string
    message_flow = str(data_flow)  # Convert dict to string
    message_salt = str(data_salt)  # Convert dict to string
    message_pressure = str(data_pressure)  # Convert dict to string 

    client1.publish("pump_inlet_temperature", message_temp)
    client2.publish("water_flow_mass_rate", message_flow)
    client3.publish("saltwater_flow_mass_rate", message_salt)
    client4.publish("pump_inlet_pressure", message_pressure)


current_time = time_start
while True:
    simulate_stream(current_time)
    current_time += datetime.timedelta(minutes=10)
    time.sleep(1)
