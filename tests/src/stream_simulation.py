'''
A stream simulation script that publishes synthetic data to MQTT topics to mimic real-time data streams.
This script simulates 4 values:
- Topic1: A Fixed Value
- Topic2: 10 - 20 - 30, cycling
- Topic3: A sinusoidal value
- Topic4: A deterministic value based on timestamp
'''


import numpy as np
import paho.mqtt.client as mqtt
import datetime
import time
import numpy as np
import math

BROKER = "mosquitto"
PORT = 1883  # Default MQTT port

time_start = datetime.datetime.strptime("2003-01-01 00:00:00", '%Y-%m-%d %H:%M:%S')
measurements = ["topic1", "topic2", "topic3", "topic4"]

print("Starting stream simulation...")
client1 = mqtt.Client(client_id="topic1")
client1.connect(BROKER, PORT)
client2 = mqtt.Client(client_id="topic2")
client2.connect(BROKER, PORT)
client3 = mqtt.Client(client_id="topic3")
client3.connect(BROKER, PORT)
client4 = mqtt.Client(client_id="topic4")
client4.connect(BROKER, PORT)

def timestamp_to_float(ts: datetime) -> float:
    """
    Deterministically transform a timestamp into a float.

    Properties:
    - Same timestamp always yields the same value
    - Smooth and continuous over time
    - Bounded output in a known range
    """

    # Convert timestamp to seconds since epoch
    t = ts.timestamp()

    # Combine multiple frequencies to avoid trivial patterns
    value = (
        math.sin(t / 60.0) * 0.5 +      # slow variation (minute scale)
        math.cos(t / 10.0) * 0.3 +      # faster variation
        math.sin(t / 3600.0) * 0.2      # very slow drift (hour scale)
    )

    return value


def simulate_stream(time):
    topic1_value = 42  # Fixed value
    topic2_value = [10, 20, 30][(time.minute // 10) % 3]  # Cycles through 10, 20, 30
    topic3_value = 50 + 10 * math.sin(2 * math.pi *(time.hour * 60 + time.minute) / 1440)  # Sinusoidal between 40 and 60
    topic4_value = timestamp_to_float(time)  # Deterministic function of timestamp

    data_1 = {"Timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "Value": topic1_value}
    data_2 = {"Timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "Value": topic2_value}
    data_3 = {"Timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "Value": topic3_value}
    data_4 = {"Timestamp": time.strftime('%Y-%m-%d %H:%M:%S'), "Value": topic4_value}
    
    client1.publish("topic1", str(data_1))
    client2.publish("topic2", str(data_2))      
    client3.publish("topic3", str(data_3))
    client4.publish("topic4", str(data_4))

current_time = time_start
while True:
    simulate_stream(current_time)
    current_time += datetime.timedelta(minutes=10)
    time.sleep(1)
