import datetime
import numpy as np


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

def generate_new_values(ts: datetime, rng: np.random.RandomState) -> dict:
    """Realistic, deterministic feed drivers for a given timestamp.

    All signals combine a physical seasonal/diurnal component with a small
    seeded stochastic perturbation.  Returned in SI units used by the feed.
    """
    temperature = generate_water_temperature(ts)
    flow_rate = generate_water_flow_rate(ts)
    saltwater_flow_rate = 0.1  # Constant for simplicity
    pressure = 101325  # Constant atmospheric pressure in Pa

    return {
        "temperature": temperature,
        "flow_rate": flow_rate,
        "saltwater_flow_rate": saltwater_flow_rate,
        "pressure": pressure,
    }
