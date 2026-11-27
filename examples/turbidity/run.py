"""Follow faults, spikes, a gap, and a late correction through the turbidity alarm.

    uv run --locked python -m examples.turbidity.run
"""

import tempfile
from pathlib import Path

import pandas as pd

import acquirium as aq
from examples.turbidity.apps import declarations

SOURCE = "turbidity-analysers"
TURBIDITY = "http://qudt.org/vocab/quantitykind/Turbidity"
START = pd.Timestamp("2026-01-01T00:00:00Z")
MINUTE = pd.Timedelta("1m")
LOCATIONS = {"filter-1": 0.15, "filter-2": 0.12, "filter-3": 0.2, "combined": 0.1}


def readings(location, normal):
    """Twenty-five minutes of readings, two a minute."""
    rows = []
    for minute in range(25):
        t = START + minute * MINUTE
        if location != "filter-3":
            rows += [(t, round(normal - 0.05, 2)), (t + MINUTE / 2, round(normal + 0.05, 2))]
            continue
        if minute == 10:
            continue  # Filter 3's readings for 00:10 are missing.
        level = 0.8 if 2 <= minute <= 20 else normal
        second = round(level + 0.05, 2)
        if minute == 5:
            second = "Fault"  # The analyser reports a fault instead of a reading.
        if minute == 22:
            second = 9.9  # A bubble passes the sensor.
        rows += [(t, round(level - 0.05, 2)), (t + MINUTE / 2, second)]
    return rows


def register(client):
    client.insert_graph_file(Path(__file__).with_name("model.ttl"), source_id="turbidity-plant")
    client.register_streams([{
        "source_id": SOURCE,
        "stream_name": location,
        "property_uri": f"urn:turbidity:{location}-turbidity",
        "unit": "http://qudt.org/vocab/unit/NTU",
        "quantity_kind": TURBIDITY,
        "value_kind": "numeric",
        "data_source": SOURCE,
    } for location in LOCATIONS])


def run(client):
    register(client)
    clean_app, alarm_app = declarations(client)
    client.deploy_app(clean_app)
    client.deploy_app(alarm_app)
    receipt = client.insert_timeseries_batch(SOURCE, {
        location: readings(location, normal) for location, normal in LOCATIONS.items()
    })["receipts"][0]
    first_epoch = client.wait(receipt)["epoch"]

    # Start alarm read
    alarm_uri = (
        client.query()
        .entity(uri="urn:turbidity:filter-3")
        .measurement(app=alarm_app.name)
        .data()
        .stream_info()["stream_uri"]
        .item()
    )
    alarm = client.stream(uri=alarm_uri)
    print(f"Alarm minutes before the late readings: {alarm.dataframe()['value'].sum():.0f}")
    # End alarm read

    # Start correction
    # The missing readings show a brief dip below the alarm limit.
    missing_minute = START + 10 * MINUTE
    receipt = client.insert_timeseries(SOURCE, "filter-3", [
        (missing_minute, 0.15),
        (missing_minute + MINUTE / 2, 0.25),
    ])["receipts"][0]
    client.wait(receipt)

    print(f"Alarm minutes after the late readings: {alarm.dataframe()['value'].sum():.0f}")
    earlier = alarm.dataframe(epoch=first_epoch)
    print(f"Alarm minutes in the original snapshot: {earlier['value'].sum():.0f}")
    # End correction


def main():
    with tempfile.TemporaryDirectory(prefix="acquirium-turbidity-") as directory:
        client = aq.init(data_dir=directory, exact_only=True, timeout=180)
        try:
            run(client)
        finally:
            aq.shutdown()


if __name__ == "__main__":
    main()
