"""Follow faults, spikes, a gap, and a late correction through the turbidity alarm.

    uv run --locked python -m examples.turbidity.run
"""

import tempfile
from datetime import timedelta
from pathlib import Path

import polars as pl

import acquirium as aq
from examples.turbidity.apps import NTU, SOURCE, TURBIDITY, declarations

START = aq.to_timestamp("2026-01-01T00:00:00 UTC")
MINUTE = timedelta(minutes=1)
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


def run(client):
    client.insert_graph_file(Path(__file__).with_name("model.ttl"), source_id="turbidity-plant")
    client.register_datasource(SOURCE)
    client.register_streams([{
        "source_id": SOURCE,
        "ref_name": location,
        "point_uri": f"urn:turbidity:{location}-turbidity",
        "unit": NTU,
        "quantity_kind": TURBIDITY,
        "value_kind": "numeric",
        "data_source": SOURCE,
    } for location in LOCATIONS])
    for app in declarations(client):
        client.deploy_app(app)

    receipt = client.insert_timeseries_batch(SOURCE, {
        location: readings(location, normal) for location, normal in LOCATIONS.items()
    })["receipts"][0]
    first = client.wait(receipt)["epoch"]

    filter3 = client.query().entity(uri="urn:turbidity:filter-3", alias="filter3")
    # A normal read returns the clean stream; view="raw" returns the analyser's samples.
    clean = client.query().measurement(alias="clean", uri="urn:turbidity:filter-3-turbidity")
    raw = client.query().measurement(alias="raw", uri="urn:turbidity:filter-3-turbidity")
    alarm = filter3.measurement(alias="alarm", app="turbidity-alarm")

    def show(label):
        table = (
            raw.dataframe(value_mode="coalesce", view="raw")
            .group_by(pl.col("time").dt.truncate("1m"))
            .agg(pl.col("raw").sort_by("time").str.join(", "))
            .join(clean.dataframe(), on="time", how="full", coalesce=True)
            .join(alarm.dataframe(), on="time", how="left")
            .sort("time")
        )
        print(label)
        with pl.Config(tbl_rows=-1, fmt_str_lengths=40):
            print(table)

    show("Filter 3")
    alarm_on = alarm.dataframe().filter(pl.col("alarm") == 1)["time"]
    print(f"Sustained alarm on: {alarm_on.min():%H:%M} to {alarm_on.max():%H:%M}")

    # The missing readings arrive late. They show a brief dip below the limit.
    late = [(START + 10 * MINUTE, 0.15), (START + 10.5 * MINUTE, 0.25)]
    receipt = client.insert_timeseries(SOURCE, "filter-3", late)["receipts"][0]
    client.wait(receipt)
    show("Filter 3 after the late readings")
    print(f"Sustained alarm, at most: {alarm.dataframe()['alarm'].max():.0f}")

    # The snapshot taken before the correction still holds the original alarm.
    stream = alarm.data().stream_info()["ref_uri"][0]
    earlier = client.stream(stream).dataframe(epoch=first)
    print(f"At the first snapshot, the alarm was on for {earlier['value'].sum():.0f} minutes")


def main():
    with tempfile.TemporaryDirectory(prefix="acquirium-turbidity-") as directory:
        client = aq.init(data_dir=directory, exact_only=True, timeout=180)
        try:
            run(client)
        finally:
            aq.shutdown()


if __name__ == "__main__":
    main()
