"""Follow a gap and a late correction through the turbidity alarm chain.

    uv run --locked python -m examples.turbidity.run
"""

import tempfile
from datetime import timedelta
from pathlib import Path

import polars as pl

import acquirium as aq
from examples.bulk_ingest.chain import NTU, SOFT_SENSORS, SOURCE, TURBIDITY

START = aq.to_timestamp("2026-01-01T00:00:00 UTC")
MINUTE = timedelta(minutes=1)


def run(client):
    # Four sampling locations, each with a turbidity point in the plant model.
    model = Path(__file__).resolve().parents[1] / "bulk_ingest" / "model.ttl"
    client.insert_graph_file(model, source_id="turbidity-layout")
    client.register_streams([{
        "source_id": SOURCE,
        "ref_name": f"turbidity-{i}",
        "point_uri": f"urn:bulk/loc-{i}-turbidity",
        "unit": NTU,
        "quantity_kind": TURBIDITY,
        "value_kind": "numeric",
    } for i in range(4)])
    for app in SOFT_SENSORS:
        client.deploy_app(app)

    # Twenty-five minutes of readings, two a minute. Filter 3 (location 2) runs
    # at 0.8 NTU from 00:02 to 00:20, and its readings for 00:10 are missing.
    streams = {}
    for i, normal in enumerate((1.5, 1.0, 0.2, 0.1)):
        readings = []
        for minute in range(25):
            if i == 2 and minute == 10:
                continue
            value = 0.8 if i == 2 and 2 <= minute <= 20 else normal
            t = START + minute * MINUTE
            readings += [(t, value - 0.05), (t + MINUTE / 2, value + 0.05)]
        streams[f"turbidity-{i}"] = readings
    receipt = client.insert_timeseries_batch(SOURCE, streams)["receipts"][0]
    first = client.wait(receipt)["epoch"]

    filter3 = client.query().entity(uri="urn:bulk/loc-2", alias="filter3")
    filled = filter3.measurement(alias="filled", app="bulk-turbidity-fill")
    alarm = filter3.measurement(alias="alarm", app="bulk-turbidity-sustained")
    count = client.query().measurement(alias="count", app="bulk-plant-turbidity-alarms")

    at_gap = filled.dataframe().filter(pl.col("time") == START + 10 * MINUTE)
    alarm_on = alarm.dataframe().filter(pl.col("alarm") == 1)["time"]
    print(f"Filled value at 00:10: {at_gap['filled'][0]:.2f} NTU")
    print(f"Sustained alarm on: {alarm_on.min():%H:%M} to {alarm_on.max():%H:%M}")
    print(f"Locations in sustained alarm, at most: {count.dataframe()['count'].max():.0f}")

    # The missing readings arrive late. They show a brief dip below the limit.
    late = [(START + 10 * MINUTE, 0.15), (START + 10.5 * MINUTE, 0.25)]
    receipt = client.insert_timeseries(SOURCE, "turbidity-2", late)["receipts"][0]
    client.wait(receipt)

    at_gap = filled.dataframe().filter(pl.col("time") == START + 10 * MINUTE)
    print(f"After the late readings, 00:10 is {at_gap['filled'][0]:.2f} NTU")
    print(f"Sustained alarm, at most: {alarm.dataframe()['alarm'].max():.0f}")
    print(f"Locations in sustained alarm, at most: {count.dataframe()['count'].max():.0f}")

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
