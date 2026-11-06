"""Follow a gap and a correction through a turbidity alarm chain.

    uv run --locked python -m examples.turbidity.run
"""

import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import acquirium as aq
from acquirium.apps import output_reference

from examples.bulk_ingest.chain import (
    NTU, SOURCE, TURBIDITY, SOFT_SENSORS, fill, plant_alarms, sustained,
)

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
FILTER = "urn:bulk/loc-2"


def at(minute, second=0):
    return START + timedelta(minutes=minute, seconds=second)


def read(client, app, group, port, *, epoch):
    reference = output_reference(app.name, group, port)
    return client.stream(reference).dataframe(epoch=epoch).sort("time")


def run(client):
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
        state = client.deploy_app(app)["app"]
        if state["status"] != "active" or state["error"] or state["warnings"]:
            raise RuntimeError(f"{app.name}: {state}")

    # Four locations, two readings per minute. Only Filter 3 goes high.
    streams = {}
    for i, baseline in enumerate((1.5, 1.0, 0.2, 0.1)):
        readings = []
        for minute in range(25):
            if i == 2 and minute == 10:
                continue
            value = 0.8 if i == 2 and 2 <= minute <= 20 else baseline
            readings.extend((at(minute, second), value + offset)
                            for second, offset in ((0, -0.05), (30, 0.05)))
        streams[f"turbidity-{i}"] = readings

    response = client.insert_timeseries_batch(SOURCE, streams)
    original = client.wait(response["receipts"][0])["epoch"]
    filled = read(client, fill, FILTER, "turbidity", epoch=original)
    gap = filled.filter(filled["time"] == at(10)).row(0, named=True)
    assert abs(gap["value"] - 0.8) < 1e-9, gap
    print("Filter 3 has no readings at 00:10. Interpolation fills 0.80 NTU.")

    alarms = read(client, sustained, FILTER, "alarm", epoch=original)
    raised = alarms.filter(alarms["value"] == 1)["time"].to_list()
    assert raised == [at(minute) for minute in range(17, 21)], raised
    print("It stays above 0.5 NTU for 15 minutes. The alarm is on at 00:17–00:20.")
    counts = read(client, plant_alarms, plant_alarms.name, "locations", epoch=original)
    assert counts.filter(counts["time"] == at(17))["value"].to_list() == [1.0]
    print("The plant count reports one location in sustained alarm.")

    # The missing readings arrive. They show a brief return below the limit.
    response = client.insert_timeseries(
        SOURCE, "turbidity-2", [(at(10), 0.15), (at(10, 30), 0.25)]
    )
    corrected = client.wait(response["receipts"][0])["epoch"]
    filled = read(client, fill, FILTER, "turbidity", epoch=corrected)
    gap = filled.filter(filled["time"] == at(10)).row(0, named=True)
    assert abs(gap["value"] - 0.2) < 1e-9, gap
    print("Late readings replace the estimate at 00:10 with 0.20 NTU.")

    alarms = read(client, sustained, FILTER, "alarm", epoch=corrected)
    counts = read(client, plant_alarms, plant_alarms.name, "locations", epoch=corrected)
    assert alarms["value"].max() == counts["value"].max() == 0
    print("That break clears the sustained alarm and the plant count for the whole run.")

    earlier = read(client, sustained, FILTER, "alarm", epoch=original)
    assert earlier.filter(earlier["value"] == 1)["time"].to_list() == raised
    print("Reading the earlier snapshot still shows the original alarm.")


def main():
    with tempfile.TemporaryDirectory(prefix="acquirium-turbidity-") as directory:
        client = aq.init(data_dir=directory, exact_only=True, timeout=180)
        try:
            run(client)
        finally:
            aq.shutdown()


if __name__ == "__main__":
    main()
