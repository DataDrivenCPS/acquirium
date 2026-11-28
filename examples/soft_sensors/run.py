"""Run a finite soft-sensor demonstration through a real local Acquirium server.

uv run --locked python -m examples.soft_sensors.run
"""

import argparse
import tempfile
from pathlib import Path
from uuid import uuid4

import acquirium as aq

from .declarations import ALARM, HOTTEST, MEAN, SOURCE, deploy, register


def show(client, label):
    epoch = client.engine_snapshot()["epoch"]
    print(f"\n{label} (published epoch {epoch})")
    for name in (MEAN, HOTTEST, ALARM):
        for operator in client.inspect_app(name)["app"]["operators"]:
            for port, reference in operator["outputs"].items():
                data = client.client.timeseries_df(reference, epoch=epoch)
                print(f"  {name}/{port}: {data.to_dicts()}")


def run(client):
    register(client)
    deploy(client)
    session = uuid4().hex

    def submit(label, streams):
        response = client.insert_timeseries_batch(
            SOURCE, streams, request_id=f"{session}:{label}"
        )
        client.wait(response["receipts"][0])
        show(client, label)

    submit(
        "initial",
        {
            "a1": [(1_000_000_000, 20)],
            "a2": [(2_000_000_000, 40)],
            "b1": [(3_000_000_000, 24)],
        },
    )
    submit("late-reading", {"a1": [(500_000_000, 26)]})
    submit("correction-clears-alarm", {"a2": [(2_000_000_000, 20)]})
    client.sparql_update(
        """
        DELETE DATA { <urn:soft-sensors:tank-a>
            <http://data.ashrae.org/standard223#hasProperty> <urn:soft-sensors:probe-a2> };
        INSERT DATA { <urn:soft-sensors:tank-b>
            <http://data.ashrae.org/standard223#hasProperty> <urn:soft-sensors:probe-a2> }
        """,
        source_id="soft-sensor-layout",
    )
    show(client, "metadata-moves-probe")
    response = client.delete_timeseries(
        SOURCE, "b1", [3_000_000_000], request_id=f"{session}:delete"
    )
    client.wait(response)
    show(client, "delete-reading")
    data = client.query().measurement(app=ALARM).dataframe(shape="narrow")
    print(f"\nAlarm semantic query: {data.to_dicts()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, help="retain this demonstration's history"
    )
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="acquirium-soft-sensors-") as temporary:
        client = aq.init(
            data_dir=args.data_dir or temporary, exact_only=True, timeout=180
        )
        try:
            run(client)
        finally:
            aq.shutdown()


if __name__ == "__main__":
    main()
