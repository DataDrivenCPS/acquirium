"""Run a finite membrane-monitoring demonstration through a local Acquirium server.

uv run --locked python -m examples.membrane_monitoring.run
"""

import argparse
import math
import tempfile
from pathlib import Path
from uuid import uuid4

import acquirium as aq

from .declarations import (
    DRIFT,
    HOUR,
    MINUTE,
    PERMEABILITY,
    SKIDS,
    SMOOTHED,
    SOURCE,
    deploy,
    register,
)


def readings(*, hours=12):
    """Simulated readings every five minutes, by stream.

    Skid 1 fouls: its permeability falls from 3.0 to 2.45 L/m2/h/bar over
    eleven hours, and its permeate conductivity rises from 08:00. Skid 2
    holds 2.5 L/m2/h/bar and 60 uS/cm."""
    streams = {}
    for step in range(hours * 12):
        time = step * 5 * MINUTE
        hour = time / HOUR
        tmp = 2.0 + 0.2 * math.sin(step / 5)
        noise = 0.5 * math.sin(step * 1.7)
        for skid, permeability, conductivity in (
            ("skid-1", 3.0 - 0.05 * hour, 50.0 + (8.0 if hour >= 8 else 0.0)),
            ("skid-2", 2.5, 60.0),
        ):
            streams.setdefault(f"{skid}-tmp", {})[time] = tmp
            streams.setdefault(f"{skid}-flux", {})[time] = permeability * tmp
            streams.setdefault(f"{skid}-conductivity", {})[time] = conductivity + noise
    return streams


def show(client, label):
    epoch = client.engine_snapshot()["epoch"]
    print(f"\n{label} (published epoch {epoch})")
    for app, port in (
        (SMOOTHED, "smoothed"),
        (DRIFT, "alarm"),
        (PERMEABILITY, "permeability"),
    ):
        for skid in SKIDS:
            reference = aq.apps.output_reference(app, skid, port)
            data = client.client.timeseries_df(reference, epoch=epoch)
            hourly = [round(v, 2) for t, v in zip(data["ts"], data["value"]) if t.minute == 0]
            short = skid.rsplit(":", 1)[1]
            print(f"  {app}/{port} {short}: {len(data)} rows; on the hour {hourly}")


def run(client):
    register(client)
    deploy(client)
    session = uuid4().hex

    def submit(label, streams):
        response = client.insert_timeseries_batch(SOURCE, streams, request_id=f"{session}:{label}")
        client.wait(response["receipts"][0])
        show(client, label)

    submit("initial", {stream: list(rows.items()) for stream, rows in readings().items()})
    # A late spike: every later smoothed value changes, and the drift statistic
    # until it returns to zero.
    spike = 2 * HOUR + 150_000_000_000
    submit("late-spike", {"skid-2-conductivity": [(spike, 120.0)]})
    client.wait(
        client.delete_timeseries(
            SOURCE, "skid-2-conductivity", [spike], request_id=f"{session}:drop"
        )
    )
    show(client, "spike-deleted")
    # A late flux reading changes its hour's permeability and every later one.
    submit("late-flux", {"skid-1-flux": [(5 * HOUR + MINUTE, 3.0)]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, help="retain this demonstration's history")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="acquirium-membrane-") as temporary:
        client = aq.init(data_dir=args.data_dir or temporary, exact_only=True, timeout=180)
        try:
            run(client)
        finally:
            aq.shutdown()


if __name__ == "__main__":
    main()
