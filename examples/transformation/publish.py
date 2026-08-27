"""Publish Celsius samples and show the Fahrenheit values derived by the server.

    uv run python examples/transformation/publish.py
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from time import monotonic, sleep

import acquirium as aq

from temperature_conversion import INPUT_SOURCE, OUTPUT_POINT


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--timeout", type=float, default=10.0, help="seconds to wait for materialized output")
    args = parser.parse_args()

    client = aq.Acquirium(server_url=args.host, server_port=args.port)
    source_id, ref_name = "temperature-example", "celsius"
    client.register_datasource(source_id)
    client.register_streams([{
        "source_id": source_id,
        "ref_name": ref_name,
        "point_uri": "urn:example:temperature:celsius",
        "label": "Example temperature in Celsius",
        "unit": "http://qudt.org/vocab/unit/DEG_C",
        "value_kind": "numeric",
        "data_source": INPUT_SOURCE,
    }])
    deadline = monotonic() + args.timeout
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    client.insert_timeseries(source_id, ref_name, [
        (start + timedelta(minutes=index), 20.0 + index) for index in range(6)
    ])

    while monotonic() < deadline:
        fahrenheit = client.client.timeseries_df(OUTPUT_POINT, value_mode="numeric")
        if fahrenheit.height:
            print(fahrenheit.to_dicts())
            return
        sleep(0.05)
    raise TimeoutError(f"the transformation did not produce Fahrenheit values within {args.timeout:g} seconds")


if __name__ == "__main__":
    main()
