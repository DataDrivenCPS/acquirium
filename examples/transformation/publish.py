"""Publish Fahrenheit samples and show the Celsius values derived by the server.

    uv run python examples/transformation/publish.py
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from time import monotonic, sleep

import acquirium as aq

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--timeout", type=float, default=10.0, help="seconds to wait for materialized output")
    args = parser.parse_args()

    client = aq.Acquirium(server_url=args.host, server_port=args.port)
    source_id, ref_name = "temperature-example", "fahrenheit"
    client.register_datasource(source_id)
    client.register_streams([{
        "source_id": source_id,
        "ref_name": ref_name,
        "point_uri": "urn:example:temperature:fahrenheit",
        "label": "Example temperature in Fahrenheit",
        "unit": "http://qudt.org/vocab/unit/DEG_F",
        "value_kind": "numeric",
    }])
    deadline = monotonic() + args.timeout
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    client.insert_timeseries(source_id, ref_name, [
        (start + timedelta(minutes=index), 68.0 + 1.8 * index) for index in range(6)
    ])

    while monotonic() < deadline:
        celsius = client.query().measurement(
            alias="celsius", app="fahrenheit-to-celsius"
        ).dataframe()
        if celsius.height:
            print(celsius.to_dicts())
            return
        sleep(0.05)
    raise TimeoutError(f"the transformation did not produce Celsius values within {args.timeout:g} seconds")


if __name__ == "__main__":
    main()
