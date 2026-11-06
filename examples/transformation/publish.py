"""Convert three readings, then correct one. Run from the repository root:

    uv run --locked python examples/transformation/publish.py
"""

import argparse
import tempfile
from datetime import datetime, timedelta, timezone

import acquirium as aq
from temperature_conversion import fahrenheit_to_celsius


def run(client, *, timeout=30):
    source = "temperature-example"
    client.register_streams([{
        "source_id": source,
        "ref_name": "fahrenheit",
        "point_uri": "urn:example:temperature:fahrenheit",
        "unit": "http://qudt.org/vocab/unit/DEG_F",
        "value_kind": "numeric",
    }])
    client.deploy_app(fahrenheit_to_celsius(client, {}))
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    readings = [(start + timedelta(minutes=i), value)
                for i, value in enumerate((68, 77, 86))]
    receipt = client.insert_timeseries(source, "fahrenheit", readings)["receipts"][0]
    client.wait(receipt, timeout=timeout)

    query = client.query().measurement(app="fahrenheit-to-celsius")
    values = query.dataframe(shape="narrow").sort("time")["value_numeric"].to_list()
    assert values == [20.0, 25.0, 30.0], values
    print(f"68, 77, 86 °F become {values} °C")

    receipt = client.insert_timeseries(
        source, "fahrenheit", [(start + timedelta(minutes=1), 68)]
    )["receipts"][0]
    client.wait(receipt, timeout=timeout)
    values = query.dataframe(shape="narrow").sort("time")["value_numeric"].to_list()
    assert values == [20.0, 20.0, 30.0], values
    print(f"Correct 77 °F to 68 °F: {values} °C")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", help="use an existing server instead of a temporary one")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if args.host:
        run(aq.Acquirium(server_url=args.host, server_port=args.port), timeout=args.timeout)
        return

    with tempfile.TemporaryDirectory(prefix="acquirium-temperature-") as directory:
        client = aq.init(data_dir=directory, exact_only=True, timeout=180)
        try:
            run(client, timeout=args.timeout)
        finally:
            aq.shutdown()


if __name__ == "__main__":
    main()
