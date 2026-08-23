"""Deploy the example apps and print recent raw and derived values."""

from __future__ import annotations

import argparse
import time

from acquirium import Acquirium

from apps import APPS, AVERAGE_C, ZONE_A_C, ZONE_B_C


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--wait", type=float, default=3.0,
                        help="seconds to let the driver and materializer run")
    args = parser.parse_args()

    aq = Acquirium(server_url=args.host, server_port=args.port, use_ssl=False)
    for app_class in APPS:
        result = aq.deploy_transformation(app_class)
        print(f"deployed {app_class.__name__}: {result['status']}")

    time.sleep(args.wait)
    print("\nlatest values")
    streams = {
        "zone_a_temperature_c": ZONE_A_C,
        "zone_b_temperature_c": ZONE_B_C,
        "average_temperature_c": AVERAGE_C,
    }
    epoch = aq.client.materialization_epochs()
    for binding in epoch.get("bindings", []):
        if binding["definition_id"] == APPS[1].__acquirium_definition__.definition_id:
            streams[f"mapped_fahrenheit:{binding['logical_key']}"] = binding["outputs"]["fahrenheit"][0]

    for name, ref_uri in streams.items():
        values = aq.client.timeseries_df(ref_uri, limit=3, order="desc")
        print(f"\n{name} ({ref_uri})")
        print(values if not values.is_empty() else "  no values yet")

    print("\nmaterialization status")
    print(aq.client.materialization_status())


if __name__ == "__main__":
    main()
