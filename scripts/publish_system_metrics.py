"""
Continuously reads system metrics via psutil and publishes them to an Acquirium backend.

Usage:
    uv run scripts/publish_system_metrics.py
    uv run scripts/publish_system_metrics.py --url http://myserver --port 8000 --interval 5
"""

import argparse
import time
from datetime import datetime, timezone

import psutil

from acquirium import Acquirium
from acquirium.internals.internals_namespaces import QUDT_QUANTITY_KIND, QUDT_UNIT

# Metadata for each metric stream: uri → (label, unit, quantity_kind)
METRICS: dict[str, tuple[str, str, str]] = {
    "urn:system:cpu_percent":       ("CPU usage",              "%",    str(QUDT_QUANTITY_KIND.DimensionlessRatio)),
    "urn:system:memory_percent":    ("RAM usage",              "%",    str(QUDT_QUANTITY_KIND.DimensionlessRatio)),
    "urn:system:memory_used_bytes": ("RAM used",               "byte", str(QUDT_UNIT.BYTE)),
    "urn:system:disk_percent":      ("Disk usage (root)",      "%",    str(QUDT_QUANTITY_KIND.DimensionlessRatio)),
    "urn:system:net_bytes_sent":    ("Network bytes sent",     "byte", str(QUDT_UNIT.BYTE)),
    "urn:system:net_bytes_recv":    ("Network bytes received", "byte", str(QUDT_UNIT.BYTE)),
}


def collect() -> dict[str, float]:
    """Collect one sample of each metric and return as {uri: value}."""
    mem  = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net  = psutil.net_io_counters()
    return {
        "urn:system:cpu_percent":       psutil.cpu_percent(interval=None),
        "urn:system:memory_percent":    mem.percent,
        "urn:system:memory_used_bytes": mem.used,
        "urn:system:disk_percent":      disk.percent,
        "urn:system:net_bytes_sent":    net.bytes_sent,
        "urn:system:net_bytes_recv":    net.bytes_recv,
    }


def register_streams(aq: Acquirium) -> None:
    """Declare each metric stream in the knowledge graph (idempotent)."""
    print("Registering streams in graph...")
    for uri, (label, unit, quantity_kind) in METRICS.items():
        aq.register_stream(
            uri,
            label=label,
            unit=unit,
            quantity_kind=quantity_kind,
        )
        print(f"  registered {uri}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish system metrics to Acquirium")
    parser.add_argument("--url",      default="localhost", help="Acquirium server host")
    parser.add_argument("--port",     default=8000, type=int, help="Acquirium server port")
    parser.add_argument("--interval", default=10.0, type=float, help="Seconds between samples")
    args = parser.parse_args()

    aq = Acquirium(server_url=args.url, server_port=args.port)

    register_streams(aq)

    print(f"Publishing to {args.url}:{args.port} every {args.interval}s")

    # Warm up cpu_percent (first call always returns 0.0)
    psutil.cpu_percent(interval=None)

    while True:
        ts = datetime.now(tz=timezone.utc)
        sample = collect()

        streams = {uri: [(ts, value)] for uri, value in sample.items()}
        result = aq.insert_timeseries_batch(streams)

        print(f"[{ts.isoformat()}] inserted {result.get('rows_inserted', '?')} rows")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
