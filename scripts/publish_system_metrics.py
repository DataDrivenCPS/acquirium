"""
Publish system metrics to an Acquirium backend.

On startup, inserts an RDF description of the host machine as an s223:Computer
with s223:hasProperty links to each metric stream.  Each stream has an
acquirium:hasExternalReference node pointing at the Acquirium TimescaleDB store.

Usage via CLI (recommended):
    acquirium run scripts/publish_system_metrics.py --config acquirium.toml
    acquirium run scripts/publish_system_metrics.py:SystemMetricsDriver \\
        --config acquirium.toml --interval 5

Usage as a standalone script (legacy):
    uv run --with psutil scripts/publish_system_metrics.py
    uv run --with psutil scripts/publish_system_metrics.py \\
        --url http://myserver --port 8000 --interval 5
"""

import platform
import socket
import time
from datetime import datetime, timezone

import psutil
from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import RDF, RDFS

from acquirium import Acquirium, Driver
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_DB_URI,
    ACQUIRIUM_REF_NAME,
    ACQUIRIUM_SOURCE_ID,
    BRICK_REF_DATABASE,
    BRICK_REF_HAS_EXTERNAL_REFERENCE,
    BRICK_REF_HAS_TIMESERIES_ID,
    BRICK_REF_STORED_AT,
    BRICK_REF_TIMESERIES_REFERENCE,
    S223,
    VIRTUAL_POINT,
)
from acquirium.internals.models import compute_handle

# Custom namespace for host-level properties not covered by S223 or QUDT.
HOST_NS = Namespace("urn:acquirium:host#")

# Metric definitions: short key → (label, unit hint, quantity_kind hint)
_METRIC_DEFS: list[tuple[str, str, str, str]] = [
    # (key,              label,                    unit,   quantity_kind)
    ("cpu_percent",       "CPU usage",              "%",    "dimensionless ratio"),
    ("memory_percent",    "RAM usage",              "%",    "dimensionless ratio"),
    ("memory_used_bytes", "RAM used",               "byte", "data size"),
    ("disk_percent",      "Disk usage (root)",      "%",    "dimensionless ratio"),
    ("net_bytes_sent",    "Network bytes sent",     "byte", "data size"),
    ("net_bytes_recv",    "Network bytes received", "byte", "data size"),
]


# ---------------------------------------------------------------------------
# URI helpers
# ---------------------------------------------------------------------------

def host_uri(hostname: str) -> URIRef:
    return URIRef(f"urn:host:{hostname}")

def stream_uri(hostname: str, key: str) -> URIRef:
    return URIRef(f"urn:host:{hostname}:{key}")

def source_id(hostname: str) -> str:
    return f"{hostname}-system-metrics"


# ---------------------------------------------------------------------------
# Host metadata
# ---------------------------------------------------------------------------

def get_host_info() -> dict:
    hostname = socket.gethostname()
    try:
        ip = socket.gethostbyname(hostname)
    except OSError:
        ip = "127.0.0.1"
    return {
        "hostname": hostname,
        "ip":       ip,
        "os":       platform.system(),
        "release":  platform.release(),
        "platform": platform.platform(),
        "machine":  platform.machine(),
    }


# ---------------------------------------------------------------------------
# Graph registration helpers
# ---------------------------------------------------------------------------

def register_host_graph(aq: Acquirium, host: dict, src_id: str) -> None:
    """Insert a one-time RDF description of the host and its metric streams."""
    hostname = host["hostname"]
    g = Graph()
    h = host_uri(hostname)

    g.add((ACQUIRIUM_DB_URI, RDF.type,   BRICK_REF_DATABASE))
    g.add((ACQUIRIUM_DB_URI, RDFS.label, Literal("Acquirium TimescaleDB")))

    g.add((h, RDF.type,                   S223.Computer))
    g.add((h, RDFS.label,                 Literal(hostname)))
    g.add((h, HOST_NS.hasHostname,        Literal(host["hostname"])))
    g.add((h, HOST_NS.hasIPAddress,       Literal(host["ip"])))
    g.add((h, HOST_NS.hasOperatingSystem, Literal(f"{host['os']} {host['release']}")))
    g.add((h, HOST_NS.hasPlatform,        Literal(host["platform"])))
    g.add((h, HOST_NS.hasMachine,         Literal(host["machine"])))

    for key, label, _unit, _qk in _METRIC_DEFS:
        s = stream_uri(hostname, key)
        handle = compute_handle(src_id, key)
        ref_node = URIRef(str(s) + "#ref")

        g.add((h, S223.hasProperty, s))
        g.add((s, RDF.type,   S223.Property))
        g.add((s, RDF.type,   VIRTUAL_POINT))
        g.add((s, RDFS.label, Literal(label)))
        g.add((s,        BRICK_REF_HAS_EXTERNAL_REFERENCE, ref_node))
        g.add((ref_node, RDF.type,                         BRICK_REF_TIMESERIES_REFERENCE))
        g.add((ref_node, BRICK_REF_HAS_TIMESERIES_ID,      Literal(handle)))
        g.add((ref_node, ACQUIRIUM_SOURCE_ID,               Literal(src_id)))
        g.add((ref_node, ACQUIRIUM_REF_NAME,                Literal(key)))
        g.add((ref_node, BRICK_REF_STORED_AT,               ACQUIRIUM_DB_URI))

    turtle = g.serialize(format="turtle")
    aq.client.insert_graph(turtle, format="turtle", replace=False)


def register_stream_metadata(aq: Acquirium, hostname: str, src_id: str) -> None:
    """Resolve and attach QUDT unit/quantity-kind metadata to each stream."""
    for key, label, unit, quantity_kind in _METRIC_DEFS:
        aq.register_stream(
            stream_uri(hostname, key),
            unit=unit,
            quantity_kind=quantity_kind,
            source_id=src_id,
            ref_name=key,
        )


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def collect() -> dict[str, float]:
    """Collect one sample of each metric, keyed by ref_name."""
    mem  = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net  = psutil.net_io_counters()
    return {
        "cpu_percent":       psutil.cpu_percent(interval=None),
        "memory_percent":    mem.percent,
        "memory_used_bytes": mem.used,
        "disk_percent":      disk.percent,
        "net_bytes_sent":    net.bytes_sent,
        "net_bytes_recv":    net.bytes_recv,
    }


# ---------------------------------------------------------------------------
# Driver class
# ---------------------------------------------------------------------------

class SystemMetricsDriver(Driver):
    """Publishes system metrics (CPU, RAM, disk, network) to Acquirium.

    Config options (under [driver] in acquirium.toml)::

        [driver]
        interval = 10.0   # seconds between samples (default 10)
    """

    def setup(self) -> None:
        host = get_host_info()
        self.hostname = host["hostname"]
        self.src_id = source_id(self.hostname)

        print(f"Host: {self.hostname} ({host['ip']})  {host['platform']}")
        print(f"Source ID: {self.src_id}")

        print("Registering datasource...")
        self.aq.register_datasource(self.src_id)

        print("Registering host graph...")
        register_host_graph(self.aq, host, self.src_id)

        print("Resolving and attaching QUDT metadata...")
        register_stream_metadata(self.aq, self.hostname, self.src_id)

        # Warm up cpu_percent — first call always returns 0.0
        psutil.cpu_percent(interval=None)

        print("Setup complete.")
        for key, label, _, _ in _METRIC_DEFS:
            print(f"  {stream_uri(self.hostname, key)}  ({label})")

    def loop(self) -> None:
        ts = datetime.now(tz=timezone.utc)
        sample = collect()
        streams = {rn: [(ts, value)] for rn, value in sample.items()}
        result = self.aq.insert_timeseries_batch(self.src_id, streams)
        print(f"[{ts.isoformat()}] inserted {result.get('rows_inserted', '?')} rows")


# ---------------------------------------------------------------------------
# Standalone entry point (legacy / direct invocation)
# ---------------------------------------------------------------------------

def _main_standalone() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Publish system metrics to Acquirium")
    parser.add_argument("--url",      default="localhost", help="Acquirium server host")
    parser.add_argument("--port",     default=8000, type=int, help="Acquirium server port")
    parser.add_argument("--interval", default=10.0, type=float, help="Seconds between samples")
    args = parser.parse_args()

    aq = Acquirium(server_url=args.url, server_port=args.port)
    driver = SystemMetricsDriver(aq, {})
    driver.setup()

    print(f"\nPublishing to {args.url}:{args.port} every {args.interval}s  (Ctrl-C to stop)\n")
    try:
        while True:
            driver.loop()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        driver.stop()


if __name__ == "__main__":
    _main_standalone()
