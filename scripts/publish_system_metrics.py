"""
Continuously reads system metrics via psutil and publishes them to an Acquirium backend.

On startup, inserts an RDF description of the host machine as an s223:Computer
with s223:hasProperty links to each metric stream.  Each stream has an
acquirium:hasExternalReference node pointing at the Acquirium TimescaleDB store.

Usage:
    uv run --with psutil scripts/publish_system_metrics.py
    uv run --with psutil scripts/publish_system_metrics.py --url http://myserver --port 8000 --interval 5
"""

import argparse
import platform
import socket
import time
from datetime import datetime, timezone

import psutil
from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import RDF, RDFS

from acquirium import Acquirium
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
    QUDT_QUANTITY_KIND,
    QUDT_UNIT,
)
from acquirium.internals.models import compute_handle

# Custom namespace for host-level properties not covered by S223 or QUDT.
HOST_NS = Namespace("urn:acquirium:host#")

# Metric definitions: short key → (label, unit hint, quantity_kind hint)
# URIs are constructed at runtime using the host's name so multiple hosts
# can publish to the same Acquirium instance without URI collisions.
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
    """Datasource identifier for this host's metrics publisher."""
    return f"{hostname}-system-metrics"


def ref_name(key: str) -> str:
    """Source-local stream identifier — just the metric key."""
    return key


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
# Graph registration
# ---------------------------------------------------------------------------

def register_host_graph(aq: Acquirium, host: dict, src_id: str) -> None:
    """Insert a one-time RDF description of the host and its metric streams.

    Graph shape (per host)::

        <urn:host:{hostname}>
            a s223:Computer ;
            rdfs:label "{hostname}" ;
            host:hasHostname, host:hasIPAddress, ... ;
            s223:hasProperty <urn:host:{hostname}:cpu_percent> ;
            ... .

        <urn:host:{hostname}:cpu_percent>
            a s223:Property, acquirium:VirtualPoint ;
            rdfs:label "CPU usage" ;
            ref:hasExternalReference <urn:host:{hostname}:cpu_percent#ref> .

        <urn:host:{hostname}:cpu_percent#ref>
            a ref:TimeseriesReference ;
            ref:hasTimeseriesId "{hostname}:cpu_percent" ;
            ref:storedAt <urn:acquirium:timescaledb> .

        <urn:acquirium:timescaledb>  a  ref:Database .
    """
    hostname = host["hostname"]
    g = Graph()
    h = host_uri(hostname)

    # Declare the Acquirium DB node once per graph (idempotent across calls)
    g.add((ACQUIRIUM_DB_URI, RDF.type,   BRICK_REF_DATABASE))
    g.add((ACQUIRIUM_DB_URI, RDFS.label, Literal("Acquirium TimescaleDB")))

    # Host node
    g.add((h, RDF.type,                   S223.Computer))
    g.add((h, RDFS.label,                 Literal(hostname)))
    g.add((h, HOST_NS.hasHostname,        Literal(host["hostname"])))
    g.add((h, HOST_NS.hasIPAddress,       Literal(host["ip"])))
    g.add((h, HOST_NS.hasOperatingSystem, Literal(f"{host['os']} {host['release']}")))
    g.add((h, HOST_NS.hasPlatform,        Literal(host["platform"])))
    g.add((h, HOST_NS.hasMachine,         Literal(host["machine"])))

    for key, label, _unit, _qk in _METRIC_DEFS:
        s = stream_uri(hostname, key)
        rn = ref_name(key)
        handle = compute_handle(src_id, rn)
        ref_node = URIRef(str(s) + "#ref")

        # Link host → stream
        g.add((h, S223.hasProperty, s))

        # Stream node
        g.add((s, RDF.type,   S223.Property))
        g.add((s, RDF.type,   VIRTUAL_POINT))
        g.add((s, RDFS.label, Literal(label)))

        # Brick-style external reference → Acquirium TimescaleDB
        # ref:hasTimeseriesId holds the globally-unique handle (actual DB key)
        g.add((s,        BRICK_REF_HAS_EXTERNAL_REFERENCE, ref_node))
        g.add((ref_node, RDF.type,                         BRICK_REF_TIMESERIES_REFERENCE))
        g.add((ref_node, BRICK_REF_HAS_TIMESERIES_ID,      Literal(handle)))
        g.add((ref_node, ACQUIRIUM_SOURCE_ID,               Literal(src_id)))
        g.add((ref_node, ACQUIRIUM_REF_NAME,                Literal(rn)))
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
            ref_name=ref_name(key),
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
        ref_name("cpu_percent"):       psutil.cpu_percent(interval=None),
        ref_name("memory_percent"):    mem.percent,
        ref_name("memory_used_bytes"): mem.used,
        ref_name("disk_percent"):      disk.percent,
        ref_name("net_bytes_sent"):    net.bytes_sent,
        ref_name("net_bytes_recv"):    net.bytes_recv,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Publish system metrics to Acquirium")
    parser.add_argument("--url",      default="localhost", help="Acquirium server host")
    parser.add_argument("--port",     default=8000, type=int, help="Acquirium server port")
    parser.add_argument("--interval", default=10.0, type=float, help="Seconds between samples")
    args = parser.parse_args()

    aq = Acquirium(server_url=args.url, server_port=args.port)

    host = get_host_info()
    hostname = host["hostname"]
    src_id = source_id(hostname)
    print(f"Host: {hostname} ({host['ip']})  {host['platform']}")
    print(f"Source ID: {src_id}")

    print("Registering datasource...")
    aq.register_datasource(src_id)

    print("Registering host graph...")
    register_host_graph(aq, host, src_id)

    print("Resolving and attaching QUDT metadata...")
    register_stream_metadata(aq, hostname, src_id)

    print(f"\nPublishing to {args.url}:{args.port} every {args.interval}s")
    for key, label, _, _ in _METRIC_DEFS:
        print(f"  {stream_uri(hostname, key)}  ({label})  [{ref_name(key)}]")
    print()

    # Warm up cpu_percent (first call always returns 0.0)
    psutil.cpu_percent(interval=None)

    while True:
        ts = datetime.now(tz=timezone.utc)
        sample = collect()
        streams = {rn: [(ts, value)] for rn, value in sample.items()}
        result = aq.insert_timeseries_batch(src_id, streams)
        print(f"[{ts.isoformat()}] inserted {result.get('rows_inserted', '?')} rows")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
