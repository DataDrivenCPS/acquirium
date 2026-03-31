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
    ACQUIRIUM_NS,
    HAS_EXTERNAL_REFERENCE,
    HAS_QUANTITY_KIND,
    HAS_UNIT,
    S223,
    STREAM,
    TIMESERIES_STREAM,
    VIRTUAL_POINT,
    QUDT_QUANTITY_KIND,
    QUDT_UNIT,
)

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

def ref_uri(hostname: str, key: str) -> URIRef:
    """URI of the external reference node for this stream.

    This URI serves double duty:
      - In the RDF graph it is the object of acquirium:hasExternalReference
        on the point_uri, typed as acquirium:TimeseriesStream.
      - In TimescaleDB it is the storage key under which data rows are kept.

    Keeping it distinct from stream_uri (the point_uri) allows the semantic
    identity of the measurement to remain stable even if the backing store
    or ingestion path changes.
    """
    return URIRef(f"urn:host:{hostname}:{key}:acquirium-ref")


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

def register_host_graph(aq: Acquirium, host: dict) -> None:
    """Insert a one-time RDF description of the host and its metric streams.

    Graph shape (per host)::

        <urn:host:{hostname}>
            a s223:Computer ;
            rdfs:label "{hostname}" ;
            host:hasHostname "{hostname}" ;
            host:hasIPAddress "{ip}" ;
            host:hasOperatingSystem "{os} {release}" ;
            host:hasPlatform "{platform}" ;
            host:hasMachine "{machine}" ;
            s223:hasProperty <urn:host:{hostname}:cpu_percent> ;
            s223:hasProperty <urn:host:{hostname}:memory_percent> ;
            ... .

        <urn:host:{hostname}:cpu_percent>
            a s223:Property, acquirium:VirtualPoint ;
            rdfs:label "CPU usage" ;
            acquirium:hasExternalReference <urn:host:{hostname}:cpu_percent:acquirium-ref> .

        <urn:host:{hostname}:cpu_percent:acquirium-ref>
            a acquirium:Stream, acquirium:TimeseriesStream ;
            acquirium:storageBackend "timescale" .
    """
    hostname = host["hostname"]
    g = Graph()
    h = host_uri(hostname)

    # Host node
    g.add((h, RDF.type,               S223.Computer))
    g.add((h, RDFS.label,             Literal(hostname)))
    g.add((h, HOST_NS.hasHostname,    Literal(host["hostname"])))
    g.add((h, HOST_NS.hasIPAddress,   Literal(host["ip"])))
    g.add((h, HOST_NS.hasOperatingSystem, Literal(f"{host['os']} {host['release']}")))
    g.add((h, HOST_NS.hasPlatform,    Literal(host["platform"])))
    g.add((h, HOST_NS.hasMachine,     Literal(host["machine"])))

    for key, label, _unit, _qk in _METRIC_DEFS:
        s = stream_uri(hostname, key)
        r = ref_uri(hostname, key)

        # Link host → stream
        g.add((h, S223.hasProperty, s))

        # Stream node — typed as both s223:Property and acquirium:VirtualPoint
        g.add((s, RDF.type,  S223.Property))
        g.add((s, RDF.type,  VIRTUAL_POINT))
        g.add((s, RDFS.label, Literal(label)))

        # External reference → acquirium TimescaleDB
        g.add((s, HAS_EXTERNAL_REFERENCE, r))
        g.add((r, RDF.type,  STREAM))
        g.add((r, RDF.type,  TIMESERIES_STREAM))
        g.add((r, ACQUIRIUM_NS.storageBackend, Literal("timescale")))

    turtle = g.serialize(format="turtle")
    aq.client.insert_graph(turtle, format="turtle", replace=False)


def register_stream_metadata(aq: Acquirium, hostname: str) -> None:
    """Resolve and attach QUDT unit/quantity-kind metadata to each stream."""
    for key, label, unit, quantity_kind in _METRIC_DEFS:
        uri = stream_uri(hostname, key)
        aq.register_stream(
            uri,
            unit=unit,
            quantity_kind=quantity_kind,
        )


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def collect(hostname: str) -> dict[str, float]:
    """Collect one sample of each metric, keyed by ref_uri.

    Data is stored in TimescaleDB under the ref_uri (not the point_uri).
    The graph's hasExternalReference triple links point_uri → ref_uri,
    so the query layer can join them at read time.
    """
    mem  = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net  = psutil.net_io_counters()
    return {
        str(ref_uri(hostname, "cpu_percent")):       psutil.cpu_percent(interval=None),
        str(ref_uri(hostname, "memory_percent")):    mem.percent,
        str(ref_uri(hostname, "memory_used_bytes")): mem.used,
        str(ref_uri(hostname, "disk_percent")):      disk.percent,
        str(ref_uri(hostname, "net_bytes_sent")):    net.bytes_sent,
        str(ref_uri(hostname, "net_bytes_recv")):    net.bytes_recv,
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
    print(f"Host: {hostname} ({host['ip']})  {host['platform']}")

    print("Registering host graph...")
    register_host_graph(aq, host)

    print("Resolving and attaching QUDT metadata...")
    register_stream_metadata(aq, hostname)

    print(f"\nPublishing to {args.url}:{args.port} every {args.interval}s")
    for key, label, _, _ in _METRIC_DEFS:
        print(f"  {stream_uri(hostname, key)}  ({label})")
    print()

    # Warm up cpu_percent (first call always returns 0.0)
    psutil.cpu_percent(interval=None)

    while True:
        ts = datetime.now(tz=timezone.utc)
        sample = collect(hostname)
        streams = {uri: [(ts, value)] for uri, value in sample.items()}
        result = aq.insert_timeseries_batch(streams)
        print(f"[{ts.isoformat()}] inserted {result.get('rows_inserted', '?')} rows")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
