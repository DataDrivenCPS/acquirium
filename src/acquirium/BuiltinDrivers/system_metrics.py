"""
Publish system metrics (CPU, RAM, disk, network) to an Acquirium backend.

Usage:
    acquirium run acquirium.BuiltinDrivers.system_metrics:SystemMetricsDriver \\
        --config acquirium.toml
"""

import platform
import socket
from datetime import datetime, timezone

import psutil
from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import RDF, RDFS

from acquirium import Acquirium, Driver
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_DB_URI,
    DATABASE,
    S223,
    VIRTUAL_POINT,
)

HOST_NS = Namespace("urn:acquirium:host#")

_METRICS: list[tuple[str, str, str, str]] = [
    # (ref_name,            label,                    unit,   quantity_kind)
    ("cpu_percent",       "CPU usage",              "%",    "dimensionless ratio"),
    ("memory_percent",    "RAM usage",              "%",    "dimensionless ratio"),
    ("memory_used_bytes", "RAM used",               "byte", "data size"),
    ("disk_percent",      "Disk usage (root)",      "%",    "dimensionless ratio"),
    ("net_bytes_sent",    "Network bytes sent",     "byte", "data size"),
    ("net_bytes_recv",    "Network bytes received", "byte", "data size"),
]


class SystemMetricsDriver(Driver):
    def setup(self) -> None:
        hostname = socket.gethostname()
        self._source_id = f"{hostname}-system-metrics"
        self._host_uri = URIRef(f"urn:host:{hostname}")
        self._stream_uri = lambda key: URIRef(f"urn:host:{hostname}:{key}")

        self.aq.register_datasource(self._source_id)
        self._insert_host_graph(hostname)
        self._register_stream_metadata()
        psutil.cpu_percent(interval=None)  # first call always returns 0.0; discard it

    def loop(self) -> None:
        ts = datetime.now(tz=timezone.utc)
        mem  = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        net  = psutil.net_io_counters()
        sample = {
            "cpu_percent":       psutil.cpu_percent(interval=None),
            "memory_percent":    mem.percent,
            "memory_used_bytes": mem.used,
            "disk_percent":      disk.percent,
            "net_bytes_sent":    net.bytes_sent,
            "net_bytes_recv":    net.bytes_recv,
        }
        self.aq.insert_timeseries_batch(self._source_id, {
            ref: [(ts, val)] for ref, val in sample.items()
        })

    def _insert_host_graph(self, hostname: str) -> None:
        try:
            ip = socket.gethostbyname(hostname)
        except OSError:
            ip = "127.0.0.1"

        g = Graph()
        g.add((ACQUIRIUM_DB_URI, RDF.type,   DATABASE))
        g.add((ACQUIRIUM_DB_URI, RDFS.label, Literal("Acquirium TimescaleDB")))
        g.add((self._host_uri, RDF.type,                   S223.Computer))
        g.add((self._host_uri, RDFS.label,                 Literal(hostname)))
        g.add((self._host_uri, HOST_NS.hasHostname,        Literal(hostname)))
        g.add((self._host_uri, HOST_NS.hasIPAddress,       Literal(ip)))
        g.add((self._host_uri, HOST_NS.hasOperatingSystem, Literal(f"{platform.system()} {platform.release()}")))
        g.add((self._host_uri, HOST_NS.hasPlatform,        Literal(platform.platform())))
        g.add((self._host_uri, HOST_NS.hasMachine,         Literal(platform.machine())))

        for ref, label, _, _ in _METRICS:
            s = self._stream_uri(ref)
            g.add((self._host_uri, S223.hasProperty, s))
            g.add((s, RDF.type,   S223.Property))
            g.add((s, RDF.type,   VIRTUAL_POINT))
            g.add((s, RDFS.label, Literal(label)))

        self.aq.insert_graph(g.serialize(format="turtle"), format="turtle", replace=False)

    def _register_stream_metadata(self) -> None:
        for ref, _, unit, quantity_kind in _METRICS:
            self.aq.register_stream(
                self._stream_uri(ref),
                unit=unit,
                quantity_kind=quantity_kind,
                source_id=self._source_id,
                ref_name=ref,
            )
