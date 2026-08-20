"""
Publish system metrics (CPU, RAM, disk, network) to an Acquirium backend.

Usage: list it under [[drivers]] in acquirium.toml:

    [[drivers]]
    spec = "acquirium.Drivers.BuiltInDrivers.system_metrics:SystemMetricsDriver"

then run `acquirium server --config acquirium.toml`.
"""

import logging
import platform
import socket
from datetime import datetime, timezone

import polars as pl
import psutil

logger = logging.getLogger("acquirium.system_metrics")
from rdflib import Graph, Literal, URIRef, Namespace
from rdflib.namespace import RDF, RDFS

from acquirium.Drivers.Driver import PollingIngestDriver
from acquirium.internals.internals_namespaces import (
    ACQUIRIUM_DB_URI,
    DATABASE,
    S223,
    VIRTUAL_POINT,
)

HOST_NS = Namespace("urn:acquirium:host#")

# Canonical QUDT URIs (not free text) so register_streams passes them
# straight through with no text resolution. `%` -> unit:PERCENT
# (qudt:hasQuantityKind quantitykind:DimensionlessRatio); a byte count
# -> unit:BYTE (qudt:hasQuantityKind quantitykind:InformationEntropy,
# QUDT's quantity kind for information amount).
_U_PERCENT = "http://qudt.org/vocab/unit/PERCENT"
_U_BYTE = "http://qudt.org/vocab/unit/BYTE"
_QK_RATIO = "http://qudt.org/vocab/quantitykind/DimensionlessRatio"
_QK_INFO = "http://qudt.org/vocab/quantitykind/InformationEntropy"

_METRICS: list[tuple[str, str, str, str]] = [
    # (ref_name,            label,                    unit,       quantity_kind)
    ("cpu_percent",       "CPU usage",              _U_PERCENT, _QK_RATIO),
    ("memory_percent",    "RAM usage",              _U_PERCENT, _QK_RATIO),
    ("memory_used_bytes", "RAM used",               _U_BYTE,    _QK_INFO),
    ("disk_percent",      "Disk usage (root)",      _U_PERCENT, _QK_RATIO),
    ("net_bytes_sent",    "Network bytes sent",     _U_BYTE,    _QK_INFO),
    ("net_bytes_recv",    "Network bytes received", _U_BYTE,    _QK_INFO),
]


class SystemMetricsDriver(PollingIngestDriver):
    def setup(self) -> None:
        hostname = socket.gethostname()
        self.source_id = f"{hostname}-system-metrics"
        self._hostname = hostname
        self._host_uri = URIRef(f"urn:host:{hostname}")
        logger.debug("system_metrics setup: hostname=%s source=%s", hostname, self.source_id)

        self._insert_host_graph(hostname)
        for ref, _, unit, quantity_kind in _METRICS:
            self.declare(
                ref,
                point_uri=str(self._stream_uri(ref)),
                unit=unit,
                quantity_kind=quantity_kind,
                value_kind="numeric",
            )
        psutil.cpu_percent(interval=None)  # first call always returns 0.0; discard it

    def collect(self) -> pl.DataFrame:
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
        logger.debug(
            "system_metrics sample ts=%s cpu=%.1f%% mem=%.1f%% disk=%.1f%%",
            ts.isoformat(), sample["cpu_percent"], sample["memory_percent"], sample["disk_percent"],
        )
        return pl.DataFrame({
            "ts": [ts] * len(sample),
            "ref_name": list(sample.keys()),
            "value": list(sample.values()),
        })

    def _stream_uri(self, key: str) -> URIRef:
        return URIRef(f"urn:host:{self._hostname}:{key}")

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

        self.insert_graph(
            g.serialize(format="turtle"),
            format="turtle",
            replace=False,
        )
