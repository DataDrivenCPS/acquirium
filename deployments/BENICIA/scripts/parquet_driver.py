"""Benicia-aware Parquet ingest driver.

Replays the wide parquet snapshots written by ``generate_historical.py`` and
wires every column to its ontology point. The built-in
:class:`ParquetIngestDriver` would give each file its own datasource and
register identity-only streams, leaving the rows orphaned from the graph. This
subclass instead:

  * ingests every file under one datasource (``source_id``), and
  * links each column ``<name>`` to its point ``<namespace><name>``
    (e.g. ``urn:ex/Effluent_Pump-out-ph``) by registering the stream with its
    ``point_uri``.

The historical columns are exactly the model's property local names, so every
column resolves to a point. The model's s223/QUDT graph is inserted on setup so
those points exist with their unit / quantity-kind semantics — this works
standalone, without ever running the simulator driver.

Config keys (in addition to the usual ``watch_dir`` / ``format`` / ``time_col``):

    spec          = "scripts/parquet_driver.py:BeniciaParquetDriver"
    watch_dir     = "data/historical"
    format        = "wide"
    time_col      = "timestamp"
    source_id     = "benicia-historical"
    namespace     = "urn:ex/"
    model         = "benicia-model-100.ttl"   # relative to this config file
    insert_graph  = true
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import rdflib

from acquirium.Drivers.BuiltInDrivers.parquet_ingest import ParquetIngestDriver
from acquirium.Drivers.BuiltInDrivers.watertap import _guess_rdf_format

from benicia_generator import get_properties, local_name

logger = logging.getLogger("acquirium.benicia.parquet")


class BeniciaParquetDriver(ParquetIngestDriver):
    """Parquet ingest that links each column to its Benicia ontology point."""

    def configure_tabular_driver(self) -> None:
        cfg = self.config.get("driver", {})
        self._source_id = str(cfg.get("source_id", "benicia-historical"))
        self._namespace = str(cfg.get("namespace", "urn:ex/"))

        model_path = self.config_dir() / str(cfg.get("model", "benicia-model-100.ttl"))
        if not model_path.exists():
            raise FileNotFoundError(f"Benicia model not found: {model_path}")
        graph = rdflib.Graph().parse(model_path, format=_guess_rdf_format(model_path))
        # ref_name -> point URI for every model property.
        self._points = {local_name(prop): str(prop) for prop in get_properties(graph)}

        self.aq.register_datasource(self._source_id)
        if bool(cfg.get("insert_graph", True)):
            self.aq.insert_graph(
                graph.serialize(format="turtle"),
                format="turtle",
                replace=False,
                source_id=self._source_id,
            )

        logger.info(
            "benicia parquet driver watching %s -> source_id=%s (%d known points)",
            self._watch_dir, self._source_id, len(self._points),
        )

    def source_id_for(self, path: Path) -> str:
        # All files share one datasource so their rows land on the points'
        # streams instead of a per-file namespace.
        return self._source_id

    def stream_specs_for_names(
        self,
        path: Path,
        source_id: str,
        raw_names: list[str],
        value_kinds: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        specs: list[dict[str, Any]] = []
        for raw_name in raw_names:
            ref_name = self.stream_name_for(raw_name)
            point_uri = self._points.get(ref_name)
            if point_uri is not None:
                specs.append({
                    "source_id": self._source_id,
                    "ref_name": ref_name,
                    "point_uri": point_uri,
                    "value_kind": "numeric",
                })
            else:
                # Column not in the model: fall back to identity-only registration.
                specs.extend(
                    super().stream_specs_for_names(path, source_id, [raw_name], value_kinds)
                )
        return specs
