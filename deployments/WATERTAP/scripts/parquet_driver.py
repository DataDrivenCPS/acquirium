"""WaterTAP-aware Parquet ingest driver.

The built-in :class:`ParquetIngestDriver` watches a directory and replays parquet
snapshots, but it gives every file its own datasource (the sanitised file path)
and registers streams with *identity only*. The resulting streams carry no
``ref:hasExternalReference`` link to the model's ontology points, so a
meaning-first query (``find_all_data().filter_by_...``) never reaches them — the
rows land in the store but are orphaned from the graph.

This driver replays the same ``data-generator.py`` parquet snapshots but wires
them to the model exactly like ``simulation_driver.py`` does:

  * every file is ingested under one datasource, ``watertap_source_id`` (so the
    parquet rows land on the *same* external-reference nodes the simulation
    driver uses — column names are the mapping's ref-names), and
  * each mapped stream is registered with its ``point_uri`` and Pyomo variable,
    which writes the ``ref:hasExternalReference`` link from the ontology point to
    the stream.

Because the links are written on setup from the model's ``watertap-mapping.json``,
this works standalone — you do not need to have ever run the simulation driver.

Extra config keys (in addition to the usual parquet keys ``watch_dir`` /
``format`` / ``time_col``):

  - ``watertap_source_id``    : datasource id, default ``"watertap"``
  - ``watertap_mapping_path`` : model ``watertap-mapping.json`` (required)
  - ``watertap_graph_path``   : model s223 ontology graph to insert on setup
  - ``watertap_insert_graph`` : insert ``watertap_graph_path`` on setup, default ``false``
  - ``watertap_insert_graph_replace`` : replace main graph when inserting, default ``false``

Paths follow the existing WaterTAP convention: ``watch_dir`` is relative to this
config file's directory, while ``watertap_mapping_path`` / ``watertap_graph_path``
are relative to the current working directory (the repo root).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from acquirium.Drivers.BuiltInDrivers.parquet_ingest import ParquetIngestDriver
from acquirium.Drivers.BuiltInDrivers.watertap import (
    _guess_rdf_format,
    _load_point_specs_from_mapping,
    _resolve_path,
)
from acquirium.internals.internals_namespaces import HAS_PYOMO_VAR

logger = logging.getLogger("acquirium.watertap.parquet")


class WaterTAPParquetDriver(ParquetIngestDriver):
    """Parquet ingest that links each column to its WaterTAP ontology point."""

    def configure_tabular_driver(self) -> None:
        cfg = self.config.get("driver", {})
        self._source_id = str(cfg.get("watertap_source_id", "watertap"))
        mapping_path = _resolve_path(
            cfg.get("watertap_mapping_path"), "watertap_mapping_path"
        )

        # ref_name -> point spec (point_uri + Pyomo var), keyed exactly on the
        # column names the data-generator writes (point URI minus namespace).
        self._specs_by_ref = {
            spec.ref_name: spec
            for spec in _load_point_specs_from_mapping(mapping_path, self._source_id)
        }

        # Register the datasource and every mapped stream up front. Registering a
        # stream with its point_uri writes the ref:hasExternalReference link, so
        # the points are wired to the streams before any rows are read.
        self.aq.register_datasource(self._source_id)

        graph_path = cfg.get("watertap_graph_path")
        if bool(cfg.get("watertap_insert_graph", False)) and graph_path:
            resolved = _resolve_path(graph_path, "watertap_graph_path")
            self.aq.insert_graph(
                resolved.read_text(),
                format=_guess_rdf_format(resolved),
                replace=bool(cfg.get("watertap_insert_graph_replace", False)),
            )

        self.aq.register_streams([self._stream_spec(spec.ref_name) for spec in self._specs_by_ref.values()])
        # Mark the mapped streams as registered so tick() does not re-register
        # them; only unmapped columns are lazily registered later.
        self._registered[self._source_id] = set(self._specs_by_ref)

        logger.info(
            "watertap parquet driver watching %s -> source_id=%s (%d mapped points)",
            self._watch_dir, self._source_id, len(self._specs_by_ref),
        )

    def source_id_for(self, path: Path) -> str:
        # All files for this model share one datasource so their rows land on the
        # points' streams instead of a per-file namespace.
        return self._source_id

    def stream_specs_for_names(
        self,
        path: Path,
        source_id: str,
        raw_names: list[str],
        value_kinds: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        # Lazily-registered columns: link the ones that are in the mapping,
        # fall back to the base (identity-only) spec for any that are not.
        specs: list[dict[str, Any]] = []
        for raw_name in raw_names:
            ref_name = self.stream_name_for(raw_name)
            if ref_name in self._specs_by_ref:
                specs.append(self._stream_spec(ref_name))
            else:
                specs.extend(
                    super().stream_specs_for_names(path, source_id, [raw_name], value_kinds)
                )
        return specs

    def _stream_spec(self, ref_name: str) -> dict[str, Any]:
        spec = self._specs_by_ref[ref_name]
        return {
            "source_id": self._source_id,
            "ref_name": spec.ref_name,
            "point_uri": spec.point_uri,
            "value_kind": "numeric",
            "properties": {HAS_PYOMO_VAR: spec.pyomo_var},
        }
