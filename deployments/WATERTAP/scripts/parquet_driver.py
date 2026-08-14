"""WaterTAP-aware Parquet ingest driver.

Replays the parquet snapshots written by ``data-generator.py``.

The driver's explicit ``read`` method delegates mechanical decoding to the
shared Parquet helper. It declares every point in the model's
``watertap-mapping.json`` up front, wiring each column to its ontology point and
Pyomo variable exactly as ``simulation_driver.py`` does. Without that the rows
land in the store but carry no ``ref:hasExternalReference`` link, so a
meaning-first query never reaches them.

Every file is ingested under one datasource, so parquet rows land on the *same*
reference nodes the simulation driver uses. Because the links come from the
mapping on setup, this works standalone — the simulation driver need never have
run.

Config keys (beyond the usual ``watch_dir`` / ``glob`` / ``format`` /
``time_col``):

  - ``source_id``    : datasource id (required)
  - ``watertap_mapping_path`` : model ``watertap-mapping.json`` (required)
  - ``watertap_graph_path``   : model s223 ontology graph to insert on setup
  - ``watertap_insert_graph`` : insert that graph on setup, default ``false``
  - ``watertap_insert_graph_replace`` : replace on insert, default ``false``

Paths follow the existing WaterTAP convention: ``watch_dir`` is relative to this
config file's directory, while the mapping and graph paths are relative to the
current working directory (the repo root).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from acquirium.Drivers.BuiltInDrivers.parquet_ingest import read_parquet_batch
from acquirium.Drivers.Driver import FileBatch, FileIngestDriver
from acquirium.Drivers.BuiltInDrivers.watertap import (
    _load_point_specs_from_mapping,
    resolve_path,
)
from acquirium.internals.internals_namespaces import HAS_PYOMO_VAR

logger = logging.getLogger("acquirium.watertap.parquet")


class WaterTAPParquetDriver(FileIngestDriver):
    """Parquet ingest that links each column to its WaterTAP ontology point."""

    def setup(self) -> None:
        cfg = self.config["driver"]
        super().setup()

        if cfg.get("watertap_insert_graph", False) and cfg.get("watertap_graph_path"):
            self.insert_graph_file(
                resolve_path(cfg["watertap_graph_path"], "watertap_graph_path"),
                replace=bool(cfg.get("watertap_insert_graph_replace", False)),
            )

        # Ref names are the mapping's point URIs minus its namespace, which is
        # exactly what the data generator writes as column headers.
        mapping_path = resolve_path(cfg.get("watertap_mapping_path"), "watertap_mapping_path")
        specs = _load_point_specs_from_mapping(mapping_path, self.source_id)
        for spec in specs:
            self.declare(
                spec.ref_name,
                point_uri=spec.point_uri,
                value_kind="numeric",
                properties={HAS_PYOMO_VAR: spec.pyomo_var},
            )

        logger.info(
            "watertap parquet: watching %s as %s (%d mapped points)",
            self.watch_dir, self.source_id, len(specs),
        )

    def read(self, path: Path, cursor: Any) -> FileBatch:
        """Read the next explicit page of one WaterTAP Parquet snapshot."""
        cfg = self.config["driver"]
        batch = read_parquet_batch(
            path,
            cursor,
            time_col=str(cfg.get("time_col", "timestamp")),
            id_col=str(cfg.get("id_col", "id")),
            value_col=str(cfg.get("value_col", "value")),
            layout=cfg.get("format"),
            date_format=cfg.get("date_format"),
            skip_cols=cfg.get("skip_cols", []),
        )
        if batch.observations is not None:
            for name in batch.observations["ref_name"].unique():
                if not self.is_declared(name):
                    self.declare(name)
        return batch
