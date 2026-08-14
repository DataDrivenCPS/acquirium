"""WaterTAP-aware Parquet ingest driver.

Replays the parquet snapshots written by ``data-generator.py``.

Reading parquet is the built-in :class:`ParquetIngestDriver`'s job; this driver
only adds meaning. It declares every point in the model's
``watertap-mapping.json`` up front, wiring each column to its ontology point and
Pyomo variable exactly as ``simulation_driver.py`` does. Without that the rows
land in the store but carry no ``ref:hasExternalReference`` link, so a
meaning-first query never reaches them.

Every file is ingested under one datasource, so parquet rows land on the *same*
reference nodes the simulation driver uses. Because the links come from the
mapping on setup, this works standalone — the simulation driver need never have
run.

Config keys (beyond the usual ``watch_dir`` / ``format`` / ``time_col``):

  - ``watertap_source_id``    : datasource id, default ``"watertap"``
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

from acquirium.Drivers.BuiltInDrivers.parquet_ingest import ParquetIngestDriver
from acquirium.Drivers.BuiltInDrivers.watertap import (
    _load_point_specs_from_mapping,
    resolve_path,
)
from acquirium.internals.internals_namespaces import HAS_PYOMO_VAR

logger = logging.getLogger("acquirium.watertap.parquet")


class WaterTAPParquetDriver(ParquetIngestDriver):
    """Parquet ingest that links each column to its WaterTAP ontology point."""

    def setup(self) -> None:
        super().setup()
        cfg = self.config["driver"]
        self.source_id = str(cfg.get("watertap_source_id", "watertap"))
        self.aq.register_datasource(self.source_id)

        if cfg.get("watertap_insert_graph", False) and cfg.get("watertap_graph_path"):
            self.insert_graph(
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
