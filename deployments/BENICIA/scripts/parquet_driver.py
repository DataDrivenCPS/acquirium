"""Benicia-aware Parquet ingest driver.

Replays the wide parquet snapshots written by ``generate_historical.py``.

Reading parquet is the built-in :class:`ParquetIngestDriver`'s job; this driver
only adds meaning. It declares every model property up front, so each column
lands on the ontology point of the same name and stays reachable from a
meaning-first query instead of being orphaned in the store. Columns absent from
the model are still ingested, with identity only.

The model's s223/QUDT graph is inserted on setup so those points exist with
their unit and quantity-kind semantics, which makes this driver standalone —
the simulator driver need never have run.

Config keys (beyond the usual ``watch_dir`` / ``format`` / ``time_col``):

    spec         = "parquet_driver.py:BeniciaParquetDriver"
    watch_dir    = "../data/historical"
    format       = "wide"
    time_col     = "timestamp"
    source_id    = "benicia-historical"
    model        = "../benicia-model-100.ttl"   # relative to this config file
    insert_graph = true
"""

from __future__ import annotations

import logging

import rdflib

from acquirium.Drivers.BuiltInDrivers.parquet_ingest import ParquetIngestDriver

from benicia_generator import get_properties, local_name

logger = logging.getLogger("acquirium.benicia.parquet")


class BeniciaParquetDriver(ParquetIngestDriver):
    """Parquet ingest that links each column to its Benicia ontology point."""

    def setup(self) -> None:
        super().setup()
        cfg = self.config["driver"]

        model_path = self.config_dir() / cfg.get("model", "benicia-model-100.ttl")
        if not model_path.exists():
            raise FileNotFoundError(f"Benicia model not found: {model_path}")
        model = rdflib.Graph().parse(model_path)  # format inferred from suffix

        self.aq.register_datasource(self.source_id)
        if cfg.get("insert_graph", True):
            self.insert_graph(model.serialize(format="turtle"))

        points = {local_name(prop): str(prop) for prop in get_properties(model)}
        for ref_name, point_uri in points.items():
            self.declare(ref_name, point_uri=point_uri, value_kind="numeric")

        logger.info(
            "benicia parquet: watching %s as %s (%d model points)",
            self.watch_dir, self.source_id, len(points),
        )
