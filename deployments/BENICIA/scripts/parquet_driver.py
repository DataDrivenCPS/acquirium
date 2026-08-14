"""Benicia-aware Parquet ingest driver.

Replays the wide parquet snapshots written by ``generate_historical.py``.

The driver's explicit ``read`` method delegates mechanical decoding to the
shared Parquet helper. It declares every model property up front, so each column
lands on the ontology point of the same name and stays reachable from a
meaning-first query instead of being orphaned in the store. Columns absent from
the model are still ingested, with identity only.

The model's s223/QUDT graph is inserted on setup so those points exist with
their unit and quantity-kind semantics, which makes this driver standalone —
the simulator driver need never have run.

Config keys (beyond the usual ``watch_dir`` / ``glob`` / ``format`` /
``time_col``):

    spec         = "parquet_driver.py:BeniciaParquetDriver"
    watch_dir    = "../data/historical"
    glob         = ["*.parquet", "*.pq"]
    format       = "wide"
    time_col     = "timestamp"
    source_id    = "benicia-historical"
    model        = "../benicia-model-100.ttl"   # relative to this config file
    insert_graph = true
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import rdflib

from acquirium.Drivers.BuiltInDrivers.parquet_ingest import read_parquet_batch
from acquirium.Drivers.Driver import FileBatch, FileIngestDriver

from benicia_generator import get_properties, local_name

logger = logging.getLogger("acquirium.benicia.parquet")


class BeniciaParquetDriver(FileIngestDriver):
    """Parquet ingest that links each column to its Benicia ontology point."""

    def setup(self) -> None:
        super().setup()
        cfg = self.config["driver"]

        model_path = self.config_dir() / cfg.get("model", "benicia-model-100.ttl")
        if not model_path.exists():
            raise FileNotFoundError(f"Benicia model not found: {model_path}")
        model = rdflib.Graph().parse(model_path)  # format inferred from suffix

        if cfg.get("insert_graph", True):
            self.insert_graph(model.serialize(format="turtle"))

        points = {local_name(prop): str(prop) for prop in get_properties(model)}
        for ref_name, point_uri in points.items():
            self.declare(ref_name, point_uri=point_uri, value_kind="numeric")

        logger.info(
            "benicia parquet: watching %s as %s (%d model points)",
            self.watch_dir, self.source_id, len(points),
        )

    def read(self, path: Path, cursor: Any) -> FileBatch:
        """Read the next explicit page of one Benicia Parquet snapshot."""
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
