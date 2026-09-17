"""Stable canonical-publication normalization and identity helpers."""
from __future__ import annotations

import hashlib

import polars as pl
import pyarrow as pa

def normalize_mutations(table: pa.Table) -> pa.Table:
    """Keep the last mutation for each canonical key in deterministic order."""
    frame = pl.from_arrow(table).unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
    return frame.sort(["ref_uri", "ts"]).to_arrow().cast(table.schema)


def payload_hash(table: pa.Table) -> str:
    """Hash the canonical Arrow representation without collapsing typed values."""
    normalized = normalize_mutations(table)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, normalized.schema) as writer:
        writer.write_table(normalized)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
