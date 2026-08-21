"""Stable canonical-publication normalization and identity helpers."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import polars as pl
import pyarrow as pa

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def normalize_mutations(table: pa.Table) -> pa.Table:
    """Keep the last mutation for each canonical key in deterministic order."""
    frame = pl.from_arrow(table).unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
    return frame.sort(["ref_uri", "ts"]).to_arrow().cast(table.schema)


def payload_hash(table: pa.Table) -> str:
    normalized = normalize_mutations(table)
    columns = (normalized.column(name).to_pylist() for name in ("ref_uri", "ts", "operation", "numeric_value", "text_value"))
    lines: list[str] = []
    for ref, ts, op, numeric, text in zip(*columns):
        timestamp = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
        micros = int((timestamp - _EPOCH).total_seconds() * 1_000_000)
        lines.append(f"{ref}|{micros}|{'u' if op == 'upsert' else 'd'}|{repr(float(numeric)) if numeric is not None else ''}|{text or ''}")
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()
