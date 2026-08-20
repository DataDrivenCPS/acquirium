"""Pure derivation functions for the continuous-batch runtime's identifiers.

These functions are backend-independent: both the DuckDB and Postgres
``ContinuousStore`` implementations import them so payload hashes and batch
ids are computed identically regardless of storage engine, and a retry
against either backend reproduces the same id. See ``continuous_batch_plan.md``
Decisions 4 and 5 for the exact specification each function implements.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Iterable

import polars as pl
import pyarrow as pa

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ts_micros(ts: datetime) -> int:
    """Return integer microseconds since the Unix epoch (UTC) for *ts*.

    A naive ``datetime`` is assumed to already be UTC, matching the DuckDB
    backend's storage convention (see ``DuckDBStore._to_utc_naive``).
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int((ts - _EPOCH).total_seconds() * 1_000_000)


def normalize_mutations(table: pa.Table) -> pa.Table:
    """Normalize a raw mutation table to one row per ``(ref_uri, ts)``.

    Expects :data:`~acquirium.Storage.continuous.types.MUTATION_SCHEMA`
    columns. When the same key appears more than once (e.g. a driver
    buffered several updates to one point before flushing), the *last*
    occurrence wins -- matching the effect of applying the mutations one at
    a time in their given order. The result is sorted by ``(ref_uri, ts)``
    so :func:`payload_hash` is stable regardless of input row order.
    """
    df = pl.from_arrow(table)
    df = df.unique(subset=["ref_uri", "ts"], keep="last", maintain_order=True)
    df = df.sort(["ref_uri", "ts"])
    return df.to_arrow().cast(table.schema)


def payload_hash(table: pa.Table) -> str:
    """Return the sha256 hex digest of a mutation table's normalized content.

    Spec (continuous_batch_plan.md Decision 4): after normalization, rows
    are hashed in ``(ref_uri, ts)`` order as
    ``f"{ref_uri}|{ts_us}|{op}|{num}|{txt}"`` lines joined by newlines, where
    ``ts_us`` is integer epoch-UTC microseconds, ``op`` is ``u``/``d``,
    ``num`` is ``repr(float(value))`` or empty, and ``txt`` is the raw text
    value or empty. Two publications with the same normalized content always
    hash identically, which is what makes a retried ``publication_id`` with
    the same mutations idempotent (see ``ContinuousStore.publish``).
    """
    normalized = normalize_mutations(table)
    refs = normalized.column("ref_uri").to_pylist()
    tss = normalized.column("ts").to_pylist()
    ops = normalized.column("operation").to_pylist()
    nums = normalized.column("numeric_value").to_pylist()
    txts = normalized.column("text_value").to_pylist()

    lines: list[str] = []
    for ref_uri, ts, op, num, txt in zip(refs, tss, ops, nums, txts):
        op_code = "u" if op == "upsert" else "d"
        num_str = repr(float(num)) if num is not None else ""
        txt_str = txt if txt is not None else ""
        lines.append(f"{ref_uri}|{_ts_micros(ts)}|{op_code}|{num_str}|{txt_str}")
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def tail_batch_id(generation: int, ranges: Iterable[tuple[str, int, int]]) -> str:
    """Derive a stable tail batch id from a generation and its input ranges.

    Spec (continuous_batch_plan.md Decision 5):
    ``sha256(f"{generation}:" + ";".join(f"{ref},{from_v},{to_v}"))`` over
    ranges sorted by ``(ref_uri, from_version, to_version)``. Because the id
    is a pure function of its inputs, retrying ``commit_app_batch`` with the
    same ranges after a lost response reproduces the same id and hits the
    idempotent claim in ``app_batch_commits``.
    """
    sorted_ranges = sorted(ranges)
    body = ";".join(f"{ref},{from_v},{to_v}" for ref, from_v, to_v in sorted_ranges)
    return hashlib.sha256(f"{generation}:{body}".encode("utf-8")).hexdigest()


def bootstrap_page_id(bootstrap_id: str, start_ordinal: int, end_ordinal: int) -> str:
    """Derive a stable bootstrap page id from its ordinal range."""
    return hashlib.sha256(
        f"{bootstrap_id}:{start_ordinal}:{end_ordinal}".encode("utf-8")
    ).hexdigest()


def app_output_publication_id(app_id: str, batch_id: str) -> str:
    """Publication id used when an app commits its tail batch outputs."""
    return f"app:{app_id}:{batch_id}"


def bootstrap_publication_id(bootstrap_id: str) -> str:
    """Publication id used for a bootstrap's single finalize-time replacement."""
    return f"bootstrap:{bootstrap_id}"
