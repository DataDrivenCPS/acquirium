"""Unit tests for InputBatch (Apps/input_batch.py)."""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest

from acquirium.Apps.input_batch import InputBatch
from acquirium.Storage.continuous.types import MUTATION_SCHEMA


def mutation_table(rows: list[tuple]) -> pa.Table:
    return pa.table(
        {
            "operation": [r[0] for r in rows],
            "ref_uri": [r[1] for r in rows],
            "ts": [r[2] for r in rows],
            "numeric_value": [r[3] for r in rows],
            "text_value": [r[4] for r in rows],
        },
        schema=MUTATION_SCHEMA,
    )


def ts(hour: int) -> datetime:
    return datetime(2026, 1, 1, hour, tzinfo=timezone.utc)


def test_ref_uris_are_distinct_and_sorted():
    batch = InputBatch.from_arrow(mutation_table([
        ("upsert", "b", ts(0), 1.0, None),
        ("upsert", "a", ts(0), 2.0, None),
        ("upsert", "a", ts(1), 3.0, None),
    ]))
    assert batch.ref_uris() == ["a", "b"]


def test_ref_uris_empty_batch():
    batch = InputBatch.from_arrow(mutation_table([]))
    assert batch.ref_uris() == []
    assert batch.is_empty()


def test_upserts_frame_excludes_deletes_and_other_refs():
    batch = InputBatch.from_arrow(mutation_table([
        ("upsert", "s1", ts(0), 1.0, None),
        ("delete", "s1", ts(1), None, None),
        ("upsert", "s2", ts(0), 9.0, None),
    ]))
    frame = batch.upserts_frame("s1")
    assert frame.columns == ["time", "value"]
    assert frame.to_dicts() == [{"time": ts(0), "value": 1.0}]


def test_upserts_frame_sorted_by_time():
    batch = InputBatch.from_arrow(mutation_table([
        ("upsert", "s1", ts(2), 3.0, None),
        ("upsert", "s1", ts(0), 1.0, None),
        ("upsert", "s1", ts(1), 2.0, None),
    ]))
    frame = batch.upserts_frame("s1")
    assert frame["time"].to_list() == [ts(0), ts(1), ts(2)]
    assert frame["value"].to_list() == [1.0, 2.0, 3.0]


def test_upserts_frame_float_cast_falls_back_to_text_value():
    batch = InputBatch.from_arrow(mutation_table([
        ("upsert", "s1", ts(0), None, "3.5"),
    ]))
    frame = batch.upserts_frame("s1", cast_value="float")
    assert frame["value"].to_list() == [3.5]


def test_upserts_frame_text_mode_prefers_text_value():
    batch = InputBatch.from_arrow(mutation_table([
        ("upsert", "s1", ts(0), None, "OK"),
        ("upsert", "s1", ts(1), 42.0, None),
    ]))
    frame = batch.upserts_frame("s1", cast_value="text")
    values = dict(zip(frame["time"].to_list(), frame["value"].to_list()))
    assert values[ts(0)] == "OK"
    assert values[ts(1)] == "42.0"


def test_delete_timestamps():
    batch = InputBatch.from_arrow(mutation_table([
        ("upsert", "s1", ts(0), 1.0, None),
        ("delete", "s1", ts(1), None, None),
        ("delete", "s1", ts(2), None, None),
        ("delete", "s2", ts(1), None, None),
    ]))
    assert batch.delete_timestamps("s1") == [ts(1), ts(2)]
    assert batch.delete_timestamps("s2") == [ts(1)]
    assert batch.delete_timestamps("s3") == []
