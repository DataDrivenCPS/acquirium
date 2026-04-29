from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from acquirium.Server.manager import Manager
from acquirium.internals.models import compute_ref_uri


class _BulkStore:
    def __init__(self) -> None:
        self.frames = []
        self.refs = []

    def ensure_stream_ref(self, point_uri, source_id, ref_name, ref_uri=None):
        self.refs.append(
            {
                "point_uri": point_uri,
                "source_id": source_id,
                "ref_name": ref_name,
                "ref_uri": str(ref_uri),
            }
        )
        return str(ref_uri)

    def bulk_insert_polars(self, df):
        self.frames.append(df)
        return len(df)


def test_insert_timeseries_batch_uses_computed_ref_uris_in_one_bulk_insert():
    mgr = Manager.__new__(Manager)
    store = _BulkStore()
    mgr.timescale = store

    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    count = mgr.insert_timeseries_batch(
        "source/file.csv",
        {
            "temp": [(ts, 72.4)],
            "state/value": [(ts, "OK"), (ts.replace(hour=1), None)],
        },
    )

    assert count == 3
    assert len(store.frames) == 1
    rows = store.frames[0].sort(["ref_uri", "ts"]).to_dicts()
    assert {row["ref_uri"] for row in rows} == {
        str(compute_ref_uri("source/file.csv", "temp")),
        str(compute_ref_uri("source/file.csv", "state/value")),
    }
    assert {row["value"] for row in rows} == {"72.4", "OK", None}
    assert {
        (ref_uri["point_uri"], ref_uri["source_id"], ref_uri["ref_name"], ref_uri["ref_uri"])
        for ref_uri in store.refs
    } == {
        (None, "source/file.csv", "temp", str(compute_ref_uri("source/file.csv", "temp"))),
        (None, "source/file.csv", "state/value", str(compute_ref_uri("source/file.csv", "state/value"))),
    }


def test_insert_timeseries_polars_uses_computed_ref_uris_in_one_bulk_insert():
    mgr = Manager.__new__(Manager)
    store = _BulkStore()
    mgr.timescale = store

    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "ts": [ts, ts.replace(hour=1), ts],
            "ref_name": ["temp", "state/value", "temp"],
            "value": ["72.4", "OK", "73.1"],
        },
        schema={
            "ts": pl.Datetime("us", "UTC"),
            "ref_name": pl.Utf8,
            "value": pl.Utf8,
        },
    )

    count = mgr.insert_timeseries_polars("source/file.csv", df)

    assert count == 3
    assert len(store.frames) == 1
    rows = store.frames[0].sort(["ref_uri", "ts"]).to_dicts()
    assert store.frames[0].columns == ["ref_uri", "ts", "value"]
    assert {row["ref_uri"] for row in rows} == {
        str(compute_ref_uri("source/file.csv", "temp")),
        str(compute_ref_uri("source/file.csv", "state/value")),
    }
    assert {row["value"] for row in rows} == {"72.4", "73.1", "OK"}
    assert {
        (ref_uri["point_uri"], ref_uri["source_id"], ref_uri["ref_name"], ref_uri["ref_uri"])
        for ref_uri in store.refs
    } == {
        (None, "source/file.csv", "temp", str(compute_ref_uri("source/file.csv", "temp"))),
        (None, "source/file.csv", "state/value", str(compute_ref_uri("source/file.csv", "state/value"))),
    }
