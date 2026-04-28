from __future__ import annotations

from datetime import datetime, timezone

from acquirium.Server.manager import Manager
from acquirium.internals.models import compute_handle


class _BulkStore:
    def __init__(self) -> None:
        self.frames = []

    def bulk_insert_polars(self, df):
        self.frames.append(df)
        return len(df)


def test_insert_timeseries_batch_uses_computed_handles_in_one_bulk_insert():
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
    rows = store.frames[0].sort(["point_uri", "ts"]).to_dicts()
    assert {row["point_uri"] for row in rows} == {
        str(compute_handle("source/file.csv", "temp")),
        str(compute_handle("source/file.csv", "state/value")),
    }
    assert {row["value"] for row in rows} == {"72.4", "OK", None}
