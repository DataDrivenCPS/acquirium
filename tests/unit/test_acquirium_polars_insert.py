from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
from acquirium.Client.acquirium import Acquirium
from acquirium.Server.direct_client import DirectAcquirium


def test_insert_timeseries_arrow_delegates_to_client():
    aq = Acquirium.__new__(Acquirium)
    captured: dict = {}

    class _FakeClient:
        def insert_timeseries_arrow(self, source_id, table):
            captured["source_id"] = source_id
            captured["table"] = table
            return {"ok": True, "rows_inserted": len(table)}

    aq.client = _FakeClient()
    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    table = pa.table(
        {
            "ts": pa.array([ts, ts.replace(hour=1)], type=pa.timestamp("us", tz="UTC")),
            "ref_name": ["temp", "state/value"],
            "value": ["72.4", "OK"],
        }
    )

    result = aq.insert_timeseries_arrow("source/file.csv", table)

    assert result == {"ok": True, "rows_inserted": 2}
    assert captured["source_id"] == "source/file.csv"
    assert captured["table"].equals(table)


def test_direct_acquirium_insert_timeseries_arrow_reaches_manager():
    class _Manager:
        def __init__(self):
            self.calls: list = []

        def insert_timeseries_arrow(self, source_id, table):
            self.calls.append((source_id, table))
            return len(table)

    manager = _Manager()
    aq = DirectAcquirium(manager)
    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    table = pa.table(
        {
            "ts": pa.array([ts, ts.replace(hour=1), ts.replace(hour=2)], type=pa.timestamp("us", tz="UTC")),
            "ref_name": ["temp", "temp", "state"],
            "value": ["1.0", "2.0", "OK"],
        }
    )

    result = aq.insert_timeseries_arrow("source/file.csv", table)

    assert result == {"ok": True, "rows_inserted": 3}
    assert len(manager.calls) == 1
    assert manager.calls[0][0] == "source/file.csv"
    assert len(manager.calls[0][1]) == 3
