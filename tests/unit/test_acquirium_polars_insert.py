from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
from acquirium.Client.acquirium import Acquirium


def test_insert_timeseries_arrow_delegates_to_client():
    aq = Acquirium.__new__(Acquirium)
    captured: dict = {}

    class _FakeClient:
        def insert_timeseries_arrow(self, source_id, table, *, publication_id=None):
            captured["source_id"] = source_id
            captured["table"] = table
            captured["publication_id"] = publication_id
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

