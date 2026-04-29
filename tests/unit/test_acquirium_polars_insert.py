from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from acquirium.Client.acquirium import Acquirium


def test_insert_timeseries_polars_default_delegates_with_correct_column_order():
    aq = Acquirium.__new__(Acquirium)
    captured: dict[str, object] = {}

    def insert_timeseries_batch(source_id, streams):
        captured["source_id"] = source_id
        captured["streams"] = streams
        return {"ok": True, "rows_inserted": sum(len(rows) for rows in streams.values())}

    aq.insert_timeseries_batch = insert_timeseries_batch
    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    df = pl.DataFrame(
        {
            "ts": [ts, ts.replace(hour=1)],
            "ref_name": ["temp", "state/value"],
            "value": ["72.4", "OK"],
        },
        schema={
            "ts": pl.Datetime("us", "UTC"),
            "ref_name": pl.Utf8,
            "value": pl.Utf8,
        },
    )

    result = aq.insert_timeseries_polars("source/file.csv", df)

    assert result == {"ok": True, "rows_inserted": 2}
    assert captured == {
        "source_id": "source/file.csv",
        "streams": {
            "temp": [(ts, "72.4")],
            "state/value": [(ts.replace(hour=1), "OK")],
        },
    }
