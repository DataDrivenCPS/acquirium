from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from acquirium.Client.data_object import BindingInfo, DataObject
from acquirium.Client.query_graph import QueryGraph


class RecordingClient:
    def __init__(self) -> None:
        self.calls = []

    def timeseries_df(self, ref_uri, **kwargs):
        self.calls.append((ref_uri, kwargs))
        return pl.DataFrame(
            {
                "ts": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
                "value": ["Manual Control"],
                "uri": [ref_uri],
            }
        )


def test_data_object_passes_value_mode_to_timeseries_client():
    client = RecordingClient()
    data = DataObject(
        _bindings=[
            BindingInfo(
                nid=1,
                point_uri="urn:point:1",
                ref_uri="urn:ref:1",
                alias="mode",
                entity_contexts=[{}],
                row_count=1,
            )
        ],
        _entity_columns=[],
        _query_graph=QueryGraph(),
        _client=client,
        _query_params={
            "start": None,
            "end": None,
            "limit": None,
            "order": "asc",
            "cast_value": None,
            "value_mode": "coalesce",
        },
    )

    df = data.dataframe(shape="narrow")

    assert df["value_text"].to_list() == ["Manual Control"]
    assert client.calls[0][1]["value_mode"] == "coalesce"
