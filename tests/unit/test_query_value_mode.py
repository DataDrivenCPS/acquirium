from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from acquirium.Client.data_object import BindingInfo, DataObject
from acquirium.Client.query_graph import QueryGraph


class RecordingClient:
    def __init__(self) -> None:
        self.calls = []

    def timeseries_df(self, stream_uri, **kwargs):
        self.calls.append((stream_uri, kwargs))
        return pl.DataFrame(
            {
                "ts": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
                "value": ["Manual Control"],
                "uri": [stream_uri],
            }
        )


def test_data_object_passes_value_mode_to_timeseries_client():
    client = RecordingClient()
    data = DataObject(
        _bindings=[
            BindingInfo(
                nid=1,
                property_uri="urn:point:1",
                stream_uri="urn:ref:1",
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


def test_a_bad_value_mode_is_rejected_before_the_request():
    import pytest

    from acquirium.Client.client import AcquiriumClient

    client = AcquiriumClient.__new__(AcquiriumClient)  # no server: nothing may be sent
    with pytest.raises(ValueError, match="value_mode must be"):
        client.timeseries_df("urn:ref:1", value_mode="x")


def test_the_server_answers_400_to_a_bad_value_mode(monkeypatch):
    import pytest
    from fastapi import HTTPException

    from acquirium.Server import app as module

    class Manager:
        def timeseries_batch(self, **kwargs):  # lazy, as the real one is
            raise AssertionError("the store must not be reached")
            yield

    class State:
        manager = Manager()
        read_batch_size = 10

    class Request:
        headers: dict = {}
        app = type("A", (), {"state": State})

    monkeypatch.setattr(module.app, "state", State, raising=False)
    with pytest.raises(HTTPException) as error:
        module.get_timeseries(Request(), uri="urn:ref:1", value_mode="x")
    assert error.value.status_code == 400
    assert "value_mode must be" in error.value.detail


def test_reprocess_app_takes_text_timestamps():
    from unittest.mock import MagicMock

    from acquirium.Client.client import AcquiriumClient

    client = AcquiriumClient.__new__(AcquiriumClient)
    client.base_url = "http://test:8000"
    client._http = MagicMock()
    client._http.post.return_value = MagicMock(status_code=200, json=lambda: {"ok": True})
    client.reprocess_app("app", "2025-01-01", datetime(2026, 1, 2, tzinfo=timezone.utc))
    assert client._http.post.call_args[1]["params"] == {
        "start": "2025-01-01T00:00:00+00:00", "end": "2026-01-02T00:00:00+00:00",
    }
