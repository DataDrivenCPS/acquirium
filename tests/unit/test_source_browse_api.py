from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Iterator
from base64 import urlsafe_b64encode
from urllib.parse import quote

import pyarrow as pa
from fastapi.testclient import TestClient

from acquirium.Server.app import app


class StubBrowseManager:
    def list_sources(self) -> list[dict[str, Any]]:
        return [
            {
                "source_id": "demo",
                "uri": "urn:acquirium:datasource:demo",
                "label": "demo",
                "stream_count": 1,
                "row_count": 2,
                "earliest": datetime(2026, 4, 28, 10, 0, tzinfo=timezone.utc),
                "latest": datetime(2026, 4, 28, 10, 5, tzinfo=timezone.utc),
            }
        ]

    def get_source(self, source_id: str) -> dict[str, Any] | None:
        if source_id != "demo":
            return None
        return {
            **self.list_sources()[0],
            "metadata": {
                "uri": "urn:acquirium:datasource:demo",
                "triples": {
                    "http://www.w3.org/2000/01/rdf-schema#label": ["demo"],
                },
            },
        }

    def list_source_streams(self, source_id: str) -> list[dict[str, Any]]:
        if source_id != "demo":
            return []
        return [
            {
                "source_id": "demo",
                "ref_name": "temp",
                "point_uri": "urn:point:temp",
                "ref_uri": "urn:ref:temp",
                "label": "Temperature",
                "stored_at": "urn:acquirium#TimescaleDB",
                "row_count": 2,
                "earliest": datetime(2026, 4, 28, 10, 0, tzinfo=timezone.utc),
                "latest": datetime(2026, 4, 28, 10, 5, tzinfo=timezone.utc),
            }
        ]

    def get_source_stream(self, source_id: str, ref_name: str) -> dict[str, Any] | None:
        if source_id != "demo" or ref_name != "temp":
            return None
        return {
            **self.list_source_streams(source_id)[0],
            "point_metadata": {
                "uri": "urn:point:temp",
                "triples": {
                    "http://www.w3.org/2000/01/rdf-schema#label": ["Temperature"],
                },
            },
            "reference_metadata": {
                "uri": "urn:ref:temp",
                "triples": {
                    "urn:acquirium#sourceId": ["demo"],
                    "urn:acquirium#refName": ["temp"],
                },
            },
        }

    def get_stream_by_ref_uri(self, ref_uri: str) -> dict[str, Any] | None:
        if ref_uri != "urn:ref:temp":
            return None
        return self.get_source_stream("demo", "temp")

    def get_source_stream_by_ref_uri(self, source_id: str, ref_uri: str) -> dict[str, Any] | None:
        if source_id != "demo":
            return None
        return self.get_stream_by_ref_uri(ref_uri)

    def timeseries_batch(
        self,
        uri: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: str = "asc",
        batch_size: int = 50_000,
    ) -> Iterator[pa.RecordBatch]:
        assert uri == "urn:ref:temp"
        rows = [
            datetime(2026, 4, 28, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 28, 10, 5, tzinfo=timezone.utc),
        ]
        vals = ["72.1", "72.4"]
        if order == "desc":
            rows = list(reversed(rows))
            vals = list(reversed(vals))
        if limit is not None:
            rows = rows[:limit]
            vals = vals[:limit]
        yield pa.record_batch(
            [
                pa.array(rows, type=pa.timestamp("us", tz="UTC")),
                pa.array(vals, type=pa.string()),
                pa.array([uri] * len(rows), type=pa.string()),
            ],
            names=["ts", "value", "uri"],
        )


@asynccontextmanager
async def _noop_lifespan(_app):
    yield


def test_source_browse_endpoints(monkeypatch):
    old_lifespan = app.router.lifespan_context
    app.router.lifespan_context = _noop_lifespan
    app.state.manager = StubBrowseManager()

    try:
        with TestClient(app) as client:
            resp = client.get("/source")
            assert resp.status_code == 200
            assert b'\n  "kind": "source-index"' in resp.content
            body = resp.json()
            assert body["kind"] == "source-index"
            assert body["count"] == 1
            assert body["sources"][0]["source_id"] == "demo"
            assert body["sources"][0]["url"].endswith("/source/demo")

            resp = client.get("/source/demo")
            assert resp.status_code == 200
            body = resp.json()
            assert body["kind"] == "source"
            assert body["metadata"]["triples"]["http://www.w3.org/2000/01/rdf-schema#label"] == ["demo"]

            resp = client.get("/source/demo/streams", params={"limit": 5, "start": "-5min"})
            assert resp.status_code == 200
            body = resp.json()
            assert body["kind"] == "stream-index"
            assert body["data_url_defaults"]["limit"] == "5"
            assert body["data_url_defaults"]["start"] == "-5min"
            assert body["streams"][0]["ref_name"] == "temp"
            assert body["streams"][0]["url"].endswith(f"/streams/by-ref?ref_uri={quote('urn:ref:temp', safe='')}")
            assert f"/streams/data?ref_uri={quote('urn:ref:temp', safe='')}" in body["streams"][0]["data_url"]
            assert "limit=5" in body["streams"][0]["data_url"]
            assert "start=-5min" in body["streams"][0]["data_url"]

            resp = client.get("/streams/by-ref", params={"ref_uri": "urn:ref:temp"})
            assert resp.status_code == 200
            body = resp.json()
            assert body["kind"] == "stream"
            assert body["ref_uri"] == "urn:ref:temp"

            resp = client.get("/source/demo/streams/by-ref", params={"ref_uri": "urn:ref:temp"})
            assert resp.status_code == 200
            body = resp.json()
            assert body["kind"] == "stream"
            assert body["ref_uri"] == "urn:ref:temp"

            resp = client.get("/source/demo/streams/temp")
            assert resp.status_code == 200
            body = resp.json()
            assert body["kind"] == "stream"
            assert body["point_metadata"]["uri"] == "urn:point:temp"

            resp = client.get("/streams/data", params={"ref_uri": "urn:ref:temp", "limit": 1, "order": "desc"})
            assert resp.status_code == 200
            body = resp.json()
            assert body["kind"] == "stream-data"
            assert body["encoding"] == "columns"
            assert body["ts_format"] == "unix_ms"
            assert body["columns"] == ["ts", "value"]
            assert body["data"] == {
                "ts": [1777370700000],
                "value": ["72.4"],
            }
            assert "start" not in body
            assert "end" not in body
            assert "start_resolved" not in body
            assert "end_resolved" not in body

            resp = client.get("/streams/data", params={"ref_uri": "urn:ref:temp", "start": "-5min", "limit": 1})
            assert resp.status_code == 200
            body = resp.json()
            assert body["start"] == "-5min"
            assert body["start_resolved"].endswith(("Z", "+00:00"))

            resp = client.get(
                "/source/demo/streams/data",
                params={"ref_uri": "urn:ref:temp", "limit": 1, "order": "desc", "format": "rows"},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["encoding"] == "rows"
            assert body["rows"] == [
                {
                    "ts": "2026-04-28T10:05:00+00:00",
                    "value": "72.4",
                    "uri": "urn:ref:temp",
                }
            ]

            resp = client.get(
                "/source/demo/streams/temp/data",
                params={"limit": 1, "order": "desc", "format": "rows"},
            )
            assert resp.status_code == 200
            body = resp.json()
            assert body["encoding"] == "rows"
            assert body["rows"] == [
                {
                    "ts": "2026-04-28T10:05:00+00:00",
                    "value": "72.4",
                    "uri": "urn:ref:temp",
                }
            ]
    finally:
        app.router.lifespan_context = old_lifespan


class StubBrowseManagerWithPathSource(StubBrowseManager):
    def list_sources(self) -> list[dict[str, Any]]:
        return [
            {
                "source_id": "dpr-trailer-data/raw/sample.csv",
                "uri": "urn:acquirium:datasource:dpr-trailer-data/raw/sample.csv",
                "label": "dpr-trailer-data/raw/sample.csv",
                "stream_count": 1,
                "row_count": 2,
                "earliest": datetime(2026, 4, 28, 10, 0, tzinfo=timezone.utc),
                "latest": datetime(2026, 4, 28, 10, 5, tzinfo=timezone.utc),
            }
        ]

    def get_source(self, source_id: str) -> dict[str, Any] | None:
        if source_id != "dpr-trailer-data/raw/sample.csv":
            return None
        return {
            **self.list_sources()[0],
            "metadata": {
                "uri": "urn:acquirium:datasource:dpr-trailer-data/raw/sample.csv",
                "triples": {
                    "http://www.w3.org/2000/01/rdf-schema#label": ["dpr-trailer-data/raw/sample.csv"],
                },
            },
        }


def test_source_browse_endpoints_allow_slashes_in_source_id():
    old_lifespan = app.router.lifespan_context
    app.router.lifespan_context = _noop_lifespan
    app.state.manager = StubBrowseManagerWithPathSource()

    try:
        with TestClient(app) as client:
            resp = client.get("/source")
            assert resp.status_code == 200
            body = resp.json()
            assert body["sources"][0]["source_id"] == "dpr-trailer-data/raw/sample.csv"
            encoded = "~b64~" + urlsafe_b64encode(
                "dpr-trailer-data/raw/sample.csv".encode("utf-8")
            ).decode("ascii").rstrip("=")
            assert body["sources"][0]["url"].endswith(f"/source/{encoded}")
            assert body["sources"][0]["streams_url"].endswith(f"/source/{encoded}/streams")

            resp = client.get(f"/source/{encoded}")
            assert resp.status_code == 200
            body = resp.json()
            assert body["source_id"] == "dpr-trailer-data/raw/sample.csv"
    finally:
        app.router.lifespan_context = old_lifespan


class StubBrowseManagerWithDuplicateStreams(StubBrowseManager):
    def list_source_streams(self, source_id: str) -> list[dict[str, Any]]:
        stream = super().list_source_streams(source_id)[0]
        return [
            {**stream, "point_uri": None, "label": None, "row_count": 1},
            stream,
        ]


def test_source_stream_index_deduplicates_by_ref_uri():
    old_lifespan = app.router.lifespan_context
    app.router.lifespan_context = _noop_lifespan
    app.state.manager = StubBrowseManagerWithDuplicateStreams()

    try:
        with TestClient(app) as client:
            resp = client.get("/source/demo/streams")
            assert resp.status_code == 200
            body = resp.json()
            assert body["count"] == 1
            assert len(body["streams"]) == 1
            assert body["streams"][0]["ref_uri"] == "urn:ref:temp"
            assert body["streams"][0]["point_uri"] == "urn:point:temp"
            assert body["streams"][0]["label"] == "Temperature"
            assert body["streams"][0]["row_count"] == 2
    finally:
        app.router.lifespan_context = old_lifespan


class StubInsertManager:
    def __init__(self) -> None:
        self.batch_calls = []
        self.single_calls = []

    def insert_timeseries_batch(self, source_id: str, streams: dict[str, list[tuple[datetime, Any]]]) -> int:
        self.batch_calls.append((source_id, streams))
        return sum(len(rows) for rows in streams.values())

    def insert_timeseries(
        self,
        *,
        source_id: str,
        ref_name: str,
        rows: list[tuple[datetime, Any]],
        point_uri: str | None = None,
        replace: bool = False,
    ) -> int:
        self.single_calls.append(
            {
                "source_id": source_id,
                "ref_name": ref_name,
                "rows": rows,
                "point_uri": point_uri,
                "replace": replace,
            }
        )
        return len(rows)


def test_insert_timeseries_endpoint_bulk_inserts_plain_batches():
    old_lifespan = app.router.lifespan_context
    app.router.lifespan_context = _noop_lifespan
    manager = StubInsertManager()
    app.state.manager = manager

    try:
        with TestClient(app) as client:
            resp = client.post(
                "/insert_timeseries",
                json=[
                    {
                        "source_id": "source-a",
                        "ref_name": "temp",
                        "values": [["2026-04-28T10:00:00Z", 1.0]],
                    },
                    {
                        "source_id": "source-a",
                        "ref_name": "pressure",
                        "values": [["2026-04-28T10:00:00Z", 2.0]],
                    },
                    {
                        "source_id": "source-b",
                        "ref_name": "flow",
                        "values": [["2026-04-28T10:00:00Z", 3.0]],
                    },
                ],
            )

            assert resp.status_code == 200
            assert resp.json() == {"ok": True, "rows_inserted": 3}
            assert len(manager.batch_calls) == 2
            assert manager.batch_calls[0][0] == "source-a"
            assert set(manager.batch_calls[0][1]) == {"temp", "pressure"}
            assert manager.batch_calls[1][0] == "source-b"
            assert set(manager.batch_calls[1][1]) == {"flow"}
            assert manager.single_calls == []
    finally:
        app.router.lifespan_context = old_lifespan


def test_insert_timeseries_endpoint_keeps_explicit_point_uri_on_single_path():
    old_lifespan = app.router.lifespan_context
    app.router.lifespan_context = _noop_lifespan
    manager = StubInsertManager()
    app.state.manager = manager

    try:
        with TestClient(app) as client:
            resp = client.post(
                "/insert_timeseries",
                json=[
                    {
                        "source_id": "source-a",
                        "ref_name": "temp",
                        "point_uri": "urn:point:temp",
                        "values": [["2026-04-28T10:00:00Z", 1.0]],
                    }
                ],
            )

            assert resp.status_code == 200
            assert resp.json() == {"ok": True, "rows_inserted": 1}
            assert manager.batch_calls == []
            assert manager.single_calls[0]["point_uri"] == "urn:point:temp"
    finally:
        app.router.lifespan_context = old_lifespan
