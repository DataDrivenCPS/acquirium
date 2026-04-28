from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Iterator
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
                "reference_uri": "urn:ref:temp",
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

    def get_stream_by_reference_uri(self, ref_uri: str) -> dict[str, Any] | None:
        if ref_uri != "urn:ref:temp":
            return None
        return self.get_source_stream("demo", "temp")

    def get_source_stream_by_reference_uri(self, source_id: str, ref_uri: str) -> dict[str, Any] | None:
        if source_id != "demo":
            return None
        return self.get_stream_by_reference_uri(ref_uri)

    def timeseries_batch(
        self,
        uri: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        order: str = "asc",
        batch_size: int = 50_000,
    ) -> Iterator[pa.RecordBatch]:
        assert uri == "urn:point:temp"
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

            resp = client.get("/source/demo/streams")
            assert resp.status_code == 200
            body = resp.json()
            assert body["kind"] == "stream-index"
            assert body["streams"][0]["ref_name"] == "temp"
            assert body["streams"][0]["url"].endswith(f"/streams/by-ref?ref_uri={quote('urn:ref:temp', safe='')}")
            assert body["streams"][0]["data_url"].endswith(f"/streams/data?ref_uri={quote('urn:ref:temp', safe='')}")

            resp = client.get("/streams/by-ref", params={"ref_uri": "urn:ref:temp"})
            assert resp.status_code == 200
            body = resp.json()
            assert body["kind"] == "stream"
            assert body["reference_uri"] == "urn:ref:temp"

            resp = client.get("/source/demo/streams/by-ref", params={"ref_uri": "urn:ref:temp"})
            assert resp.status_code == 200
            body = resp.json()
            assert body["kind"] == "stream"
            assert body["reference_uri"] == "urn:ref:temp"

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
                    "uri": "urn:point:temp",
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
                    "uri": "urn:point:temp",
                }
            ]
    finally:
        app.router.lifespan_context = old_lifespan
