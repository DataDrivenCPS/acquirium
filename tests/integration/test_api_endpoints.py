"""Integration tests for FastAPI endpoints — talks to the running server.

Requires: All services running via `make testing-up`.
Server at localhost:8000, TimescaleDB at localhost:5432, Mosquitto at localhost:1883 by default.
"""

import os
import pytest
import requests
from datetime import datetime, timezone

import pyarrow.ipc as ipc
import pyarrow as pa

from acquirium.internals.models import compute_ref_uri


BASE_URL = (
    f"http://{os.getenv('ACQUIRIUM_TEST_SERVER_HOST', 'localhost')}:"
    f"{os.getenv('ACQUIRIUM_TEST_SERVER_PORT', '8000')}"
)

MINIMAL_TURTLE = """\
@prefix ex: <http://example.org/api_test/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:equip1 a ex:Pump ;
    rdfs:label "Test Pump" .

ex:equip1 ex:hasProperty ex:point1 .
ex:point1 a ex:DataPoint ;
    rdfs:label "Flow Rate" .
"""


# ── Health & Status ────────────────────────────────────────


class TestHealthStatus:
    def test_health(self):
        resp = requests.get(f"{BASE_URL}/health")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_embedding_status(self):
        resp = requests.get(f"{BASE_URL}/embedding_status")
        assert resp.status_code == 200
        data = resp.json()
        assert "graph" in data
        assert "qudt" in data
        assert "state" in data["graph"]


# ── Graph Endpoints ────────────────────────────────────────


class TestGraphEndpoints:
    def test_insert_and_sparql(self):
        resp = requests.post(f"{BASE_URL}/insert_graph", json={
            "rdf_graph": MINIMAL_TURTLE,
            "format": "turtle",
            "replace": False,
        })
        assert resp.status_code == 200

        resp = requests.get(f"{BASE_URL}/sparql_json", params={
            "query": "SELECT ?s WHERE { ?s a <http://example.org/api_test/Pump> }",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["rows"]) >= 1

    def test_insert_replace(self):
        resp = requests.post(f"{BASE_URL}/insert_graph", json={
            "rdf_graph": MINIMAL_TURTLE,
            "format": "turtle",
            "replace": True,
        })
        assert resp.status_code == 200

    def test_export_graph(self):
        requests.post(f"{BASE_URL}/insert_graph", json={
            "rdf_graph": MINIMAL_TURTLE,
            "format": "turtle",
            "replace": True,
        })
        resp = requests.get(f"{BASE_URL}/export_graph", params={"format": "turtle"})
        assert resp.status_code == 200
        assert len(resp.text) > 0

    def test_insert_malformed(self):
        resp = requests.post(f"{BASE_URL}/insert_graph", json={
            "rdf_graph": "this is {{{ not valid turtle",
            "format": "turtle",
            "replace": False,
        })
        assert resp.status_code >= 400


# ── Timeseries Endpoints ──────────────────────────────────


class TestTimeseriesEndpoints:
    TEST_SOURCE = "test"
    TEST_REF_NAME = "api_ts_ref"
    TEST_POINT = "urn:test:api_ts_point"

    def _register_stream(self):
        ref_uri = compute_ref_uri(self.TEST_SOURCE, self.TEST_REF_NAME)
        graph = f"""\
@prefix acq: <urn:acquirium#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .

<{self.TEST_POINT}> ref:hasExternalReference <{ref_uri}> .
<{ref_uri}> a acq:Stream ;
    acq:sourceId "{self.TEST_SOURCE}" ;
    acq:refName "{self.TEST_REF_NAME}" ;
    acq:valueKind "numeric" .
"""
        resp = requests.post(f"{BASE_URL}/insert_graph", json={
            "rdf_graph": graph,
            "format": "turtle",
            "replace": False,
            "wait_for_embedding": False,
        })
        assert resp.status_code == 200

    def _insert_data(self, n=5):
        self._register_stream()
        values = [
            [datetime(2025, 1, 1, h, 0, tzinfo=timezone.utc).isoformat(), float(h)]
            for h in range(n)
        ]
        resp = requests.post(
            f"{BASE_URL}/insert_timeseries",
            json=[{
                "source_id": self.TEST_SOURCE,
                "ref_name": self.TEST_REF_NAME,
                "point_uri": self.TEST_POINT,
                "replace": False,
                "values": values,
            }],
        )
        return resp

    def test_insert_and_query(self):
        resp = self._insert_data(5)
        assert resp.status_code == 200

        resp = requests.get(f"{BASE_URL}/timeseries", params={
            "uri": self.TEST_POINT,
        })
        assert resp.status_code == 200
        # Response is Arrow IPC
        reader = ipc.open_stream(resp.content)
        table = reader.read_all()
        assert table.num_rows >= 5

    def test_time_range(self):
        self._insert_data(24)
        resp = requests.get(f"{BASE_URL}/timeseries", params={
            "uri": self.TEST_POINT,
            "start": "2025-01-01T05:00:00Z",
            "end": "2025-01-01T10:00:00Z",
        })
        assert resp.status_code == 200
        reader = ipc.open_stream(resp.content)
        table = reader.read_all()
        assert table.num_rows <= 6

    def test_order(self):
        self._insert_data(5)
        resp = requests.get(f"{BASE_URL}/timeseries", params={
            "uri": self.TEST_POINT,
            "order": "desc",
        })
        assert resp.status_code == 200
        reader = ipc.open_stream(resp.content)
        table = reader.read_all()
        if table.num_rows >= 2:
            ts_col = table.column("ts")
            assert ts_col[0].as_py() >= ts_col[1].as_py()

    def test_timeseries_info(self):
        self._insert_data(3)
        resp = requests.post(f"{BASE_URL}/timeseries_info", json={
            "uris": [self.TEST_POINT],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert self.TEST_POINT in data
        assert data[self.TEST_POINT]["row_count"] >= 3

    def test_replace(self):
        self._insert_data(10)
        self._register_stream()
        values = [
            [datetime(2025, 6, 1, tzinfo=timezone.utc).isoformat(), 999.0],
        ]
        resp = requests.post(
            f"{BASE_URL}/insert_timeseries",
            json=[{
                "source_id": self.TEST_SOURCE,
                "ref_name": self.TEST_REF_NAME,
                "point_uri": self.TEST_POINT,
                "replace": True,
                "values": values,
            }],
        )
        assert resp.status_code == 200

    def test_empty_uri(self):
        resp = requests.get(f"{BASE_URL}/timeseries", params={
            "uri": "urn:test:api_nonexistent",
        })
        assert resp.status_code == 200
        reader = ipc.open_stream(resp.content)
        table = reader.read_all()
        assert table.num_rows == 0

# ── Log Endpoints ──────────────────────────────────────────


class TestLogEndpoints:
    LOG_POINT = "urn:test:api_log_point"

    def _cleanup(self):
        requests.delete(f"{BASE_URL}/delete_logs", params={"point_uri": self.LOG_POINT})

    def test_insert_and_query(self):
        self._cleanup()
        resp = requests.post(f"{BASE_URL}/insert_log", params={
            "point_uri": self.LOG_POINT,
            "log_timestamp": "2025-06-15T10:00:00Z",
            "message": "api test log",
        })
        assert resp.status_code == 200

        resp = requests.get(f"{BASE_URL}/query_logs", params={
            "point_uri": self.LOG_POINT,
        })
        assert resp.status_code == 200
        logs = resp.json()
        assert len(logs) >= 1
        assert any("api test log" in log.get("message", "") for log in logs)

    def test_with_observation_period(self):
        self._cleanup()
        resp = requests.post(f"{BASE_URL}/insert_log", params={
            "point_uri": self.LOG_POINT,
            "log_timestamp": "2025-06-15T10:00:00Z",
            "observation_start": "2025-06-15T09:00:00Z",
            "observation_end": "2025-06-15T11:00:00Z",
            "message": "observed event",
        })
        assert resp.status_code == 200

    def test_delete_logs(self):
        requests.post(f"{BASE_URL}/insert_log", params={
            "point_uri": self.LOG_POINT,
            "log_timestamp": "2025-06-20T10:00:00Z",
            "message": "to delete",
        })
        resp = requests.delete(f"{BASE_URL}/delete_logs", params={
            "point_uri": self.LOG_POINT,
        })
        assert resp.status_code == 200

        resp = requests.get(f"{BASE_URL}/query_logs", params={
            "point_uri": self.LOG_POINT,
        })
        logs = resp.json()
        assert len(logs) == 0

    def test_no_period(self):
        self._cleanup()
        resp = requests.post(f"{BASE_URL}/insert_log", params={
            "point_uri": self.LOG_POINT,
            "log_timestamp": "2025-06-15T10:00:00Z",
            "message": "no period log",
        })
        assert resp.status_code == 200


# ── SPARQL Endpoints ───────────────────────────────────────


class TestSparqlEndpoints:
    def test_select(self):
        requests.post(f"{BASE_URL}/insert_graph", json={
            "rdf_graph": MINIMAL_TURTLE,
            "format": "turtle",
            "replace": True,
        })
        resp = requests.get(f"{BASE_URL}/sparql_json", params={
            "query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "columns" in data
        assert "rows" in data

    def test_with_union(self):
        resp = requests.get(f"{BASE_URL}/sparql_json", params={
            "query": "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }",
            "use_union": True,
        })
        assert resp.status_code == 200

    def test_malformed(self):
        resp = requests.get(f"{BASE_URL}/sparql_json", params={
            "query": "SELEKT * WERE { broken }",
        })
        assert resp.status_code >= 400


# ── Text Resolution ────────────────────────────────────────


class TestResolveText:
    def test_basic(self):
        resp = requests.get(f"{BASE_URL}/resolve_text", params={
            "text": "pump",
            "min_score": 0.1,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "matches" in data

    def test_kind_filter(self):
        resp = requests.get(f"{BASE_URL}/resolve_text", params={
            "text": "temperature",
            "kind": "class",
            "min_score": 0.1,
        })
        assert resp.status_code == 200
        data = resp.json()
        for match in data.get("matches", []):
            assert match["kind"] == "class"

    def test_no_match(self):
        resp = requests.get(f"{BASE_URL}/resolve_text", params={
            "text": "xyzzynonexistent42",
            "min_score": 0.9,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert len(data.get("matches", [])) == 0
