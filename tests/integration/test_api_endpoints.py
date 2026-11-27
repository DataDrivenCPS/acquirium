"""The HTTP API, called with requests against a server started with aq.init()."""

from datetime import datetime, timezone

import pyarrow.ipc as ipc
import pytest
import requests

from acquirium.core import time_ns


@pytest.fixture(scope="module")
def url(own_acquirium):
    return own_acquirium.client.base_url


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
    def test_health(self, url):
        resp = requests.get(f"{url}/health")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_embedding_status(self, url):
        resp = requests.get(f"{url}/embedding_status")
        assert resp.status_code == 200
        data = resp.json()
        assert "graph" in data
        assert "qudt" in data
        assert "state" in data["graph"]


# ── Graph Endpoints ────────────────────────────────────────


class TestGraphEndpoints:
    def test_insert_and_sparql(self, url):
        resp = requests.post(f"{url}/insert_graph", json={
            "rdf_graph": MINIMAL_TURTLE,
            "format": "turtle",
            "replace": False,
            "source_id": "plant",
        })
        assert resp.status_code == 200

        resp = requests.get(f"{url}/sparql_json", params={
            "query": "SELECT ?s WHERE { ?s a <http://example.org/api_test/Pump> }",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["rows"]) >= 1

    def test_insert_replace(self, url):
        resp = requests.post(f"{url}/insert_graph", json={
            "rdf_graph": MINIMAL_TURTLE,
            "format": "turtle",
            "replace": True,
            "source_id": "plant",
        })
        assert resp.status_code == 200

    def test_export_graph(self, url):
        requests.post(f"{url}/insert_graph", json={
            "rdf_graph": MINIMAL_TURTLE,
            "format": "turtle",
            "replace": True,
            "source_id": "plant",
        })
        resp = requests.get(f"{url}/export_graph", params={"format": "turtle"})
        assert resp.status_code == 200
        assert len(resp.text) > 0

    def test_insert_malformed(self, url):
        resp = requests.post(f"{url}/insert_graph", json={
            "rdf_graph": "this is {{{ not valid turtle",
            "format": "turtle",
            "replace": False,
            "source_id": "plant",
        })
        assert resp.status_code >= 400


# ── Timeseries Endpoints ──────────────────────────────────


def test_timeseries_reads_and_replaces_one_day_of_samples(url, own_acquirium):
    point = "urn:test:api_ts_point"
    own_acquirium.register_streams([
        {"source_id": "test", "stream_name": "hourly", "property_uri": point, "value_kind": "numeric"},
    ])

    def write(request_id, readings, *, replace=False):
        """Write or replace readings through the HTTP endpoint."""
        response = requests.post(f"{url}/insert_timeseries", json=[{
            "source_id": "test", "stream_name": "hourly", "request_id": request_id,
            "replace": replace, "values": readings,
        }])
        assert response.status_code == 200, response.text
        return response.json()

    def values(uri=point, **params):
        """Read the value column from the Arrow endpoint."""
        response = requests.get(f"{url}/timeseries", params={"uri": uri, **params})
        assert response.status_code == 200, response.text
        return ipc.open_stream(response.content).read_all()["value"].to_pylist()

    # Write one reading per hour, then read the whole stream and a half-open range.
    initial = write("one-day", [
        [datetime(2025, 1, 1, hour, tzinfo=timezone.utc).isoformat(), float(hour)]
        for hour in range(24)
    ])
    assert values() == list(range(24))
    assert values(start="2025-01-01T05:00:00Z", end="2025-01-01T10:00:00Z", order="desc") == [
        9.0, 8.0, 7.0, 6.0, 5.0,
    ]
    info = requests.post(f"{url}/timeseries_info", json={"uris": [point]})
    assert info.status_code == 200, info.text
    assert info.json()[point]["row_count"] == 24

    # Delete one reading. The same Arrow route still serves it at the earlier epoch.
    epoch = own_acquirium.wait(initial["receipts"][0])["epoch"]
    removed = requests.post(f"{url}/delete_timeseries", json={
        "source_id": "test", "stream_name": "hourly",
        "request_id": "delete-hour", "timestamps": [time_ns("2025-01-01T05:00:00Z")],
    })
    assert removed.status_code == 200, removed.text
    assert removed.json()["published"]
    interval = {"start": "2025-01-01T05:00:00Z", "end": "2025-01-01T06:00:00Z"}
    assert values(**interval) == []
    assert values(epoch=epoch, **interval) == [5.0]

    # Replacement leaves one reading. The string bounds distinguish adjacent nanoseconds.
    timestamp = "2025-06-01T00:00:00.000000001Z"
    assert write("replacement", [[timestamp, 999.0]], replace=True)["rows_inserted"] == 1
    assert values() == [999.0]
    assert values(start=timestamp, end="2025-06-01T00:00:00.000000002Z") == [999.0]
    assert values(end=timestamp) == []
    assert values("urn:test:api_nonexistent") == []


# ── Logbook Endpoints ───────────────────────────────────────


class TestLogEndpoints:
    LOG_POINT = "urn:test:api_log_point"

    def _cleanup(self, url):
        requests.delete(f"{url}/delete_logs", params={"property_uri": self.LOG_POINT})

    def test_insert_and_query(self, url):
        self._cleanup(url)
        resp = requests.post(f"{url}/insert_log", params={
            "property_uri": self.LOG_POINT,
            "log_timestamp": "2025-06-15T10:00:00Z",
            "message": "api test log",
        })
        assert resp.status_code == 200

        resp = requests.get(f"{url}/query_logs", params={
            "property_uri": self.LOG_POINT,
        })
        assert resp.status_code == 200
        logs = resp.json()
        assert len(logs) >= 1
        assert any("api test log" in log.get("message", "") for log in logs)

    def test_with_observation_period(self, url):
        self._cleanup(url)
        resp = requests.post(f"{url}/insert_log", params={
            "property_uri": self.LOG_POINT,
            "log_timestamp": "2025-06-15T10:00:00Z",
            "observation_start": "2025-06-15T09:00:00Z",
            "observation_end": "2025-06-15T11:00:00Z",
            "message": "observed event",
        })
        assert resp.status_code == 200

    def test_delete_logs(self, url):
        requests.post(f"{url}/insert_log", params={
            "property_uri": self.LOG_POINT,
            "log_timestamp": "2025-06-20T10:00:00Z",
            "message": "to delete",
        })
        resp = requests.delete(f"{url}/delete_logs", params={
            "property_uri": self.LOG_POINT,
        })
        assert resp.status_code == 200

        resp = requests.get(f"{url}/query_logs", params={
            "property_uri": self.LOG_POINT,
        })
        logs = resp.json()
        assert len(logs) == 0

    def test_no_period(self, url):
        self._cleanup(url)
        resp = requests.post(f"{url}/insert_log", params={
            "property_uri": self.LOG_POINT,
            "log_timestamp": "2025-06-15T10:00:00Z",
            "message": "no period log",
        })
        assert resp.status_code == 200


# ── SPARQL Endpoints ───────────────────────────────────────


class TestSparqlEndpoints:
    @staticmethod
    def _insert_graph(url):
        response = requests.post(f"{url}/insert_graph", json={
            "rdf_graph": MINIMAL_TURTLE,
            "format": "turtle",
            "replace": True,
            "source_id": "plant",
        })
        assert response.status_code == 200

    def test_select(self, url):
        self._insert_graph(url)
        resp = requests.get(f"{url}/sparql_json", params={
            "query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "columns" in data
        assert "rows" in data

    def test_with_union(self, url):
        resp = requests.get(f"{url}/sparql_json", params={
            "query": "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }",
            "include_dependencies": True,
        })
        assert resp.status_code == 200

    def test_malformed(self, url):
        resp = requests.get(f"{url}/sparql_json", params={
            "query": "SELEKT * WERE { broken }",
        })
        assert resp.status_code >= 400

    def test_protocol_select_returns_standard_json(self, url):
        self._insert_graph(url)
        resp = requests.get(f"{url}/sparql", params={
            "query": "SELECT ?s WHERE { ?s ?p ?o } LIMIT 1",
        }, headers={"Accept": "application/sparql-results+json"})
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/sparql-results+json")
        assert "head" in resp.json()

    def test_protocol_ask_returns_standard_json(self, url):
        self._insert_graph(url)
        resp = requests.get(f"{url}/sparql", params={
            "query": "ASK { ?s ?p ?o }",
        })
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/sparql-results+json")
        assert resp.json()["boolean"] is True

    def test_protocol_construct_negotiates_turtle(self, url):
        self._insert_graph(url)
        resp = requests.get(f"{url}/sparql", params={
            "query": "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 1",
        }, headers={"Accept": "text/turtle"})
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/turtle")
        assert resp.text

    def test_protocol_describe_negotiates_ntriples(self, url):
        self._insert_graph(url)
        resp = requests.get(f"{url}/sparql", params={
            "query": "DESCRIBE <urn:example:Thing>",
        }, headers={"Accept": "application/n-triples"})
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/n-triples")


# ── Text Resolution ────────────────────────────────────────


class TestResolveText:
    def test_basic(self, url):
        resp = requests.get(f"{url}/resolve_text", params={
            "text": "pump",
            "min_score": 0.1,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "matches" in data

    def test_kind_filter(self, url):
        resp = requests.get(f"{url}/resolve_text", params={
            "text": "temperature",
            "kind": "class",
            "min_score": 0.1,
        })
        assert resp.status_code == 200
        data = resp.json()
        for match in data.get("matches", []):
            assert match["kind"] == "class"

    def test_no_match(self, url):
        resp = requests.get(f"{url}/resolve_text", params={
            "text": "xyzzynonexistent42",
            "min_score": 0.9,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert len(data.get("matches", [])) == 0
