"""The HTTP API, called with requests against a server started with aq.init()."""

import uuid
from datetime import datetime, timezone

import pyarrow.ipc as ipc
import pytest
import requests


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


class TestTimeseriesEndpoints:
    TEST_SOURCE = "test"
    TEST_REF_NAME = "api_ts_ref"
    TEST_POINT = "urn:test:api_ts_point"

    @pytest.fixture(scope="class", autouse=True)
    def _register_stream(self, own_acquirium):
        own_acquirium.register_datasource(self.TEST_SOURCE)
        own_acquirium.register_streams([{"source_id": self.TEST_SOURCE, "ref_name": self.TEST_REF_NAME,
                                         "point_uri": self.TEST_POINT, "value_kind": "numeric"}])

    def _post_rows(self, url, values, *, replace=False):
        return requests.post(f"{url}/insert_timeseries", json=[{
            "source_id": self.TEST_SOURCE,
            "ref_name": self.TEST_REF_NAME,
            "request_id": str(uuid.uuid4()),
            "replace": replace,
            "values": values,
        }])

    def _insert_data(self, url, n=5):
        return self._post_rows(url, [
            [datetime(2025, 1, 1, h, 0, tzinfo=timezone.utc).isoformat(), float(h)]
            for h in range(n)
        ])

    def test_insert_and_query(self, url):
        resp = self._insert_data(url, 5)
        assert resp.status_code == 200

        resp = requests.get(f"{url}/timeseries", params={
            "uri": self.TEST_POINT,
        })
        assert resp.status_code == 200
        # Response is Arrow IPC
        reader = ipc.open_stream(resp.content)
        table = reader.read_all()
        assert table.num_rows >= 5

    def test_time_range(self, url):
        self._insert_data(url, 24)
        resp = requests.get(f"{url}/timeseries", params={
            "uri": self.TEST_POINT,
            "start": "2025-01-01T05:00:00Z",
            "end": "2025-01-01T10:00:00Z",
        })
        assert resp.status_code == 200
        reader = ipc.open_stream(resp.content)
        table = reader.read_all()
        assert table.num_rows <= 6

    def test_order(self, url):
        self._insert_data(url, 5)
        resp = requests.get(f"{url}/timeseries", params={
            "uri": self.TEST_POINT,
            "order": "desc",
        })
        assert resp.status_code == 200
        reader = ipc.open_stream(resp.content)
        table = reader.read_all()
        if table.num_rows >= 2:
            ts_col = table.column("ts")
            assert ts_col[0].as_py() >= ts_col[1].as_py()

    def test_timeseries_info(self, url):
        self._insert_data(url, 3)
        resp = requests.post(f"{url}/timeseries_info", json={
            "uris": [self.TEST_POINT],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert self.TEST_POINT in data
        assert data[self.TEST_POINT]["row_count"] >= 3

    def test_replace_rows(self, url):
        self._insert_data(url, 10)
        resp = self._post_rows(url, [[datetime(2025, 6, 1, tzinfo=timezone.utc).isoformat(), 999.0]],
                               replace=True)
        assert resp.status_code == 200
        assert resp.json()["rows_inserted"] == 1
        result = requests.get(f"{url}/timeseries", params={"uri": self.TEST_POINT})
        assert result.status_code == 200
        assert ipc.open_stream(result.content).read_all().column("value").to_pylist() == [999.0]

    def test_empty_uri(self, url):
        resp = requests.get(f"{url}/timeseries", params={
            "uri": "urn:test:api_nonexistent",
        })
        assert resp.status_code == 200
        reader = ipc.open_stream(resp.content)
        table = reader.read_all()
        assert table.num_rows == 0

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
