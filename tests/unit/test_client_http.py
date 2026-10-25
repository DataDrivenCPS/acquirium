"""AcquiriumClient against a local server started with aq.init().

Calls that must not reach a server use a client pointed at a port nobody
listens on, so any request fails the test.
"""
import socket
from datetime import datetime, timezone
from hashlib import sha256
from unittest.mock import patch
from uuid import uuid4

import pytest
import requests
from rdflib import Dataset, Graph
from requests import HTTPError

from acquirium import Acquirium
from acquirium.Client.client import AcquiriumClient


@pytest.fixture
def client(ac):
    return ac.client


@pytest.fixture
def offline():
    """A client whose server is not there."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return AcquiriumClient(server_url="127.0.0.1", server_port=port)


@pytest.fixture
def ns():
    """A namespace no other test writes to."""
    return f"urn:client-test:{uuid4().hex}/"


def _objects(client, subject, predicate):
    query = f"SELECT ?o WHERE {{ <{subject}> <{predicate}> ?o }}"
    return [row[0] for row in client.sparql_query(query, wait_for_fresh=True)["rows"]]


# ── Without a server ───────────────────────────────────────


class TestClientInit:
    def test_http_url(self):
        c = AcquiriumClient(server_url="localhost", server_port=8000, use_ssl=False)
        assert c.base_url == "http://localhost:8000"
        assert c.address == "http://localhost:8000"

    def test_https_url(self):
        assert AcquiriumClient(server_url="example.com", server_port=443, use_ssl=True).base_url == "https://example.com:443"

    def test_custom_port(self):
        assert AcquiriumClient(server_url="myhost", server_port=9999).base_url == "http://myhost:9999"


def test_missing_graph_file_raises(offline):
    with pytest.raises(FileNotFoundError):
        offline.insert_graph_file("no/such/file.ttl", source_id="plant")


def test_insert_graph_file_unknown_suffix_requires_format(offline, tmp_path):
    path = tmp_path / "model.unknown"
    path.write_text("@prefix ex: <urn:ex#> .")
    with pytest.raises(ValueError, match="cannot infer RDF format"):
        offline.insert_graph_file(path, source_id="s")


def test_insert_graph_rejects_path_objects(offline, tmp_path):
    path = tmp_path / "model.ttl"
    path.write_text("@prefix ex: <urn:ex#> .")
    with pytest.raises(TypeError, match="insert_graph_file"):
        offline.insert_graph(path, source_id="s")


def test_unreachable_server_errors_propagate(offline):
    with pytest.raises(requests.ConnectionError):
        offline.sparql_query("SELECT * WHERE { ?s ?p ?o }")


class TestConstructorHealthGate:
    def test_healthy_server_constructs_immediately(self, client):
        host, port = client.base_url.removeprefix("http://").split(":")
        aq = Acquirium(server_url=host, server_port=int(port))
        assert aq.address == client.base_url

    def test_unreachable_server_raises_connectionerror(self, offline):
        port = int(offline.base_url.rsplit(":", 1)[1])
        with pytest.raises(ConnectionError, match=r"did not answer /health.*Is the server"):
            Acquirium(server_url="127.0.0.1", server_port=port, health_timeout=0.3)

    def test_health_timeout_none_skips_check(self, offline):
        port = int(offline.base_url.rsplit(":", 1)[1])
        assert Acquirium(server_url="127.0.0.1", server_port=port, health_timeout=None).address == offline.base_url


class TestResolveWithoutServer:
    def test_uri_passthrough_skips_server(self, offline):
        assert offline.resolve("urn:a", "class") == "urn:a"
        assert offline.resolve("urn:a", top_k=3)[0]["match_stage"] == "passthrough"

    def test_pinned_and_empty_fields_never_reach_the_server(self, offline):
        assert offline.resolve({"pinned": ("urn:x", "unit"), "empty": (None, "unit")}) == {"pinned": "urn:x", "empty": None}


# ── Experiments ────────────────────────────────────────────


class TestExperimentTransport:
    @pytest.fixture(autouse=True)
    def no_module_requests(self):
        # Every call must reuse the client's pooled session.
        with patch("requests.post", side_effect=AssertionError("use the client session")), \
                patch("requests.get", side_effect=AssertionError("use the client session")):
            yield

    def _run(self, client, name="transport"):
        study = client.define_experiment(name)
        variable = client.declare_experiment_variable(study["template_id"], label="x", role="output", kind="scalar", metadata={})
        return study, variable, client.start_experiment(study["template_id"], {})

    def test_observations_and_finish_reuse_client_session(self, client):
        study, variable, run = self._run(client)
        client.observe_experiment(run["run_id"], variable["variable_id"], value=12.5)
        client.finish_experiment(run["run_id"])
        assert [item["value"] for item in client.list_experiment_observations(study["template_id"])] == [12.5]
        assert client.get_experiment(run["run_id"])["status"] == "succeeded"

    @pytest.mark.parametrize("media_type", [None, "application/json"])
    def test_file_upload_uses_session_and_preserves_bytes(self, client, tmp_path, media_type):
        study = client.define_experiment("upload")
        variable = client.declare_experiment_variable(study["template_id"], label="input", role="input", kind="file", metadata={})
        run = client.start_experiment(study["template_id"], {})
        source = tmp_path / "input.json"
        source.write_bytes(b'{"power": 42}')
        saved = client.attach_experiment_file(run["run_id"], variable["variable_id"], source, media_type)
        assert saved == {"digest": sha256(source.read_bytes()).hexdigest(), "filename": source.name, "byte_length": 13}
        assert client.experiment_artifact(saved["digest"]) == source.read_bytes()

    @pytest.mark.parametrize("upload", [False, True])
    def test_session_errors_keep_server_details(self, client, tmp_path, upload):
        _, _, run = self._run(client, "first")
        _, foreign, _ = self._run(client, "second")
        source = tmp_path / "input.txt"
        source.write_text("input")
        with pytest.raises(HTTPError, match="different study"):
            if upload:
                client.attach_experiment_file(run["run_id"], foreign["variable_id"], source)
            else:
                client.observe_experiment(run["run_id"], foreign["variable_id"], value=42)


# ── Graph ──────────────────────────────────────────────────


class TestInsertGraph:
    def test_text_content_defaults_to_turtle(self, client, ns):
        # Regression: multiline text without prefix markers raised NameError.
        client.insert_graph(f"PREFIX ex: <{ns}>\nex:s ex:p ex:o .", replace=False, source_id="plant")
        assert _objects(client, f"{ns}s", f"{ns}p") == [f"{ns}o"]

    def test_source_id_owns_what_replace_replaces(self, client, ns):
        client.insert_graph(f"<{ns}a> <{ns}p> <{ns}one> .", source_id="driver/a")
        client.insert_graph(f"<{ns}b> <{ns}p> <{ns}one> .", source_id="driver/b")
        client.insert_graph(f"<{ns}a> <{ns}p> <{ns}two> .", replace=True, source_id="driver/a")
        assert _objects(client, f"{ns}a", f"{ns}p") == [f"{ns}two"]
        assert _objects(client, f"{ns}b", f"{ns}p") == [f"{ns}one"]

    @pytest.mark.parametrize("suffix,format", [
        (".ttl", "turtle"), (".n3", "n3"), (".xml", "xml"), (".rdf", "xml"),
        pytest.param(".trix", "trix", marks=pytest.mark.xfail(
            strict=True, reason="the server parses into a Graph, which drops TriX's named graphs"))])
    def test_insert_graph_infers_format_from_a_path(self, client, ns, tmp_path, suffix, format):
        path = tmp_path / f"model{suffix}"
        # TriX serializes named graphs, so rdflib needs a dataset for it.
        graph = Dataset() if format == "trix" else Graph()
        path.write_text(graph.parse(data=f"<{ns}a> <{ns}p> <{ns}b> .", format="turtle").serialize(format=format))
        client.insert_graph_file(path, source_id="s")
        assert _objects(client, f"{ns}a", f"{ns}p") == [f"{ns}b"]

    def test_insert_graph_explicit_format_is_not_overridden(self, client, ns, tmp_path):
        path = tmp_path / "model.xml"
        path.write_text(f"<{ns}a> <{ns}p> <{ns}b> .")
        client.insert_graph_file(path, format="turtle", source_id="s")
        assert _objects(client, f"{ns}a", f"{ns}p") == [f"{ns}b"]

    @pytest.mark.xfail(strict=True, reason="validate() raises when the server has no SHACL shapes")
    def test_validate_graph(self, client):
        assert client.validate_graph()["conforms"] is True


class TestSparql:
    def test_query_returns_columns_and_rows(self, client, ns):
        client.insert_graph(f"<{ns}s> <{ns}p> <{ns}o> .", source_id="plant")
        result = client.sparql_query(f"SELECT ?o WHERE {{ <{ns}s> <{ns}p> ?o }}", wait_for_fresh=True)
        assert (result["columns"], result["rows"]) == (["o"], [[f"{ns}o"]])

    def test_long_queries_fit_in_the_request(self, client):
        # Queries go in a POST body: VALUES-heavy queries exceed URL length limits.
        values = " ".join(f"<urn:client-test:x{i}>" for i in range(3000))
        assert len(client.sparql_query(f"SELECT ?x WHERE {{ VALUES ?x {{ {values} }} }}")["rows"]) == 3000

    def test_update_source_id_targets_an_owned_graph(self, client, ns):
        triple = f"<{ns}a> <{ns}p> <{ns}b>"
        client.sparql_update(f"INSERT DATA {{ {triple} }}", source_id="app:demo")
        client.sparql_update(f"INSERT DATA {{ {triple} }}", source_id="app:other")
        client.sparql_update(f"DELETE WHERE {{ <{ns}a> ?p ?o }}", source_id="app:demo")
        assert _objects(client, f"{ns}a", f"{ns}p") == [f"{ns}b"]  # still in app:other
        client.sparql_update(f"DELETE WHERE {{ <{ns}a> ?p ?o }}", source_id="app:other")
        assert _objects(client, f"{ns}a", f"{ns}p") == []

    def test_graph_status_and_compatibility_version(self, client, ns):
        before = client.graph_status()
        assert {"source_version", "published_version", "is_current", "rebuild_in_progress"} <= before.keys()
        client.insert_graph(f"<{ns}s> <{ns}p> <{ns}o> .", source_id="plant")
        assert client.graph_version() == client.graph_status()["source_version"] > before["source_version"]


# ── Resolve ────────────────────────────────────────────────


class TestResolve:
    def test_single_text_returns_best_uri(self, client):
        assert client.resolve("pump", "class") == "http://data.ashrae.org/standard223#Pump"

    def test_single_text_no_match_is_none(self, client):
        assert client.resolve("zzzznonexistent") is None

    def test_top_k_returns_candidates(self, client):
        matches = client.resolve("pump", "class", top_k=3)
        assert 1 < len(matches) <= 3
        assert matches[0]["uri"] == "http://data.ashrae.org/standard223#Pump"

    def test_record_form_joint_resolution(self, client):
        assert client.resolve({"eu": ("gal/min", "unit"), "qty": ("zzzznonexistent", "quantity_kind"),
                               "pinned": ("urn:x", "unit"), "empty": (None, "unit")}) == {
            "eu": "http://qudt.org/vocab/unit/GAL_US-PER-MIN", "qty": None, "pinned": "urn:x", "empty": None}

    def test_bound_curies_expand(self, client):
        assert client.resolve("watr:UltrafiltrationUnit", "class") == "urn:nawi-water-ontology#UltrafiltrationUnit"
        assert client.resolve("quantitykind:Pressure", "quantity_kind", top_k=2) == [{
            "uri": "http://qudt.org/vocab/quantitykind/Pressure",
            "kind": "quantity_kind", "score": 1.0, "match_stage": "passthrough",
        }]
        assert client.resolve({"qk": ("quantitykind:Pressure", "quantity_kind"), "u": ("mg/l", "unit")}) == {
            "qk": "http://qudt.org/vocab/quantitykind/Pressure", "u": "http://qudt.org/vocab/unit/MilliGM-PER-L"}

    @pytest.mark.parametrize("text", ["nope:Pump", "flow: water", "a:b:c"])
    def test_other_text_is_matched_not_passed_through(self, client, text):
        assert client.resolve(text, "class") != text


class TestResolveConversion:
    def test_success(self, client):
        out = client.resolve_conversion("mg/l", "g/l")
        assert (out["from"]["uri"], out["to"]["uri"]) == (
            "http://qudt.org/vocab/unit/MilliGM-PER-L", "http://qudt.org/vocab/unit/GM-PER-L")
        assert out["factors"]["compatible"] is True

    def test_error_becomes_valueerror(self, client):
        with pytest.raises(ValueError, match="no convertible pair"):
            client.resolve_conversion("mg/l", "psi")


# ── Logs ───────────────────────────────────────────────────


class TestInsertLog:
    def test_insert_log_full_and_minimal(self, client, ns):
        point = f"{ns}p1"
        start, end = datetime(2025, 1, 1, tzinfo=timezone.utc), datetime(2025, 1, 2, tzinfo=timezone.utc)
        assert client.insert_log(point_uri=point, log_time=start, observation_start=start,
                                 observation_end=end, log_message="test log")["ok"]
        assert client.insert_log(point_uri=point, log_message="minimal log")["ok"]
        logs = {entry.message: entry for entry in client.query_logs(point_uri=point)}
        assert logs.keys() == {"test log", "minimal log"}
        assert (logs["test log"].period.start, logs["test log"].period.end) == (start, end)
