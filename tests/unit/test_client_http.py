"""Tests for AcquiriumClient HTTP methods with mocked requests."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from acquirium.Client.client import AcquiriumClient


@pytest.fixture
def client():
    return AcquiriumClient(server_url="localhost", server_port=8000, use_ssl=False)


@pytest.fixture
def ssl_client():
    return AcquiriumClient(server_url="example.com", server_port=443, use_ssl=True)


# ── Constructor ────────────────────────────────────────────


class TestClientInit:
    def test_http_url(self, client):
        assert client.base_url == "http://localhost:8000"

    def test_https_url(self, ssl_client):
        assert ssl_client.base_url == "https://example.com:443"

    def test_custom_port(self):
        c = AcquiriumClient(server_url="myhost", server_port=9999)
        assert c.base_url == "http://myhost:9999"


class TestInsertGraph:
    @patch("acquirium.Client.client.requests")
    def test_insert_graph_does_not_scan_external_references(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        client.insert_graph("@prefix ex: <urn:ex/> .", replace=False, source_id="plant")

        mock_requests.post.assert_called_once()
        assert mock_requests.post.call_args.args[0] == "http://localhost:8000/insert_graph"

    @patch("acquirium.Client.client.requests")
    def test_multiline_text_without_markers_is_content(self, mock_requests, client):
        ## regression: used to raise NameError (undefined `p`)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        rdf = "PREFIX ex: <urn:ex/>\nex:s ex:p ex:o ."
        client.insert_graph(rdf, replace=False, source_id="plant")

        assert mock_requests.post.call_args.kwargs["json"]["rdf_graph"] == rdf

    @patch("acquirium.Client.client.requests")
    def test_source_id_is_sent_when_graph_has_an_owner(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        client.insert_graph("@prefix ex: <urn:ex/> .", source_id="driver/a")

        assert mock_requests.post.call_args.kwargs["json"]["source_id"] == "driver/a"

    def test_missing_graph_file_raises(self, client):
        with pytest.raises(FileNotFoundError):
            client.insert_graph_file("no/such/file.ttl", source_id="plant")

    @patch("acquirium.Client.client.requests")
    def test_validate_graph_posts_to_validation_endpoint(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"conforms": True, "report": "", "results_text": ""}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        assert client.validate_graph()["conforms"] is True
        assert mock_requests.post.call_args.args[0] == "http://localhost:8000/validate_graph"


# ── sparql_query ───────────────────────────────────────────


class TestSparqlQuery:
    @patch("acquirium.Client.client.requests")
    def test_success_posts_json_body(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "head": {"vars": ["s"]},
            "results": {"bindings": [{"s": {"type": "uri", "value": "urn:test"}}]},
        }
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        result = client.sparql_query("SELECT * WHERE { ?s ?p ?o }")
        assert result == {"columns": ["s"], "rows": [["urn:test"]]}
        # POST with a JSON body: VALUES-heavy queries exceed URL length limits
        call = mock_requests.post.call_args
        assert call.args[0].endswith("/sparql_json")
        assert call.kwargs["json"] == {
            "query": "SELECT * WHERE { ?s ?p ?o }",
            "include_dependencies": True,
            "wait_for_fresh": False,
        }

    @patch("acquirium.Client.client.requests")
    def test_wait_for_fresh_is_sent_when_requested(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"columns": [], "rows": []}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        client.sparql_query("SELECT * WHERE { ?s ?p ?o }", wait_for_fresh=True)

        assert mock_requests.post.call_args.kwargs["json"]["wait_for_fresh"] is True

    @patch("acquirium.Client.client.requests")
    def test_error_propagation(self, mock_requests, client):
        mock_requests.post.side_effect = Exception("Connection refused")
        with pytest.raises(Exception, match="Connection refused"):
            client.sparql_query("SELECT * WHERE { ?s ?p ?o }")


class TestSparqlUpdate:
    @patch("acquirium.Client.client.requests")
    def test_source_id_targets_an_owned_graph(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"ok": True}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        client.sparql_update("DELETE WHERE { ?s ?p ?o }", source_id="app:demo")

        assert mock_requests.post.call_args.kwargs["json"] == {
            "update": "DELETE WHERE { ?s ?p ?o }",
            "source_id": "app:demo",
        }


class TestGraphStatus:
    @patch("acquirium.Client.client.requests")
    def test_graph_status_and_compatibility_version(self, mock_requests, client):
        mock_requests.get.return_value = _get_resp({
            "source_version": 7,
            "published_version": 6,
            "is_current": False,
            "rebuild_in_progress": True,
        })

        status = client.graph_status()

        assert status["published_version"] == 6
        assert client.graph_version() == 7
        assert mock_requests.get.call_args.args[0].endswith("/graph_version")


# ── resolve ────────────────────────────────────────────────


def _get_resp(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    resp.ok = True
    return resp


class TestResolve:
    @patch("acquirium.Client.client.requests")
    def test_single_text_returns_best_uri(self, mock_requests, client):
        mock_requests.get.return_value = _get_resp(
            {"matches": [{"uri": "urn:a", "score": 0.9}]})
        assert client.resolve("pump", "class") == "urn:a"
        assert "resolve_text" in mock_requests.get.call_args.args[0]

    @patch("acquirium.Client.client.requests")
    def test_single_text_no_match_is_none(self, mock_requests, client):
        mock_requests.get.return_value = _get_resp({"matches": []})
        assert client.resolve("zzzznonexistent") is None

    @patch("acquirium.Client.client.requests")
    def test_top_k_returns_candidates(self, mock_requests, client):
        matches = [{"uri": "urn:a", "score": 0.9}, {"uri": "urn:b", "score": 0.5}]
        mock_requests.get.return_value = _get_resp({"matches": matches})
        assert client.resolve("pump", "class", top_k=3) == matches

    @patch("acquirium.Client.client.requests")
    def test_uri_passthrough_skips_server(self, mock_requests, client):
        assert client.resolve("urn:a", "class") == "urn:a"
        assert client.resolve("urn:a", top_k=3)[0]["match_stage"] == "passthrough"
        mock_requests.get.assert_not_called()

    @patch("acquirium.Client.client.requests")
    def test_record_form_joint_resolution(self, mock_requests, client):
        mock_requests.post.return_value = _get_resp(
            {"matches": {"eu": [{"uri": "urn:gpm"}], "qty": []}})
        out = client.resolve({"eu": ("gal/min", "unit"),
                              "qty": ("flow", "quantity_kind"),
                              "pinned": ("urn:x", "unit"),
                              "empty": (None, "unit")})
        assert out == {"eu": "urn:gpm", "qty": None,
                       "pinned": "urn:x", "empty": None}
        body = mock_requests.post.call_args.kwargs["json"]
        # pinned/None fields never reach the server
        assert {f["name"] for f in body["fields"]} == {"eu", "qty"}

    @patch("acquirium.Client.client.requests")
    def test_kind_param_forwarded(self, mock_requests, client):
        mock_requests.get.return_value = _get_resp({"matches": []})
        client.resolve("pump", "class")
        assert mock_requests.get.call_args.kwargs["params"]["kind"] == "class"


class TestResolveConversion:
    @patch("acquirium.Client.client.requests")
    def test_success(self, mock_requests, client):
        payload = {"from": {"uri": "urn:mgL"}, "to": {"uri": "urn:gL"},
                   "factors": {"from_uri": "urn:mgL", "to_uri": "urn:gL",
                               "compatible": True}}
        mock_requests.post.return_value = _get_resp(payload)
        out = client.resolve_conversion("mg/l", "grams per liter")
        assert out == payload
        assert "resolve_conversion" in mock_requests.post.call_args.args[0]

    @patch("acquirium.Client.client.requests")
    def test_error_becomes_valueerror(self, mock_requests, client):
        resp = MagicMock()
        resp.ok = False
        resp.headers = {"content-type": "application/json"}
        resp.json.return_value = {"detail": "no convertible pair"}
        mock_requests.post.return_value = resp
        with pytest.raises(ValueError, match="no convertible pair"):
            client.resolve_conversion("mg/l", "volts")



# ── insert_log ─────────────────────────────────────────────


class TestInsertLog:
    @patch("acquirium.Client.client.requests")
    def test_insert_log_full(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "ok"}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        result = client.insert_log(
            point_uri="urn:test:p1",
            log_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            observation_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
            observation_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
            log_message="test log",
        )
        assert result["status"] == "ok"

    @patch("acquirium.Client.client.requests")
    def test_insert_log_minimal(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "ok"}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        result = client.insert_log(
            point_uri="urn:test:p1",
            log_message="minimal log",
        )
        assert result["status"] == "ok"


class TestConstructorHealthGate:
    @patch("acquirium.Client.client.requests")
    def test_healthy_server_constructs_immediately(self, mock_requests):
        from acquirium import Acquirium
        resp = MagicMock()
        resp.json.return_value = {"status": "ok"}
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp
        aq = Acquirium(server_url="localhost", server_port=8000)
        assert "health" in mock_requests.get.call_args.args[0]
        assert aq.client.base_url == "http://localhost:8000"

    @patch("acquirium.Client.client.requests")
    def test_unreachable_server_raises_connectionerror(self, mock_requests):
        from acquirium import Acquirium
        mock_requests.get.side_effect = OSError("connection refused")
        with pytest.raises(ConnectionError, match=r"did not answer /health.*Is the server"):
            Acquirium(server_url="localhost", server_port=9999, health_timeout=0.3)

    @patch("acquirium.Client.client.requests")
    def test_health_timeout_none_skips_check(self, mock_requests):
        from acquirium import Acquirium
        Acquirium(server_url="localhost", server_port=9999, health_timeout=None)
        mock_requests.get.assert_not_called()


# ------------------------------------------------------------------ insert_graph


@pytest.mark.parametrize("suffix,expected", [
    (".ttl", "turtle"),
    (".n3", "n3"),
    (".xml", "xml"),
    (".rdf", "xml"),
    (".trix", "trix"),
])
def test_insert_graph_infers_format_from_a_path(tmp_path, suffix, expected):
    path = tmp_path / f"model{suffix}"
    path.write_text("@prefix ex: <urn:ex#> .\nex:a a ex:B .\n")
    client = AcquiriumClient()

    with patch("acquirium.Client.client.requests.post") as post:
        post.return_value = MagicMock(status_code=200)
        client.insert_graph_file(path, source_id="s")

    body = post.call_args.kwargs["json"]
    assert body["format"] == expected
    assert body["rdf_graph"] == path.read_text()


def test_insert_graph_explicit_format_is_not_overridden(tmp_path):
    path = tmp_path / "model.ttl"
    path.write_text("@prefix ex: <urn:ex#> .\nex:a a ex:B .\n")
    client = AcquiriumClient()

    with patch("acquirium.Client.client.requests.post") as post:
        post.return_value = MagicMock(status_code=200)
        client.insert_graph_file(path, format="n3", source_id="s")

    assert post.call_args.kwargs["json"]["format"] == "n3"


def test_insert_graph_file_unknown_suffix_requires_format(tmp_path):
    path = tmp_path / "model.unknown"
    path.write_text("@prefix ex: <urn:ex#> .")
    with pytest.raises(ValueError, match="cannot infer RDF format"):
        AcquiriumClient().insert_graph_file(path, source_id="s")


def test_insert_graph_text_content_defaults_to_turtle():
    client = AcquiriumClient()
    turtle = "@prefix ex: <urn:ex#> .\nex:a a ex:B .\n"

    with patch("acquirium.Client.client.requests.post") as post:
        post.return_value = MagicMock(status_code=200)
        client.insert_graph(turtle, source_id="s")

    body = post.call_args.kwargs["json"]
    assert body["format"] == "turtle"
    assert body["rdf_graph"] == turtle


def test_insert_graph_rejects_path_objects(tmp_path):
    path = tmp_path / "model.ttl"
    path.write_text("@prefix ex: <urn:ex#> .")
    with pytest.raises(TypeError, match="insert_graph_file"):
        AcquiriumClient().insert_graph(path, source_id="s")
