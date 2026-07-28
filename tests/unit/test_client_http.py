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

        client.insert_graph("@prefix ex: <urn:ex/> .", replace=False)

        mock_requests.post.assert_called_once()
        assert mock_requests.post.call_args.args[0] == "http://localhost:8000/insert_graph"


# ── sparql_query ───────────────────────────────────────────


class TestSparqlQuery:
    @patch("acquirium.Client.client.requests")
    def test_success(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"results": {"bindings": []}}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_resp

        result = client.sparql_query("SELECT * WHERE { ?s ?p ?o }")
        assert result == {"results": {"bindings": []}}
        mock_requests.get.assert_called_once()

    @patch("acquirium.Client.client.requests")
    def test_error_propagation(self, mock_requests, client):
        mock_requests.get.side_effect = Exception("Connection refused")
        with pytest.raises(Exception, match="Connection refused"):
            client.sparql_query("SELECT * WHERE { ?s ?p ?o }")


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



# ── register_app / run_app / stop_app / list_app_runs ──────


class TestAppMethods:
    @patch("acquirium.Client.client.requests")
    def test_register_app_success(self, mock_requests, client):
        from acquirium.internals.models import AppSpec

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"app_id": "app123"}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        spec = AppSpec(name="test_app")
        result = client.register_app(spec)
        assert result["app_id"] == "app123"

    @patch("acquirium.Client.client.requests")
    def test_run_app_success(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"run_id": "run456"}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        result = client.run_app("app123")
        assert result["run_id"] == "run456"

    @patch("acquirium.Client.client.requests")
    def test_stop_app_success(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "stopped"}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        result = client.stop_app(run_id="run456")
        assert result["status"] == "stopped"

    @patch("acquirium.Client.client.requests")
    def test_list_app_runs_success(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"runs": [{"run_id": "r1"}, {"run_id": "r2"}]}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_resp

        result = client.list_app_runs()
        assert len(result["runs"]) == 2

    @patch("acquirium.Client.client.requests")
    def test_list_app_runs_empty(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"runs": []}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_resp

        result = client.list_app_runs()
        assert result["runs"] == []


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
