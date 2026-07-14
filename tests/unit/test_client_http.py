"""Tests for AcquiriumClient HTTP methods with mocked requests."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from rdflib import URIRef

from acquirium.Client.client import AcquiriumClient
from acquirium.Server.direct_client import _DirectClient


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


# ── resolve_text ───────────────────────────────────────────


class TestResolveText:
    @patch("acquirium.Client.client.requests")
    def test_with_matches(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "matches": [{"uri": "urn:a", "score": 0.9}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_resp

        result = client.resolve_text("pump")
        # resolve_text returns response.json().get("matches", []) -> a list
        assert len(result) == 1
        assert result[0]["uri"] == "urn:a"

    @patch("acquirium.Client.client.requests")
    def test_empty_result(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"matches": []}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_resp

        result = client.resolve_text("zzzznonexistent")
        assert result == []

    @patch("acquirium.Client.client.requests")
    def test_with_kind_filter(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"matches": []}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = mock_resp

        client.resolve_text("pump", kind="class")
        call_kwargs = mock_requests.get.call_args
        # Verify kind param was passed
        assert "class" in str(call_kwargs)


class TestResolveRecord:
    @patch("acquirium.Client.client.requests")
    def test_resolve_record_forwards_context(self, mock_requests, client):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"matches": {"unit": [{"uri": "urn:u"}]}}
        mock_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = mock_resp

        out = client.resolve_record(
            {"unit": ("kg", "unit")},
            context=["http://qudt.org/vocab/quantitykind/Mass"],
        )

        assert out["unit"][0]["uri"] == "urn:u"
        body = mock_requests.post.call_args.kwargs["json"]
        assert body["context"] == ["http://qudt.org/vocab/quantitykind/Mass"]

    def test_resolve_record_uris_passes_through_uri_and_uses_it_as_context(self, client):
        captured = {}

        def fake_resolve_record(fields, top_k=5, min_score=0.5, context=None):
            captured["fields"] = fields
            captured["top_k"] = top_k
            captured["min_score"] = min_score
            captured["context"] = context
            return {"unit": [{"uri": "http://qudt.org/vocab/unit/KiloGM"}]}

        client.resolve_record = fake_resolve_record

        out = client.resolve_record_uris({
            "quantity_kind": ("http://qudt.org/vocab/quantitykind/Mass", "quantity_kind"),
            "unit": ("kg", "unit"),
        })

        assert out == {
            "quantity_kind": "http://qudt.org/vocab/quantitykind/Mass",
            "unit": "http://qudt.org/vocab/unit/KiloGM",
        }
        assert captured["fields"] == {"unit": ("kg", "unit")}
        assert captured["top_k"] == 1
        assert captured["min_score"] == 0.5
        assert captured["context"] == ["http://qudt.org/vocab/quantitykind/Mass"]

    def test_resolve_record_uris_passes_through_uriref_and_uses_it_as_context(self, client):
        captured = {}

        def fake_resolve_record(fields, top_k=5, min_score=0.5, context=None):
            captured["fields"] = fields
            captured["top_k"] = top_k
            captured["min_score"] = min_score
            captured["context"] = context
            return {"unit": [{"uri": "http://qudt.org/vocab/unit/KiloGM"}]}

        client.resolve_record = fake_resolve_record
        qk = URIRef("http://qudt.org/vocab/quantitykind/Mass")

        out = client.resolve_record_uris({
            "quantity_kind": (qk, "quantity_kind"),
            "unit": ("kg", "unit"),
        })

        assert out == {
            "quantity_kind": qk,
            "unit": "http://qudt.org/vocab/unit/KiloGM",
        }
        assert captured["fields"] == {"unit": ("kg", "unit")}
        assert captured["top_k"] == 1
        assert captured["min_score"] == 0.5
        assert captured["context"] == ["http://qudt.org/vocab/quantitykind/Mass"]


class TestDirectClientResolveRecord:
    def test_resolve_record_uris_passes_through_uri_and_uses_it_as_context(self):
        manager = MagicMock()
        manager.resolve_record.return_value = {
            "unit": [{"uri": "http://qudt.org/vocab/unit/KiloGM"}]
        }
        client = _DirectClient(manager, origin="test")

        out = client.resolve_record_uris({
            "quantity_kind": ("http://qudt.org/vocab/quantitykind/Mass", "quantity_kind"),
            "unit": ("kg", "unit"),
        })

        assert out == {
            "quantity_kind": "http://qudt.org/vocab/quantitykind/Mass",
            "unit": "http://qudt.org/vocab/unit/KiloGM",
        }
        manager.resolve_record.assert_called_once_with(
            {"unit": ("kg", "unit")},
            top_k=1,
            min_score=0.5,
            context=["http://qudt.org/vocab/quantitykind/Mass"],
        )

    def test_resolve_record_uris_passes_through_uriref_and_uses_it_as_context(self):
        manager = MagicMock()
        manager.resolve_record.return_value = {
            "unit": [{"uri": "http://qudt.org/vocab/unit/KiloGM"}]
        }
        client = _DirectClient(manager, origin="test")
        qk = URIRef("http://qudt.org/vocab/quantitykind/Mass")

        out = client.resolve_record_uris({
            "quantity_kind": (qk, "quantity_kind"),
            "unit": ("kg", "unit"),
        })

        assert out == {
            "quantity_kind": qk,
            "unit": "http://qudt.org/vocab/unit/KiloGM",
        }
        manager.resolve_record.assert_called_once_with(
            {"unit": ("kg", "unit")},
            top_k=1,
            min_score=0.5,
            context=["http://qudt.org/vocab/quantitykind/Mass"],
        )


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
