"""Tests for server helper functions in acquirium.Server.app."""

from datetime import timedelta
import json
from unittest.mock import Mock

import pytest
from escapement import Fault

from acquirium import Actor, App
from acquirium.Server.actors import ActorSupervisor
from acquirium.Server.app import (
    _accepted_sparql_formats,
    _deploy_config_apps,
    _engine_http_error,
    _self_connect_cfg,
    _sparql_results_to_rows,
)
from acquirium.Server.manager import pick_convertible_pair


def test_engine_fault_returns_retryable_http_status():
    error = _engine_http_error(Fault("database connection lost"))
    assert error.status_code == 503
    assert error.headers == {"Retry-After": "1"}
    assert error.detail == "database connection lost"


@pytest.mark.parametrize("entries, valid_actors", [
    pytest.param({"spec": "actor"}, 0, id="table-instead-of-array"),
    pytest.param(["invalid", {"spec": "actor"}], 1, id="bad-entry-before-valid-actor"),
])
def test_bad_actor_config_does_not_block_app_deployment(monkeypatch, entries, valid_actors):
    class Handler:
        def initial_state(self):
            return None

        def on_timer(self, tick):
            raise AssertionError("test should only load the declaration")

    actor = Actor("sample", Handler, {}, {}, {"sample": timedelta(seconds=1)})
    app = App("other-app", {}, {}, {})
    client = Mock()
    supervisor = ActorSupervisor(Mock())
    start = Mock()
    monkeypatch.setattr(supervisor, "start", start)
    monkeypatch.setattr("acquirium.Client.acquirium.Acquirium", Mock(return_value=client))
    monkeypatch.setattr("acquirium.Server.actors.load_app_target", Mock(return_value=actor))
    monkeypatch.setattr("acquirium.Server.app.load_app_target", Mock(return_value=app))

    # A bad actor entry should leave the remaining declarations deployable.
    _deploy_config_apps({"actors": entries, "apps": [{"spec": "app"}]}, supervisor)

    assert start.call_count == valid_actors
    client.deploy_app.assert_called_once_with(app)


def test_actor_class_receives_toml_parameters(monkeypatch):
    class Handler:
        @classmethod
        def configure(cls, client, *, interval_seconds):
            return Actor("sample", cls, {}, {"out": "numeric"},
                         {"sample": timedelta(seconds=interval_seconds)})

        def initial_state(self):
            return None

        def on_timer(self, tick):
            raise AssertionError("test should only load the declaration")

    client = Mock()
    supervisor = ActorSupervisor(Mock())
    start = Mock()
    monkeypatch.setattr(supervisor, "start", start)
    monkeypatch.setattr("acquirium.Server.actors.load_app_target", Mock(return_value=Handler))

    # Load a handler class with a four-second interval from the actor config.
    supervisor.start_configured(client, {
        "actors": [{"spec": "example.py:Handler", "interval_seconds": 4}],
    })

    # Its declaration registers the output and supplies the configured interval to the supervisor.
    client.register_streams.assert_called_once_with([
        {"source_id": "actor:sample", "stream_name": "out", "value_kind": "numeric"},
    ])
    start.assert_called_once_with(
        Actor("sample", Handler, {}, {"out": "numeric"}, {"sample": timedelta(seconds=4)})
    )


def test_local_runtime_self_address_overrides_reusable_driver_config(monkeypatch):
    monkeypatch.setenv("ACQUIRIUM_SELF_HOST", "127.0.0.1")
    monkeypatch.setenv("ACQUIRIUM_SELF_PORT", "49152")
    config = {
        "server": {"port": 8000},
        "driver": {
            "server_url": "acquirium.example.org",
            "server_port": 443,
            "use_ssl": True,
        },
    }

    assert _self_connect_cfg(config) == ("127.0.0.1", 49152, False)


class TestSparqlResultsToRows:
    def test_select_preserves_column_order_values_and_datatypes(self):
        columns = ["s", "label", "n", "d"]
        integer = "http://www.w3.org/2001/XMLSchema#integer"
        date = "http://www.w3.org/2001/XMLSchema#date"
        bindings = [
            {
                "s": {"type": "uri", "value": "urn:a"},
                "label": {"type": "literal", "value": "Pump"},
                "n": {"type": "literal", "value": "2019", "datatype": integer},
            },
            {
                "s": {"type": "uri", "value": "urn:b"},
                "d": {"type": "literal", "value": "2024-03-12", "datatype": date},
            },
        ]
        payload = {"head": {"vars": columns}, "results": {"bindings": bindings}}

        # Keep the lexical values in the rows and put their datatypes in a parallel table.
        assert _sparql_results_to_rows(json.dumps(payload).encode()) == {
            "columns": columns,
            "rows": [["urn:a", "Pump", "2019", None], ["urn:b", None, None, "2024-03-12"]],
            "datatypes": [["iri", None, integer, None], ["iri", None, None, date]],
        }

    def test_ask_preserves_rows_contract(self):
        assert _sparql_results_to_rows(b'{"head":{},"boolean":true}') == {
            "columns": [], "rows": [[True]],
        }


class TestSparqlProtocolNegotiation:
    def test_defaults_to_results_json_and_turtle(self):
        results, graph = _accepted_sparql_formats("*/*")

        assert results.media_type == "application/sparql-results+json"
        assert graph.media_type == "text/turtle"

    def test_honors_weighted_graph_format(self):
        results, graph = _accepted_sparql_formats(
            "application/sparql-results+json;q=0.5, application/n-triples;q=1",
        )

        assert results.media_type == "application/sparql-results+json"
        assert graph.media_type == "application/n-triples"


@pytest.mark.parametrize("compatible, expected", [
    pytest.param({("a1", "b1"), ("a2", "b2")}, ("a1", "b1"), id="first-ranked-pair"),
    pytest.param({("a2", "b1")}, ("a2", "b1"), id="only-compatible-pair"),
    pytest.param({("a1", "b2"), ("a2", "b1"), ("a2", "b2")}, ("a1", "b2"), id="tie-favors-source"),
    pytest.param(set(), None, id="no-compatible-pair"),
])
def test_unit_pair_selection_uses_compatibility_and_rank(compatible, expected):
    assert pick_convertible_pair(["a1", "a2"], ["b1", "b2"], lambda a, b: (a, b) in compatible) == expected
