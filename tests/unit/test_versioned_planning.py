"""Planning over model versions: a view's query runs on every version of the
model, and each row gets its life, the times at which it is a row."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from rdflib import Graph, URIRef

import acquirium.Storage.graph_store as graph_store_module
from acquirium.internals.intervals import NEG_INF, POS_INF, from_micros, to_micros
from acquirium.Materialization.models import App, output
from acquirium.Materialization.planner import BindingPlanner, Deployment
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.graph_registry import PLANT_GRAPH_URI
from acquirium.Storage.model_history import ModelHistory

DAY = 86_400 * 1_000_000
BASE = to_micros(datetime(2026, 1, 1, tzinfo=timezone.utc))
REF = "https://brickschema.org/schema/Brick/ref#hasExternalReference"


class FakeOntoEnv:
    def __init__(self, *args, **kwargs):
        self.add = MagicMock()
        self.close = MagicMock()

    @classmethod
    def connect(cls, *args, **kwargs):
        return cls()

    def import_dependencies(self, graph):
        return None


@pytest.fixture
def graphs(tmp_path, monkeypatch):
    monkeypatch.setattr(graph_store_module, "now_micros", lambda: BASE + 100 * DAY)
    monkeypatch.setattr(graph_store_module, "OntoEnv", FakeOntoEnv)
    monkeypatch.setattr(graph_store_module, "load_bundled_graph", lambda fname, canonical: Graph())
    monkeypatch.setattr(graph_store_module.OxigraphGraphStore, "_start_query_rebuild_locked", lambda self: None)
    timeseries = DuckDBStore(tmp_path / "ts.duckdb")
    store = graph_store_module.OxigraphGraphStore(
        store_path=tmp_path / "graph", env_root=tmp_path / "env", history=ModelHistory(timeseries),
    )
    yield store
    store.close()
    timeseries.close()


class EveryMeasurement(App):
    name = "every-measurement"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="input")
    def transform(self, inputs, output, context): pass


def point(n: int) -> str:
    return f"<urn:ex/p{n}> <{REF}> <urn:ex/r{n}> ."


def test_rows_get_their_lives_from_the_model_versions(graphs):
    # p1 is in the model from the start; p2 is added for days 10-20 and p3
    # from day 30 on, both after the fact.
    graphs.insert_graph(point(1), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI))
    graphs.insert_graph(point(2), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI),
                        effective_from=from_micros(BASE + 10 * DAY), effective_until=from_micros(BASE + 20 * DAY))
    graphs.insert_graph(point(3), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI),
                        effective_from=from_micros(BASE + 30 * DAY))
    assert graphs.model_versions() == [(NEG_INF, BASE + 10 * DAY), (BASE + 10 * DAY, BASE + 20 * DAY),
                                       (BASE + 20 * DAY, BASE + 30 * DAY), (BASE + 30 * DAY, POS_INF)]

    history = BindingPlanner(graphs).match_history(Deployment.from_class(EveryMeasurement))
    found = {row.streams["input"][0]["ref_uri"]: life for row, life in history}
    assert found == {
        "urn:ex/r1": [(NEG_INF, POS_INF)],
        "urn:ex/r2": [(BASE + 10 * DAY, BASE + 20 * DAY)],
        "urn:ex/r3": [(BASE + 30 * DAY, POS_INF)],
    }


def test_a_row_that_leaves_and_returns_has_two_runs(graphs):
    graphs.insert_graph(point(1), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI))
    graphs.sparql_update(f"DELETE DATA {{ {point(1)} }}", graph_uri=URIRef(PLANT_GRAPH_URI),
                         effective_from=from_micros(BASE + 5 * DAY), effective_until=from_micros(BASE + 7 * DAY))
    (row, life), = BindingPlanner(graphs).match_history(Deployment.from_class(EveryMeasurement))
    assert life == [(NEG_INF, BASE + 5 * DAY), (BASE + 7 * DAY, POS_INF)]


def test_versions_with_equal_content_share_one_inferred_model(graphs):
    graphs.insert_graph(point(1), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI))
    graphs.sparql_update(f"DELETE DATA {{ {point(1)} }}", graph_uri=URIRef(PLANT_GRAPH_URI),
                         effective_from=from_micros(BASE + 5 * DAY), effective_until=from_micros(BASE + 7 * DAY))
    before = graphs.model_version(BASE)
    after = graphs.model_version(BASE + 8 * DAY)
    assert before is after
    assert graphs.model_version(BASE + 6 * DAY).digest != before.digest
