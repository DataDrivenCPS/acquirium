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


QK = "http://qudt.org/schema/qudt/hasQuantityKind"
TEMPERATURE = "http://qudt.org/vocab/quantitykind/Temperature"


def point(n: int) -> str:
    return f"<urn:ex/p{n}> <{REF}> <urn:ex/r{n}> .\n<urn:ex/p{n}> <{QK}> <{TEMPERATURE}> ."


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


# -------------------- the temporal plan --------------------
from acquirium.Materialization.admission import AdmissionError  # noqa: E402
from acquirium.Materialization.deployments import DeploymentChange  # noqa: E402
from acquirium.Materialization.temporal import view_graph  # noqa: E402


class RawOnly(App):
    name = "raw-only"
    grouping = "per_match"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="input", quantity_kind=TEMPERATURE)
    def transform(self, inputs, output, context): pass


class AverageOfRawOnly(App):
    name = "average"
    grouping = "all_matches"
    outputs = {"mean": output.named("plant-mean", value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="input", app="raw-only")
    def transform(self, inputs, output, context): pass


class ReadsTheMean(App):
    name = "reads-mean"
    grouping = "all_matches"
    outputs = {"out": output.stream(value_kind="numeric")}
    def build_query(self, plant): return plant.query().measurement(alias="mean", app="average")
    def transform(self, inputs, output, context): pass


class EveryMeasurementAgain(EveryMeasurement):
    name = "every-measurement-again"


def deployed(*apps, start=NEG_INF):
    return [DeploymentChange(r, app.name, Deployment.from_class(app).to_json(), start, POS_INF)
            for r, app in enumerate(apps, start=1)]


def test_a_query_sees_generated_outputs_while_their_instances_are_present(graphs):
    graphs.insert_graph(point(1), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI))
    graphs.insert_graph(point(2), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI),
                        effective_from=from_micros(BASE + 10 * DAY), effective_until=from_micros(BASE + 20 * DAY))
    plan = BindingPlanner(graphs).temporal_plan(deployed(RawOnly, AverageOfRawOnly))
    by_view = {d.view: d for d in plan.definitions}
    # raw-only has one instance per sensor; average's rows are their outputs,
    # alive exactly while each instance is present
    raw_outputs = {row.outputs["out"]: row.life for row in by_view["raw-only"].rows}
    average_inputs = {row.streams["input"][0]: row.life for row in by_view["average"].rows}
    assert average_inputs == raw_outputs
    assert sorted(raw_outputs.values()) == [((NEG_INF, POS_INF),), ((BASE + 10 * DAY, BASE + 20 * DAY),)]
    assert plan.schedule.selection_order.index("raw-only") < plan.schedule.selection_order.index("average")


def test_a_named_output_exists_wherever_its_view_is_deployed(graphs):
    # average has no input rows at all before day 10, yet its named output
    # exists and reads-mean's query binds it over the whole deployment
    graphs.insert_graph(point(1), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI),
                        effective_from=from_micros(BASE + 10 * DAY))
    changes = deployed(AverageOfRawOnly, ReadsTheMean)
    changes.append(DeploymentChange(3, RawOnly.name, Deployment.from_class(RawOnly).to_json(),
                                    BASE + 10 * DAY, POS_INF))
    plan = BindingPlanner(graphs).temporal_plan(changes)
    (row,) = {d.view: d for d in plan.definitions}["reads-mean"].rows
    assert row.life == ((NEG_INF, POS_INF),)
    # reading a named output is a read edge of the view graph, not selection
    graph = view_graph(plan.definitions)
    assert ("average", "reads-mean", 0) in graph.reads
    assert ("reads-mean", "average") not in graph.selects


def test_queries_that_select_each_others_generated_outputs_are_rejected(graphs):
    # two unfiltered views each match the other's generated outputs
    graphs.insert_graph(point(1), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI))
    with pytest.raises(AdmissionError, match="generated outputs"):
        BindingPlanner(graphs).temporal_plan(deployed(EveryMeasurement, EveryMeasurementAgain))


def test_a_query_never_sees_its_own_views_outputs(graphs):
    # unfiltered, every-measurement also matches measurements other views
    # write, but never its own outputs, so it settles on the raw stream
    graphs.insert_graph(point(1), format="nt", graph_uri=URIRef(PLANT_GRAPH_URI))
    plan = BindingPlanner(graphs).temporal_plan(deployed(EveryMeasurement))
    (definition,) = plan.definitions
    assert [row.streams["input"] for row in definition.rows] == [("urn:ex/r1",)]
