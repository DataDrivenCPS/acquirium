"""Experiment metadata: saved through the server, and the ledger's graph handling in-process.

Tests of what users see go through a local server started with aq.init(). The
ones that need a failure, a count of graph work, or a graph edited underneath
the ledger build the server's ledger in-process against a memory graph.
"""
import json
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import polars as pl
import pytest
from rdflib import Dataset, Literal, URIRef
from rdflib.namespace import RDF
from requests import HTTPError

from acquirium.Client.explore.attributes import ATTR_NS
from acquirium.ExperimentMetadata import EXPERIMENT, variable_record, annotation_uri, run_uri, variable_uri
from acquirium.Experiments import ExperimentStore
from acquirium.Storage.ledger import DuckDBLedger
from acquirium.Storage.graph_registry import source_graph_uri
from acquirium.internals.internals_namespaces import HAS_UNIT

PSI = "http://qudt.org/vocab/unit/PSI"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class MemoryGraphStore:
    """An RDF store without inference; queries and updates execute as SPARQL."""

    def __init__(self):
        self.dataset = Dataset(default_union=True)
        self.version = 0
        self.updates = 0

    source_graph_uri = staticmethod(source_graph_uri)

    def insert_graph(self, content, *, replace, graph_uri):
        graph = self.dataset.graph(graph_uri)
        if replace:
            graph.remove((None, None, None))
        for triple in content:
            graph.add(triple)
        self.version += 1

    def sparql_update(self, update, *, graph_uri):
        self.dataset.graph(graph_uri).update(update)
        self.version += 1
        self.updates += 1

    def metadata_update(self, update, *, graph_uri):
        self.dataset.update(update)
        self.version += 1
        self.updates += 1

    def query(self, query, **kwargs):
        payload = json.loads(self.dataset.query(query).serialize(format="json"))
        columns = payload["head"]["vars"]
        rows, datatypes = [], []
        for binding in payload["results"]["bindings"]:
            rows.append([binding.get(name, {}).get("value") for name in columns])
            datatypes.append([
                "iri" if binding.get(name, {}).get("type") == "uri"
                else binding.get(name, {}).get("datatype")
                for name in columns
            ])
        return {"columns": columns, "rows": rows, "datatypes": datatypes}

    sparql_query = query

    def clear_graphs(self, graph_uris=(), *, prefix=None):
        targets = {URIRef(uri) for uri in graph_uris}
        if prefix is not None:
            targets.update(graph.identifier for graph in self.dataset.graphs() if str(graph.identifier).startswith(prefix))
        for uri in targets:
            self.dataset.graph(uri).remove((None, None, None))
        self.version += 1
        return len(targets)

    def flush_metadata(self):
        pass

    def metadata_documents(self, subjects):
        from acquirium.Metadata import read_documents
        from types import SimpleNamespace
        return read_documents(SimpleNamespace(sparql_query=self.sparql_query), subjects)

    def metadata_triples(self, subjects, *, graph_uri):
        graph = self.dataset.graph(URIRef(graph_uri))
        return {str(subject): [[str(s), str(p), o.n3()] for s,p,o in graph.triples((URIRef(subject), None, None))]
                for subject in subjects}

    def graph_status(self):
        return {"source_version": self.version}



def _values(ac, sparql):
    """The first column of a query against the server's graph."""
    return [row[0] for row in ac.client.sparql_query(sparql)["rows"]]


def _mentioning(ac, identifier):
    """Nodes in the server's graph whose URI or values mention ``identifier``."""
    return set(_values(ac, f"SELECT DISTINCT ?s WHERE {{ ?s ?p ?o "
                           f"FILTER(CONTAINS(STR(?s), '{identifier}') || CONTAINS(STR(?o), '{identifier}')) }}"))


# Through the server


def test_metadata_filters_are_scoped_and_observations_follow_selection(ac):
    study = ac.study.define("cases")
    cost = study.output("cost").scalar()
    first = study.start({"scenario": "baseline", "settings": {"flow": 10}, "tags": ["a", "b"]})
    cost.record(12.5)
    first.finish()
    # Attribute discovery must work before any collection query has run.
    assert ac.attr.settings.flow.name == "settings.flow"
    second = study.start({"scenario": "trial", "settings": {"flow": 20}, "tags": ["b"]})
    cost.record(11.0)
    second.finish()
    other = ac.study.define("other")
    other.start({"scenario": "baseline", "settings": {"flow": 100}})

    assert [run.run_id for run in study.experiments.where(scenario="baseline").all()] == [first.run_id]
    high_flow = study.experiments.where(ac.attr.settings.flow >= 15)
    assert [run.run_id for run in high_flow.all()] == [second.run_id]
    assert high_flow.observations(cost).frame()["value"].to_list() == [11.0]
    assert len(study.experiments.where(tags="b").all()) == 2
    absent = study.experiments.where(scenario="absent")
    assert absent.all() == []
    assert absent.observations().all() == []

    query = study.experiments.query().include("settings", "tags")
    frame = query.metadata(include_dependencies=False)
    assert frame.height == 2
    assert frame.schema["experiment.settings"] == pl.Struct({"flow": pl.Int64})
    assert frame.schema["experiment.tags"] == pl.List(pl.String)


def test_scalar_variable_unit_strings_and_uris_share_semantic_filter(ac):
    study = ac.study.define("pressure")
    pressure = study.output("pressure").scalar(unit="PSI", settings={"sensor": "A"})
    other = ac.study.define("other")
    other.output("pressure").scalar(unit="PSI")

    assert study.variables.where(unit="psi").all() == [pressure]
    assert study.variables.where(unit=URIRef(PSI)).all() == [pressure]
    assert pressure.unit == "PSI"  # Saved objects retain their original shape and values.
    assert _values(ac, f"SELECT ?unit WHERE {{ <{pressure.uri}> <{HAS_UNIT}> ?unit }}") == [PSI]
    frame = study.variables.query().include("unit").metadata(include_dependencies=False)
    assert frame["variable.unit"].to_list() == ["unit:PSI"]
    objects = study.variables.query().include("unit", "unit.raw").metadata_objects(include_dependencies=False)
    assert objects[0]["variable"]["unit"] == PSI
    assert objects[0]["variable"]["unit.raw"] == "PSI"
    assert study.variables.where(**{"unit.raw": "PSI"}).all() == [pressure]
    assert study.variables.where(role="input").where(unit="PSI").all() == []
    assert study.variables.where(role="input").query().metadata().height == 0


def test_server_reads_return_saved_metadata(ac):
    study = ac.study.define("recorded objects")
    pressure = study.output("pressure").scalar(unit="PSI", settings={"mode": "baseline"})
    run = study.start({"settings": {"mode": "baseline"}, "tags": ["A", "B"]})
    pressure.record(1)
    pressure.record(2)
    run.annotate_stream("urn:site:stream", start=NOW, metadata={"treatment": "A"})
    run.finish()
    study.start({"settings": {"mode": "trial", "duration": 60}}).finish()
    study.output("other").scalar(unit=PSI, settings={"duration": 60})

    reopened = ac.study.get("recorded objects")
    saved = {"settings": {"mode": "baseline"}, "tags": ["A", "B"]}
    assert reopened.variables["pressure"].metadata == {"settings": {"mode": "baseline"}}
    assert reopened.experiments.get(run.run_id).metadata == saved
    assert [item.metadata for item in reopened.experiments.all()] == [saved, {"settings": {"mode": "trial", "duration": 60}}]
    assert ac.client.list_experiments(study.study_id, run_ids=[run.run_id])[0]["metadata"] == saved
    observations = ac.client.list_experiment_observations(study.study_id)
    assert ac.client.list_experiment_observations(study.study_id, run_ids=[run.run_id]) == observations
    assert [item["experiment_metadata"] for item in observations] == [saved] * 2
    assert [item["variable_metadata"] for item in observations] == [reopened.variables["pressure"].declaration.document()] * 2
    assert reopened.stream_annotations()[0].metadata == {"treatment": "A"}


@pytest.mark.parametrize("values", [
    {"settings": {"enabled": True, "threshold": 1.5}},
    {"tags": ["A", "B", "A"]},
    {"items": [{"name": "A"}, {"name": "B", "count": 2}]},
    {"matrix": [[1, 2], [3, 4]]},
    {"tags": ["A", None, "B"]},
    {"datasheet": "https://example.org/input"},
    {"id": "pump-7"},
    {"id": "urn:example:pump-7"},
])
def test_shared_metadata_object_round_trip(ac, values):
    study = ac.study.define("round trip")
    run = study.start(values)
    assert ac.study.get("round trip").experiments.get(run.run_id).metadata == values


def test_native_objects_keep_each_runs_own_fields_without_experiment_cleanup(ac):
    study = ac.study.define("native field presence")
    baseline = study.start({"settings": {"mode": "baseline"}})
    baseline.finish()
    trial = study.start({"settings": {"mode": "trial", "duration": 60}})
    trial.finish()
    query = study.experiments.query().include("settings")
    objects = query.metadata_objects()
    settings_by_run = {item["experiment"]["id"]: item["experiment"]["settings"] for item in objects}
    assert settings_by_run == {
        baseline.uri: {"mode": "baseline"},
        trial.uri: {"mode": "trial", "duration": 60},
    }
    settings = query.metadata()["experiment.settings"].to_list()
    baseline_settings = next(item for item in settings if item["mode"] == "baseline")
    assert baseline_settings == {
        "duration": None, "mode": "baseline",
    }
    single = query.where(run_id=baseline.run_id).metadata_objects()
    assert single == [{"experiment": {"id": baseline.uri, "settings": {"mode": "baseline"}}}]


def test_null_empty_metadata_is_removed_but_recorded_json_is_preserved(ac):
    study = ac.study.define("values")
    output = study.output("result").json(unit=None, tags=[], info={}, note="")
    run = study.start({"tags": [], "notes": None, "settings": {"flow": 10, "unused": {}}})
    output.record({"tags": [], "notes": None, "settings": {}})
    assert output.metadata == {"note": "", "tags": [], "info": {}}
    assert run.metadata == {"settings": {"flow": 10, "unused": {}}}
    assert run.observations().latest().value == {"tags": [], "notes": None, "settings": {}}


def test_selection_refreshes_after_writes(ac):
    study = ac.study.define("recovery")
    study.start({"scenario": "baseline"}).finish()
    selected = study.experiments.where(scenario="trial")
    assert selected.all() == []
    saved = study.start({"scenario": "trial"})
    assert [run.run_id for run in selected.all()] == [saved.run_id]
    saved.finish()
    assert study.experiments.where(status="succeeded", scenario="trial").all()[0].run_id == saved.run_id


def test_lifecycle_writes_keep_metadata_edits(ac):
    study = ac.study.define("concurrent lifecycle")
    run = study.start({"scenario": "baseline"})
    ac.insert_metadata(run.uri, {"scenario": "edited"})
    run.finish()
    assert ac.client.get_experiment(run.run_id)["metadata"] == {"scenario": "edited"}
    assert len(study.experiments.where(scenario="edited", status="succeeded").all()) == 1


def test_variable_metadata_edits_keep_the_user_namespace(ac):
    study = ac.study.define("edited declaration")
    study.output("pressure").scalar(unit="PSI", metadata={"saved": [None, {}, []]})
    ac.insert_metadata(study.output["pressure"].uri, {"category": "edited"})
    reopened = ac.study.get("edited declaration").output["pressure"]
    assert reopened.metadata == {"saved": [None, {}, []], "category": "edited"}
    assert reopened.unit == "PSI"


def test_invalid_metadata_fails_before_anything_is_saved(ac):
    study = ac.study.define("validation")
    with pytest.raises(HTTPError, match="[Cc]ould not resolve"):
        study.output("pressure").scalar(unit="not-a-unit")
    with pytest.raises(HTTPError, match="reserved"):
        study.start({"status": "pretend"})
    with pytest.raises(HTTPError, match="invalid metadata key"):
        study.start({"solver.options": 1})
    assert ac.client.list_experiment_variables(study.study_id) == []
    assert ac.client.list_experiments(study.study_id) == []


def test_empty_study_native_queries_and_lifecycle_filter_override(ac):
    study = ac.study.define("empty")
    assert study.variables.where(role="input").query().metadata().height == 0
    assert study.experiments.where(status="succeeded").query().metadata().height == 0
    run = study.start({"scenario": "baseline"})
    selected = study.experiments.where(status="running", scenario="baseline")
    assert [item.run_id for item in selected.all()] == [run.run_id]
    run.finish()
    assert selected.all() == []
    assert [item.run_id for item in selected.where(status="succeeded").all()] == [run.run_id]


def test_graph_queries_see_writes_made_just_before(ac):
    study = ac.study.define("discovery")
    run = study.start({"operator": "ana"})
    assert _values(ac, f"SELECT ?v WHERE {{ <{run.uri}> <{ATTR_NS}operator> ?v }}") == ["ana"]


def test_each_new_run_key_is_filterable_right_after_the_run(ac):
    study = ac.study.define("keys")
    for index in range(4):
        run = study.start({f"setting{index}": index})
        run.finish()
        # Discovery is cached by graph version; each new key must still be found.
        selected = study.experiments.where(getattr(ac.attr, f"setting{index}") == index)
        assert [found.run_id for found in selected.all()] == [run.run_id]


def test_deleted_records_leave_the_metadata_graph(ac):
    study = ac.study.define("cases")
    cost = study.output("cost").scalar(unit="PSI")
    level = study.output("level").timeseries()
    runs = []
    for scenario in ("baseline", "trial"):
        run = study.start({"scenario": scenario})
        cost.record(1.0)
        level.record([(NOW, 1.0)])
        run.finish()
        runs.append(run)
    annotation = runs[0].annotate_stream("urn:plant:flow", start=NOW)
    # A run's node, its annotations, and its recorded streams all name the run.
    assert {run_uri(runs[0].run_id), annotation_uri(annotation.annotation_id)} < _mentioning(ac, runs[0].run_id)

    runs[0].delete()
    assert _mentioning(ac, runs[0].run_id) == set()
    assert run_uri(runs[1].run_id) in _mentioning(ac, runs[1].run_id)
    assert study.experiments.where(scenario="baseline").all() == []

    study.delete()
    assert _mentioning(ac, runs[1].run_id) == set()
    assert _mentioning(ac, study.study_id) == set()

    other = ac.study.define("other")
    other_level = other.output("level").timeseries()
    run = other.start({"scenario": "baseline"})
    other_level.record([(NOW, 1.0)])
    run.finish()
    ac.study.clear(confirm=True)
    assert _mentioning(ac, run.run_id) == set()
    assert _mentioning(ac, other.study_id) == set()
    assert ac.study.all() == []


def test_scalars_follow_attribute_selections(ac):
    study = ac.study.define("sweep")
    cost = study.output("cost").scalar(unit="PSI")
    for flow in (10, 20, 30):
        run = study.start({"settings": {"flow": flow}})
        cost.record(flow * 1.5)
        run.finish()
    high_flow = study.experiments.where(ac.attr.settings.flow >= 15)
    assert high_flow.scalars(metadata=["settings.flow"]).select("settings.flow", "cost").rows() == [(20, 30.0), (30, 45.0)]


def test_native_experiment_objects_use_shared_lossless_documents(ac):
    study = ac.study.define("native documents")
    run = study.start({"tags": ["A", None, "B"], "metadata": {"any.key": [None, {}, []]}})
    obj = study.experiments.query().include("tags", "metadata").metadata_objects()[0]["experiment"]
    assert obj["tags"] == ["A", None, "B"]
    assert obj["metadata"] == {"any.key": [None, {}, []]}
    assert ac.client.get_experiment(run.run_id)["metadata"]["tags"] == obj["tags"]


def test_compatibility_metadata_filters_match_complete_saved_fields(ac):
    study = ac.study.define("exact metadata")
    first = study.start({"tags": ["A", "B"], "settings": {"flow": 10}})
    first.finish()
    study.start({"tags": ["B", "A"], "settings": {"flow": 10, "mode": "trial"}}).finish()
    study.start({"tags": ["A", None, "B"]}).finish()
    runs = lambda metadata: [item["run_id"] for item in ac.client.list_experiments(study.study_id, metadata=metadata)]
    assert runs({"tags": ["A", "B"]}) == [first.run_id]
    assert runs({"settings": {"flow": 10}}) == [first.run_id]
    assert len(runs({"absent": None})) == 3


def test_variable_user_queries_use_namespace_and_semantics_keep_their_paths(ac):
    study = ac.study.define("normalized query paths")
    variable = study.output("pressure").scalar(unit="PSI", category="simulation")
    assert variable.metadata == {"category": "simulation"}
    assert variable.unit == "PSI"
    assert study.variables.where(**{"metadata.category": "simulation"}).all() == [variable]
    assert study.variables.where(unit="PSI").all() == [variable]
    with pytest.raises(ValueError, match="category"):
        study.variables.where(category="simulation").all()


# The ledger in-process


@pytest.fixture
def local(tmp_path, samples):
    """The server's ledger on a DuckDB store and a memory graph."""
    store = DuckDBLedger(tmp_path / "experiments.duckdb", samples=samples, recreate=True)
    graph = MemoryGraphStore()

    def resolve(text, kind):
        return PSI if kind == "unit" and text in {"PSI", "psi"} else None

    ledger = ExperimentStore(store, graph_store=graph, resolve=resolve)
    yield SimpleNamespace(store=store, graph=graph, ledger=ledger)
    store.close()


def _study(ledger, name, variables=(), runs=()):
    """Define a Study, declare ``(label, kind, metadata)`` outputs, start runs with metadata."""
    study = ledger.define(name)["template_id"]
    declared = [ledger.declare(study, label, "output", kind, metadata) for label, kind, metadata in variables]
    started = [ledger.start(study, metadata) for metadata in runs]
    return study, declared, started


def _pending(store):
    with store._own_conn() as conn:
        return conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0]


def test_saved_metadata_reads_shared_objects_instead_of_sql_values(local, monkeypatch):
    ledger = local.ledger
    study, declared, _ = _study(ledger, "objects", [
        ("pressure", "scalar", {"unit": "PSI", "settings": {"mode": "baseline"}}),
        ("other pressure", "scalar", {"unit": PSI, "settings": {"duration": 60}})])
    saved = ledger.variables(study)
    stale = [{**item, "metadata": {"stale_sql_value": True}} for item in saved]

    import acquirium.ExperimentMetadata as shared
    original = shared.read_documents
    calls = []

    def tracked(client, subjects, **kwargs):
        calls.append(subjects)
        return original(client, subjects, **kwargs)

    monkeypatch.setattr(shared, "read_documents", tracked)
    result = ledger.read_metadata(stale, EXPERIMENT.Variable, "variable_id")
    assert [item["metadata"] for item in result] == [item["metadata"] for item in saved]
    assert calls and all(set(subjects) == {item["variable_id"] for item in declared} for subjects in calls)


def test_container_shapes_come_from_shared_objects(local):
    ledger = local.ledger
    study, _, (first,) = _study(local.ledger, "containers", runs=[{"tag": ["A", "B"]}])
    ledger.finish(first["run_id"], "succeeded")
    values = {"items": [{"name": "A"}, {"name": "B", "count": 2}], "matrix": [[1, 2], [3, 4]], "tag": "A",
              "id": "pump-7"}
    run = ledger.start(study, values)
    record = {**ledger.run(run["run_id"]), "metadata": {}}  # Without SQL snapshots, these must come from the graph.
    assert ledger.read_metadata([record], EXPERIMENT.Run, "run_id")[0]["metadata"] == values


def test_legacy_uuid_declarations_backfill_without_changing_observation_ids(local):
    ledger, store, graph = local.ledger, local.store, local.graph
    study, (variable,), (run,) = _study(ledger, "legacy declarations", [("pressure", "scalar", {"unit": "PSI"})],
                                        [{"scenario": "baseline"}])
    ledger.observe(run["run_id"], variable["variable_id"], value=12.5)
    ledger.finish(run["run_id"], "succeeded")
    legacy_id = "b54c74d2-cfa6-4c08-a732-a5f7fefb5c4d"
    with store._lock, store._write_conn() as conn:
        conn.execute("UPDATE experiment_variables SET variable_id=? WHERE variable_id=?", [legacy_id, variable["variable_id"]])
        conn.execute("UPDATE experiment_observations SET variable_id=? WHERE variable_id=?", [legacy_id, variable["variable_id"]])
        from acquirium.Storage.metadata_outbox import enqueue
        record = variable_record(study, dict(variable_id=legacy_id, label="pressure", role="output", kind="scalar", metadata={"unit": "PSI"}, created_at=datetime.now(timezone.utc)))
        ledger._queue_metadata(conn, ledger.metadata_graph.record_update(record))
        enqueue(conn, {"operation": "delete", "subject": variable_uri(variable["variable_id"])})
    declarations = ledger.variables(study)
    assert [item["variable_id"] for item in declarations] == [legacy_id]
    assert declarations[0]["metadata"]["unit"] == "PSI"
    legacy = URIRef(variable_uri(legacy_id))
    assert (legacy, RDF.type, EXPERIMENT.Variable) in graph.dataset
    assert (legacy, HAS_UNIT, URIRef(PSI)) in graph.dataset
    observations = ledger.observations(study)
    assert [(item["variable_id"], item["value"]) for item in observations] == [(legacy_id, 12.5)]


def test_object_reads_repair_failed_publication_and_do_not_hide_read_errors(local, monkeypatch):
    ledger, store, graph = local.ledger, local.store, local.graph
    _, _, (run,) = _study(ledger, "read recovery", runs=[{"scenario": "baseline"}])
    with monkeypatch.context() as patch:
        patch.setattr(graph, "metadata_update", Mock(side_effect=OSError("graph unavailable")))
        with pytest.raises(OSError, match="graph unavailable"):
            ledger.read_metadata([ledger.run(run["run_id"])], EXPERIMENT.Run, "run_id")
    assert _pending(store) > 0
    result = ledger.read_metadata([ledger.run(run["run_id"])], EXPERIMENT.Run, "run_id")
    assert result[0]["metadata"] == {"scenario": "baseline"}
    assert _pending(store) == 0
    monkeypatch.setattr(graph, "sparql_query", Mock(side_effect=OSError("read failed")))
    with pytest.raises(OSError, match="read failed"):
        ledger.read_metadata([ledger.run(run["run_id"])], EXPERIMENT.Run, "run_id")


def test_backfill_annotations_and_restart_preserve_source_objects(local):
    ledger, graph = local.ledger, local.graph
    _, _, (saved,) = _study(ledger, "old", runs=[{"scenario": "baseline"}])
    item = ledger.annotate_stream(saved["run_id"], "urn:site:stream", NOW, metadata={"treatment": "A"})
    ledger.close_stream_annotation(item["annotation_id"], NOW)
    ledger.sync_metadata_graph()
    reopened = ExperimentStore(local.store, graph_store=graph, resolve=ledger.metadata_graph.resolve)
    reopened.sync_metadata_graph()
    assert (URIRef(run_uri(saved["run_id"])), RDF.type, EXPERIMENT.Run) in graph.dataset
    assert (URIRef(annotation_uri(item["annotation_id"])), URIRef(ATTR_NS + "treatment"), Literal("A")) in graph.dataset
    assert reopened.run(saved["run_id"])["metadata"] == {"scenario": "baseline"}


class NamespacedGraphStore(MemoryGraphStore):
    """A graph that binds namespaces, as the server's graph store does."""

    def namespace_manager(self):
        self.dataset.bind("plant", "urn:plant#")
        return self.dataset.namespace_manager


def test_uri_values_under_a_bound_namespace_are_written_as_uris(tmp_path, samples):
    store = DuckDBLedger(tmp_path / "experiments.duckdb", samples=samples, recreate=True)
    graph = NamespacedGraphStore()
    ledger = ExperimentStore(store, graph_store=graph, resolve=lambda *_: None)
    study = ledger.define("namespaces")["template_id"]
    values = {"feeds": "urn:plant#tank", "datasheet": "https://example.org/ds.pdf"}
    variable = ledger.declare(study, "level", "output", "scalar", dict(values))
    run = ledger.start(study, dict(values))
    annotation = ledger.annotate_stream(run["run_id"], "urn:site:stream", NOW, metadata=dict(values))
    ledger.sync_metadata_graph()
    # a variable keeps its metadata under the metadata key
    for subject, prefix in ((variable_uri(variable["variable_id"]), "metadata."), (run_uri(run["run_id"]), ""),
                            (annotation_uri(annotation["annotation_id"]), "")):
        assert (URIRef(subject), URIRef(ATTR_NS + prefix + "feeds"), URIRef("urn:plant#tank")) in graph.dataset
        assert (URIRef(subject), URIRef(ATTR_NS + prefix + "datasheet"),
                Literal("https://example.org/ds.pdf")) in graph.dataset
    assert ledger.run(run["run_id"])["metadata"] == values
    store.close()


def test_recording_values_does_no_graph_work(local):
    ledger, graph = local.ledger, local.graph
    _, (cost,), (run,) = _study(ledger, "values", [("cost", "scalar", {})], [{}])
    ledger.sync_metadata_graph()
    before = graph.updates
    ledger.observe(run["run_id"], cost["variable_id"], value=1)
    ledger.observe(run["run_id"], cost["variable_id"], value=2)
    assert graph.updates == before


def test_missing_authoritative_document_fails_without_sql_fallback(local):
    from acquirium.Metadata import document_predicate
    ledger = local.ledger
    _, _, (run,) = _study(ledger, "authoritative", runs=[{"scenario": "baseline"}])
    ledger.sync_metadata_graph()
    local.graph.dataset.remove((URIRef(run_uri(run["run_id"])), document_predicate("run_id"), None))
    with pytest.raises(RuntimeError, match="no document"):
        ledger.run(run["run_id"])
    with local.store._own_conn() as conn:
        assert conn.execute("SELECT status FROM experiment_runs WHERE run_id=?", [run["run_id"]]).fetchone() == ("running",)


@pytest.mark.parametrize("missing_kind", [EXPERIMENT.Run, EXPERIMENT.Variable])
def test_observation_hydration_requires_both_authoritative_documents(local, missing_kind):
    from acquirium.ExperimentMetadata import node_uri
    from acquirium.Metadata import document_predicate
    ledger = local.ledger
    _, (variable,), (run,) = _study(ledger, "missing observation metadata", [("result", "scalar", {})], [{}])
    ledger.sync_metadata_graph()
    identifier, field = ((run["run_id"], "run_id") if missing_kind == EXPERIMENT.Run
                         else (variable["variable_id"], "variable_id"))
    local.graph.dataset.remove((URIRef(node_uri(missing_kind, identifier)), document_predicate(field), None))
    with pytest.raises(RuntimeError, match="no document"):
        ledger.metadata.read_observation_metadata([
            {"experiment_id": run["run_id"], "variable_id": variable["variable_id"]}])


def test_hydration_batches_kinds_legacy_ids_and_duplicate_rows(local, monkeypatch):
    from acquirium.Metadata import document_triples
    ledger, graph = local.ledger, local.graph
    study, (variable,), (run,) = _study(ledger, "mixed hydration", [("pressure", "scalar", {"unit": "psi"})],
                                        [{"metadata": {"exact.key": [None, {}, []]}}])
    declaration = ledger.variables(study)[0]["metadata"]
    run_id, variable_id = run["run_id"], variable["variable_id"]
    # A legacy variable's bare UUID can equal a run ID; they remain distinct nodes.
    legacy = {"unit": "PSI", "metadata": {"legacy": [None, {}, []]}}
    experiments = graph.dataset.graph(source_graph_uri("experiments"))
    for triple in document_triples(variable_uri(run_id), {"variable_id": run_id, **legacy}):
        experiments.add(triple)
    read = Mock(wraps=graph.metadata_documents)
    monkeypatch.setattr(graph, "metadata_documents", read)
    rows = [{"experiment_id": run_id, "variable_id": identifier, "value": index}
            for index, identifier in enumerate([run_id, variable_id, run_id])]
    result = ledger.metadata.read_observation_metadata(rows)
    assert [item["value"] for item in result] == [0, 1, 2]
    assert [item["variable_metadata"] for item in result] == [legacy, declaration, legacy]
    assert all(item["experiment_metadata"] == {"metadata": {"exact.key": [None, {}, []]}} for item in result)
    read.assert_called_once()
    assert set(read.call_args.args[0]) == {run_uri(run_id), variable_uri(variable_id), variable_uri(run_id)}
    assert ledger.metadata.read(EXPERIMENT.Variable, [run_id, run_id]) == {run_id: legacy}
    read.reset_mock()
    assert ledger.metadata.read(EXPERIMENT.Variable, []) == {}
    assert ledger.metadata.read_observation_metadata([]) == []
    assert ledger.metadata.read_metadata([], EXPERIMENT.Run, "run_id") == []
    read.assert_not_called()


def test_restart_retains_resolved_units_without_a_resolver(local):
    ledger, graph = local.ledger, local.graph
    study, (pressure,), (run,) = _study(ledger, "pinned units", [("pressure", "scalar", {"unit": "psi"})],
                                        [{"scenario": "baseline"}])
    declaration = ledger.variables(study)[0]["metadata"]
    ledger.sync_metadata_graph()
    reopened = ExperimentStore(local.store, graph_store=graph, resolve=lambda text, kind: None)
    assert reopened.variables(study)[0]["metadata"] == declaration
    reopened.finish(run["run_id"], "succeeded")
    assert reopened.run(run["run_id"])["metadata"] == {"scenario": "baseline"}
    assert (URIRef(variable_uri(pressure["variable_id"])), HAS_UNIT, URIRef(PSI)) in graph.dataset
    missing = ExperimentStore(local.store, graph_store=MemoryGraphStore())
    with pytest.raises(RuntimeError, match="restore its graph backup"):
        missing.run(run["run_id"])


def test_reads_wait_for_durable_graph_delivery(local, monkeypatch):
    ledger, graph = local.ledger, local.graph
    _, _, (run,) = _study(ledger, "publication race", runs=[{"scenario": "a"}])
    entered, release = threading.Event(), threading.Event()
    update = graph.metadata_update

    def slow_update(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return update(*args, **kwargs)
    monkeypatch.setattr(graph, "metadata_update", slow_update)
    results = []
    first = threading.Thread(target=lambda: results.append(ledger.run(run["run_id"])))
    first.start()
    assert entered.wait(5)
    second = threading.Thread(target=lambda: results.append(ledger.run(run["run_id"])))
    second.start()
    second.join(.1)
    assert second.is_alive()
    release.set()
    first.join(5)
    second.join(5)
    assert len(results) == 2
    assert all(item["metadata"] == {"scenario": "a"} for item in results)


def test_writes_between_reads_publish_in_one_graph_update(local):
    ledger, graph = local.ledger, local.graph
    study, (pressure,), _ = _study(ledger, "sweep", [("pressure", "scalar", {"unit": "psi"})])
    ledger.sync_metadata_graph()
    before = graph.updates
    for flow in range(20):
        run = ledger.start(study, {"settings": {"flow": flow}})
        ledger.observe(run["run_id"], pressure["variable_id"], value=flow)
        ledger.finish(run["run_id"], "succeeded")
    assert graph.updates == before  # writes only queue their nodes
    # The server publishes queued writes before it answers a graph query.
    ledger.publish_queued_metadata()
    assert graph.updates == before + 1
    # Each run is published once, in its final state.
    statuses = {str(o) for o in graph.dataset.objects(None, URIRef(ATTR_NS + "status"))}
    assert statuses == {"succeeded"}


def test_each_read_hydrates_its_result_batch_once(local, monkeypatch):
    ledger, graph = local.ledger, local.graph
    study, (output,), (run,) = _study(ledger, "batch hydration", [("result", "scalar", {})], [{"scenario": "one"}])
    ledger.observe(run["run_id"], output["variable_id"], value=1)
    ledger.sync_metadata_graph()
    read = Mock(wraps=graph.metadata_documents)
    monkeypatch.setattr(graph, "metadata_documents", read)
    # What the server's variable, run, and observation routes return.
    for fetch in (lambda: ledger.variables(study), lambda: ledger.runs(study),
                  lambda: ledger.run(run["run_id"]), lambda: ledger.observations(study)):
        read.reset_mock()
        fetch()
        assert read.call_count == 1


def test_an_experiment_store_needs_a_sample_store(tmp_path):
    store = DuckDBLedger(tmp_path / "experiments.duckdb")
    try:
        with pytest.raises(ValueError, match="sample store"):
            ExperimentStore(store, graph_store=MemoryGraphStore())
    finally:
        store.close()
