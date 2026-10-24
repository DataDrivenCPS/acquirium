"""Exercise the experiment adapter with the actual PR #120 writer/compiler."""
import json
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import polars as pl
import pytest
from rdflib import Dataset, Literal, URIRef
from rdflib.namespace import RDF

from acquirium.Client.explore.attributes import ATTR_NS
from acquirium.Client.explore.expr import AttrProxy
from acquirium.ExperimentMetadata import EXPERIMENT, variable_record, annotation_uri, run_uri, variable_uri
from acquirium.Experiments import ExperimentStore, StudyService
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.graph_registry import source_graph_uri
from acquirium.internals.internals_namespaces import HAS_UNIT

PSI = "http://qudt.org/vocab/unit/PSI"


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


class _Acquirium(SimpleNamespace):
    @property
    def attr(self):
        # Like Acquirium.attr: a fresh proxy, so discovery sees new keys.
        return AttrProxy(self.client)


@pytest.fixture
def metadata_api(tmp_path):
    store = DuckDBStore(tmp_path / "experiments.duckdb", recreate=True)
    graph = MemoryGraphStore()

    def resolve(text, kind):
        return PSI if kind == "unit" and text in {"PSI", "psi"} else None

    ledger = ExperimentStore(store, graph_store=graph, resolve=resolve)
    client = Mock()
    client.base_url = f"memory:{id(graph)}"
    client.define_experiment.side_effect = ledger.define
    client.get_experiment_study.side_effect = ledger.study
    client.list_experiment_studies.side_effect = ledger.studies
    client.list_experiment_variables.side_effect = ledger.variables
    client.declare_experiment_variable.side_effect = ledger.declare
    client.start_experiment.side_effect = ledger.start
    client.list_experiments.side_effect = ledger.runs
    client.get_experiment.side_effect = ledger.run
    client.finish_experiment.side_effect = lambda run_id, failed=False, error=None: ledger.finish(run_id, "failed" if failed else "succeeded", error)
    client.observe_experiment.side_effect = ledger.observe
    client.record_experiment_batch.side_effect = ledger.record_batch
    client.list_experiment_observations.side_effect = ledger.observations
    client.sync_experiment_metadata.side_effect = ledger.sync_metadata_graph

    def graph_version():
        # The server publishes queued experiment writes before reporting a version.
        ledger.publish_queued_metadata()
        return graph.version

    client.graph_version.side_effect = graph_version

    def sparql_query(*args, **kwargs):
        # The server publishes queued experiment writes before graph queries.
        ledger.publish_queued_metadata()
        return graph.query(*args, **kwargs)

    client.sparql_query.side_effect = sparql_query
    client.compact_uri.side_effect = str

    def resolve_query(fields, min_score=0.4):
        return {key: resolve(text, kind) for key, (text, kind) in fields.items()}

    client.resolve.side_effect = resolve_query
    ac = _Acquirium(client=client)
    service = StudyService(ac)
    yield SimpleNamespace(store=store, graph=graph, ledger=ledger, client=client, ac=ac, service=service)
    store.close()


def test_metadata_filters_are_scoped_and_observations_follow_selection(metadata_api):
    api = metadata_api
    study = api.service.define("cases")
    cost = study.output("cost").scalar()
    first = study.start({"scenario": "baseline", "settings": {"flow": 10}, "tags": ["a", "b"]})
    cost.record(12.5)
    first.finish()
    # Attribute discovery must work before any collection query has run.
    assert api.ac.attr.settings.flow.name == "settings.flow"
    second = study.start({"scenario": "trial", "settings": {"flow": 20}, "tags": ["b"]})
    cost.record(11.0)
    second.finish()
    other = api.service.define("other")
    other.start({"scenario": "baseline", "settings": {"flow": 100}})

    assert [run.run_id for run in study.experiments.where(scenario="baseline").all()] == [first.run_id]
    high_flow = study.experiments.where(api.ac.attr.settings.flow >= 15)
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


def test_scalar_variable_unit_strings_and_uris_share_semantic_filter(metadata_api):
    api = metadata_api
    study = api.service.define("pressure")
    pressure = study.output("pressure").scalar(unit="PSI", settings={"sensor": "A"})
    other = api.service.define("other")
    other.output("pressure").scalar(unit="PSI")

    assert study.variables.where(unit="psi").all() == [pressure]
    assert study.variables.where(unit=URIRef(PSI)).all() == [pressure]
    assert pressure.unit == "PSI"  # Saved objects retain their original shape and values.
    assert (URIRef(pressure.variable_id), HAS_UNIT, URIRef(PSI)) in api.graph.dataset
    frame = study.variables.query().include("unit").metadata(include_dependencies=False)
    assert frame["variable.unit"].to_list() == [PSI]
    objects = study.variables.query().include("unit", "unit.raw").metadata_objects(include_dependencies=False)
    assert objects[0]["variable"]["unit"] == PSI
    assert objects[0]["variable"]["unit.raw"] == "PSI"
    assert study.variables.where(**{"unit.raw": "PSI"}).all() == [pressure]
    assert study.variables.where(role="input").where(unit="PSI").all() == []
    assert study.variables.where(role="input").query().metadata().height == 0


def test_saved_metadata_reads_shared_objects_instead_of_sql_values(metadata_api, monkeypatch):
    api = metadata_api
    study = api.service.define("objects")
    first = study.output("pressure").scalar(unit="PSI", settings={"mode": "baseline"})
    second = study.output("other pressure").scalar(unit=PSI, settings={"duration": 60})
    snapshots = api.ledger.variables(study.study_id)
    for item in snapshots:
        item["metadata"] = {"stale_sql_value": True}

    import acquirium.ExperimentMetadata as shared
    original = shared.read_documents
    calls = []

    def tracked(client, subjects, **kwargs):
        calls.append(subjects)
        return original(client, subjects, **kwargs)

    monkeypatch.setattr(shared, "read_documents", tracked)
    result = api.ledger.read_metadata(snapshots, EXPERIMENT.Variable, "variable_id")
    values = {item["variable_id"]: item["metadata"] for item in result}
    assert values[first.variable_id] == first.declaration.document()
    assert values[second.variable_id] == second.declaration.document()
    assert calls and all(set(subjects) == {first.variable_id, second.variable_id} for subjects in calls)


def test_run_annotation_and_observation_metadata_use_shared_objects(metadata_api):
    api = metadata_api
    study = api.service.define("recorded objects")
    output = study.output("result").scalar(note="measured")
    run = study.start({"settings": {"mode": "baseline"}, "tags": ["A", "B"]})
    output.record(1)
    output.record(2)
    run.finish()
    study.start({"settings": {"mode": "trial", "duration": 60}}).finish()
    result = api.ledger.read_metadata(api.ledger.runs(study.study_id), EXPERIMENT.Run, "run_id")
    assert result[0]["metadata"] == {"settings": {"mode": "baseline"}, "tags": ["A", "B"]}
    assert result[1]["metadata"] == {"settings": {"mode": "trial", "duration": 60}}
    rows = api.ledger.read_observation_metadata(api.ledger.observations(study.study_id))
    assert [item["experiment_metadata"] for item in rows] == [result[0]["metadata"]] * 2
    assert [item["variable_metadata"] for item in rows] == [output.declaration.document()] * 2
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    annotation = api.ledger.annotate_stream(run.run_id, "urn:site:stream", now, metadata={"treatment": "A"})
    saved = api.ledger.read_metadata([annotation], EXPERIMENT.StreamAnnotation, "annotation_id")
    assert saved[0]["metadata"] == {"treatment": "A"}


def test_native_objects_keep_each_runs_own_fields_without_experiment_cleanup(metadata_api):
    study = metadata_api.service.define("native field presence")
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


def test_legacy_uuid_declarations_backfill_without_changing_observation_ids(metadata_api):
    api = metadata_api
    study = api.service.define("legacy declarations")
    variable = study.output("pressure").scalar(unit="PSI")
    run = study.start({"scenario": "baseline"})
    variable.record(12.5)
    run.finish()
    legacy_id = "b54c74d2-cfa6-4c08-a732-a5f7fefb5c4d"
    with api.store._lock, api.store._write_conn() as conn:
        conn.execute("UPDATE experiment_variables SET variable_id=? WHERE variable_id=?", [legacy_id, variable.variable_id])
        conn.execute("UPDATE experiment_observations SET variable_id=? WHERE variable_id=?", [legacy_id, variable.variable_id])
        from acquirium.Storage.metadata_outbox import enqueue
        record = variable_record(study.study_id, dict(variable_id=legacy_id, label="pressure", role="output", kind="scalar", metadata={"unit": "PSI"}, created_at=datetime.now(timezone.utc)))
        api.ledger._queue_metadata(conn, api.ledger.metadata_graph.record_update(record))
        enqueue(conn, {"operation": "delete", "subject": variable.uri})
    reopened = api.service.get(study.study_id)
    pressure = reopened.variables["pressure"]
    assert pressure.variable_id == legacy_id
    assert pressure.uri == variable_uri(legacy_id)
    assert (URIRef(pressure.uri), RDF.type, EXPERIMENT.Variable) in api.graph.dataset
    assert reopened.variables.where(unit="PSI").all() == [pressure]
    declarations = api.ledger.read_metadata(api.ledger.variables(study.study_id), EXPERIMENT.Variable, "variable_id")
    assert declarations[0]["variable_id"] == legacy_id
    assert declarations[0]["metadata"]["unit"] == "PSI"
    assert pressure.observations().latest().value == 12.5
    assert api.ledger.observations(study.study_id)[0]["variable_id"] == legacy_id


def test_object_reads_repair_failed_publication_and_do_not_hide_read_errors(metadata_api, monkeypatch):
    api = metadata_api
    study = api.service.define("read recovery")
    run = study.start({"scenario": "baseline"})
    with monkeypatch.context() as patch:
        patch.setattr(api.graph, "metadata_update", Mock(side_effect=OSError("graph unavailable")))
        with pytest.raises(OSError, match="graph unavailable"):
            api.ledger.read_metadata([api.ledger.run(run.run_id)], EXPERIMENT.Run, "run_id")
    with api.store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0] > 0
    result = api.ledger.read_metadata([api.ledger.run(run.run_id)], EXPERIMENT.Run, "run_id")
    assert result[0]["metadata"] == {"scenario": "baseline"}
    with api.store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0] == 0
    monkeypatch.setattr(api.graph, "sparql_query", Mock(side_effect=OSError("read failed")))
    with pytest.raises(OSError, match="read failed"):
        api.ledger.read_metadata([api.ledger.run(run.run_id)], EXPERIMENT.Run, "run_id")


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
def test_shared_metadata_object_round_trip(metadata_api, values):
    api = metadata_api
    study = api.service.define("round trip")
    run = study.start(values)
    result = api.ledger.read_metadata([api.ledger.run(run.run_id)], EXPERIMENT.Run, "run_id")
    assert result[0]["metadata"] == values


def test_container_shapes_come_from_shared_objects(metadata_api):
    api = metadata_api
    study = api.service.define("containers")
    study.start({"tag": ["A", "B"]}).finish()
    values = {"items": [{"name": "A"}, {"name": "B", "count": 2}], "matrix": [[1, 2], [3, 4]], "tag": "A",
              "id": "pump-7"}
    run = study.start(values)
    record = api.ledger.run(run.run_id)
    record["metadata"] = {}  # Without SQL snapshots, these must come from the graph.
    result = api.ledger.read_metadata([record], EXPERIMENT.Run, "run_id")
    assert result[0]["metadata"] == values


def test_http_reads_hydrate_saved_metadata_through_shared_objects(metadata_api, monkeypatch):
    from importlib import import_module
    from fastapi.testclient import TestClient
    from acquirium import Acquirium

    api = metadata_api
    module = import_module("acquirium.Server.app")
    monkeypatch.setattr(module.app.state, "manager", SimpleNamespace(experiments=api.ledger), raising=False)
    http = TestClient(module.app)
    ac = Acquirium(health_timeout=None)
    ac.client.base_url = "http://testserver"
    ac.client._http = http
    try:
        study = ac.study.define("http objects")
        pressure = study.output("pressure").scalar(unit="PSI", settings={"mode": "baseline"})
        run = study.start({"scenario": "baseline"})
        pressure.record(12.5)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        run.annotate_stream("urn:site:stream", start=now, metadata={"treatment": "A"})
        run.finish()
        study.output("other").scalar(unit=PSI, settings={"duration": 60})
        reopened = ac.study.get("http objects")
        assert reopened.variables["pressure"].metadata == {"settings": {"mode": "baseline"}}
        assert reopened.experiments.get(run.run_id).metadata == {"scenario": "baseline"}
        assert reopened.experiments.all()[0].metadata == {"scenario": "baseline"}
        selected = ac.client.list_experiments(study.study_id, run_ids=[run.run_id])
        assert selected[0]["metadata"] == {"scenario": "baseline"}
        observations = ac.client.list_experiment_observations(study.study_id)
        selected_observations = ac.client.list_experiment_observations(study.study_id, run_ids=[run.run_id])
        assert selected_observations == observations
        assert observations[0]["experiment_metadata"] == {"scenario": "baseline"}
        assert observations[0]["variable_metadata"] == reopened.variables["pressure"].declaration.document()
        assert reopened.stream_annotations()[0].metadata == {"treatment": "A"}
    finally:
        http.close()


def test_null_empty_metadata_is_removed_but_recorded_json_is_preserved(metadata_api):
    api = metadata_api
    study = api.service.define("values")
    output = study.output("result").json(unit=None, tags=[], info={}, note="")
    run = study.start({"tags": [], "notes": None, "settings": {"flow": 10, "unused": {}}})
    output.record({"tags": [], "notes": None, "settings": {}})
    assert output.metadata == {"note": "", "tags": [], "info": {}}
    assert run.metadata == {"settings": {"flow": 10, "unused": {}}}
    assert run.observations().latest().value == {"tags": [], "notes": None, "settings": {}}


def test_lazy_selection_refreshes_after_writes_and_graph_failure(metadata_api, monkeypatch):
    api = metadata_api
    study = api.service.define("recovery")
    study.start({"scenario": "baseline"}).finish()
    selected = study.experiments.where(scenario="trial")
    assert selected.all() == []
    publisher = api.ledger.metadata_graph
    saved = study.start({"scenario": "trial"})
    with monkeypatch.context() as patch:
        patch.setattr(api.graph, "metadata_update", Mock(side_effect=OSError("graph unavailable")))
        with pytest.raises(OSError, match="graph unavailable"):
            selected.all()
    with api.store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0] > 0
    assert [run.run_id for run in selected.all()] == [saved.run_id]
    with api.store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0] == 0
    saved.finish()
    assert study.experiments.where(status="succeeded", scenario="trial").all()[0].run_id == saved.run_id


def test_lifecycle_delivery_preserves_metadata_edits(metadata_api):
    api = metadata_api
    study = api.service.define("concurrent lifecycle")
    run = study.start({"scenario": "baseline"})
    api.ledger.sync_metadata_graph()
    from acquirium.Client.metadata import plan_write, update_text
    write = plan_write(run.uri, {"scenario": "edited"}, role="entity", resolve=api.ledger._resolve)
    api.graph.sparql_update(update_text([write]), graph_uri=source_graph_uri("experiments"))
    run.finish()
    assert api.ledger.run(run.run_id)["metadata"] == {"scenario": "edited"}
    assert len(study.experiments.where(scenario="edited", status="succeeded").all()) == 1


def test_variable_descriptive_graph_edits_keep_the_user_namespace(metadata_api):
    api = metadata_api
    study = api.service.define("edited declaration")
    variable = study.output("pressure").scalar(unit="PSI", metadata={"saved": [None, {}, []]})
    api.ledger.sync_metadata_graph()
    from acquirium.Client.metadata import plan_write, update_text
    write = plan_write(variable.uri, {"category": "edited"}, role="data", resolve=api.ledger._resolve)
    api.graph.sparql_update(update_text([write]), graph_uri=source_graph_uri("metadata"))
    reopened = api.service.get("edited declaration").output["pressure"]
    assert reopened.metadata == {"saved": [None, {}, []], "category": "edited"}
    assert reopened.unit == "PSI"

def test_backfill_annotations_and_restart_preserve_source_objects(metadata_api):
    api = metadata_api
    study = api.service.define("old")
    saved = study.start({"scenario": "baseline"})
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    item = api.ledger.annotate_stream(saved.run_id, "urn:site:stream", now, metadata={"treatment": "A"})
    api.ledger.close_stream_annotation(item["annotation_id"], now)
    api.ledger.sync_metadata_graph()
    graph = api.graph
    reopened = ExperimentStore(api.store, graph_store=graph, resolve=api.ledger.metadata_graph.resolve)
    reopened.sync_metadata_graph()
    assert (URIRef(saved.uri), RDF.type, EXPERIMENT.Run) in graph.dataset
    assert (URIRef(annotation_uri(item["annotation_id"])), URIRef(ATTR_NS + "treatment"), Literal("A")) in graph.dataset
    assert reopened.run(saved.run_id)["metadata"] == {"scenario": "baseline"}


class NamespacedGraphStore(MemoryGraphStore):
    """A graph that binds namespaces, as the server's graph store does."""

    def namespace_manager(self):
        self.dataset.bind("plant", "urn:plant#")
        return self.dataset.namespace_manager


def test_uri_values_under_a_bound_namespace_are_written_as_uris(tmp_path):
    store = DuckDBStore(tmp_path / "experiments.duckdb", recreate=True)
    graph = NamespacedGraphStore()
    ledger = ExperimentStore(store, graph_store=graph, resolve=lambda *_: None)
    study = ledger.define("namespaces")["template_id"]
    values = {"feeds": "urn:plant#tank", "datasheet": "https://example.org/ds.pdf"}
    variable = ledger.declare(study, "level", "output", "scalar", dict(values))
    run = ledger.start(study, dict(values))
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    annotation = ledger.annotate_stream(run["run_id"], "urn:site:stream", now, metadata=dict(values))
    ledger.sync_metadata_graph()
    # a variable keeps its metadata under the metadata key
    for subject, prefix in ((variable_uri(variable["variable_id"]), "metadata."), (run_uri(run["run_id"]), ""),
                            (annotation_uri(annotation["annotation_id"]), "")):
        assert (URIRef(subject), URIRef(ATTR_NS + prefix + "feeds"), URIRef("urn:plant#tank")) in graph.dataset
        assert (URIRef(subject), URIRef(ATTR_NS + prefix + "datasheet"),
                Literal("https://example.org/ds.pdf")) in graph.dataset
    assert ledger.run(run["run_id"])["metadata"] == values
    store.close()


def test_invalid_metadata_fails_before_sql_insert_and_recording_avoids_graph_work(metadata_api):
    api = metadata_api
    study = api.service.define("validation")
    with pytest.raises(ValueError, match="[Cc]ould not resolve"):
        study.output("pressure").scalar(unit="unknown")
    with pytest.raises(ValueError, match="reserved"):
        study.start({"status": "pretend"})
    with pytest.raises(ValueError, match="invalid metadata key"):
        study.start({"solver.options": 1})
    assert api.ledger.variables(study.study_id) == []
    assert api.ledger.runs(study.study_id) == []
    output = study.output("cost").scalar()
    study.start()
    api.ledger.sync_metadata_graph()
    before = api.graph.updates
    output.record(1)
    output.record(2)
    assert api.graph.updates == before


def test_empty_study_native_queries_and_lifecycle_filter_override(metadata_api):
    study = metadata_api.service.define("empty")
    assert study.variables.where(role="input").query().metadata().height == 0
    assert study.experiments.where(status="succeeded").query().metadata().height == 0
    run = study.start({"scenario": "baseline"})
    selected = study.experiments.where(status="running", scenario="baseline")
    assert [item.run_id for item in selected.all()] == [run.run_id]
    run.finish()
    assert selected.all() == []
    assert [item.run_id for item in selected.where(status="succeeded").all()] == [run.run_id]


def test_missing_authoritative_document_fails_without_sql_fallback(metadata_api):
    api = metadata_api
    study = api.service.define("authoritative")
    run = study.start({"scenario": "baseline"})
    api.ledger.sync_metadata_graph()
    from acquirium.Metadata import document_predicate
    api.graph.dataset.remove((URIRef(run.uri), document_predicate("run_id"), None))
    with pytest.raises(RuntimeError, match="no document"):
        api.ledger.run(run.run_id)
    with api.store._own_conn() as conn:
        assert conn.execute("SELECT status FROM experiment_runs WHERE run_id=?", [run.run_id]).fetchone() == ("running",)


@pytest.mark.parametrize("missing_kind", [EXPERIMENT.Run, EXPERIMENT.Variable])
def test_observation_hydration_requires_both_authoritative_documents(metadata_api, missing_kind):
    from acquirium.ExperimentMetadata import node_uri
    from acquirium.Metadata import document_predicate
    api = metadata_api
    study = api.service.define("missing observation metadata")
    variable = study.output("result").scalar()
    run = study.start()
    api.ledger.sync_metadata_graph()
    identifier, field = ((run.run_id, "run_id") if missing_kind == EXPERIMENT.Run
                         else (variable.variable_id, "variable_id"))
    api.graph.dataset.remove((URIRef(node_uri(missing_kind, identifier)), document_predicate(field), None))
    with pytest.raises(RuntimeError, match="no document"):
        api.ledger.metadata.read_observation_metadata([
            {"experiment_id": run.run_id, "variable_id": variable.variable_id}])


def test_hydration_batches_kinds_legacy_ids_and_duplicate_rows(metadata_api, monkeypatch):
    from acquirium.Metadata import document_triples
    api = metadata_api
    study = api.service.define("mixed hydration")
    variable = study.output("pressure").scalar(unit="psi")
    run = study.start({"metadata": {"exact.key": [None, {}, []]}})
    api.ledger.sync_metadata_graph()
    # A legacy variable's bare UUID can equal a run ID; they remain distinct nodes.
    legacy = {"unit": "PSI", "metadata": {"legacy": [None, {}, []]}}
    graph = api.graph.dataset.graph(source_graph_uri("experiments"))
    for triple in document_triples(variable_uri(run.run_id), {"variable_id": run.run_id, **legacy}):
        graph.add(triple)
    read = Mock(wraps=api.graph.metadata_documents)
    monkeypatch.setattr(api.graph, "metadata_documents", read)
    rows = [{"experiment_id": run.run_id, "variable_id": identifier, "value": index}
            for index, identifier in enumerate([run.run_id, variable.variable_id, run.run_id])]
    result = api.ledger.metadata.read_observation_metadata(rows)
    assert [item["value"] for item in result] == [0, 1, 2]
    assert [item["variable_metadata"] for item in result] == [legacy, variable.declaration.document(), legacy]
    assert all(item["experiment_metadata"] == {"metadata": {"exact.key": [None, {}, []]}} for item in result)
    read.assert_called_once()
    assert set(read.call_args.args[0]) == {run.uri, variable.uri, variable_uri(run.run_id)}
    assert api.ledger.metadata.read(EXPERIMENT.Variable, [run.run_id, run.run_id]) == {run.run_id: legacy}
    read.reset_mock()
    assert api.ledger.metadata.read(EXPERIMENT.Variable, []) == {}
    assert api.ledger.metadata.read_observation_metadata([]) == []
    assert api.ledger.metadata.read_metadata([], EXPERIMENT.Run, "run_id") == []
    read.assert_not_called()


def test_restart_retains_resolved_units_without_a_resolver(metadata_api):
    api = metadata_api
    study = api.service.define("pinned units")
    pressure = study.output("pressure").scalar(unit="psi")
    run = study.start({"scenario": "baseline"})
    api.ledger.sync_metadata_graph()
    reopened = ExperimentStore(api.store, graph_store=api.graph, resolve=lambda text, kind: None)
    assert reopened.variables(study.study_id)[0]["metadata"] == pressure.declaration.document()
    reopened.finish(run.run_id, "succeeded")
    assert reopened.run(run.run_id)["metadata"] == {"scenario": "baseline"}
    assert (URIRef(pressure.variable_id), HAS_UNIT, URIRef(PSI)) in api.graph.dataset
    missing = ExperimentStore(api.store, graph_store=MemoryGraphStore())
    with pytest.raises(RuntimeError, match="restore its graph backup"):
        missing.run(run.run_id)

def test_reads_wait_for_durable_graph_delivery(metadata_api, monkeypatch):
    api = metadata_api
    study = api.service.define("publication race")
    run = study.start({"scenario": "a"})
    entered, release = threading.Event(), threading.Event()
    update = api.graph.metadata_update
    def slow_update(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return update(*args, **kwargs)
    monkeypatch.setattr(api.graph, "metadata_update", slow_update)
    results = []
    first = threading.Thread(target=lambda: results.append(api.ledger.run(run.run_id)))
    first.start()
    assert entered.wait(5)
    second = threading.Thread(target=lambda: results.append(api.ledger.run(run.run_id)))
    second.start()
    second.join(.1)
    assert second.is_alive()
    release.set()
    first.join(5)
    second.join(5)
    assert len(results) == 2
    assert all(item["metadata"] == {"scenario": "a"} for item in results)

def test_writes_between_reads_publish_in_one_graph_update(metadata_api):
    api = metadata_api
    study = api.service.define("sweep")
    pressure = study.output("pressure").scalar(unit="psi")
    api.ledger.sync_metadata_graph()
    before = api.graph.updates
    runs = []
    for flow in range(20):
        run = study.start({"settings": {"flow": flow}})
        pressure.record(flow)
        run.finish()
        runs.append(run)
    assert api.graph.updates == before  # writes only queue their nodes
    selected = study.experiments.where(api.ac.attr.settings.flow >= 15, status="succeeded")
    assert [run.run_id for run in selected.all()] == [run.run_id for run in runs[15:]]
    assert api.graph.updates == before + 1
    # Each run is published once, in its final state.
    statuses = {str(o) for o in api.graph.dataset.objects(None, URIRef(ATTR_NS + "status"))}
    assert statuses == {"succeeded"}


def test_general_graph_queries_see_queued_writes(metadata_api):
    api = metadata_api
    study = api.service.define("discovery")
    api.ledger.sync_metadata_graph()
    run = study.start({"operator": "ana"})
    with api.store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0] > 0
    api.ledger.publish_queued_metadata()
    with api.store._own_conn() as conn:
        assert conn.execute("SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0] == 0
    assert (URIRef(run.uri), URIRef(ATTR_NS + "operator"), Literal("ana")) in api.graph.dataset


def test_each_new_run_key_is_filterable_right_after_the_run(metadata_api):
    api = metadata_api
    study = api.service.define("keys")
    for index in range(4):
        run = study.start({f"setting{index}": index})
        run.finish()
        # Discovery is cached by graph version; each new key must still be found.
        selected = study.experiments.where(getattr(api.ac.attr, f"setting{index}") == index)
        assert [found.run_id for found in selected.all()] == [run.run_id]


def test_server_graph_version_includes_queued_experiment_writes():
    from acquirium.Server.manager import Manager

    manager = object.__new__(Manager)
    calls = []
    manager.experiments = Mock(publish_queued_metadata=lambda: calls.append("publish"))
    manager.graph_store = Mock(graph_status=lambda: calls.append("status") or {"source_version": 7})
    assert manager.graph_version() == 7
    assert calls == ["publish", "status"]


def test_deleted_records_leave_the_metadata_graph(metadata_api):
    api = metadata_api
    study = api.service.define("cases")
    cost = study.output("cost").scalar(unit="PSI")
    runs = []
    for scenario in ("baseline", "trial"):
        run = study.start({"scenario": scenario})
        cost.record(1.0)
        run.finish()
        runs.append(run)
    annotation = api.ledger.annotate_stream(runs[0].run_id, "urn:plant:flow", "2026-01-01T00:00:00Z")
    # Each run's stream points live in that run's own source graph.
    run_graphs = [api.graph.dataset.graph(source_graph_uri(f"experiment/{run.run_id}")) for run in runs]
    for run, graph in zip(runs, run_graphs):
        graph.add((URIRef(f"urn:point:{run.run_id}"), RDF.type, URIRef("urn:w#Point")))
    api.ledger.sync_metadata_graph()
    ledger_graph = api.graph.dataset.graph(source_graph_uri("experiments"))

    api.ledger.delete_runs([runs[0].run_id])
    subjects = {str(subject) for subject in ledger_graph.subjects()}
    assert run_uri(runs[0].run_id) not in subjects
    assert annotation_uri(annotation["annotation_id"]) not in subjects
    assert run_uri(runs[1].run_id) in subjects
    assert (len(run_graphs[0]), len(run_graphs[1])) == (0, 1)
    assert study.experiments.where(scenario="baseline").all() == []

    api.ledger.delete_study(study.study_id)
    classes = {str(cls) for cls in (EXPERIMENT.Study, EXPERIMENT.Run, EXPERIMENT.Variable, EXPERIMENT.StreamAnnotation)}
    assert {str(subject) for subject in ledger_graph.subjects()} <= classes | {"urn:acquirium:experiments:store"}
    assert len(run_graphs[1]) == 0

    other = api.service.define("other")
    other.start({"scenario": "baseline"}).finish()
    other_graph = api.graph.dataset.graph(source_graph_uri(f"experiment/{other.experiments.all()[0].run_id}"))
    other_graph.add((URIRef("urn:point:other"), RDF.type, URIRef("urn:w#Point")))
    api.ledger.clear()
    assert {str(subject) for subject in ledger_graph.subjects()} <= classes | {"urn:acquirium:experiments:store"}
    assert len(other_graph) == 0
    assert api.service.all() == []


def test_scalars_follow_attribute_selections(metadata_api):
    api = metadata_api
    study = api.service.define("sweep")
    cost = study.output("cost").scalar(unit="PSI")
    for flow in (10, 20, 30):
        run = study.start({"settings": {"flow": flow}})
        cost.record(flow * 1.5)
        run.finish()
    high_flow = study.experiments.where(api.ac.attr.settings.flow >= 15)
    assert high_flow.scalars(metadata=["settings.flow"]).select("settings.flow", "cost").rows() == [(20, 30.0), (30, 45.0)]


def test_native_experiment_objects_use_shared_lossless_documents(metadata_api):
    api = metadata_api
    study = api.service.define("native documents")
    run = study.start({"tags": ["A", None, "B"], "metadata": {"any.key": [None, {}, []]}})
    obj = study.experiments.query().include("tags", "metadata").metadata_objects()[0]["experiment"]
    assert obj["tags"] == ["A", None, "B"]
    assert obj["metadata"] == {"any.key": [None, {}, []]}
    assert api.ledger.run(run.run_id)["metadata"]["tags"] == obj["tags"]


def test_compatibility_metadata_filters_match_complete_saved_fields(metadata_api):
    api = metadata_api
    study = api.service.define("exact metadata")
    first = study.start({"tags": ["A", "B"], "settings": {"flow": 10}})
    first.finish()
    study.start({"tags": ["B", "A"], "settings": {"flow": 10, "mode": "trial"}}).finish()
    study.start({"tags": ["A", None, "B"]}).finish()
    assert [item["run_id"] for item in api.ledger.runs(study.study_id, metadata={"tags": ["A", "B"]})] == [first.run_id]
    assert [item["run_id"] for item in api.ledger.runs(study.study_id, metadata={"settings": {"flow": 10}})] == [first.run_id]
    assert len(api.ledger.runs(study.study_id, metadata={"absent": None})) == 3


def test_http_hydrates_each_result_batch_once(metadata_api, monkeypatch):
    from importlib import import_module
    from fastapi.testclient import TestClient
    api = metadata_api
    study = api.service.define("batch hydration")
    output = study.output("result").scalar()
    run = study.start({"scenario": "one"})
    output.record(1)
    api.ledger.sync_metadata_graph()
    module = import_module("acquirium.Server.app")
    monkeypatch.setattr(module.app.state, "manager", SimpleNamespace(experiments=api.ledger), raising=False)
    read = Mock(wraps=api.graph.metadata_documents)
    monkeypatch.setattr(api.graph, "metadata_documents", read)
    http = TestClient(module.app)
    try:
        for path in (f"/experiments/templates/{study.study_id}/variables",
                     f"/experiments/templates/{study.study_id}/runs",
                     f"/experiments/runs/{run.run_id}",
                     f"/experiments/templates/{study.study_id}/observations"):
            read.reset_mock()
            assert http.get(path).status_code == 200
            assert read.call_count == 1
    finally:
        http.close()


def test_variable_user_queries_use_namespace_and_semantics_keep_their_paths(metadata_api):
    study = metadata_api.service.define("normalized query paths")
    variable = study.output("pressure").scalar(unit="PSI", category="simulation")
    assert variable.metadata == {"category": "simulation"}
    assert variable.unit == "PSI"
    assert study.variables.where(**{"metadata.category": "simulation"}).all() == [variable]
    assert study.variables.where(unit="PSI").all() == [variable]
    with pytest.raises(ValueError, match="category"):
        study.variables.where(category="simulation").all()
