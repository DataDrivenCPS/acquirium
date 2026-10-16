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
from acquirium.ExperimentMetadata import EXPERIMENT, annotation_uri, run_uri, variable_uri
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

    def graph_status(self):
        return {"source_version": self.version}


@pytest.fixture
def metadata_api(tmp_path):
    store = DuckDBStore(tmp_path / "experiments.duckdb", recreate=True)
    graph = MemoryGraphStore()

    def resolve(text, kind):
        return PSI if kind == "unit" and text in {"PSI", "psi"} else None

    ledger = ExperimentStore(store, tmp_path / "artifacts", graph_store=graph, resolve=resolve)
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
    client.list_experiment_observations.side_effect = ledger.observations
    client.sync_experiment_metadata.side_effect = ledger.sync_metadata_graph
    client.graph_version.side_effect = lambda: graph.version

    def sparql_query(*args, **kwargs):
        # The server publishes queued experiment writes before graph queries.
        ledger.publish_queued_metadata()
        return graph.query(*args, **kwargs)

    client.sparql_query.side_effect = sparql_query
    client.compact_uri.side_effect = str

    def resolve_query(fields, min_score=0.4):
        return {key: resolve(text, kind) for key, (text, kind) in fields.items()}

    client.resolve.side_effect = resolve_query
    ac = SimpleNamespace(client=client, attr=AttrProxy(client))
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
    assert pressure.metadata["unit"] == "PSI"  # Saved objects retain their original shape and values.
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

    from acquirium.Client.explore.core import Query
    original = Query.metadata_objects
    calls = []

    def tracked(query, **kwargs):
        calls.append(query)
        return original(query, **kwargs)

    monkeypatch.setattr(Query, "metadata_objects", tracked)
    result = api.ledger.read_metadata(snapshots, EXPERIMENT.Variable, "variable_id")
    values = {item["variable_id"]: item["metadata"] for item in result}
    assert values[first.variable_id] == {"unit": "PSI", "settings": {"mode": "baseline"}}
    assert values[second.variable_id] == {"unit": PSI, "settings": {"duration": 60}}
    assert len(calls) == 1


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
    assert [item["variable_metadata"] for item in rows] == [{"note": "measured"}] * 2
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
    api.ledger._graph_dirty = True
    reopened = api.service.get(study.study_id)
    pressure = reopened.variables["pressure"]
    assert pressure.variable_id == legacy_id
    assert pressure.uri == variable_uri(legacy_id)
    assert (URIRef(pressure.uri), RDF.type, EXPERIMENT.Variable) in api.graph.dataset
    assert reopened.variables.where(unit="PSI").all() == [pressure]
    declarations = api.ledger.read_metadata(api.ledger.variables(study.study_id), EXPERIMENT.Variable, "variable_id")
    assert declarations[0]["variable_id"] == legacy_id
    assert declarations[0]["metadata"] == {"unit": "PSI"}
    assert pressure.observations().latest().value == 12.5
    assert api.ledger.observations(study.study_id)[0]["variable_id"] == legacy_id


def test_object_reads_repair_failed_publication_and_do_not_hide_read_errors(metadata_api, monkeypatch):
    api = metadata_api
    study = api.service.define("read recovery")
    run = study.start({"scenario": "baseline"})
    with monkeypatch.context() as patch:
        patch.setattr(api.ledger.metadata_graph, "update", Mock(side_effect=OSError("graph unavailable")))
        with pytest.raises(OSError, match="graph unavailable"):
            api.ledger.read_metadata([api.ledger.run(run.run_id)], EXPERIMENT.Run, "run_id")
    assert api.ledger._graph_dirty
    result = api.ledger.read_metadata([api.ledger.run(run.run_id)], EXPERIMENT.Run, "run_id")
    assert result[0]["metadata"] == {"scenario": "baseline"}
    assert not api.ledger._graph_dirty
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
])
def test_shared_metadata_object_round_trip(metadata_api, values):
    api = metadata_api
    study = api.service.define("round trip")
    run = study.start(values)
    result = api.ledger.read_metadata([api.ledger.run(run.run_id)], EXPERIMENT.Run, "run_id")
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
        assert reopened.variables["pressure"].metadata == {"unit": "PSI", "settings": {"mode": "baseline"}}
        assert reopened.experiments.get(run.run_id).metadata == {"scenario": "baseline"}
        assert reopened.experiments.all()[0].metadata == {"scenario": "baseline"}
        selected = ac.client.list_experiments(study.study_id, run_ids=[run.run_id])
        assert selected[0]["metadata"] == {"scenario": "baseline"}
        observations = ac.client.list_experiment_observations(study.study_id)
        selected_observations = ac.client.list_experiment_observations(study.study_id, run_ids=[run.run_id])
        assert selected_observations == observations
        assert observations[0]["experiment_metadata"] == {"scenario": "baseline"}
        assert observations[0]["variable_metadata"] == reopened.variables["pressure"].metadata
        assert reopened.stream_annotations()[0].metadata == {"treatment": "A"}
    finally:
        http.close()


def test_null_empty_metadata_is_removed_but_recorded_json_is_preserved(metadata_api):
    api = metadata_api
    study = api.service.define("values")
    output = study.output("result").json(unit=None, tags=[], info={}, note="")
    run = study.start({"tags": [], "notes": None, "settings": {"flow": 10, "unused": {}}})
    output.record({"tags": [], "notes": None, "settings": {}})
    assert output.metadata == {"note": ""}
    assert run.metadata == {"settings": {"flow": 10}}
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
        patch.setattr(publisher, "update", Mock(side_effect=OSError("graph unavailable")))
        with pytest.raises(OSError, match="graph unavailable"):
            selected.all()
    assert api.ledger._graph_dirty
    assert [run.run_id for run in selected.all()] == [saved.run_id]
    assert not api.ledger._graph_dirty
    saved.finish()
    assert study.experiments.where(status="succeeded", scenario="trial").all()[0].run_id == saved.run_id


def test_stale_lifecycle_publication_cannot_revert_completed_run(metadata_api):
    api = metadata_api
    study = api.service.define("concurrent lifecycle")
    run = study.start({"scenario": "baseline"})
    stale = api.ledger._run_record(api.ledger.run(run.run_id))
    run.finish()
    api.ledger._publish_metadata(stale)
    assert len(study.experiments.where(scenario="baseline", status="succeeded").all()) == 1
    query = study.experiments.query().include("status")
    assert query.metadata()["experiment.status"].to_list() == ["succeeded"]


def test_backfill_annotations_and_restart_preserve_source_objects(metadata_api):
    api = metadata_api
    study = api.service.define("old")
    saved = study.start({"scenario": "baseline"})
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    item = api.ledger.annotate_stream(saved.run_id, "urn:site:stream", now, metadata={"treatment": "A"})
    api.ledger.close_stream_annotation(item["annotation_id"], now)
    graph = MemoryGraphStore()
    reopened = ExperimentStore(api.store, api.ledger.artifact_dir, graph_store=graph, resolve=api.ledger.metadata_graph.resolve)
    reopened.sync_metadata_graph()
    assert (URIRef(saved.uri), RDF.type, EXPERIMENT.Run) in graph.dataset
    assert (URIRef(annotation_uri(item["annotation_id"])), URIRef(ATTR_NS + "treatment"), Literal("A")) in graph.dataset
    assert reopened.run(saved.run_id)["metadata"] == {"scenario": "baseline"}


def test_invalid_metadata_fails_before_sql_insert_and_recording_avoids_graph_work(metadata_api):
    api = metadata_api
    study = api.service.define("validation")
    with pytest.raises(ValueError, match="Could not resolve"):
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


def test_invalid_legacy_record_is_left_out_without_blocking_other_reads(metadata_api):
    api = metadata_api
    study = api.service.define("legacy")
    run = study.start()
    run.finish()
    healthy = study.start({"scenario": "baseline"})
    other = api.service.define("unrelated")
    unrelated = other.start({"scenario": "baseline"})
    with api.store._lock, api.store._write_conn() as conn:
        conn.execute("UPDATE experiment_runs SET metadata_json=? WHERE run_id=?",
                     [json.dumps({"solver.options": 1}), run.run_id])
    api.ledger._graph_dirty = True
    status = api.ledger.sync_metadata_graph()
    assert [(item["id"], item["study_id"]) for item in status["unpublished"]] == [(run.run_id, study.study_id)]
    assert api.ledger.run(run.run_id)["metadata"] == {"solver.options": 1}
    # The bad record's own reads and its Study's graph queries still fail loudly.
    with pytest.raises(ValueError, match="invalid metadata key"):
        api.ledger.read_metadata([api.ledger.run(run.run_id)], EXPERIMENT.Run, "run_id")
    with pytest.raises(ValueError, match="invalid metadata key"):
        study.experiments.where(scenario="baseline").all()
    # Everything else reads, including the same Study's other runs.
    assert api.ledger.read_metadata([api.ledger.run(healthy.run_id)], EXPERIMENT.Run, "run_id")[0]["metadata"] == {"scenario": "baseline"}
    assert [item.run_id for item in other.experiments.where(scenario="baseline").all()] == [unrelated.run_id]
    # Repairing the SQL row publishes it on the next read.
    with api.store._lock, api.store._write_conn() as conn:
        conn.execute("UPDATE experiment_runs SET metadata_json=? WHERE run_id=?",
                     [json.dumps({"solver": {"options": 1}}), run.run_id])
    assert api.ledger.read_metadata([api.ledger.run(run.run_id)], EXPERIMENT.Run, "run_id")[0]["metadata"] == {"solver": {"options": 1}}
    assert api.ledger.sync_metadata_graph()["unpublished"] == []


def test_rebuild_reuses_first_unit_resolution(metadata_api):
    api = metadata_api
    study = api.service.define("pinned units")
    pressure = study.output("pressure").scalar(unit="psi")
    run = api.service.define("unrelated").start({"scenario": "baseline"})
    # The resolver forgets "psi", e.g. while its index reloads after a restart.
    api.ledger._live_resolve = lambda text, kind: None
    graph = MemoryGraphStore()
    reopened = ExperimentStore(api.store, api.ledger.artifact_dir, graph_store=graph, resolve=lambda text, kind: None)
    assert reopened.sync_metadata_graph()["unpublished"] == []
    assert (URIRef(pressure.variable_id), HAS_UNIT, URIRef(PSI)) in graph.dataset
    assert reopened.read_metadata([reopened.run(run.run_id)], EXPERIMENT.Run, "run_id")[0]["metadata"] == {"scenario": "baseline"}


def test_reads_wait_for_a_committed_record_to_reach_the_graph(metadata_api):
    api = metadata_api
    ledger = api.ledger
    study = ledger.define("publication race")
    committed, release = threading.Event(), threading.Event()
    publish = ledger._publish_metadata

    def slow_publish(record):
        # Pause between the SQL commit and graph publication.
        if record[1] == EXPERIMENT.Run:
            committed.set()
            assert release.wait(5)
        return publish(record)

    ledger._publish_metadata = slow_publish
    writer = threading.Thread(target=ledger.start, args=(study["template_id"], {"scenario": "a"}))
    writer.start()
    assert committed.wait(5)
    items = ledger.runs(study["template_id"])
    assert len(items) == 1  # SQL has the run; its graph node is not written yet
    result = []
    reader = threading.Thread(target=lambda: result.extend(ledger.read_metadata(items, EXPERIMENT.Run, "run_id")))
    reader.start()
    reader.join(0.2)
    assert reader.is_alive()  # the read waits rather than missing the node
    release.set()
    writer.join(5)
    reader.join(5)
    assert result[0]["metadata"] == {"scenario": "a"}


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
    assert api.ledger._pending
    api.ledger.publish_queued_metadata()
    assert not api.ledger._pending
    assert (URIRef(run.uri), URIRef(ATTR_NS + "operator"), Literal("ana")) in api.graph.dataset
