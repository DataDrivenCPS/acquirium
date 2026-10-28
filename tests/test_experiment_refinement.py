"""Declaration upgrades and atomic recording contracts on both SQL backends."""
import base64
from datetime import datetime, timezone

import pytest
from rdflib import Literal, URIRef

from acquirium.ExperimentDeclarations import Declaration, normalize_metadata, prepare_declaration
from acquirium.ExperimentMetadata import EXPERIMENT, variable_record, variable_uri
from acquirium.Experiments import ExperimentStore
from acquirium.Metadata import read_documents
from acquirium.Storage.metadata_outbox import enqueue, execute
from tests.unit.test_experiment_metadata import MemoryGraphStore

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def ledger_for(store, tmp_path):
    graph = MemoryGraphStore()
    ledger = ExperimentStore(store, graph_store=graph,
                             resolve=lambda *_: "urn:unit:power")
    study = ledger.define("refinement")
    return ledger, graph, study["template_id"]


def counts(store):
    """Rows per ledger table, and the samples in the ledger's Escapement."""
    with store._own_conn() as conn:
        result = {table: execute(conn, f"SELECT count(*) FROM {table}").fetchone()[0]
                  for table in ("experiment_streams", "experiment_observations", "experiment_artifacts",
                                "experiment_artifact_chunks", "experiment_metadata_outbox")}
    result["samples"] = len(store.samples.core.engine.snapshot().readings())
    return result


@pytest.mark.parametrize("failure", ["registration", "samples", "artifact", "observation", "outbox"])
def test_recording_rolls_back_every_sql_owner(experiment_ledger, tmp_path, monkeypatch, failure):
    store = experiment_ledger
    ledger, graph, study = ledger_for(store, tmp_path)
    series = ledger.declare(study, "power", "output", "timeseries", {"unit": "power"})
    file = ledger.declare(study, "report", "output", "file", {})
    run = ledger.start(study, {})["run_id"]
    ledger.sync_metadata_graph()
    before = counts(store)
    entries = [{"form": "samples", "variable_id": series["variable_id"], "samples": [(T0, 1)]},
               {"form": "artifact", "variable_id": file["variable_id"],
                "artifact": {"filename": "report.txt", "data": base64.b64encode(b"report").decode()}}]
    with monkeypatch.context() as patch:
        if failure == "samples":
            # Samples are written last; an ingest that fails publishes nothing.
            def fail(*args, **kwargs):
                raise OSError("injected failure")
            patch.setattr(store.samples, "write", fail)
        elif failure in {"registration", "outbox"}:
            owner, name = (ledger.metadata, "enqueue") if failure == "outbox" else (ledger, "_own_streams")
            original = getattr(owner, name)
            def fail(*args, **kwargs):
                original(*args, **kwargs)
                raise OSError("injected failure")
            patch.setattr(owner, name, fail)
        else:
            original = ledger._execute
            table = "experiment_artifacts" if failure == "artifact" else "experiment_observations"
            def fail(conn, statement, parameters=None):
                result = original(conn, statement, parameters)
                if statement.startswith(f"INSERT INTO {table}"):
                    raise OSError("injected failure")
                return result
            patch.setattr(ledger, "_execute", fail)
        with pytest.raises(OSError, match="injected"):
            ledger.record_batch(run, entries)
    assert counts(store) == before
    receipts = ledger.record_batch(run, entries)
    assert [r["sequence"] for r in receipts] == [1, 2]
    assert counts(store)["samples"] == 1
    ledger.finish(run, "succeeded")
    assert ledger.clear()["samples"] == 1
    assert counts(store)["experiment_artifact_chunks"] == 0


def legacy_document(ledger, graph, study, *, conflicting=False):
    variable = ledger.declare(study, "power", "output", "scalar", {})
    ledger.sync_metadata_graph()
    metadata = {"unit": "old spelling", "category": "legacy", "metadata": {"empty": [None, {}, []]}}
    if conflicting:
        metadata["metadata"]["category"] = "different"
    record = variable_record(study, {**variable, "metadata": metadata})
    update = ledger.metadata_graph.record_update(record)
    with ledger.store._lock, ledger.store._write_conn() as conn:
        enqueue(conn, update)
        execute(conn, "UPDATE experiment_metadata_state SET declaration_format=0, declaration_upgrade_version=NULL")
    # Deliver the original representation, as an older process would.
    ledger.metadata._publish_pending()
    return variable, metadata


def test_legacy_conflict_aborts_before_normalized_mutations(experiment_ledger, tmp_path):
    ledger, graph, study = ledger_for(experiment_ledger, tmp_path)
    variable, old = legacy_document(ledger, graph, study, conflicting=True)
    uri = variable_uri(variable["variable_id"])
    document = read_documents(graph, [uri])[uri]
    before = counts(experiment_ledger)
    with pytest.raises(ValueError, match="category.*repair"):
        ledger.variables(study)
    assert read_documents(graph, [uri])[uri] == document
    assert counts(experiment_ledger) == before
    with experiment_ledger._own_conn() as conn:
        assert execute(conn, "SELECT declaration_upgrade_version FROM experiment_metadata_state").fetchone()[0] is None


def test_interrupted_declaration_upgrade_replays_without_resolver(experiment_ledger, tmp_path, monkeypatch):
    ledger, graph, study = ledger_for(experiment_ledger, tmp_path)
    variable, old = legacy_document(ledger, graph, study)
    with monkeypatch.context() as patch:
        patch.setattr(graph, "flush_metadata", lambda: (_ for _ in ()).throw(OSError("interrupted flush")))
        with pytest.raises(OSError, match="interrupted"):
            ledger.variables(study)
    with experiment_ledger._own_conn() as conn:
        format_, scheduled, published = execute(conn, "SELECT declaration_format,declaration_upgrade_version,published_version FROM experiment_metadata_state").fetchone()
    assert format_ == 0 and scheduled > published
    restarted = ExperimentStore(experiment_ledger, graph_store=graph,
        resolve=lambda *_: (_ for _ in ()).throw(AssertionError("upgrade must reuse semantic terms")))
    saved = restarted.variables(study)[0]
    assert saved["variable_id"] == variable["variable_id"]
    assert saved["metadata"]["unit"] == "old spelling"
    assert saved["metadata"]["metadata"] == {"category": "legacy", "empty": [None, {}, []]}
    assert saved["metadata"]["_resolved_semantics"]["unit"] == "urn:unit:power"
    assert restarted.declare(study, "power", "output", "scalar", old)["created"] is False
    with experiment_ledger._own_conn() as conn:
        assert execute(conn, "SELECT declaration_format FROM experiment_metadata_state").fetchone()[0] == 1
        assert execute(conn, "SELECT count(*) FROM experiment_metadata_outbox").fetchone()[0] == 0


def test_source_clear_is_an_outbox_ordering_barrier(experiment_ledger, tmp_path):
    ledger, graph, _ = ledger_for(experiment_ledger, tmp_path)
    ledger.sync_metadata_graph()
    target = str(graph.source_graph_uri("experiment/run"))
    subject, predicate = "urn:test:point", "urn:test:value"
    def patch(value):
        return {"operation": "patch", "subject": subject, "predicates": [], "graph_uri": target,
                "triples": [[subject, predicate, Literal(value).n3()]]}
    with experiment_ledger._lock, experiment_ledger._write_conn() as conn:
        enqueue(conn, patch("before"))
        enqueue(conn, {"clear": [target]})
        enqueue(conn, patch("after"))
    ledger.sync_metadata_graph()
    assert list(graph.dataset.graph(URIRef(target)).objects(URIRef(subject), URIRef(predicate))) == [Literal("after")]


def test_normalization_preserves_namespaced_values_and_merges_equal_duplicates():
    payload = {"a.b": [None, {}, []], "category": "same"}
    converted = normalize_metadata({"unit": "psi", "category": "same", "metadata": payload})
    assert converted["metadata"] == payload
    assert converted["unit"] == "psi"
    with pytest.raises(ValueError, match="category"):
        normalize_metadata({"category": "other", "metadata": payload})
    assert prepare_declaration("x", "output", "scalar", converted).document() == converted


def test_declaration_codec_preserves_typed_keys_and_detaches_documents():
    document = normalize_metadata({"keys": ["pump"], "unit": "original spelling",
        "metadata": {"nested": [None, {}, []]}, "observed_by_key": [
            {"key": [True], "observed": "urn:boolean"},
            {"key": [1], "observed": "urn:integer"},
            {"key": [1.0], "observed": "urn:float"}]})
    prepared = prepare_declaration("flow", "output", "timeseries", document)
    decoded = Declaration.from_document("flow", "output", "timeseries", prepared.document())
    assert decoded.document() == document
    assert [type(row["key"][0]) for row in decoded.document()["observed_by_key"]] == [bool, int, float]
    assert prepared.equivalent("output", "timeseries", {**document, "_resolved_semantics": {"unit": "urn:unit"}})
    document["metadata"]["nested"].append("changed")
    assert decoded.metadata == prepared.metadata == {"nested": [None, {}, []]}
    assert not prepared.equivalent("output", "timeseries", document)


@pytest.mark.parametrize("change", ["terminal", "missing", "foreign"])
def test_recording_rechecks_sql_state_after_preparation(experiment_ledger, tmp_path, monkeypatch, change):
    ledger, _, study = ledger_for(experiment_ledger, tmp_path)
    variable = ledger.declare(study, "flow", "output", "timeseries", {})["variable_id"]
    other = ledger.define("other")["template_id"]
    run = ledger.start(study, {})["run_id"]
    prepare = ledger._prepare_recording
    def changed_state(*args):
        batch = prepare(*args)
        if change == "terminal":
            ledger.finish(run, "succeeded")
        else:
            with experiment_ledger._lock, experiment_ledger._write_conn() as conn:
                if change == "missing":
                    execute(conn, "DELETE FROM experiment_variables WHERE variable_id=?", [variable])
                else:
                    execute(conn, "UPDATE experiment_variables SET template_id=? WHERE variable_id=?", [other, variable])
        return batch
    monkeypatch.setattr(ledger, "_prepare_recording", changed_state)
    with pytest.raises(KeyError if change == "missing" else ValueError):
        ledger.record_batch(run, [{"form": "samples", "variable_id": variable, "samples": [(T0, 1)]}])
    after = counts(experiment_ledger)
    assert after["samples"] == after["experiment_streams"] == after["experiment_observations"] == 0


def test_recording_non_utc_postgres_preserves_sample_and_ledger_times(experiment_ledger, tmp_path):
    store = experiment_ledger
    from acquirium.Storage.ledger import PostgresLedger
    if isinstance(store, PostgresLedger):
        store.conn.execute("SET TIME ZONE 'America/Denver'")
    ledger, _, study = ledger_for(store, tmp_path)
    variable = ledger.declare(study, "flow", "output", "timeseries", {})
    run = ledger.start(study, {})["run_id"]
    receipt = ledger.record_batch(run, [{"form": "samples", "variable_id": variable["variable_id"],
        "samples": [("2026-01-01T01:00:00+01:00", 7)]}])[0]
    observation = ledger.observations(study)[0]
    assert observation["range_start"] == observation["range_end"] == T0
    assert observation["recorded_at"] == receipt["recorded_at"]
    [sample] = store.samples.core.engine.snapshot().readings()
    assert (sample.event_time_ns, sample.value) == (int(T0.timestamp()) * 1_000_000_000, 7.0)


def test_stream_delivery_preserves_each_triples_subject(experiment_ledger, tmp_path):
    from rdflib.namespace import RDFS
    from acquirium.internals.internals_namespaces import ACQUIRIUM_DB_URI, ACQUIRIUM_SOURCE_ID
    ledger, graph, study = ledger_for(experiment_ledger, tmp_path)
    variable = ledger.declare(study, "flow", "output", "timeseries", {})
    run = ledger.start(study, {})["run_id"]
    ref = ledger.record_batch(run, [{"form": "samples", "variable_id": variable["variable_id"], "samples": [(T0, 1)]}])[0]["ref_uri"]
    ledger.sync_metadata_graph()
    point = URIRef(ref + "__point")
    assert set(graph.dataset.objects(point, RDFS.label)) == {Literal("flow")}
    assert set(graph.dataset.objects(ACQUIRIUM_DB_URI, RDFS.label)) == {Literal("Acquirium Escapement")}
    assert (URIRef(ref), ACQUIRIUM_SOURCE_ID, Literal(f"experiment/{run}")) in graph.dataset
    assert not list(graph.dataset.objects(point, ACQUIRIUM_SOURCE_ID))


def test_upgrade_keeps_user_overrides_and_semantic_assertions_in_their_sources(experiment_ledger, tmp_path):
    from acquirium.Client.metadata import plan_write, update_text
    from acquirium.internals.internals_namespaces import HAS_UNIT
    ledger, graph, study = ledger_for(experiment_ledger, tmp_path)
    variable, _ = legacy_document(ledger, graph, study)
    subject = variable_uri(variable["variable_id"])
    owner = graph.source_graph_uri("metadata")
    write = plan_write(subject, {"category": "edited", "unit": "urn:unit:override"}, role="data", resolve=lambda *_: None)
    graph.sparql_update(update_text([write]), graph_uri=owner)
    saved = ledger.variables(study)[0]["metadata"]
    assert saved["metadata"] == {"category": "edited", "empty": [None, {}, []]}
    assert saved["unit"] == "urn:unit:override"
    base = graph.dataset.graph(graph.source_graph_uri("experiments"))
    user = graph.dataset.graph(owner)
    assert set(base.objects(URIRef(subject), HAS_UNIT)) == {URIRef("urn:unit:power")}
    assert set(user.objects(URIRef(subject), HAS_UNIT)) == {URIRef("urn:unit:override")}
    assert saved["_resolved_semantics"]["unit"] == "urn:unit:override"


def test_resolvers_run_before_sql_writer_transactions(experiment_ledger, tmp_path):
    store = experiment_ledger
    def resolve(text, kind):
        owned = getattr(store._lock, "_is_owned", None)
        assert not (owned() if owned is not None else store._lock.locked())
        return "urn:resolved:" + kind
    ledger = ExperimentStore(store, graph_store=MemoryGraphStore(), resolve=resolve)
    study = ledger.define("resolver boundary")["template_id"]
    ledger.declare(study, "flow", "output", "timeseries", {"unit": "flow units"})
    run = ledger.start(study, {"type": "run type"})["run_id"]
    ledger.annotate_stream(run, "urn:stream", T0, metadata={"type": "annotation type"})
    ledger.finish(run, "succeeded")
    ledger.sync_metadata_graph()


def test_committed_sample_intent_replays_after_graph_flush_failure(experiment_ledger, tmp_path, monkeypatch):
    ledger, graph, study = ledger_for(experiment_ledger, tmp_path)
    variable = ledger.declare(study, "flow", "output", "timeseries", {})
    run = ledger.start(study, {})["run_id"]
    receipt = ledger.record_batch(run, [{"form": "samples", "variable_id": variable["variable_id"], "samples": [(T0, 3)]}])[0]
    with monkeypatch.context() as patch:
        patch.setattr(graph, "flush_metadata", lambda: (_ for _ in ()).throw(OSError("flush failed")))
        with pytest.raises(OSError, match="flush failed"):
            ledger.sync_metadata_graph()
    assert counts(experiment_ledger)["samples"] == 1
    assert counts(experiment_ledger)["experiment_observations"] == 1
    assert counts(experiment_ledger)["experiment_metadata_outbox"] > 0
    reopened = ExperimentStore(experiment_ledger, graph_store=graph)
    assert reopened.observations(study)[0]["ref_uri"] == receipt["ref_uri"]
    assert counts(experiment_ledger)["experiment_metadata_outbox"] == 0
