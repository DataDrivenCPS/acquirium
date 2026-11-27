"""Declaration normalization and atomic recording contracts on both SQL backends."""
import base64
from datetime import datetime, timezone

import pytest
from rdflib import Literal, URIRef
from rdflib.namespace import RDFS, XSD

from acquirium.ExperimentDeclarations import Declaration, normalize_metadata, prepare_declaration
from acquirium.Experiments import ExperimentStore
from acquirium.Storage.metadata_outbox import enqueue, execute
from tests.epoch_helpers import all_readings

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def counts(store):
    """Rows per ledger table, and the samples in the ledger's Escapement."""
    with store._own_conn() as conn:
        result = {table: execute(conn, f"SELECT count(*) FROM {table}").fetchone()[0]
                  for table in ("experiment_streams", "experiment_observations", "experiment_artifacts",
                                "experiment_artifact_chunks", "experiment_metadata_outbox")}
    result["samples"] = len(all_readings(store.samples.core))
    return result


@pytest.mark.parametrize("failure", ["registration", "samples", "artifact", "observation", "outbox"])
def test_failed_recording_leaves_no_observation_and_can_retry(ledger, monkeypatch, failure):
    store = ledger.store
    study = ledger.define("failed recording")["template_id"]
    # Record a sample and an attachment together so a late failure can leave
    # both a written sample and partially inserted SQL rows.
    series = ledger.declare(study, "power", "output", "timeseries", {})
    file = ledger.declare(study, "report", "output", "file", {})
    run = ledger.start(study, {})["run_id"]
    ledger.sync_metadata_graph()
    before = counts(store)
    entries = [{"form": "samples", "variable_id": series["variable_id"], "samples": [(T0, 1)]},
               {"form": "artifact", "variable_id": file["variable_id"],
                "artifact": {"filename": "report.txt", "data": base64.b64encode(b"report").decode()}}]
    with monkeypatch.context() as patch:
        if failure == "samples":
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
    # Observations, artifacts, and graph intent commit together or not at
    # all. Samples precede them: once written they stay, unreferenced, in a
    # stream reserved for the run.
    after = counts(store)
    for table in ("experiment_observations", "experiment_artifacts", "experiment_artifact_chunks", "experiment_metadata_outbox"):
        assert after[table] == before[table]
    assert after["samples"] == (0 if failure in {"registration", "samples"} else 1)
    assert after["experiment_streams"] == (0 if failure == "registration" else 1)
    assert ledger.observations(study) == []

    # Retry the same batch, then verify that clearing it removes its stored data.
    receipts = ledger.record_batch(run, entries)
    assert [r["sequence"] for r in receipts] == [1, 2]
    assert counts(store)["samples"] == 1
    ledger.finish(run, "succeeded")
    assert ledger.clear()["samples"] == 1
    assert counts(store)["experiment_artifact_chunks"] == counts(store)["experiment_streams"] == counts(store)["samples"] == 0


def test_source_clear_is_an_outbox_ordering_barrier(ledger, source_graph):
    graph = source_graph
    ledger.sync_metadata_graph()
    target = str(graph.source_graph_uri("experiment/run"))
    subject, predicate = "urn:test:point", "urn:test:value"
    def patch(value):
        return {"operation": "patch", "subject": subject, "predicates": [], "graph_uri": target,
                "triples": [[subject, predicate, Literal(value).n3()]]}
    with ledger.store._lock, ledger.store._write_conn() as conn:
        enqueue(conn, patch("before"))
        enqueue(conn, {"clear": [target]})
        enqueue(conn, patch("after"))
    ledger.sync_metadata_graph()
    assert list(graph.data_graph("experiment/run").objects(URIRef(subject), URIRef(predicate))) == [
        Literal("after", datatype=XSD.string),
    ]


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
def test_recording_rechecks_sql_state_after_preparation(ledger, monkeypatch, change):
    study = ledger.define("recheck before recording")["template_id"]
    variable = ledger.declare(study, "flow", "output", "timeseries", {})["variable_id"]
    other = ledger.define("other")["template_id"]
    run = ledger.start(study, {})["run_id"]
    prepare = ledger._prepare_recording
    def changed_state(*args):
        batch = prepare(*args)
        if change == "terminal":
            ledger.finish(run, "succeeded")
        else:
            with ledger.store._lock, ledger.store._write_conn() as conn:
                if change == "missing":
                    execute(conn, "DELETE FROM experiment_variables WHERE variable_id=?", [variable])
                else:
                    execute(conn, "UPDATE experiment_variables SET template_id=? WHERE variable_id=?", [other, variable])
        return batch
    monkeypatch.setattr(ledger, "_prepare_recording", changed_state)
    with pytest.raises(KeyError if change == "missing" else ValueError):
        ledger.record_batch(run, [{"form": "samples", "variable_id": variable, "samples": [(T0, 1)]}])
    after = counts(ledger.store)
    assert after["samples"] == after["experiment_streams"] == after["experiment_observations"] == 0


def test_recording_non_utc_postgres_preserves_sample_and_ledger_times(ledger):
    store = ledger.store
    from acquirium.Storage.ledger import PostgresLedger
    if isinstance(store, PostgresLedger):
        store.conn.execute("SET TIME ZONE 'America/Denver'")
    study = ledger.define("zoned recording")["template_id"]
    variable = ledger.declare(study, "flow", "output", "timeseries", {})
    run = ledger.start(study, {})["run_id"]
    receipt = ledger.record_batch(run, [{"form": "samples", "variable_id": variable["variable_id"],
        "samples": [("2026-01-01T01:00:00+01:00", 7)]}])[0]
    observation = ledger.observations(study)[0]
    assert observation["range_start"] == observation["range_end"] == T0
    assert observation["recorded_at"] == receipt["recorded_at"]
    [(_, time_ns, value)] = all_readings(store.samples.core)
    assert (time_ns, value) == (int(T0.timestamp()) * 1_000_000_000, 7.0)


def test_stream_delivery_preserves_each_triples_subject(ledger, source_graph):
    from acquirium.internals.internals_namespaces import ACQUIRIUM_DB_URI, ACQUIRIUM_SOURCE_ID
    study = ledger.define("stream graph subjects")["template_id"]
    variable = ledger.declare(study, "flow", "output", "timeseries", {})
    run = ledger.start(study, {})["run_id"]
    ref = ledger.record_batch(run, [{"form": "samples", "variable_id": variable["variable_id"], "samples": [(T0, 1)]}])[0]["stream_uri"]
    ledger.sync_metadata_graph()
    point = URIRef(ref + "__point")
    asserted = source_graph.data_graph(f"experiment/{run}")
    assert set(asserted.objects(point, RDFS.label)) == {Literal("flow", datatype=XSD.string)}
    assert set(asserted.objects(ACQUIRIUM_DB_URI, RDFS.label)) == {
        Literal("Acquirium Escapement", datatype=XSD.string),
    }
    assert (URIRef(ref), ACQUIRIUM_SOURCE_ID, Literal(f"experiment/{run}", datatype=XSD.string)) in asserted
    assert not list(asserted.objects(point, ACQUIRIUM_SOURCE_ID))


def test_resolvers_run_before_sql_writer_transactions(experiment_ledger, source_graph):
    store = experiment_ledger
    def resolve(text, kind):
        owned = getattr(store._lock, "_is_owned", None)
        assert not (owned() if owned is not None else store._lock.locked())
        return "urn:resolved:" + kind
    ledger = ExperimentStore(store, graph_store=source_graph, resolve=resolve)
    study = ledger.define("resolver boundary")["template_id"]
    ledger.declare(study, "flow", "output", "timeseries", {"unit": "flow units"})
    run = ledger.start(study, {"type": "run type"})["run_id"]
    ledger.annotate_stream(run, "urn:stream", T0, metadata={"type": "annotation type"})
    ledger.finish(run, "succeeded")
    ledger.sync_metadata_graph()
