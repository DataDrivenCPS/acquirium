"""Epoch control-plane behavior for query-driven transformations."""

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from acquirium.Materialization.api import Transformation, outputs
from acquirium.Materialization.epoch_reconciler import TopologyEpochReconciler
from acquirium.Materialization.impact import pointwise
from acquirium.Materialization.topology import resolve_bindings
from acquirium.Materialization.definitions import definition_for
from acquirium.Materialization.epochs import EpochClaimError, StaleEpochError
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.epoch_duckdb import TopologyEpochDuckDB
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest
from acquirium.internals.internals_namespaces import HAS_EXTERNAL_REFERENCE


UTC = timezone.utc


class Graph:
    def __init__(self, refs=()):
        self.refs = tuple(refs)

    def sparql_query(self, query, **kwargs):
        if "urn:not-present" in query:
            refs = ()
        else:
            selected = tuple(ref for ref in self.refs if ref in query)
            refs = selected or self.refs
        return {
            "columns": ["v0", "ext0", "unit0", "extunit0"],
            "rows": [[f"urn:point:{ref}", ref, None, None] for ref in refs],
        }


class AddOne(Transformation):
    name = "add-one"
    outputs = {"output": outputs.stream(value_kind="numeric", ref_uri="urn:derived")}
    impact = pointwise()

    def build_query(self, aq):
        return aq.query().measurement(alias="input")

    def transform(self, inputs, context):
        context.outputs.declare("output", for_input=inputs).write(
            inputs.values.select("time", (inputs.values["value"] + 1).alias("value"))
        )


class PerRow(AddOne):
    name = "per-row"
    invocation = "per_row"
    outputs = {"output": outputs.stream(value_kind="numeric", prefix="urn:per-row")}

    def build_query(self, aq):
        return aq.query().measurement(alias="input")


class SelectRef(Transformation):
    input_ref = "urn:raw"
    output_ref = "urn:derived"
    name = "select-ref"
    outputs = {"output": outputs.stream(value_kind="numeric", ref_uri="urn:derived")}
    impact = pointwise()

    def build_query(self, aq):
        return aq.query().measurement(
            alias="input", **{HAS_EXTERNAL_REFERENCE: self.input_ref}
        )

    def transform(self, inputs, context):
        context.outputs.declare("output", for_input=inputs).write(
            inputs.values.select("time", (inputs.values["value"] + 1).alias("value"))
        )


class NoMatches(Transformation):
    name = "no-matches"
    outputs = {"output": outputs.stream(value_kind="numeric", ref_uri="urn:none")}

    def build_query(self, aq):
        return aq.query().measurement(alias="input", quantity_kind="urn:not-present")

    def transform(self, inputs, context):
        context.outputs.declare("output", for_input=inputs).write(inputs.values)


def _definition(target, *, name=None, output=None):
    output_spec = (
        outputs.stream(value_kind="numeric", prefix="urn:per-row")
        if target.invocation == "per_row" and output is None
        else outputs.stream(value_kind="numeric", ref_uri=output or "urn:derived")
    )
    return definition_for(
        target,
        name=name or target.name,
        invocation=target.invocation,
        outputs={"output": output_spec},
        impact=pointwise(),
    )


def _runtime(tmp_path, refs=("urn:raw",), target=AddOne, *, output=None):
    store = DuckDBStore(tmp_path / "epochs.duckdb", recreate=True)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    PublicationDuckDB(store).publish(PublicationRequest("raw", pa.Table.from_pylist([
        {"operation": "upsert", "ref_uri": ref, "ts": start,
         "numeric_value": float(index + 1), "text_value": None}
        for index, ref in enumerate(refs)
    ], schema=MUTATION_SCHEMA)))
    runtime = TopologyEpochDuckDB(store)
    definition = _definition(target, output=output)
    graph = Graph(refs)
    definition_id = runtime.register_definition(definition)
    runtime.deploy_definition(definition.name, definition_id, graph)
    return store, runtime, definition, graph, start


def test_epoch_persists_query_resolved_binding_and_runs_through_output_set(tmp_path):
    store, runtime, definition, graph, start = _runtime(tmp_path)
    reconciler = TopologyEpochReconciler(runtime, graph)
    try:
        epoch = reconciler.ensure_graph_epoch(7, "graph-digest")
        assert reconciler.run_until_idle("worker") == 3
        binding = runtime.epoch_bindings(epoch)[0]
        assert binding.inputs == {"input": ["urn:raw"]}
        assert binding.outputs == {"output": ["urn:derived"]}
        with store._own_conn() as conn:
            value = conn.execute("""SELECT t.numeric_value FROM timeseries t
                JOIN ref_ids r ON r.ref_id = t.ref_id
                WHERE r.ref_uri = 'urn:derived' AND t.ts = ?""", [start.replace(tzinfo=None)]).fetchone()
        assert value == (2.0,)
        assert runtime.active_epoch_id() == epoch
    finally:
        reconciler.close()
        store.close()


def test_per_row_query_creates_one_binding_and_output_per_result_row(tmp_path):
    store, runtime, definition, graph, _ = _runtime(
        tmp_path, refs=("urn:a", "urn:b"), target=PerRow
    )
    reconciler = TopologyEpochReconciler(runtime, graph)
    try:
        epoch = reconciler.ensure_graph_epoch(1, "per-row")
        assert reconciler.run_until_idle("worker") == 5
        bindings = runtime.epoch_bindings(epoch)
        assert len(bindings) == 2
        output_refs = {binding.outputs["output"][0] for binding in bindings}
        assert len(output_refs) == 2
        with store._own_conn() as conn:
            assert conn.execute("SELECT count(*) FROM ref_ids WHERE ref_uri LIKE 'urn:per-row:%'").fetchone() == (2,)
    finally:
        reconciler.close()
        store.close()


def test_query_with_no_matches_builds_an_active_empty_epoch(tmp_path):
    store, runtime, definition, graph, _ = _runtime(
        tmp_path, refs=("urn:raw",), target=NoMatches, output="urn:none"
    )
    reconciler = TopologyEpochReconciler(runtime, graph)
    try:
        epoch = reconciler.ensure_graph_epoch(1, "empty")
        assert reconciler.run_until_idle("worker") == 1
        assert runtime.epoch_bindings(epoch) == ()
        assert runtime.active_epoch_id() == epoch
    finally:
        reconciler.close()
        store.close()


def test_state_revision_pins_query_resolved_binding_before_construction(tmp_path):
    store, _, definition, graph, _ = _runtime(tmp_path)
    revisions = ["state-a"]
    resolved_ids = []

    def resolve_state(binding_id):
        resolved_ids.append(binding_id)
        return revisions[0]

    runtime = TopologyEpochDuckDB(store, state_revision_resolver=resolve_state)
    definition_id = runtime.register_definition(definition)
    runtime.deploy_definition(definition.name, definition_id, graph)
    try:
        epoch = runtime.ensure_epoch(1, "state", graph)
        revisions[0] = "state-b"
        runtime.construct_epoch(epoch, graph)
        binding = runtime.epoch_bindings(epoch)[0]
        assert resolved_ids == [binding.binding_id]
        assert binding.state_revision == "state-a"
    finally:
        store.close()


def test_expired_claim_cannot_commit_a_query_transform_result(tmp_path):
    store, runtime, definition, graph, start = _runtime(tmp_path)
    reconciler = TopologyEpochReconciler(runtime, graph)
    try:
        epoch = reconciler.ensure_graph_epoch(1, "stale")
        reconciler.run_once("constructor")
        claim = runtime.claim_next_work("worker", duration=timedelta(seconds=1))
        snapshot = runtime.snapshot(claim)
        import time
        time.sleep(1.1)
        replacement = pa.Table.from_pylist([{
            "ref_uri": "urn:derived", "ts": start,
            "numeric_value": 9.0, "text_value": None,
        }])
        with pytest.raises(EpochClaimError):
            runtime.commit_work(snapshot, replacement, claim)
    finally:
        reconciler.close()
        store.close()


def test_query_resolver_returns_stable_logical_keys_for_repeated_rows():
    first = resolve_bindings(_definition(PerRow, name="stable", output="urn:stable"), Graph(("urn:a", "urn:b")))
    second = resolve_bindings(_definition(PerRow, name="stable", output="urn:stable"), Graph(("urn:b", "urn:a")))
    assert {item.logical_key for item in first} == {item.logical_key for item in second}
