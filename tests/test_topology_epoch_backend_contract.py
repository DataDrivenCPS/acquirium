"""Equivalent control-plane trace for the two durable backends."""
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from acquirium.Materialization.api import Transformation, outputs
from acquirium.Materialization.definitions import definition_for
from acquirium.Materialization.impact import pointwise
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.epoch_duckdb import TopologyEpochDuckDB
from acquirium.Storage.materialization.epoch_postgres import TopologyEpochPostgres
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.publication.postgres import PublicationPostgres
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest


UTC = timezone.utc


class ContractTransformation(Transformation):
    outputs = {"output": outputs.stream(value_kind="numeric", ref_uri="urn:contract:out")}
    impact = pointwise()

    def build_query(self, aq):
        return aq.query().measurement(alias="input")

    def transform(self, inputs, context):
        context.outputs.declare("output", for_input=inputs).write(inputs.values)


class ContractGraph:
    def sparql_query(self, query, **kwargs):
        return {"columns": ["v0", "ext0", "unit0", "extunit0"],
                "rows": [["urn:contract:point", "urn:epoch-contract:in", None, None]]}


@pytest.fixture(params=["duckdb", "postgres"])
def epoch_backend(request, tmp_path, pg_dsn):
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "epoch-contract.duckdb", recreate=True)
        try:
            yield store, TopologyEpochDuckDB(store), PublicationDuckDB(store)
        finally:
            store.close()
    else:
        try:
            runtime = TopologyEpochPostgres(pg_dsn)
            publication = PublicationPostgres(pg_dsn)
        except Exception as error:
            pytest.skip(f"PostgreSQL unavailable: {error}")
        try:
            yield None, runtime, publication
        finally:
            runtime.close()
            publication.close()


def test_equivalent_epoch_construction_claim_commit_seal_trace(epoch_backend):
    store, runtime, publication = epoch_backend
    marker = "epoch-contract"
    start = datetime(2026, 1, 1, tzinfo=UTC)
    publication.publish(PublicationRequest(marker, pa.Table.from_pylist([
        {"operation": "upsert", "ref_uri": f"urn:{marker}:in", "ts": start,
         "numeric_value": 4.0, "text_value": None},
    ], schema=MUTATION_SCHEMA)))
    definition = definition_for(
        ContractTransformation,
        name=marker,
        invocation="whole_query",
        outputs={"output": outputs.stream(value_kind="numeric", ref_uri=f"urn:{marker}:out")},
        impact=pointwise(),
    )
    definition_id = runtime.register_definition(definition)
    graph = ContractGraph()
    runtime.deploy_definition(definition.name, definition_id, graph)
    epoch = runtime.ensure_epoch(1, marker)
    summary = runtime.construct_epoch(epoch, graph)
    assert summary.component_count == 1
    claim = runtime.claim_next_work("contract-worker")
    snapshot = runtime.snapshot(claim)
    runtime.commit_work(snapshot, pa.Table.from_pylist([
        {"ref_uri": f"urn:{marker}:out", "ts": start,
         "numeric_value": 5.0, "text_value": None},
    ]), claim)
    seal = runtime.claim_next_component("contract-sealer")
    assert seal is not None
    runtime.seal_component(seal)
    assert runtime.active_epoch_id() == epoch
