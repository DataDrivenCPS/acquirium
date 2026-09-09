"""The revision-frontier behavior must be identical on both SQL backends."""
from datetime import datetime, timezone
import os
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import psycopg
from psycopg.conninfo import make_conninfo

from acquirium.Materialization import (
    App, Binding, InProcessExecutor, OutputPort, RevisionStore, Scheduler, StreamDescriptor, output,
)
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.timescale_store import TimescaleStore


class Copy(App):
    grouping = "per_match"
    backfill = True
    outputs = {"out": output.stream(value_kind="numeric")}

    def transform(self, inputs, output, context):
        source = inputs["source"].collect()
        output["out"] = pa.table({"time": source["time"], "value": source["value"]})


@pytest.fixture(params=["duckdb", "timescale"])
def materialization_store(request, tmp_path):
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "contract.duckdb")
    else:
        dsn = os.getenv("ACQUIRIUM_TEST_PG_DSN")
        if not dsn:
            pytest.skip("ACQUIRIUM_TEST_PG_DSN is required for the Timescale contract target")
        # The API integration suite shares its database with the server.  Use
        # an isolated schema so ``recreate`` cannot drop the server's tables.
        schema = f"materialization_contract_{uuid.uuid4().hex}"
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{schema}"')
        # Use only the private schema.  Including ``public`` would cause
        # ``CREATE TABLE IF NOT EXISTS`` to reuse the live server's tables.
        # TimescaleStore qualifies the extension function it needs from public.
        isolated_dsn = make_conninfo(dsn, options=f"-c search_path={schema}")
        store = TimescaleStore(dsn=isolated_dsn, recreate=True)
    try:
        yield store
    finally:
        store.close()
        if request.param == "timescale":
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.integration
def test_materialization_revision_frontier_contract(materialization_store):
    """Inputs, output visibility, and the durable frontier agree per backend."""
    store = materialization_store
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows("urn:input", [(timestamp, 2.0)], value_kind="numeric")
    binding = Binding(
        "copy", "digest", {"source": (StreamDescriptor("urn:input"),)},
        {"out": OutputPort("urn:output", "out", "urn:point:output", Copy.outputs["out"])},
    )
    revisions = RevisionStore(store)
    scheduler = Scheduler(revisions, InProcessExecutor())

    assert scheduler.run_once(binding, Copy())
    assert not scheduler.run_once(binding, Copy())
    assert revisions.current_revision() == 2
    assert list(store.timeseries("urn:output", value_mode="numeric"))[0].column("value").to_pylist() == [2.0]

    # A correction is another revision and is seen exactly once by the same
    # binding, on both implementations.
    store.upsert_rows("urn:input", [(timestamp, 5.0)], value_kind="numeric")
    assert scheduler.run_once(binding, Copy())
    assert not scheduler.run_once(binding, Copy())
    assert revisions.current_revision() == 4
    assert list(store.timeseries("urn:output", value_mode="numeric"))[0].column("value").to_pylist() == [5.0]


def test_batch_snapshot_survives_concurrent_ingestion(materialization_store, monkeypatch):
    store = materialization_store
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    store.upsert_rows('urn:left', [(stamp, 2.)], value_kind='numeric')
    store.upsert_rows('urn:right', [(stamp, 4.)], value_kind='numeric')
    binding = Binding('snapshot', 'digest',
                      {'left': (StreamDescriptor('urn:left'),), 'right': (StreamDescriptor('urn:right'),)},
                      {'out': OutputPort('urn:output', 'out', 'urn:point:output', Copy.outputs['out'])})
    revisions = RevisionStore(store)
    revisions.initialise(binding, True)
    original = revisions._stream_set
    def concurrent_write(conn, alias, *args):
        result = original(conn, alias, *args)
        if alias == 'left':
            store.upsert_rows('urn:right', [(stamp, 100.)], value_kind='numeric')
        return result
    monkeypatch.setattr(revisions, '_stream_set', concurrent_write)
    batch = revisions.next_batch(binding)
    assert batch.inputs['right'].collect()['value'].to_pylist() == [4.]
    assert revisions.current_revision() > batch.context.to_revision


def test_materializer_control_schema_and_deployment_roundtrip(materialization_store):
    from acquirium.Materialization.runtime import Materializer
    from acquirium.Materialization.planner import Deployment
    from tests.unit.test_incremental_materialization import LineageCopy, LineageGraph
    store = materialization_store
    runtime = Materializer(store, LineageGraph())
    try:
        declaration = Deployment.from_class(LineageCopy)
        runtime.deploy(declaration)
        runtime.refresh()
        assert runtime._deployments() == (declaration,)
        assert len(runtime.dag()['nodes']) == 1
        with store._own_conn() as conn:
            assert conn.execute('SELECT context_hash FROM materialization_lineage').fetchone()[0]
    finally:
        runtime.close()


@pytest.mark.parametrize('kind', ['numeric', 'text'])
def test_empty_replacement_propagates_to_descendants(materialization_store, kind):
    from acquirium.Materialization import ApplicationGraph
    class Alarm(Copy):
        grouping = "per_match"
        def transform(self, inputs, output, context):
            table = inputs['source'].collect()
            table = table.filter(pc.greater(table['value'], 5.))
            output['out'] = pa.table({'time': table['time'],
                                      'value': table['value'] if kind == 'numeric' else pa.array(['alarm'] * table.num_rows, pa.string())})
    store = materialization_store
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    def binding(name, source):
        return Binding(name, 'digest', {'source': (StreamDescriptor(source),)},
                       {'out': OutputPort('urn:' + name, name, 'urn:point:' + name, output.stream(value_kind=kind))})
    root, child = binding('alarm', 'urn:input'), binding('child', 'urn:alarm')
    scheduler = Scheduler(RevisionStore(store))
    try:
        graph = ApplicationGraph([root, child])
        apps = {root.signature: Alarm(), child.signature: Copy()}
        store.upsert_rows('urn:input', [(stamp, 6.)], value_kind='numeric')
        scheduler.run_until_idle(graph, apps)
        assert sum(b.num_rows for b in store.timeseries('urn:child', value_mode=kind)) == 1
        store.upsert_rows('urn:input', [(stamp, 1.)], value_kind='numeric')
        scheduler.run_until_idle(graph, apps)
        assert sum(b.num_rows for b in store.timeseries('urn:child', value_mode=kind)) == 0
    finally:
        scheduler.close()
