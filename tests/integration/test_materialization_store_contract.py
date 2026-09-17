"""The revision-frontier behavior must be identical on both SQL backends."""
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pytest

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


@pytest.mark.parametrize('empty', [False, True])
def test_ingestion_replacement_chain_and_pending_backfill_restart(materialization_store, empty):
    from datetime import timedelta
    from acquirium.Materialization import ApplicationGraph

    store = materialization_store
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    def binding(name, source):
        return Binding(name, 'digest', {'source': (StreamDescriptor(source),)},
                       {'out': OutputPort('urn:' + name, name, 'urn:point:' + name, Copy.outputs['out'])})
    root, child = binding('root', 'urn:input'), binding('child', 'urn:root')
    graph = ApplicationGraph([root, child])
    apps = {root.signature: Copy(), child.signature: Copy()}
    rows = [(stamp + timedelta(days=n), float(n)) for n in range(3)]
    store.upsert_rows('urn:input', rows, value_kind='numeric')
    revisions = RevisionStore(store)
    scheduler = Scheduler(revisions)
    try:
        assert scheduler.run_once(root, Copy())
        assert root.progress_key in revisions.pending_keys()
        replacement = [] if empty else [(stamp, 10.), (stamp + timedelta(days=4), 40.)]
        store.replace_rows('urn:input', replacement, value_kind='numeric')
        scheduler.close()
        # Reopen storage as well as scheduler state to exercise durable recovery.
        store.close()
        store = (DuckDBStore(store.db_path) if isinstance(store, DuckDBStore)
                 else TimescaleStore(dsn=store.dsn))
        scheduler = Scheduler(RevisionStore(store))
        scheduler.run_until_idle(graph, apps)
        def values(ref):
            return [v for b in store.timeseries(ref, value_mode='numeric') for v in b.column('value').to_pylist()]
        assert values('urn:root') == values('urn:child') == ([] if empty else [10., 40.])
        store.replace_rows('urn:input', [(stamp, 20.)], value_kind='numeric')
        scheduler.run_until_idle(graph, apps)
        assert values('urn:child') == [20.]
        store.replace_rows('urn:input', [], value_kind='numeric')
        scheduler.run_until_idle(graph, apps)
        assert values('urn:root') == values('urn:child') == []
    finally:
        scheduler.close()
        store.close()


def _copy_binding(name='copy', source='urn:input', kind='numeric'):
    return Binding(name, 'digest', {'source': (StreamDescriptor(source, value_kind=kind),)},
                   {'out': OutputPort('urn:' + name, name, 'urn:point:' + name, output.stream(value_kind=kind))})


def _physical_state(store):
    with store._own_conn() as conn:
        return (
            conn.execute('SELECT current_revision FROM system_state').fetchone(),
            conn.execute('SELECT ref_id, ts, numeric_value, text_value, deleted, last_revision FROM timeseries ORDER BY ref_id, ts').fetchall(),
            conn.execute('SELECT * FROM stream_resets ORDER BY ref_uri').fetchall(),
            conn.execute('SELECT * FROM binding_progress ORDER BY progress_key').fetchall(),
            conn.execute('SELECT * FROM materialization_work ORDER BY progress_key').fetchall(),
        )


@pytest.mark.parametrize('pending', [False, True])
def test_reset_rejects_prepared_work_and_retries_atomically(materialization_store, monkeypatch, pending):
    from datetime import timedelta
    store = materialization_store
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    binding = _copy_binding()
    revisions = RevisionStore(store)
    scheduler = Scheduler(revisions)
    rows = [(stamp + timedelta(days=n), float(n)) for n in range(3 if pending else 1)]
    store.upsert_rows('urn:input', rows, value_kind='numeric')
    revisions.initialise(binding, True)
    try:
        old_batch = revisions.next_batch(binding)
        old_results = InProcessExecutor().execute(Copy(), old_batch, binding.outputs)
        store.replace_rows('urn:input', [(stamp, 10.)], value_kind='numeric')
        assert not revisions.commit(binding, old_batch, old_results)
        batch = revisions.next_batch(binding)
        assert batch.context.full_reset
        results = InProcessExecutor().execute(Copy(), batch, binding.outputs)
        before = _physical_state(store)
        insert = store._insert_frame
        def fail(*args):
            insert(*args)
            raise RuntimeError('failed rebuild')
        with monkeypatch.context() as patch:
            patch.setattr(store, '_insert_frame', fail)
            with pytest.raises(RuntimeError, match='failed rebuild'):
                revisions.commit(binding, batch, results)
        assert _physical_state(store) == before
        assert revisions.commit(binding, batch, results)
        assert not revisions.pending_keys()
        assert not scheduler.run_once(binding, Copy())
        with store._own_conn() as conn:
            assert conn.execute('SELECT count(*) FROM timeseries WHERE deleted').fetchone()[0] == 0
            assert conn.execute('SELECT count(*) FROM timeseries').fetchone()[0] == 2
    finally:
        scheduler.close()


def test_repeated_resets_rebuild_all_inputs_and_descendants_without_retained_keys(materialization_store):
    from datetime import timedelta
    from acquirium.Materialization import ApplicationGraph
    class Sum(App):
        backfill = True
        grouping = 'per_match'
        outputs = Copy.outputs
        def transform(self, inputs, output, context):
            import polars as pl
            # changes must expose unchanged sibling rows during a full reset.
            frames = [pl.from_arrow(item.changes) for item in inputs.values()]
            output['out'] = pl.concat(frames).group_by('time').agg(pl.col('value').sum()).select('time', 'value')
    store = materialization_store
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    root = Binding('sum', 'digest', {'left': (StreamDescriptor('urn:left'),), 'right': (StreamDescriptor('urn:right'),)},
                   {'out': OutputPort('urn:sum', 'sum', 'urn:point:sum', Copy.outputs['out'])})
    child = _copy_binding('child', 'urn:sum')
    graph = ApplicationGraph([root, child])
    apps = {root.signature: Sum(), child.signature: Copy()}
    scheduler = Scheduler(RevisionStore(store))
    try:
        store.upsert_rows('urn:left', [(stamp, 1.)], value_kind='numeric')
        store.upsert_rows('urn:right', [(stamp, 2.)], value_kind='numeric')
        scheduler.run_until_idle(graph, apps)
        # Only the latest reset needs retention, even if consumers miss several.
        for n in range(1, 5):
            store.replace_rows('urn:left', [(stamp + timedelta(days=n), float(n))], value_kind='numeric')
        scheduler.run_until_idle(graph, apps)
        assert not scheduler.errors
        values = [v for batch in store.timeseries('urn:child', value_mode='numeric') for v in batch.column('value').to_pylist()]
        assert values == [2., 4.]
        with store._own_conn() as conn:
            assert conn.execute('SELECT count(*) FROM timeseries').fetchone()[0] == 6
            assert conn.execute('SELECT count(*) FROM timeseries WHERE deleted').fetchone()[0] == 0
            assert conn.execute('SELECT count(*) FROM stream_resets').fetchone()[0] == 3
        store.replace_rows('urn:left', [], value_kind='numeric')
        store.replace_rows('urn:right', [], value_kind='numeric')
        scheduler.run_until_idle(graph, apps)
        assert not scheduler.errors
        with store._own_conn() as conn:
            assert conn.execute('SELECT count(*) FROM timeseries').fetchone()[0] == 0
            assert conn.execute('SELECT count(*) FROM stream_resets').fetchone()[0] == 4
    finally:
        scheduler.close()


@pytest.mark.parametrize('kind', ['numeric', 'text'])
def test_empty_reset_propagates_even_without_retained_extent(materialization_store, kind):
    from acquirium.Materialization import ApplicationGraph
    store = materialization_store
    root, child = _copy_binding('root', kind=kind), _copy_binding('child', 'urn:root', kind=kind)
    scheduler = Scheduler(RevisionStore(store))
    try:
        graph = ApplicationGraph([root, child])
        apps = {root.signature: Copy(), child.signature: Copy()}
        scheduler.run_until_idle(graph, apps)
        store.replace_rows('urn:input', [], value_kind=kind)
        scheduler.run_until_idle(graph, apps)
        assert not scheduler.errors
        with store._own_conn() as conn:
            assert conn.execute('SELECT count(*) FROM timeseries').fetchone()[0] == 0
            assert conn.execute('SELECT count(*) FROM stream_resets').fetchone()[0] == 3
        # A second reset arriving after a rebuild snapshot makes it stale too.
        stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
        value = 2. if kind == 'numeric' else 'two'
        store.replace_rows('urn:input', [(stamp, value)], value_kind=kind)
        batch = scheduler.store.next_batch(root)
        results = InProcessExecutor().execute(Copy(), batch, root.outputs)
        store.replace_rows('urn:input', [], value_kind=kind)
        assert not scheduler.store.commit(root, batch, results)
        scheduler.run_until_idle(graph, apps)
        assert not scheduler.errors
        with store._own_conn() as conn:
            assert conn.execute('SELECT count(*) FROM timeseries').fetchone()[0] == 0
    finally:
        scheduler.close()


@pytest.mark.parametrize('mode', ['bucket', 'rolling', 'whole'])
def test_reset_windowed_results_match_fresh_materialization(materialization_store, mode):
    from dataclasses import replace
    from datetime import timedelta
    import polars as pl
    from acquirium.Materialization import ApplicationGraph, align

    class Calculate(Copy):
        def transform(self, inputs, output, context):
            if mode == 'bucket':
                output['out'] = align(inputs, '1m').rename({'source': 'value'})
            elif mode == 'rolling':
                output['out'] = inputs['source'].df().sort('time').select(
                    'time', pl.col('value').rolling_mean_by('time', window_size='1m'))
            else:
                output['out'] = inputs['source'].df().select('time', pl.col('value').mean().alias('value'))

    store = materialization_store
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    binding = replace(_copy_binding(), every=timedelta(minutes=1) if mode == 'bucket' else None,
                      lookback=None if mode == 'whole' else timedelta(minutes=1) if mode == 'rolling' else timedelta())
    fresh = replace(binding, application_name='fresh', inputs={'source': (StreamDescriptor('urn:fresh-input'),)},
                    outputs={'out': OutputPort('urn:fresh', 'fresh', 'urn:point:fresh', Copy.outputs['out'])})
    scheduler = Scheduler(RevisionStore(store))
    try:
        store.upsert_rows('urn:input', [(stamp - timedelta(days=2), 99.)], value_kind='numeric')
        assert scheduler.run_once(binding, Calculate())
        rows = [(stamp + timedelta(seconds=s), v) for s, v in [(10, 2.), (40, 4.), (70, 8.)]]
        store.replace_rows('urn:input', rows, value_kind='numeric')
        store.upsert_rows('urn:fresh-input', rows, value_kind='numeric')
        scheduler.run_until_idle(ApplicationGraph([binding, fresh]), {binding.signature: Calculate(), fresh.signature: Calculate()})
        assert not scheduler.errors
        def rows_at(ref):
            return [(r['ts'], r['value']) for b in store.timeseries(ref, value_mode='numeric') for r in b.to_pylist()]
        assert rows_at('urn:copy') == rows_at('urn:fresh')
        assert rows_at('urn:copy')
    finally:
        scheduler.close()
