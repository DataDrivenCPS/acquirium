from dataclasses import replace
from datetime import timedelta

import polars as pl

import pytest

from acquirium.Materialization.runtime import Materializer
from acquirium.Materialization.planner import Deployment
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import LineageCopy, LineageGraph
from tests.unit.test_incremental_materialization import Copy, ConcurrentProbeExecutor, NOW, _port
from acquirium.Materialization import ApplicationGraph, Binding, RevisionStore, Scheduler, StreamDescriptor
from acquirium.Materialization import align
from acquirium.Materialization import TimeWindow
from acquirium.Materialization import output
from acquirium.Materialization.planner import BindingPlanner


class SumFleet(LineageCopy):
    name = 'sum-fleet'
    grouping = 'all_matches'
    backfill = True
    outputs = {'out': output.named('fleet-sum', value_kind='numeric')}

    def transform(self, inputs, output, context):
        output['out'] = inputs['input'].df().group_by('time').agg(pl.col('value').sum())


class TimedCopy(LineageCopy):
    name = 'timed-copy'
    min_interval = '10s'

    def transform(self, inputs, output, context):
        output['out'] = inputs['input'].collect().select(['time', 'value'])


def test_unrelated_writes_do_not_start_the_execution_rate_limit(tmp_path, monkeypatch):
    monkeypatch.setattr('acquirium.Materialization.runtime.monotonic', lambda: 100.)
    store = DuckDBStore(tmp_path / 'throttle.duckdb')
    runtime = Materializer(store, LineageGraph())
    try:
        runtime.deploy(Deployment.from_class(TimedCopy))
        assert not runtime.run_once()
        store.upsert_rows('urn:unrelated', [(NOW, 1.)], value_kind='numeric')
        assert not runtime.run_once()
        store.upsert_rows('urn:input', [(NOW, 2.)], value_kind='numeric')
        assert runtime.run_once()
    finally:
        runtime.close()
        store.close()


class FleetGraph(LineageGraph):
    def __init__(self, refs):
        super().__init__()
        self.refs = refs

    def sparql_query(self, query, **kwargs):
        return {'columns': ['v0', 'ext0'],
                'rows': [[ref + ':point', ref] for ref in self.refs]}


def test_membership_removal_repairs_named_history_after_restart(tmp_path):
    path = tmp_path / 'membership.duckdb'
    store = DuckDBStore(path)
    runtime = Materializer(store, FleetGraph(['urn:a', 'urn:b']))
    store.upsert_rows('urn:a', [(NOW, 2.)], value_kind='numeric')
    store.upsert_rows('urn:b', [(NOW + timedelta(hours=1), 6.)], value_kind='numeric')
    runtime.deploy(Deployment.from_class(SumFleet))
    while runtime.run_once():
        pass
    ref = runtime._dag.bindings[0].outputs['out'].ref_uri
    assert len(stored(store, ref)) == 2
    runtime.close()
    store.close()
    store = DuckDBStore(path)
    runtime = Materializer(store, FleetGraph(['urn:a']))
    try:
        while runtime.run_once():
            pass
        assert [r['value'] for r in stored(store, ref)] == [2.]
        runtime.close()
        runtime = Materializer(store, FleetGraph([]))
        while runtime.run_once():
            pass
        assert not stored(store, ref)
    finally:
        runtime.close()
        store.close()


def test_grouping_is_independent_of_output_naming():
    class GeneratedAggregate(SumFleet):
        outputs = {'out': output.stream(value_kind='numeric')}
    # Durable entrypoints cannot be local classes; verify the declaration
    # independently with a replaced deployment targeting the importable app.
    declaration = replace(Deployment.from_class(SumFleet), outputs=GeneratedAggregate.outputs)
    dag, _ = BindingPlanner(FleetGraph(['urn:a', 'urn:b'])).compile([declaration], 1)
    assert len(dag.bindings) == 1
    assert len(dag.bindings[0].inputs['input']) == 2


def test_ambiguous_entity_matches_are_rejected():
    class Ambiguous(FleetGraph):
        def sparql_query(self, query, **kwargs):
            return {'columns': ['v0', 'ext0'], 'rows': [['urn:p1', 'urn:a'], ['urn:p2', 'urn:a']]}
    with pytest.raises(ValueError, match='different entity bindings'):
        BindingPlanner(Ambiguous([])).compile([Deployment.from_class(LineageCopy)], 1)


def test_deployment_roundtrip_and_grouping_default():
    import json
    declaration = replace(Deployment.from_class(SumFleet), batch_delay=timedelta(seconds=2))
    assert Deployment.from_json(declaration.to_json()) == declaration
    payload = json.loads(declaration.to_json())
    payload.pop('grouping')
    assert Deployment.from_json(json.dumps(payload)).grouping == 'per_match'
    payload['unexpected_setting'] = 2
    with pytest.raises(ValueError, match='unknown deployment fields'):
        Deployment.from_json(json.dumps(payload))


def test_http_deployment_uses_explicit_grouping_and_rejects_unknown_fields():
    import json
    from pydantic import ValidationError
    from acquirium.Server.app import AppRegistration
    payload = json.loads(Deployment.from_class(SumFleet).to_json())
    payload.pop('grouping')
    registration = AppRegistration.model_validate(payload)
    assert registration.grouping == 'per_match'
    assert Deployment.from_json(registration.model_dump_json()).grouping == 'per_match'
    with pytest.raises(ValidationError):
        AppRegistration.model_validate({**payload, 'grouping': None})
    with pytest.raises(ValidationError):
        AppRegistration.model_validate({**payload, 'unexpected_setting': 1})


def test_partitioned_backfill_and_reprocessing_resume_after_restart(tmp_path):
    path = tmp_path / 'restart.duckdb'
    binding = copy_binding('copy')
    store = DuckDBStore(path)
    revisions = RevisionStore(store)
    scheduler = Scheduler(revisions)
    rows = [(NOW + timedelta(days=n), float(n + 1)) for n in range(3)]
    store.upsert_rows('urn:input', rows, value_kind='numeric')
    assert scheduler.run_once(binding, Copy())
    assert binding.progress_key in revisions.pending_keys()
    assert revisions.initialise(binding) == 0
    store.upsert_rows('urn:input', [(NOW, 10.)], value_kind='numeric')
    scheduler.close()
    store.close()

    store = DuckDBStore(path)
    revisions = RevisionStore(store)
    scheduler = Scheduler(revisions)
    try:
        scheduler.run_until_idle(ApplicationGraph([binding]), {binding.signature: Copy()})
        assert [r['value'] for r in stored(store, 'urn:copy')] == [10., 2., 3.]
        progress = revisions.initialise(binding)
        revisions.request_reprocess([binding], TimeWindow(NOW, NOW + timedelta(days=2)))
        class Doubled(Copy):
            def transform(self, inputs, output, context):
                output['out'] = inputs['source'].df().select('time', pl.col('value') * 2)
        assert scheduler.run_once(binding, Doubled())
        scheduler.close()
        store.close()
        store = DuckDBStore(path)
        revisions = RevisionStore(store)
        scheduler = Scheduler(revisions)
        scheduler.run_until_idle(ApplicationGraph([binding]), {binding.signature: Doubled()})
        assert [r['value'] for r in stored(store, 'urn:copy')] == [20., 4., 6.]
        assert not revisions.pending_keys()
        assert revisions.initialise(binding) >= progress
    finally:
        scheduler.close()
        store.close()


def stored(store, ref):
    return [row for batch in store.timeseries(ref, value_mode='numeric') for row in batch.to_pylist()]


class MinuteMean(Copy):
    every = '1m'

    def transform(self, inputs, output, context):
        output['out'] = align(inputs, '1m').rename({'source': 'value'})


class RollingMean(Copy):
    lookback = '1m'

    def transform(self, inputs, output, context):
        output['out'] = inputs['source'].df().sort('time').select(
            'time', pl.col('value').rolling_mean_by('time', window_size='1m'))


@pytest.mark.parametrize('app_type', [MinuteMean, RollingMean])
def test_window_results_are_invariant_to_batching_and_corrections(tmp_path, app_type):
    results = []
    rows = [(NOW + timedelta(seconds=s), v) for s, v in [(10, 2.), (40, 6.), (70, 8.), (320, 10.)]]
    for split in [False, True]:
        store = DuckDBStore(tmp_path / f'{split}.duckdb')
        scheduler = Scheduler(RevisionStore(store))
        b = replace(copy_binding('mean'),
                    every=timedelta(minutes=1) if app_type is MinuteMean else None,
                    lookback=timedelta(minutes=1) if app_type is RollingMean else timedelta())
        try:
            for chunk in ([[row] for row in rows] if split else [rows]):
                store.upsert_rows('urn:input', chunk, value_kind='numeric')
                scheduler.run_once(b, app_type())
            store.upsert_rows('urn:input', [(rows[1][0], 4.)], value_kind='numeric')
            scheduler.run_once(b, app_type())
            results.append(stored(store, 'urn:mean'))
        finally:
            scheduler.close()
            store.close()
    assert results[0] == results[1]
    assert results[0][0]['value'] == (3. if app_type is MinuteMean else 2.)


def test_corrected_alarm_is_removed_from_downstream_stream(tmp_path):
    class Alarm(Copy):
        def transform(self, inputs, output, context):
            output['out'] = inputs['source'].df().filter(pl.col('value') > 5).select('time', 'value')

    store = DuckDBStore(tmp_path / 'alarms.duckdb')
    scheduler = Scheduler(RevisionStore(store))
    root, child = copy_binding('alarm'), copy_binding('child', 'urn:alarm')
    graph = ApplicationGraph([root, child])
    apps = {root.signature: Alarm(), child.signature: Copy()}
    try:
        store.upsert_rows('urn:input', [(NOW, 6.)], value_kind='numeric')
        scheduler.run_until_idle(graph, apps)
        assert stored(store, 'urn:child')[0]['value'] == 6.
        store.upsert_rows('urn:input', [(NOW, 1.)], value_kind='numeric')
        scheduler.run_until_idle(graph, apps)
        assert not stored(store, 'urn:alarm')
        assert not stored(store, 'urn:child')
    finally:
        scheduler.close()
        store.close()


def copy_binding(name, source='urn:input'):
    return Binding(name, 'digest', {'source': (StreamDescriptor(source),)},
                   {'out': _port(ref=f'urn:{name}')})


def test_pool_capacity_and_failed_branch_isolation(tmp_path):
    class Broken(Copy):
        def transform(self, inputs, output, context):
            raise ValueError('broken sensor')

    store = DuckDBStore(tmp_path / 'workers.duckdb')
    executor = ConcurrentProbeExecutor()
    scheduler = Scheduler(RevisionStore(store), executor, max_workers=2)
    try:
        store.upsert_rows('urn:input', [(NOW, 1.0)], value_kind='numeric')
        roots = [copy_binding(f'copy{i}') for i in range(8)]
        child = copy_binding('child', 'urn:copy1')
        blocked = copy_binding('blocked', 'urn:copy0')
        apps = {b.signature: Copy() for b in [*roots, child, blocked]}
        apps[roots[0].signature] = Broken()
        assert scheduler.run_graph_once(ApplicationGraph([*roots, child, blocked]), apps)
        assert executor.peak == 2
        assert roots[0].signature in scheduler.errors
        assert list(store.timeseries('urn:child', value_mode='numeric'))
        with store._own_conn() as conn:
            assert conn.execute('SELECT consumed_revision FROM binding_progress WHERE progress_key=?', [blocked.progress_key]).fetchone() is None
    finally:
        scheduler.close()
        store.close()


def test_obsolete_generation_cannot_publish(tmp_path):
    store = DuckDBStore(tmp_path / 'fence.duckdb')
    try:
        store.upsert_rows('urn:input', [(NOW, 1.0)], value_kind='numeric')
        revisions = RevisionStore(store)
        binding = replace(copy_binding('copy'), generation='old')
        revisions.initialise(binding, True)
        batch = revisions.next_batch(binding)
        revisions.active_bindings = {binding.progress_key: 'new'}
        assert not revisions.commit(binding, batch, {})
        assert revisions.initialise(binding) == 0
    finally:
        store.close()


def test_invalid_deployment_does_not_replace_active_definition(tmp_path):
    store = DuckDBStore(tmp_path / 'state.duckdb')
    try:
        runtime = Materializer(store, LineageGraph())
        original = Deployment.from_class(LineageCopy)
        runtime.deploy(original)
        with pytest.raises((ImportError, ValueError)):
            runtime.deploy(replace(original, entrypoint='missing_review_app:Broken'))
        assert runtime._deployments() == (original,)
    finally:
        store.close()
