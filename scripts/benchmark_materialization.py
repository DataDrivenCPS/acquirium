"""Measure the precompiled DuckDB runtime without graph/import startup costs.

Run: .venv/bin/python scripts/benchmark_materialization.py
Uses only a temporary database; reports backfill, correction, and idle timings.
"""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter

from acquirium.Materialization import App, ApplicationGraph, Binding, StreamDescriptor, output
from acquirium.Materialization.planner import output_port
from acquirium.Materialization.runtime import Materializer
from acquirium.Storage.duckdb_store import DuckDBStore


class Copy(App):
    backfill = True
    outputs = {'out': output.stream(value_kind='numeric')}

    def transform(self, inputs, output, context):
        output['out'] = inputs['source'].collect().select(['time', 'value'])


class Graph:
    def graph_status(self):
        return {'published_version': 1}


def main():
    count, points = 8, 6000
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with TemporaryDirectory(prefix='acquirium-benchmark-') as directory:
        store = DuckDBStore(Path(directory) / 'data.duckdb')
        runtime = Materializer(store, Graph(), max_workers=2)
        try:
            bindings = []
            for i in range(count):
                ref = f'urn:input:{i}'
                store.upsert_rows(ref, [(stamp + timedelta(seconds=30*n), float(n)) for n in range(points)], value_kind='numeric')
                inputs = {'source': (StreamDescriptor(ref),)}
                bindings.append(Binding(f'copy-{i}', 'benchmark', inputs,
                                        {'out': output_port(f'copy-{i}', 'out', inputs, Copy.outputs['out'])}))
            runtime._dag = ApplicationGraph(bindings)
            runtime._applications = {b.signature: Copy() for b in bindings}
            runtime._graph_revision = 1
            start = perf_counter()
            while runtime.run_once():
                pass
            backfill = perf_counter() - start
            actual = sum(batch.num_rows for b in bindings for batch in store.timeseries(b.outputs['out'].ref_uri, value_mode='numeric'))
            assert actual == count * points
            start = perf_counter()
            store.upsert_rows('urn:input:0', [(stamp, -1.)], value_kind='numeric')
            while runtime.run_once():
                pass
            correction = perf_counter() - start
            start = perf_counter()
            for _ in range(100):
                assert not runtime.run_once()
            idle = (perf_counter() - start) / 100
            print(json.dumps({'streams': count, 'rows': actual, 'workers': 2,
                              'backfill_seconds': backfill, 'correction_seconds': correction,
                              'idle_tick_seconds': idle}, indent=2))
        finally:
            runtime.close()
            store.close()


if __name__ == '__main__':
    main()
