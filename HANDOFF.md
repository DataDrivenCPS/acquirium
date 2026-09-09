# Materialization development handoff

Work in `/tmp/acquirium-materialization-backends` on
`gtf-materialization-backends`.

## Processing contract

Derived streams describe the current corrected inputs. Calculations use the
latest available readings. Assigned output ports replace their output window,
including removing results that disappear; unassigned ports are unchanged.
Removal revisions propagate downstream. The input read window includes the
context required by complete buckets and trailing or leading dependencies.

Apps implement `build_query(plant)` and `transform(inputs, output, context)`.
`grouping` selects `per_match` or `all_matches` independently of
`output.stream` or `output.named` identity. `every`, `lookback`, and
`lookahead` determine windows; `batch_delay`, `min_interval`, and `backfill`
control scheduling and initial history.

## Implementation

- `models.py`: authoring types, windows, bindings, DAG, and dataframe helpers.
- `planner.py`: deployment serialization and semantic query compilation.
- `revision_store.py`: snapshots, durable work cursors, and atomic publication.
- `scheduler.py`: bounded execution and per-binding failures.
- `runtime.py`: deployment activation, graph refresh, timing, and status.
- `checks.py` and `local.py`: server and caller-process dry runs.
- `worker.py`: import and source-digest verification.

All modules above are under `src/acquirium/Materialization/`.
Schemas are created with their full definitions. Deployment JSON rejects
unknown fields. Output declarations are OutputSpec objects, and every batch
has an explicit output window.

## Verification

Run the unit/store suite:

```bash
.venv/bin/python -m pytest -q tests/unit tests/test_duckdb_store.py tests/test_publication_store_contract.py tests/test_timeseries_store_contract.py
```

Run backend contracts with `ACQUIRIUM_TEST_PG_DSN` set to an isolated
TimescaleDB test instance:

```bash
.venv/bin/python -m pytest -q tests/integration/test_materialization_store_contract.py
```

The contract tests create private PostgreSQL schemas. The standalone
`scripts/benchmark_materialization.py` probe uses a temporary DuckDB database.

## Documentation

- [App guide](docs/apps.md)
- [App reference](docs/reference/apps.md)
- [Processing contract](docs/materialization-contract.md)
- [Backend and developer guide](docs/materialization-implementation.md)
