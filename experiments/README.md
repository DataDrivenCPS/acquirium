# Experiments: semantic incremental views on Benicia

Every script here starts its own server under `experiments/runs/<name>/<timestamp>/`
(ignored by git), loads the synthetic Benicia plant, deploys the six views in
`views.py`, drives changes through the public client, and leaves result tables
(`*.parquet`, `*.csv`), a `config.json`, and figures (`*.png`, `*.pdf`) in that
directory. The server's data is deleted at the end of a run; its event log
(`events.jsonl`) is kept.

Run from the repository root with the project environment:

```bash
uv run python -m experiments.rq1_correctness --sequences 3 --ops 15   # ΔX/ΔG/ΔF sequences vs. oracle
uv run python -m experiments.rq2_scalability --replicas 1,2,4 --views 1,3,6 --workers 1,2
uv run python -m experiments.rq3_latency --minutes 60 --rate 300        # arrival-to-visibility CDFs
uv run python -m experiments.rq4_cost --hours 4                         # cost vs. recomputation
```

Each script prints its summary table and the run directory. Defaults are
sized to finish in minutes on a laptop; the paper runs raise the sizes.

## Pieces

| Module | Role |
|---|---|
| `common.py` | `Run`: config, server start/stop through `aq.init`, event-log reading; `wait_quiescent` |
| `benicia.py` | the model with `k` namespaced replicas, data generation, stream registration, Arrow inserts |
| `views.py` | V1–V6 and `deploy`; each view carries an `oracle` with the same computation in plain Polars |
| `replay.py` | row-by-row replay at a rate, with held-back (late) readings and corrections; keeps the canonical data |
| `changes.py` | graph edits (remove, add, swap a measurement point), window deletion, view deploy/remove/reparameterize/reprocess |
| `oracle.py` | `World` (points in the graph, views deployed), `expected` (from-scratch outputs keyed by derived stream URI), `compare` |
| `metrics.py` | latency along a chain, per-view cost, replan timings from `events.jsonl` |
| `plots.py` | matplotlib defaults, CDF helper |

The event log is produced by the server when `[server] materialization_event_log`
is set, which `common.py` does for every run. Its line kinds are `ingest`,
`invocation`, `commit`, `rejected`, `failure` and `plan`; see
`src/acquirium/Materialization/events.py`.

## The views

| | name | shape | input |
|---|---|---|---|
| V1 | `conc-5m` | per match, 5-minute buckets | every concentration measurement (31 streams) |
| V2 | `conc-1h-rolling` | per match, 1-hour lookback | V1 |
| V3 | `conc-high` | per match, text alarm, `threshold` parameter | V2 |
| V4 | `alarm-count-1h` | all matches, hourly buckets, named output | V3 |
| V5 | `flow-total-5m` | all matches, 5-minute buckets, named output | every flow-rate measurement (8 streams) |
| V6 | `ph-daily-range` | per match, daily buckets | every pH measurement (11 streams) |

Root views select by quantity kind, so adding, decommissioning or swapping a
point changes their bindings and everything downstream.

## Known platform behaviors the harness works around

- Deploying several views back to back can be rejected with "graph changed
  while materialization plan was being compiled": each deployment publishes
  lineage, which advances the graph version while the next compiles.
  `views.deploy` waits for a current graph and retries that rejection.
- `aq.align` names columns by stream label. Derived streams of unlabeled
  points share one generated label, which makes an aggregate over them fail
  with a duplicate column. The harness registers every point with a label.
- Views must not declare on their outputs the quantity kind they select by,
  or they match their own output ("an application binding cannot consume its
  own output").
- `aq.align` over inputs with no rows returns only a `time` column, so an
  aggregate must handle the empty case itself before `sum_horizontal`.
- A reprocess request is refused while the binding still has pending work
  ("binding already has pending work"); `changes.reprocess` waits and retries.
- A deployment revokes the running generation. A binding caught mid-tick
  records "binding is no longer active" and keeps that flag until its next
  run, so `wait_quiescent` treats that message as a stale diagnostic.
- Removing one point from a per-match view's matches repairs every sibling
  binding, because each binding fingerprints the whole match table.
