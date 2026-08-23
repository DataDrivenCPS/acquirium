# Benchmarks

These scripts are reporting harnesses rather than CI performance tests. Run
them with the locked project environment so the results can be compared with
the same Python and dependency versions.

## Microbatch materialization sweep

`microbatch_materialization.py` exercises the local DuckDB materialization
path:

1. Start an independent synthetic input driver that offers batches at the
   requested rate.
2. Resolve and construct the topology epoch.
3. Concurrently plan changed ranges, execute the cheap application body, and
   seal the materialized components.

The application body emits one constant row per configured output stream. This
keeps application computation small so the measurements mostly describe
publication, topology, scheduling, executor, claim, and sealing overhead.

Run the default sweep with:

```bash
uv run python benchmarks/microbatch_materialization.py
```

The default sweep is the Cartesian product of the values supplied to the
dimension flags. For a quicker smoke run:

```bash
uv run python benchmarks/microbatch_materialization.py \
  --input-streams 1,8 \
  --app-input-width 1,8 \
  --app-output-width 1,4 \
  --publish-rate-hz 0 \
  --app-count 1,4 \
  --chain-depth 1,2 \
  --microbatches 5
```

### What `--microbatches` means

`--microbatches N` controls the number of measured source-update rounds in
each sweep case. In every round, the benchmark publishes a new microbatch to
all configured raw input streams. A concurrent materializer observes those
publications, plans the resulting changes, runs the reconciler, and seals the
outputs. Several offered batches may be coalesced into one durable frontier
when the driver is faster than the materializer.

There is also one initial seed publication before the measured rounds. That
seed creates the initial materialized epoch and acts as a warm-up/topology
activation step; it is reported separately as
`initial_publish_seconds` and `initial_materialization_seconds`. The `N`
offered batches appear as rows in `cycles.csv`; `results.csv` reports their
driver rate, end-to-end completion time, drain time, and maximum pending work.

For example, `--microbatches 5` means one seed round plus five measured rounds,
not five input streams or five application instances. Increasing it usually
makes per-case averages less sensitive to one-time effects, at the cost of a
longer run. `--rows-per-stream` controls how many rows each stream carries in
each of those rounds.

`--microbatches` and `--publish-rate-hz` are therefore orthogonal:

- `--microbatches` controls how many update rounds are executed.
- `--publish-rate-hz` controls the target start rate for those rounds.

For example, `--microbatches 100 --publish-rate-hz 10` offers 100 measured
rounds on a 10 Hz schedule, taking at least about ten seconds before accounting
for processing. `--microbatches 3 --publish-rate-hz 0` offers only three rounds
as quickly as the driver can publish them.

The runner is an **open-load** workload relative to materialization: the driver
does not wait for planning, application execution, or sealing before offering
the next batch. Once the driver has offered all `N` batches, the benchmark
continues draining materialization work until the active epoch is idle. A
positive requested rate faster than the platform can sustain produces publish
lateness and pending work; compare `publish_rate_hz` with
`driver_publish_rate_hz` and inspect `progress.csv`. The driver itself uses one
serial publication thread, so if the publication API call is slower than the
requested interval, the actual driver rate will also fall behind.

The structural flags are also intentionally independent, so some combinations
create fan-out or reuse rather than a strict partition:

- `input-streams` is the total raw stream pool; `app-input-width` is the
  number selected by each app. If width is larger than the available pool,
  the effective width is smaller.
- `app-count` creates that many apps per stage. If there are fewer streams than
  apps, multiple apps can consume the same stream, which measures fan-out.
- With chaining, the next stage sees the preceding stage’s output streams. A
  downstream requested width can therefore be limited by
  `app-count * app-output-width`.
- `app-output-width` increases the number of owned output streams and, when
  chaining, increases the stream pool available to the next stage.

For a clean scaling experiment, vary one structural dimension at a time and
hold the others fixed. Use `--microbatches` and `--publish-rate-hz` to control
duration and arrival pacing independently of topology.

### Sweep dimensions

All dimension flags accept comma-separated values. `--publish-rate-hz 0` means
the driver is unpaced; a positive value requests that offered publish rounds
start at that rate. If the publication API or materialization path cannot keep
up, the benchmark records the actual driver rate and backlog instead of
silently turning the workload into a synchronous loop.

With no flags, the script uses `1,8,32` input streams, `1,4` input width,
`1,4` output width, `0,10` Hz, `1,4` apps per stage, chain depths `1,2`, and
three measured microbatches per case.

| Flag | Meaning |
| --- | --- |
| `--input-streams` | Number of synthetic raw input streams updated per round. |
| `--app-input-width` | Requested number of input streams selected by each app. The report also records effective width when the preceding stage has fewer streams. |
| `--app-output-width` | Number of output streams declared by each app. |
| `--publish-rate-hz` | Target rate for measured raw publication rounds; `0` is unpaced. |
| `--app-count` | Number of apps in each chain stage. Total apps is this value times `--chain-depth`. |
| `--chain-depth` | Number of materialization stages in series. `1` is a single stage; larger values form an app chain. |
| `--microbatches` | Number of measured source-update rounds after the initial seed round. |
| `--rows-per-stream` | Rows included in each input stream during each round. |
| `--workers` | Size of the local executor pool. |
| `--repeats` | Number of repetitions for every Cartesian-product point. |
| `--max-cases` | Optional deterministic truncation of the sweep. |

### Run artifacts

By default, every invocation creates a new UTC-timestamped directory under
`benchmarks/`, for example:

```text
benchmarks/microbatch_20260823T174756Z/
├── config.json
├── cycles.csv
├── progress.csv
├── report.md
├── results.csv
└── figures/
    ├── backlog_envelope.png
    ├── input_width_vs_streams.png
    └── scaling_dimensions.png
```

Use `--output-dir PATH` to choose a different new directory. The path must not
already exist, which prevents accidentally overwriting a previous run.

`results.csv` has one row per case and includes setup, initial, offered-load,
end-to-end, drain, throughput, pending-work, and output-row metrics.
`cycles.csv` retains the driver timing and lateness for each offered batch.
`progress.csv` samples materializer progress and pending work while the driver
is active and while the final backlog drains. `report.md` embeds the generated
Matplotlib figures and links to the raw CSVs; `config.json` records the sweep
arguments and expanded case list.

## Workload recipes

The flags describe different aspects of one synthetic topology. They can be
combined, but these recipes make the intended use of each dimension clearer.

### 1:1 applications across many streams

Use one app per input stream, with each app consuming one stream:

```bash
uv run python benchmarks/microbatch_materialization.py \
  --input-streams 1,8,32 \
  --app-input-width 1 \
  --app-count 1,8,32 \
  --app-output-width 1 \
  --chain-depth 1 \
  --publish-rate-hz 0 \
  --microbatches 5
```

This emphasizes app-count, binding, claim, and scheduling overhead.

### One wide app consuming many streams

Keep one app and increase its input width:

```bash
uv run python benchmarks/microbatch_materialization.py \
  --input-streams 1,8,32,128 \
  --app-input-width 1,8,32,128 \
  --app-count 1 \
  --app-output-width 1 \
  --chain-depth 1 \
  --publish-rate-hz 0 \
  --microbatches 5
```

This emphasizes snapshot and input-resolution cost as one binding becomes
wider. When a requested width exceeds the streams available at a stage, the
report records the smaller effective width.

### Output fan-out

Hold input topology fixed and increase the number of outputs per app:

```bash
uv run python benchmarks/microbatch_materialization.py \
  --input-streams 16 \
  --app-input-width 16 \
  --app-count 1,4 \
  --app-output-width 1,4,16 \
  --chain-depth 1 \
  --publish-rate-hz 0 \
  --microbatches 5
```

This emphasizes output declaration, staging, and publication width.

### App chaining

Increase `--chain-depth` to measure propagation through serial stages:

```bash
uv run python benchmarks/microbatch_materialization.py \
  --input-streams 16 \
  --app-input-width 4 \
  --app-count 1,4 \
  --app-output-width 1,4 \
  --chain-depth 1,2,4 \
  --publish-rate-hz 0 \
  --microbatches 5
```

The number of applications is `app-count * chain-depth`, and downstream apps
consume the preceding stage’s synthetic outputs.

### Sustained-rate workload

Use more measured rounds and a positive target rate when studying steady-state
operation:

```bash
uv run python benchmarks/microbatch_materialization.py \
  --input-streams 32 \
  --app-input-width 8 \
  --app-count 4 \
  --app-output-width 4 \
  --chain-depth 2 \
  --microbatches 100 \
  --publish-rate-hz 10
```

Use `--publish-rate-hz 0` for an unpaced driver. Use a positive rate to test
whether a specific offered arrival rate is sustainable and how much pending
work accumulates when it is not.
