---
title: Write and deploy soft sensors
---

A soft sensor is an Acquirium declaration plus a pure Escapement function.
Acquirium resolves semantic inputs, groups, units, and output descriptions.
Escapement supplies the declared input window, executes Python in child processes,
and publishes source and derived results in one epoch.

Run the [tank-temperature chain](../../examples/soft_sensors/README.md):

```sh
uv sync --locked --extra mqtt
uv run --locked python -m examples.soft_sensors.run
```

It computes ten-second means per tank, the hottest tank per plant, and an alarm.
It demonstrates late readings, corrections, graph edits, deletion, and snapshots.

## Select inputs and describe outputs

`Selection.from_query(query, alias="temperature", group="tank")` compiles the
query DSL. `alias` becomes the input role and `group` identifies the entity whose
streams feed one operator. Explicit SPARQL selections work too:

```python
import acquirium as aq

selection = aq.Selection(
    query="""
        PREFIX s223: <http://data.ashrae.org/standard223#>
        PREFIX ref: <https://brickschema.org/schema/Brick/ref#>
        PREFIX acq: <urn:acquirium#>
        SELECT ?stream ?group WHERE {
            ?group a <urn:soft-sensors:Tank> ; s223:hasProperty ?point .
            ?point ref:hasExternalReference ?stream .
            ?stream acq:sourceId "soft-sensor-probes" .
        }
    """,
    role="temperature",
)
output = aq.Output(unit="http://qudt.org/vocab/unit/DEG_C", on_group=True)
```

`on_group=True` adds `group s223:hasProperty output_point` to the app-owned RDF
graph. The group must be a URI. Downstream selections discover the point through
that entity and its `acq:producedBy` app tag. Acquirium registers its canonical
output reference automatically. Membership changes keep output identities stable.
Scope raw-input selections to their intended sources or exclude the app's own
outputs to avoid cycles.

Input rows carry their resolved role, reference, and frozen semantic context.
Select a parameter such as `?threshold` in SPARQL to freeze it in input context.
Use `target_unit` to freeze conversion factors, then apply them explicitly in the
function, as the example does. New parameters or membership activate a new complete
plan and repair retained history. Functions do not query the live graph, obtain
undeclared history, or use wall-clock time.

## Return the complete result for one window

Write a self-contained module with a function of this form:

```python
import polars as pl

SCHEMA = {
    "event_time_ns": pl.Int64,
    "sample_key": pl.Int64,
    "value": pl.Float64,
    "quality": pl.UInt32,
}

def bucket_mean(frame, context):
    value = frame["value"].mean()
    rows = [] if value is None else [(context["group"], 0, float(value), 0)]
    return {"mean": pl.DataFrame(rows, schema=SCHEMA, orient="row")}
```

Inputs have `role`, `stream`, `event_time_ns`, `sample_key`, `value`, `quality`,
and JSON `context` columns. `context["group"]` is the event timestamp, bucket
start, or rolling grid point in UTC nanoseconds, distinct from the semantic entity grouping streams into
an operator. Context also identifies the operator and declared inputs, including
inputs with no readings in this window.

Return every declared port with exactly the four output columns above, in that
order and with those dtypes. Use finite numeric values, sample key zero, and the
provided group timestamp. An empty frame retracts any previous result for that
window. Return the complete window result, including unchanged output rows.

`Pointwise()` sees one exact timestamp. It does not forward-fill or perform an
as-of join across asynchronous probes. `FixedBucket(width_ns, origin_ns=0)` sees a
half-open UTC bucket and emits at its start. Late readings revise that bucket and
dependent outputs immediately. Decide whether aggregates weight observations or
streams equally; this example weights observations.

`Rolling(lookback_ns, step_ns, lookahead_ns=0, origin_ns=0)` emits on the grid
`origin + k·step`; the group at `t` sees every reading in `[t - lookback,
t + lookahead]`, both ends inclusive, so windows overlap. A ten-minute trailing
mean every minute is `Rolling(lookback_ns=600 * SECOND, step_ns=60 * SECOND)`; a
correction at noon repairs the windows from 12:00 through 12:10. Grid points with
no readings in their span produce nothing. Recompute cost grows with
`lookback / step`, since every repaired window rereads its whole span. Recurrent
model state and clock-driven missing-data detection need native core extensions.

## Deploy reproducible calculation versions

```python
from pathlib import Path
import acquirium as aq

app = aq.App(
    name="tank-mean",
    inputs=[selection],
    outputs={"mean": output},
    function=aq.PythonFunction.from_file(Path("/srv/sensors/v1/calculate.py"), "bucket_mean"),
    window=aq.FixedBucket(10_000_000_000),
)
state = client.deploy_app(app)["app"]
if state["status"] != "active" or state["error"]:
    raise RuntimeError(state)
```

Paths refer to the server's filesystem. The supported contract today is a
self-contained calculation file plus the server's installed Python environment:

1. Install the locked server environment with `uv sync --locked`. Pin additional
   calculation packages in that environment's manifest and lockfile. Keep the
   lockfile, interpreter/platform information, and function digest with the
   release. The default worker interpreter is the server's interpreter.
2. Copy calculations to a new immutable version directory on the server.
   `PythonFunction.from_file` records the digest; the executor verifies it before
   invocation. Editing that file in place causes a digest mismatch.
3. Keep helpers in the pinned file, or install them as versioned, locked packages.
   Workers copy calculations into a temporary directory; adjacent modules and
   paths relative to `__file__` are unavailable.
4. Freeze small model coefficients in the calculation or selected input context.
   Larger assets must come from an immutable installed package in the locked
   environment. Model files, imported helpers, and dependencies are **not** covered
   by the calculation file's digest.
5. Record dependency/model identities in the pinned calculation (for example as
   release digest constants) when those assets affect results. Changing only an
   installed package does not change the function digest or trigger a rebuild.
   Drain accepted work before changing the shared environment, deploy new
   calculations at new paths, and check active/pending state. Keep previous code
   and environment releases available for recovery. New versions repair history;
   deployment is not a hot module reload.

This is a release discipline, not a hermetic bundle or per-app environment API.
Separate environments and content-addressed code/model bundles belong in
Escapement when required. Immutable models and transparent caches can stay loaded
in a worker. Results must remain independent of invocation order, replica choice,
and process lifetime. Stateful algorithms require a future explicit checkpoint
and replay contract; arbitrary mutable Python globals do not supply one.
Complete any work spawned by an invocation before returning; only immutable
models and transparent caches persist between calls.

## Execution, failure, and publication

The native executor leases a persistent Python process for each affected
operator's groups. It imports a digest-identified module once and reuses it across
entrypoints and epochs. Independent operators can use separate replicas, up to
the native concurrency and resident-process bounds. Each process serves one job
at a time; its job's groups run serially. Rows travel over Arrow files and finite
context over JSON. Escapement owns execution and scheduling.

By default, Acquirium allows two concurrent operators and two resident Python
processes. Configure `python_pool_size`, `python_max_worker_jobs`, and
`python_max_worker_age` in [server configuration](../reference/server-config.md).
Workers grow on demand and recycle between jobs. Native timeouts/output bounds
still apply. The current pool does not scale adaptively or split one operator's
groups among replicas. Inspect `client.engine_status()["python_workers"]` for
launch/reuse, recycling/failure, and resident/idle/active counters.

A worker failure publishes none of its epoch, including source changes, and
pauses the queue. Failed epochs discard cached processes, so retry starts fresh.
The mutation remains accepted. Inspect the failure, restore the
pinned artifact or repair the installed dependency, then resume:

```python
status = client.engine_status()
client.retry_engine()
published = client.wait(receipt, timeout=30)
```

Retry resumes the same accepted prefix. Redeliveries reuse their original
source/request ID and payload. A new replacement plan cannot bypass failed work.
Capture one published epoch for related range reads and pass `epoch=`; DataObject
also pins its multi-stream fetch. Restart reconciles declarations and resumes
accepted work.

Send emails or webhooks from a consumer of committed changes, outside the
calculation. Historical repair may retract or replace an alarm and function
evaluation can run again. Such consumers need a durable cursor and delivery
policy; the numeric example makes no external notification calls.
