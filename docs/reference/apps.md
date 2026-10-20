---
title: Native app declarations
---

App names use letters, digits, underscores, dots, and hyphens.

An app declares semantic input selections and one versioned Escapement function.
The engine executes that function over pointwise timestamps, fixed UTC buckets, or
rolling windows.

```python
from pathlib import Path
import acquirium as aq

query = client.query().measurement(alias="temperature", unit="http://qudt.org/vocab/unit/DEG_F")
app = aq.App(
    name="fahrenheit-to-celsius",
    inputs=[aq.Selection.from_query(query, alias="temperature")],
    outputs={"celsius": aq.Output(unit="http://qudt.org/vocab/unit/DEG_C")},
    function=aq.PythonFunction.from_file(Path("temperature_conversion.py"), "convert"),
    window=aq.Pointwise(),
)
client.deploy_app(app)
```

`Selection.from_query` freezes the query as SPARQL, not its current matches.
Acquirium reruns it when the graph changes. `alias` names a measurement and becomes
its function role. An optional `group` names an entity alias: all matching streams
for that entity feed one operator. Without `group`, each input reference has its
own operator. Multiple selections can give inputs different roles.

`Selection(query=..., stream="stream", group="group", role="temperature")` also
accepts explicit SPARQL returning the named stream/group columns. `group=None`
combines selected inputs into one app-wide group. `target_unit` freezes conversion
factors in each input's context; functions apply these factors explicitly.
`exclude_app` excludes an app's output references from a broad selection, useful
when an app's outputs share the input quantity kind.

`Selection.outputs_of(app, port, role=...)` selects one output port of another
app. Each upstream group feeds its own operator under the same group; pass
`combine=True` to feed every upstream group into one operator. Deploying the
selection fails while the upstream app or port is missing, and removing an
upstream app fails while a deployed app selects it. Every output point carries `acq:producedBy`,
`acq:outputPort`, and `acq:outputGroup`, so hand-written SPARQL can select a port
the same way.

`parameters` gives per-group operator parameters, on any selection:

```python
aq.Selection.outputs_of(fill, "turbidity", role="filled", parameters={
    "urn:dpr/uf-unit-out": {"threshold": 0.5},
    "urn:dpr/conn-baf-4-outlet-junction-to-mb1-valve": {"threshold": 5.0},
})
```

Values are any JSON values. They become the group operator's
`parameters`, which the function reads typed as `context["parameters"]`, and
they are part of its definition: a new value is a new operator version and
repairs that group's history. Two selections giving one group different values
for a name fail deployment. Only the listed groups are selected; the app reports
the groups it skipped as warnings.

`deploy_app(app, dry_run=True)` reports the operators, groups, and input rows a
deploy would add, rebuild, or re-evaluate, without changing anything; see
[deploy and inspect apps](../how-to/check-deploy-apps.md).

`deploy_app` and `inspect_app` report `matches`, the number of streams each input
role selected, and a `warnings` entry for each role that selected none. An empty
selection is valid, for example before a driver registers its streams.

`Output` declares optional unit, quantity kind, and label. Set `on_group=True`
to attach its point to the semantic group using `s223:hasProperty`; the group must
be a URI. Downstream selections and the query DSL can then find measurements
through the entity they describe. These links live in the app-owned graph.
Output references are
stable functions of app name, semantic group, and port; changing membership does
not rename the group's result. Source IDs for derived references are reserved as
`derived:<app-name>`.

Group membership has no history: the current graph decides which streams feed
each group, for all time. When a graph change moves a stream from one group to
another, the old group's results are rebuilt without any of that stream's
readings and the new group's are rebuilt with all of them, as if it had always
belonged there. A stream that stops matching drops out of every past result the
same way. Recording that a stream moved at a particular time, and keeping
its earlier readings with its old group, is not supported yet.

A Python function has the native signature:

```python
def convert(frame, context):
    import polars as pl
    return {"celsius": frame.select(
        "event_time_ns", "sample_key",
        ((pl.col("value") - 32) * 5 / 9).alias("value"), "quality",
    )}
```

Input columns include `role`, `stream`, `event_time_ns`, `sample_key`, `value`,
`quality`, and JSON `context` (the selection's SPARQL columns, as strings). The
function's `context` holds `window_ns` (the window timestamp), `parameters`, and
`operator`, whose `window` gives the window sizes. `escapement.emit`, `empty` and,
for batch functions, `emit_windows` build output frames. Each returned port frame must have ordered columns
`event_time_ns: Int64`, `sample_key: Int64`, `value: Float64`, `quality: UInt32`.
Use sample key zero. Every port must be returned; an empty frame retracts its
previous group output. Pointwise results use the input timestamp; fixed-bucket
results use the bucket start; rolling results use their grid point. Choose
`aq.FixedBucket(width_ns, origin_ns=0)` for buckets, or
`aq.Rolling(lookback_ns, step_ns, lookahead_ns=0, origin_ns=0)` for overlapping
windows: the group at grid point `t = origin + k·step` reads the inclusive span
`[t - lookback, t + lookahead]`, and a late or corrected reading repairs every
window that reads it. Functions cannot read live graph/network state or undeclared history.

The module digest pins the function bytes. Deployment paths refer to files
available on the server; initial local development assumes a shared filesystem.
Keep versions at immutable paths, preserving files needed by accepted work.
Imported dependencies are not yet a hermetic artifact bundle. See
[write soft sensors](../how-to/write-soft-sensors.md) for the environment, model
artifact, subprocess, and deployment contract, and the
[complete chain](../../examples/soft_sensors/README.md) for a runnable example.

Simple aggregates need no Python. `aq.Native.mean(port, per_stream=False)`,
`sum`, `max`, `min` and `count` write one value per window to their single port
from every input row in the window, and nothing for a window with none.
`aq.Native.threshold(port, above)` passes a window's single value through while
it is above `above`. They run in the engine process, without a Python worker.
An argument can come from each group's parameters:

```python
aq.App(
    name="high-tank-temperature",
    inputs=[aq.Selection(query, parameters={"urn:plant": {"limit": 27.0}})],
    outputs={"alarm": aq.Output()},
    function=aq.Native.threshold("alarm", above=aq.Parameter("limit")),
)
```

Each group's operator gets the function with its value filled in; deployment
fails when a group lacks the parameter.

Use `client.list_apps()`, `client.inspect_app(name)`, or `acquirium app list/inspect`
to inspect declarations and active operators. `client.remove_app(name)` removes
operators and retracts their current outputs; historical versions remain in the
core. Output ownership remains reserved by Escapement. Code changes rebuild the
operator's retained history. Custom output timestamps, full
history, the old class-based transform API, reprocess commands, and
wall-clock scheduling options are not supported in this initial replacement.

A failed function pauses the entire native epoch queue. Inspect
`client.engine_status()`, repair the artifact/environment, and explicitly call
`client.retry_engine()` (`POST /engine/retry`). Accepted work remains durable. Replacing a plan does not
bypass failed work.
