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

`Output` declares optional unit, quantity kind, and label. Set `on_group=True`
to attach its point to the semantic group using `s223:hasProperty`; the group must
be a URI. Downstream selections and the query DSL can then find measurements
through the entity they describe. These links live in the app-owned graph.
Output references are
stable functions of app name, semantic group, and port; changing membership does
not rename the group's result. Source IDs for derived references are reserved as
`derived:<app-name>`.

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
`quality`, and JSON `context`. Each returned port frame must have ordered columns
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

Use `client.list_apps()`, `client.inspect_app(name)`, or `acquirium app list/inspect`
to inspect declarations and active operators. `client.remove_app(name)` removes
operators and retracts their current outputs; historical versions remain in the
core. Output ownership remains reserved by Escapement. Code changes rebuild the
operator's retained history. Custom output timestamps, full
history, the old class-based transform API, dry-run/reprocess commands, and
wall-clock scheduling options are not supported in this initial replacement.

A failed function pauses the entire native epoch queue. Inspect
`client.engine_status()`, repair the artifact/environment, and explicitly call
`client.retry_engine()` (`POST /engine/retry`). Accepted work remains durable. Replacing a plan does not
bypass failed work.
