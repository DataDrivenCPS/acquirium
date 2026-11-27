---
title: App inputs
---

## Inputs

Start with a stream handle or query as shown in
[Querying Data](../../tutorials/querying-data.md). A `Selection` binds that source
to an app input and describes how the app uses it, including grouping,
replacement views, and as-of reads. `inputs` maps each input name to one
selection or a list that feeds the same input.

Suppose the `lab` source registers two numeric streams, `reading` and `gain`,
and the plant model connects both to the same probe. The gain is the multiplier
used to correct a reading. Use the registered stream names and let
`context(alias="probe")` find the equipment each measurement belongs to. Both
apps can then use that equipment as their group. The `calibration` app publishes
a `gain` output at each calibration timestamp.

```python
import acquirium as aq

readings = (
    client.query()
    .measurement(stream=client.stream(source="lab", name="reading"), alias="reading")
    .context(alias="probe")
)
gains = (
    client.query()
    .measurement(stream=client.stream(source="lab", name="gain"), alias="gain")
    .context(alias="probe")
)

calibration = aq.App(
    name="calibration",
    inputs={"gain": aq.Selection.from_query(gains, measurement="gain", group="probe")},
    outputs={"gain": aq.Output()},
    function=aq.Native.mean("gain"),
)
```

Save the calculation below as `calibrate.py`. The input frame labels each
row with its input name. `emit` returns a result at the current output time.

```python
import polars as pl
from acquirium.functions import emit


def calibrate(frame, context):
    reading = frame.filter(pl.col("input") == "reading")["value"].drop_nulls()
    gain = frame.filter(pl.col("input") == "gain")["value"].drop_nulls()
    if reading.is_empty() or gain.is_empty():
        return {"corrected": emit(context, None)}
    return {"corrected": emit(context, reading.item() * gain.item())}
```

The `calibrated` app runs at each raw reading's timestamp. It binds the
reading to the same probe group and takes that probe's latest published gain
at or before the reading, up to one hour old. Deploy the gain publisher first.

```python
from pathlib import Path

import pandas as pd

calibrated = aq.App(
    name="calibrated",
    inputs={
        "reading": aq.Selection.from_query(readings, measurement="reading", group="probe"),
        "gain": aq.Selection.outputs_of(calibration, "gain", as_of=pd.Timedelta("1h")),
    },
    outputs={"corrected": aq.Output()},
    function=aq.PythonFunction.from_file(Path("calibrate.py"), "calibrate"),
)

client.deploy_app(calibration)
client.deploy_app(calibrated)
```

Add one gain and two readings, then read the corrected output.

```python
client.insert_timeseries("lab", "gain", [("2026-01-01T00:00:00Z", 2)])
client.insert_timeseries("lab", "reading", [
    ("2026-01-01T00:30:00Z", 3),
    ("2026-01-01T01:15:00Z", 4),
])

corrected = client.query().measurement(app="calibrated").dataframe(shape="narrow")
print(corrected.select("time", "value_numeric"))
```

The 00:30 reading uses the gain of 2 and produces 6. By 01:15, the gain is
older than the one-hour limit, so that reading produces no output.

```text
shape: (1, 2)
┌─────────────────────────┬───────────────┐
│ time                    ┆ value_numeric │
│ ---                     ┆ ---           │
│ datetime[ns, UTC]       ┆ f64           │
╞═════════════════════════╪═══════════════╡
│ 2026-01-01 00:30:00 UTC ┆ 6.0           │
└─────────────────────────┴───────────────┘
```

Names must be nonempty. The function's input frame carries each row's input
name in its `input` column, so a function with several inputs can tell their
rows apart. Selections listed under one name must agree on `as_of` and `view`.

## Input selections

`Selection.from_stream(stream, as_of=None, view=None)` binds a stream handle or
URI. Inputs declared this way feed one shared calculation. Set `as_of` to a
maximum age, such as `pd.Timedelta("1h")`, to read the latest sample at each output timestamp.

`Selection.from_query(query, measurement=..., group=None, parameters=None, view=None)`
turns a query into a selection. `measurement` is the alias of the query's
measurement node whose streams the input reads. Without a `group`, each matched
stream gets its own calculation. With an entity alias as `group`, all streams
for that entity feed one calculation.

The selection stores the query, rather than a fixed list of matches. Acquirium
resolves it again when the graph changes. Scope queries to the intended points
or sources; `exclude_app` can exclude an app's own outputs from a broad query.

`Selection(query, stream="stream", group="group")` accepts SPARQL
returning the named columns. Here `group=None` combines all selected streams into
one app-wide group. Returned columns are available as strings in each input
row's JSON `context`.

`target_unit` adds resolved conversion factors to that row context. The function
must apply the factors; declaring a target unit does not convert the frame.

`Selection.outputs_of(app, port)` binds a named output from another app and
keeps its groups. Use `combine=True` to feed all of those groups into one
calculation.
