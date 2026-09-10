# Experiments

Experiments record script- or notebook-driven work without changing how that
work executes. Define a reusable study once, declare its variables at module
scope, then start one durable run for every scenario.

```python
import acquirium as aq

ac = aq.Acquirium()
study = ac.study.define("load-shift")

configuration = study.input("configuration").json()
total_cost = study.output("total operating cost").scalar(unit="USD")
tank_volume = study.output("tank volume").timeseries(
    observed=ac.point("urn:plant:tank-volume"), unit="M3"
)
solver_log = study.log("solver events")

for config in configurations:
    e = study.start(metadata={"scenario": config["name"]})
    try:
        configuration.record(config)
        result = solve(config)  # ordinary application code
        total_cost.record(result.cost)
        tank_volume.record(result.volume_rows)
        solver_log.record({"event": "solve-complete"})
        e.finish()
    except Exception as error:
        e.fail(error)
        raise
```

## Variables

`input`, `output`, and `log` are roles. The type method selects the
value form: `json()`, `text()`, `scalar(unit=...)`, `file(media_type=...)`,
`timeseries(observed=..., unit=...)`, or the append-only `log()` variable.

The label is required and unique within the study. Every returned handle has
`record(value)`: JSON/text/scalar handles record a value, log handles record an
event, file handles copy a path into Acquirium's content-addressed artifact
store, and time-series handles write ordinary `(timestamp, value)` rows.
Repeated calls add observations to the run's history. The existing `set()`,
`append()`, `attach()`, and `add()` methods remain available.

Declarations receive a server UTC timestamp; observations also receive a
per-run sequence number. For JSON, text, scalar, and log handles,
`record(value, occurred_at=...)` can specify when an external event happened.
Time-series rows retain their own sample timestamps. File and time-series
handles reject `occurred_at`.

## Lookup, iteration, and exploratory variables

`study.input`, `study.output`, and `study.log` are callable collections. Calling
one declares a variable; indexing retrieves its handle. Iteration yields
handles in declaration order, and `items()` yields label/handle pairs:

```python
assert study.output["total operating cost"] is total_cost

for output in study.output:
    print(output.label, output.kind)

for label, output in study.output.items():
    print(label, output.kind)
```

Collections also support `len()`, label membership, `keys()`, and `values()`.
They contain handles declared through this `Study` object; they do not fetch
other variables from earlier script sessions. Redeclaring a matching variable
registers its handle locally and reuses the persistent declaration. Repeating
the declaration on the same object returns the same handle. Conflicting roles,
types, or metadata (including units) raise an error.

Use the same constructor to add exploratory variables during a run:

```python
e = study.start()
total_cost.record(results.total_cost)
peak_load = study.output("peak load").scalar(unit="KiloW")  # UserWarning if new
peak_load.record(results.peak_load)
e.finish()
```

Creating a new variable during an active run emits a `UserWarning` and adds it
to the reusable study. Looking up a handle or redeclaring a matching persistent
variable does not warn. Recording requires an active run.

### Assignment convenience

Prefer `handle.record(value)` for standard outputs. If you do not have a handle
at the recording site, assign through the active experiment instead:

```python
experiment = study.start()
experiment.output["total operating cost"] = results.total_cost
experiment.output["exploratory score"] = 0.95  # UserWarning if new

# Assignment preserves handles; subsequent recording uses the same object.
score = study.output["exploratory score"]
score.record(0.97)
experiment.finish()
```

For an output already declared on this `Study` object, assignment calls its
`record()` method, preserving its type, units, and other metadata. For a new
label, integers and floats infer a scalar without units, strings infer text,
and other JSON-compatible values (including booleans, lists, dictionaries,
and `None`) infer JSON. New declarations emit the same warning as explicit
declarations during a run. Files and time series should be declared explicitly;
assignment does not infer them from paths or lists of rows.

Repeated assignments record observations rather than replacing the handle or
erasing history. A finished experiment rejects assignment even if a newer run
is active. `experiment.output` is a mapping from labels to study handles;
iterate its `.values()` for handles, or iterate `study.output` directly.
Retrieved handles retain the usual behavior of recording to the study's active
run. Assignment itself is bound to the receiving experiment.

When reopening a study from a previous script session, explicitly redeclare
outputs with their original metadata before assigning to them. An inferred
declaration that conflicts with an existing persistent declaration raises an
error rather than changing its metadata.

## Time series and graph links

Use a time-series output when a run produces values that change over time: a
forecast, simulated tank level, control schedule, or optimizer trajectory.
It is still normal Acquirium data. The experiment API chooses a unique stream
for this run and writes it through Acquirium's usual storage path, so the
result can be queried or plotted the same way as data from a driver.

`observed` says *what physical or modeled thing the values describe*. It is the
URI of the observable property in the plant knowledge graph. For example, this
declares that the values are the storage tank's volume, not merely a column of
numbers named `volume`:

```python
tank_volume = study.output("tank volume").timeseries(
    observed=ac.resolve("tank volume"),
    unit="M3",
)
```

`ac.resolve("tank volume")` uses Acquirium's normal text resolver to find the
best matching graph resource. It is convenient in a notebook after the plant
graph has been loaded. For a reusable production script, prefer a stable URI;
`ac.point(uri)` wraps it in the same small graph-resource object. Passing the
URI string directly is equivalent:

```python
facility_load = study.output("facility net load").timeseries(
    observed="urn:flex-pse-example:pump-tank-battery#facility-net-load",
    unit="KiloW-HR",
)
```

After starting a run, add ordinary `(timestamp, value)` rows:

```python
e = study.start(metadata={"scenario": "baseline"})
tank_volume.record([
    (aq.timestamp("2025-07-01T00:00:00Z"), 200.0),
    (aq.timestamp("2025-07-01T01:00:00Z"), 245.0),
])
e.finish()
```

`aq.timestamp()` accepts an ISO-8601 string and returns a timezone-aware UTC
`datetime`. `record()` also accepts ISO strings directly, so the compact form
`("2025-07-01T00:00:00Z", 200.0)` works when no `datetime` object is needed.

The run receives a distinct source/ref identity, so a second scenario cannot
overwrite this one. Acquirium records the stream's point URI, which keeps the
result connected to the tank-volume property in the knowledge graph. The same
data can be fetched with the normal stream client once its reference URI is
known; experiment provenance additionally records which run wrote it and the
time range written.

Runs are terminal: after `finish()` or `fail()`, variable mutation is rejected.
Start the study again for the next scenario; the declared variable objects are
reused but each run's values remain isolated.
