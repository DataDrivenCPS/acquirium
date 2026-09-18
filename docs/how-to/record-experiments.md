# Record experiments

Use experiments to record the inputs, outputs, and events produced by a script
or notebook without changing how the calculation itself runs. For a complete
example, start with [Your first experiment](../tutorials/first-experiment.py).

## Wrap an analysis in an experiment

Define a reusable Study and declare its variables before starting a run:

```python
import acquirium as aq

ac = aq.Acquirium()
study = ac.study.define("operating-scenarios")

configuration = study.input("configuration").json()
cost = study.output("total operating cost").scalar(unit="USD")
solver_log = study.log("solver events")
```

Start one Experiment for each execution of the analysis. Finish successful
runs and mark unsuccessful runs as failed:

```python
for config in configurations:
    experiment = study.start(metadata={"scenario": config["name"]})
    try:
        configuration.record(config)
        result = solve_model(config)
        cost.record(result.total_cost)
        solver_log.record({"event": "solve-complete"})
        experiment.finish()
    except Exception as error:
        experiment.fail(error)
        raise
```

The same handles write to whichever Experiment is active on the Study. A Study
allows one active Experiment at a time, and a finished or failed Experiment
accepts no more observations.

## Choose what to record

All handles provide `record(value)`. The declaration determines the accepted
value and how Acquirium stores it:

| Declaration | Value passed to `record()` |
|---|---|
| `study.input("configuration").json()` | A dictionary or other JSON value |
| `study.input("operator note").text()` | A string |
| `study.output("cost").scalar(unit="USD")` | A number |
| `study.input("configuration file").file(media_type="application/json")` | A file path |
| `study.output("permeate flow").timeseries(observed=point, unit="KiloGM-PER-SEC")` | Numeric timestamp/value pairs |
| `study.log("solver events")` | An event, usually a dictionary |

Inputs and outputs support the same five value types. Labels must be nonempty
and unique across all inputs, outputs, and logs in a Study.

A file handle copies the file into Acquirium when `record()` is called:

```python
configuration_file = study.input("configuration file").file(
    media_type="application/json"
)

experiment = study.start()
configuration_file.record("config.json")
experiment.finish()
```

The copy is content-addressed by its SHA-256 digest. Editing the local file
later does not change the recorded artifact.

The type-specific methods `set()`, `append()`, `attach()`, and `add()` remain
available for values, logs, files, and time-series rows, respectively.

## Record progress and event times

Repeated calls add observations to the run. This is useful for solver progress,
intermediate objectives, and diagnostic events:

```python
solver_log.record({"event": "presolve-complete"})
cost.record(1240.0)
cost.record(1195.0)
```

The server assigns every observation a UTC receipt time and a sequence number
within the run. If an event occurred earlier, record that time separately:

```python
solver_log.record(
    {"event": "converged"},
    occurred_at=aq.timestamp("2026-08-01T12:00:00Z"),
)
```

`occurred_at` applies to JSON, text, scalar, and log records. Time-series rows
carry their own timestamps, and file records use the server's receipt time.

## Add an output while exploring

You can declare an output after a run starts:

```python
experiment = study.start()
result = solve_model(config)

cost.record(result.total_cost)
peak_load = study.output("peak load").scalar(unit="KiloW")
peak_load.record(result.peak_load)

experiment.finish()
```

Acquirium emits a `UserWarning` when this creates a new variable. The variable
becomes part of the reusable Study, so earlier Experiments may not contain it.
Redeclaring an existing variable with matching metadata does not warn.

For quick exploratory values, assign through the active Experiment:

```python
experiment = study.start()
experiment.output["total operating cost"] = result.total_cost
experiment.output["exploratory score"] = 0.95
experiment.finish()
```

For a new label, integers and floats infer a unitless scalar, strings infer
text, and booleans, lists, dictionaries, and `None` infer JSON. Declare files
and time series explicitly because their required metadata cannot be inferred
from the value. Prefer a declared handle in reusable analysis code.

## Reuse a Study in another session

Study definitions persist. Calling `define()` again with the same name reuses
the stored Study and loads its previous variable handles. Use `get()` when the
process is only reading existing experiments:

```python
study = ac.study.define("operating-scenarios")
cost = study.output["total operating cost"]

historical = ac.study.get("operating-scenarios")
historical.variables.frame()
```

Calling the declaration syntax remains idempotent. The role, type, unit, and
other metadata must match the stored declaration; Acquirium rejects conflicts
rather than changing how earlier runs should be interpreted.

Collections can also retrieve or enumerate the handles declared through the
current Study object:

```python
assert study.output["total operating cost"] is cost

for output in study.output:
    print(output.label, output.kind)

for label, output in study.output.items():
    print(label, output)
```

`study.input` and `study.log` support the same collection operations.

For all declarations together, including their persisted metadata:

```python
for variable in historical.variables:
    print(variable.label, variable.role, variable.kind, variable.metadata)

timeseries_variables = historical.variables.where(kind="timeseries").frame()
```

## Connect a time-series result to the facility

Set `observed` to the URI of the observable property that the values describe.
For example, the seawater RO model identifies the water mass flow at its outlet
as `urn:swro/RO-out-flow-mass-water`:

```python
permeate_flow = study.output("permeate flow").timeseries(
    observed=ac.point("urn:swro/RO-out-flow-mass-water"),
    unit="KiloGM-PER-SEC",
)

experiment = study.start(metadata={"scenario": "baseline"})
recorded_flow = permeate_flow.record([
    ("2026-08-01T12:00:00Z", 55.2),
    ("2026-08-01T12:15:00Z", 55.0),
])
experiment.finish()

frame = recorded_flow.dataframe()
```

Use the property URI, not the equipment URI. Passing the URI string directly
is equivalent to `ac.point(uri)`. When exploring a loaded plant model,
`ac.resolve(text)` can help find a property; verify the result before putting
it into reusable code.

Every run writes to a distinct source named `experiment/<run_id>`, so two
scenarios can record the same timestamps for the same property without
overwriting each other. The result is an ordinary Acquirium time series and is
available through the usual query and stream APIs.

`record()` returns the `RecordedSeries` used above, so fetching that run's
samples does not require constructing the internal source name.

For example, this finds mass-flow streams connected to the RO stage, including
the recorded output:

```python
(ac.query()
 .entity(uri="urn:swro/RO", alias="ro")
 .measurement(
     quantity_kind="http://qudt.org/vocab/quantitykind/MassFlowRate",
     alias="flow",
 )
 .metadata())
```

Linking streams to the same property makes them discoverable together; it does
not automatically select scenarios, align timestamps, or normalize units.

See [Load a plant model](load-a-plant-model.md) and
[A driver against an existing plant model](../tutorials/driver-with-a-plant-model.md)
for ways to find the available properties.

## Refer to stored input data

Use `use()` when an Experiment consumes a stream that is already in Acquirium:

```python
source_data = study.input("source data").json()

experiment = study.start()
source_data.use(
    stream_ref_uri,
    interval=(
        aq.timestamp("2026-08-01T12:00:00Z"),
        aq.timestamp("2026-08-02T12:00:00Z"),
    ),
)
experiment.finish()
```

This records the stream reference and optional interval, not a copy of the
rows. Later corrections to the stream can therefore change the data returned
by that reference. Record a file artifact when the analysis needs an unchanged
snapshot.

For interface details, see the [Experiment reference](../reference/experiments.md).
[How experiments preserve context](../explanation/experiments.md) explains the
relationship between the run ledger, files, and time-series storage.
