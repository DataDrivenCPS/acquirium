# Record experiments

Record a script's inputs, outputs, and events as an Experiment. For a runnable
example, see [Your first experiment](../tutorials/first-experiment.py).

## Wrap an analysis in an experiment

Connect to your Acquirium server, then declare the values you want to record.
Inputs describe what goes into the calculation; outputs hold its results.
The type specifies how Acquirium stores each value.

```python
import acquirium as aq

ac = aq.Acquirium()
study = ac.study.define("operating-scenarios")

# A structured input used by your calculation.
configuration = study.input("configuration").json()
# One numeric result per record, with its unit.
cost = study.output("total operating cost").scalar(unit="USD")
# Events and diagnostics saved with each run.
solver_log = study.log("solver events")
```

Start an Experiment for each run. Replace `solve_model(config)` with your
existing calculation; this example reads its `total_cost` result.

```python
configurations = [
    {"name": "baseline", "flow_m3_per_hour": 10.0},
    {"name": "higher throughput", "flow_m3_per_hour": 12.0},
]

for config in configurations:
    # Acquirium: start a run and save its inputs.
    experiment = study.start(metadata={"scenario": config["name"]})
    try:
        configuration.record(config)

        # Your model: run the calculation as usual.
        result = solve_model(config)

        # Acquirium: save the results and end the run.
        cost.record(result.total_cost)
        solver_log.record({"event": "solve-complete"})
        experiment.finish()
    except Exception as error:
        # Keep the recorded inputs and mark this run failed.
        experiment.fail(error)
        raise
```

Variable handles write to the Study object's active Experiment. Each object
allows one active run. Finished and failed runs reject further variable writes.

## Choose what to record

Use `record(value)` for every variable type:

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
# File input: retain the bytes used by this run.
configuration_file = study.input("configuration file").file(
    media_type="application/json"
)

experiment = study.start()
configuration_file.record("config.json")
experiment.finish()
```

Files are stored by SHA-256 digest. Later edits to the local file leave the
recorded copy unchanged.

You can also use `set()` for values, `append()` for logs, `attach()` for files,
and `add()` for time-series rows.

## Record progress and event times

Each call adds an observation. Record progress and intermediate results as they
occur:

```python
experiment = study.start()
solver_log.record({"event": "presolve-complete"})
cost.record(1240.0)
cost.record(1195.0)
```

The server assigns a UTC receipt time and sequence number. Use `occurred_at`
to record when an event happened:

```python
event_time = aq.timestamp("2026-08-01T12:00:00Z")
solver_log.record(
    {"event": "converged"},
    occurred_at=event_time,
)
experiment.finish()
```

`occurred_at` applies to JSON, text, scalar, and log records. Time-series rows
carry their own timestamps, and file records use the server's receipt time.

## Add an output while exploring

You can declare an output after a run starts:

```python
experiment = study.start()
result = solve_model(config)

cost.record(result.total_cost)
# Declare a new result discovered while working on the model.
peak_load = study.output("peak load").scalar(unit="KiloW")
peak_load.record(result.peak_load)

experiment.finish()
```

New declarations during a run emit `UserWarning` and become part of the Study.
Earlier runs may lack those variables. Matching redeclarations do not warn.

For quick exploratory values, assign through the active Experiment:

```python
experiment = study.start()
experiment.output["total operating cost"] = result.total_cost
# A new label creates a unitless scalar variable.
experiment.output["exploratory score"] = 0.95
experiment.finish()
```

New labels infer a unitless scalar for numbers, text for strings, and JSON for
booleans, lists, dictionaries, and `None`. Declare files and time series
explicitly. Use declared handles in reusable code.

## Reuse a Study in another session

`define(name)` reopens an existing Study or creates it. `get(name)` requires
an existing Study. Both load its saved declarations:

```python
# Reuse the Study when running more calculations.
study = ac.study.define("operating-scenarios")
cost = study.output["total operating cost"]

# Load saved declarations for analysis.
historical = ac.study.get("operating-scenarios")
historical.variables.frame()
```

Repeating a declaration returns the existing variable. Its role, type, unit,
and metadata must match; conflicts raise an error.

Index collections by label or iterate over their variables:

```python
assert study.output["total operating cost"] is cost

for output in study.output:
    print(output.label, output.kind)

outputs_by_label = study.output.items()
for label, output in outputs_by_label:
    print(label, output)
```

`study.input` and `study.log` support the same collection operations.

Use `study.variables` for all declarations and their metadata:

```python
for variable in historical.variables:
    print(variable.label, variable.role, variable.kind, variable.metadata)

timeseries_variables = historical.variables.where(kind="timeseries")
timeseries_table = timeseries_variables.frame()
```

## Connect a time-series result to the facility

Set `observed` to the URI of the observable property that the values describe.
To find properties in a loaded plant model, query the model's property links.
For example, this lists the properties on the seawater RO model's outlet:

```python
S223 = "http://data.ashrae.org/standard223#"
property_class = S223 + "QuantifiableObservableProperty"
property_links = [S223 + "hasProperty"]

# Start at the outlet in the plant model.
plant_query = ac.query()
outlet = plant_query.entity(uri="urn:swro/RO-out", alias="outlet")

# Follow its property links.
outlet_properties = outlet.related(
    property_class,
    via=property_links,
    max_depth=1,
    alias="property",
)
property_table = outlet_properties.metadata()
property_table

# Get full URIs to use in variable declarations.
property_uris = outlet_properties.resolved_nodes(alias="property")
print(property_uris)
```

This works before any streams are registered. `metadata()` displays matches;
`resolved_nodes()` returns full URIs. Select the property for your output.
Here, water mass flow is `urn:swro/RO-out-flow-mass-water`:

```python
permeate_property_uri = "urn:swro/RO-out-flow-mass-water"
# A computed trajectory linked to the modeled flow property.
permeate_flow = study.output("permeate flow").timeseries(
    observed=permeate_property_uri,
    unit="KiloGM-PER-SEC",
)

# These rows would come from your calculation.
flow_rows = [
    ("2026-08-01T12:00:00Z", 55.2),
    ("2026-08-01T12:15:00Z", 55.0),
]

experiment = study.start(metadata={"scenario": "baseline"})
recorded_flow = permeate_flow.record(flow_rows)
experiment.finish()

# Read this run's saved samples.
frame = recorded_flow.dataframe()
```

`observed` takes a property URI, either as a string or `ac.point(uri)`.
`ac.resolve(text)` searches ontology concepts such as classes and quantity
kinds. Use plant queries to find property instances.

Each run writes under `experiment/<run_id>`, keeping scenario results separate.
Read them through Acquirium's query and stream APIs.

If there is no plant model or known property URI, omit `observed`:

```python
simulated_power = study.output("simulated power").timeseries(unit="kW")
```

The run source and variable label identify the stream. `record()` returns a
`RecordedSeries`; call `.dataframe()` to read its samples.

For example, this finds mass-flow streams connected to the RO stage, including
the recorded output:

```python
mass_flow_kind = "http://qudt.org/vocab/quantitykind/MassFlowRate"

# Find mass-flow measurements on the RO stage.
equipment_query = ac.query()
ro = equipment_query.entity(uri="urn:swro/RO", alias="ro")
flow_query = ro.measurement(quantity_kind=mass_flow_kind, alias="flow")

# Include references to distinguish streams on the same property.
flow_data = flow_query.data()
flow_streams = flow_data.metadata(include_ref_uris=True)
flow_streams
```

A shared property lets queries find related streams. Select scenarios, align
timestamps, and convert units yourself when comparing them.

See [Load a plant model](load-a-plant-model.md) and
[A driver against an existing plant model](../tutorials/driver-with-a-plant-model.md)
for ways to find the available properties.

## Refer to stored input data

Use `use()` when an Experiment consumes a stream that is already in Acquirium:

```python
# Replace these names with the ones from your driver configuration.
stream_source = "site-driver"
stream_name = "biogas-flow"
stream_ref_uri = str(ac.reference_uri(stream_source, stream_name))

# Time-series input: reference samples already collected at the site.
source_data = study.input("source data").timeseries()
input_start = aq.timestamp("2026-08-01T12:00:00Z")
input_end = aq.timestamp("2026-08-02T12:00:00Z")
input_interval = (input_start, input_end)

experiment = study.start()
source_data.use(stream_ref_uri, interval=input_interval)
experiment.finish()
```

`use()` stores the stream reference and interval. Reads include later
corrections to the source data. Record a file for a fixed snapshot.

## Tag an existing live stream

Use `annotate_stream()` to associate an existing stream interval with a run.
Set both bounds for past data, or omit the end for an ongoing trial. Find the
stream URI through discovery or `ac.reference_uri(source, name)`.

```python
study = ac.study.define("membrane trials")
# Declare a trial result now so another session can record it later.
biogas_efficiency = study.output("biogas efficiency").scalar()
experiment = study.start(metadata={"digester": "A"})
# Keep this ID to resume the trial in another session.
saved_run_id = experiment.run_id

# Associate the existing stream with this trial from its start time onward.
trial_start = aq.timestamp("2026-08-01T00:00:00Z")
annotation = experiment.annotate_stream(
    stream_ref_uri,
    start=trial_start,
    metadata={"membrane": "candidate 2"},
)

```

Pass `end=` to add a closed interval, including after the Experiment finishes.
`annotation.dataframe()` reads current samples with inclusive bounds.

Find annotations across Studies by stream and overlapping time window:

```python
search_start = aq.timestamp("2026-08-05T00:00:00Z")
search_end = aq.timestamp("2026-08-06T00:00:00Z")
matches = ac.study.stream_annotations(
    ref_uri=stream_ref_uri,
    start=search_start,
    end=search_end,
)
for match in matches:
    print(match.template_id, match.run_id, match.metadata)
```

Use `study.stream_annotations(...)` or `experiment.stream_annotations(...)`
to narrow the search. Concurrent runs can annotate the same stream. You can
close an interval after its Experiment finishes.

To continue a running trial in another session, reopen its Study and resume it:

```python
# Reopen the Study and bind its handles to the saved running trial.
study = ac.study.get("membrane trials")
experiment = study.resume(saved_run_id)
biogas_efficiency = study.output["biogas efficiency"]
biogas_efficiency.record(0.82)
```

When the trial ends, retrieve its annotation, close the interval, and finish:

```python
trial_annotations = experiment.stream_annotations()
annotation = trial_annotations[0]
trial_end = aq.timestamp("2026-08-12T18:00:00Z")
annotation.close(trial_end)
experiment.finish()
```

`resume()` requires a running trial. Use separate Study objects for concurrent
runs.

See the [reference](../reference/experiments.md) for the API and
[explanation](../explanation/experiments.md) for storage and run behavior.
