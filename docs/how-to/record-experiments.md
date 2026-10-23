# Record experiments

Record a script's inputs, outputs, and events as an Experiment. For a runnable
example, see [Your first experiment](../tutorials/first-experiment.py).

Declare variables, record a run, then select saved runs and read their results.
The [API reference](../reference/experiments.md) groups the available methods by task.
For model components bound to reader functions, see
[Record a whole model](record-a-whole-model.md), including the Pyomo adapter.

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
    # Metadata makes runs searchable; record() saves the calculation's inputs.
    run_metadata = {
        "scenario": config["name"],
        "settings": {"flow_m3_per_hour": config["flow_m3_per_hour"]},
    }
    experiment = study.start(metadata=run_metadata)
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

Descriptive metadata uses the same attributes as plant queries. Unit strings
such as `"USD"` or `"kW"` must resolve to known units; full unit URIs also work.
Run metadata uses simple key names such as `scenario` or `settings`; top-level
nulls and empty containers omit run fields. Variable user metadata is a JSON
object under `metadata`, preserving arbitrary keys, nulls, and empty containers.
Semantic fields such as units have explicit variable properties. Values passed to `.json().record()` or a log's
`record()` remain data, including their nulls and empty containers.

## Choose what to record

Use `record(value)` for every variable type:

| Declaration | Value passed to `record()` |
|---|---|
| `study.input("configuration").json()` | A dictionary or other JSON value |
| `study.input("operator note").text()` | A string |
| `study.output("cost").scalar(unit="USD")` | A number |
| `study.input("configuration file").file(media_type="application/json")` | A file path |
| `study.output("pump schedule").table()` | A Polars, pandas, or Arrow table |
| `study.output("model state").object(codec=aq.PickleCodec)` | Any object the codec can save |
| `study.output("permeate flow").timeseries(observed=point, unit="KiloGM-PER-SEC")` | Numeric timestamp/value pairs |
| `study.log("solver events")` | An event, usually a dictionary |

Inputs and outputs support the same seven value types. Labels must be nonempty
and unique across all inputs, outputs, and logs in a Study.

NumPy values, tuples, and dates are converted for you: `cost.record(np.float64(12.5))`
records `12.5`, and an array inside a JSON value becomes a list. A scalar
records only finite numbers and booleans, and text only strings; other values
raise before anything is sent. See the
[reference](../reference/experiments.md#recording) for the conversion rules,
and [Record your own types](#record-your-own-types) for other classes.

A table handle saves a table as a Parquet file, and reads it back as Polars:

```python
pump_schedule = study.output("pump schedule").table()

experiment = study.start()
pump_schedule.record(schedule_frame)
experiment.finish()

saved_schedule = experiment.observations(pump_schedule).latest().dataframe()
```

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

Use `record()` throughout your script. The
[reference](../reference/experiments.md#recording) lists the type-specific alternatives.

## Record your own types

Recording raises `TypeError` for a class it cannot convert to JSON. Register a
handler that turns your class into a JSON value:

```python
from dataclasses import asdict, dataclass

@dataclass
class PumpSetting:
    pump: str
    speed_rpm: float

# Record every PumpSetting as a dictionary of its fields.
aq.register_value_handler(PumpSetting, asdict)

pump_settings = study.input("pump settings").json()

experiment = study.start()
pump_settings.record([PumpSetting("P1", 1450.0), PumpSetting("P2", 1200.0)])
experiment.finish()

saved_settings = experiment.observations(pump_settings).latest().value
# [{'pump': 'P1', 'speed_rpm': 1450.0}, {'pump': 'P2', 'speed_rpm': 1200.0}]
```

The handler applies wherever the class appears: in a recorded value, inside a
list or dict, or in run and variable metadata. It also covers subclasses. Its
result is converted again, so it may contain NumPy values or dates.

Handlers run in your Python session and are not saved. Register them before
recording, for example where the class is defined. Reading a run back returns
the saved JSON, not an instance of your class.

The variable's type still applies. A handler that returns a number can record
to a scalar; one that returns a dictionary needs a JSON or log variable.

## Save objects to restore later

To keep an object you will load back but never search inside, such as a
model's full state, declare an object variable with a codec. The codec turns
the object into bytes when you record it, and back when you load it:

```python
class ModelStateCodec:
    media_type = "application/json"

    def dumps(self, model) -> bytes:
        return json.dumps(model.to_dict()).encode()

    def loads(self, data: bytes):
        return Model.from_dict(json.loads(data))

model_state = study.output("model state").object(codec=ModelStateCodec)

experiment = study.start()
model_state.record(model)
experiment.finish()

restored = experiment.observations(model_state).latest().load()
```

Define the codec at module level and pass the class; `load()` imports it by
name, in this or a later session. `aq.PickleCodec`, `aq.CloudpickleCodec`,
`aq.JsonCodec`, and `aq.BytesCodec` are built in. Prefer a format your library
defines over a pickle when you can: a pickle runs code when it loads, and
usually loads only with the library versions that wrote it. See the
[reference](../reference/experiments.md#object-variables-and-codecs).

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

## Find runs and variables by metadata

Filter saved runs with the same `.where()` syntax used by plant queries:

```python
baseline_runs = historical.experiments.where(scenario="baseline")
baseline_results = baseline_runs.observations().frame()
```

Filter on a nested field to compare higher-flow runs:

```python
# The settings metadata was saved when each run started.
high_flow_runs = historical.experiments.where(
    ac.attr.settings.flow_m3_per_hour > 10
)
# Use a variable handle from the reopened Study.
historical_cost = historical.variables["total operating cost"]
high_flow_results = high_flow_runs.observations(historical_cost).frame()
```

Find the output declaration by its unit:

```python
# Select numeric outputs by their semantic unit.
cost_variables = historical.variables.where(role="output", unit="USD")
cost_variable_table = cost_variables.frame()
```

Filters stay within the selected Study. Unit filters match equivalent spellings:
`unit="kW"` also matches a variable declared with `unit="KiloW"`.

## Read saved scalar results

Choose the runs first, then the variable. A recording table has one row per
`record()` call, including the value and receipt time:

```python
saved_cost = cost_variables["total operating cost"]
baseline_cost_recordings = baseline_runs.observations(saved_cost)
baseline_cost_history = baseline_cost_recordings.frame()
baseline_cost_history.select("experiment_id", "recorded_at", "value")
```

Use `.frame()` for declarations, runs, and recordings. Use `.dataframe()` to
fetch time-series samples, as shown below.

To compare runs, read one row per run with a column per scalar variable:

```python
baseline_scalars = baseline_runs.scalars(metadata=["scenario"])
```

[Compare runs with formulas](compare-experiment-runs.md) shows how to compute
and rank from this table.

## Inspect saved metadata

Use `.query()` when you want to choose metadata columns:

```python
variable_query = historical.variables.query()
variable_metadata = variable_query.include("unit").metadata()

run_query = baseline_runs.query()
run_metadata_table = run_query.include("scenario", "settings").metadata()
```

For Python dictionaries, use the separate object method:

```python
unit_objects = variable_query.include("unit", "unit.raw").metadata_objects()
```

`.frame()` includes saved metadata objects. The graph metadata table displays
semantic units as URIs and nested values as typed structs or lists. An object's
`.metadata` keeps its saved fields and supplied unit string. See the
[reference](../reference/experiments.md#saved-objects-and-metadata-tables) for the
read formats and [metadata rules](../reference/experiments.md#metadata-rules).

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

To read this result later, reopen the Study and retrieve the run by its saved ID:

```python
flow_study = ac.study.get("operating-scenarios")
saved_flow = flow_study.variables["permeate flow"]
saved_flow_run = flow_study.experiments.get(experiment.run_id)
flow_recordings = saved_flow_run.observations(saved_flow)
flow_recording = flow_recordings.latest()
saved_flow_samples = flow_recording.dataframe()
```

This example saves one flow batch. `.latest()` selects that recording; with
incremental writes it selects only the last batch's interval.

`observed` takes a property URI, either as a string or `ac.point(uri)`.
`ac.resolve(text)` searches ontology concepts such as classes and quantity
kinds. Use plant queries to find property instances.

Each run writes under `experiment/<run_id>`, keeping scenario results separate.
The run's samples get their own stream point with `observed` as an attribute,
so plant queries on the property still return only site measurements. Find
every run that simulated the property through the stream query API:

```python
simulated_flows = ac.query().measurement(alias="flow", observed=permeate_property_uri)
simulated_flows.include("run_id", "study_id").metadata_objects()
```

If there is no plant model or known property URI, omit `observed`:

```python
simulated_power = study.output("simulated power").timeseries(unit="kW")
```

The run source and variable label identify the stream. `record()` returns a
`Stream` handle; call `.dataframe()` to read its samples as `time` and
`value` columns.

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

When an Experiment uses a stream that is already in Acquirium, record a range
of it:

```python
# Replace these names with the ones from your driver configuration.
biogas = ac.stream("site-driver", "biogas-flow")

# Time-series input: samples collected at the site.
source_data = study.input("source data").timeseries()
input_range = biogas.between("2026-08-01T12:00:00Z", "2026-08-02T12:00:00Z")

experiment = study.start()
source_data.record(input_range)
experiment.finish()
```

The run stores the stream and range, not the samples. Each read returns the
stream's current samples in that range, so it includes later corrections and
data that arrives after the run, as long as it falls inside the range.

To keep the samples exactly as they are now, record the rows instead:

```python
experiment = study.start()
source_data.record(input_range.dataframe())
experiment.finish()
```

The copy is stored under the run's own source and does not change when the
site stream does. `ac.stream(ref_uri)` also accepts a stream's reference URI.

A recorded range can be another run's input. `observation.interval` is the
stream and range an observation covers:

```python
previous = study.experiments.where(scenario="baseline").observations("source data").latest()

experiment = study.start({"scenario": "rerun"})
source_data.record(previous.interval)
experiment.finish()
```

A run whose stream another run uses this way cannot be deleted on its own;
delete the two together.

## Tag an existing live stream

Use `annotate_stream()` to associate an existing stream interval with a run.
Set both bounds for past data, or omit the end for an ongoing trial. Find the
stream URI through discovery or `ac.stream(source, name).ref_uri`.

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

## Check storage and delete runs

`usage()` counts what experiments store and estimates its size in bytes:

```python
study = ac.study.get("membrane trials")
study.usage()        # this Study
ac.study.usage()     # every Study

failed_runs = study.experiments.where(status="failed")
failed_runs.usage()  # only the selected runs
```

Delete runs you no longer need, or a whole Study:

```python
failed_runs.delete()          # the selected runs
experiment.delete()           # one run
study.delete()                # the Study, its declarations, and every run
ac.study.clear(confirm=True)  # every Study and run
```

Each call returns the number of runs, observations, samples, and files it
removed. A file another run still uses is kept.

Running runs cannot be deleted. If a session ended without finishing its run,
mark the run failed first:

```python
study.resume(saved_run_id).fail(RuntimeError("abandoned"))
```

Deleting a run is also refused when another run recorded one of its streams as
an input. Delete those runs together. `clear()` removes experiment records
only; the plant model, metadata, and other streams stay.

See the [reference](../reference/experiments.md) for the API and
[explanation](../explanation/experiments.md) for storage and run behavior.
