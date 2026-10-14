# Experiment reference

A Study declares variables. Its Experiments record values from individual runs.

## Entry points

| Interface | Returns | Meaning |
|---|---|---|
| `ac.study.define(name)` | `Study` | Define or reuse a Study by name |
| `ac.study.get(name_or_id)` | `Study` | Reopen a Study and load its variables |
| `ac.study.all()` | `list[Study]` | List saved Studies |
| `ac.study.stream_annotations(ref_uri=None, start=None, end=None)` | `list[StreamAnnotation]` | Find stream intervals across Studies |
| `ac.point(uri)` | `Point` | Wrap a property URI for a time-series declaration |
| `ac.resolve(text, kind=None, min_score=0.6)` | `Point` | Resolve vocabulary text to an ontology concept |
| `aq.timestamp(value)` | `datetime` | Parse a string or normalize a datetime to UTC |

Study names must contain a non-whitespace character. `define(name)` reuses an
existing Study with that name.

`ac.resolve()` searches ontology concepts. Find property instances with
[plant queries](../how-to/record-experiments.md#connect-a-time-series-result-to-the-facility),
then pass their full URIs to `observed`.

## Study

| Member | Meaning |
|---|---|
| `study_id` | Persistent Study identifier |
| `name` | Persistent Study name |
| `input` | Callable collection of input variables |
| `output` | Callable collection of output variables |
| `log` | Callable collection of log variables |
| `variables` | All persisted declarations, across roles |
| `experiments` | Queryable collection of current and historical Experiments |
| `start(metadata=None)` | Start and return an `Experiment` |
| `resume(run_id)` | Continue recording a saved running Experiment |
| `stream_annotations(run_id=None, ref_uri=None, start=None, end=None)` | Find this Study's stream intervals |

Each Study object allows one active Experiment. `start()` and `resume()` raise
`RuntimeError` if a run is active. `resume()` requires a run from this Study
with status `running`.

### Variable collections

Calling `study.input(label)` or `study.output(label)` returns a builder. Select
the value type with one of these methods:

The role says whether a value is an input or result. The type selects its
storage. Your calculation supplies the value through `record()`.

| Builder method | Kind | Metadata |
|---|---|---|
| `.json(**metadata)` | `json` | Arbitrary keyword metadata |
| `.text(**metadata)` | `text` | Arbitrary keyword metadata |
| `.scalar(unit=None, **metadata)` | `scalar` | Optional unit plus arbitrary metadata |
| `.file(media_type=None, **metadata)` | `file` | Optional media type plus arbitrary metadata |
| `.timeseries(observed=None, unit=None, **metadata)` | `timeseries` | Optional property URI and unit; registered as numeric |

`study.log(label, **metadata)` declares a `log` variable directly.

Labels must be nonempty and unique within the Study. Matching redeclarations
return the same variable handle on that Study object. Changes to its role,
kind, or metadata raise `ValueError`.

The role-specific collections support indexing by label, label membership,
`len()`, iteration, `keys()`, `values()`, and `items()`. Iteration yields
handles in declaration order. Both `define()` and `get()` load saved
declarations.

`study.variables` contains all three roles. IDs are reference URIs derived from
the Study ID and label. Index by label or ID, use `.all()` for handles,
`.where(role=..., kind=...)` to filter, or `.frame()` for a Polars table.

The frame includes IDs, labels, roles, kinds, creation times, and metadata.
Metadata keys with non-null values also appear as columns. Keys that conflict
with built-in columns stay in `metadata`:

```python
# Load the saved declarations.
study = ac.study.get("operating-scenarios")
variable_table = study.variables.frame()

# Retrieve one output by its label.
cost = study.variables["total operating cost"]

# Select the variables that record time-series samples.
series_selection = study.variables.where(kind="timeseries")
series_variables = series_selection.all()
```

Declaring a new variable during an active Experiment emits `UserWarning`.

## ExperimentVariable

Every variable handle exposes `variable_id`, `label`, `role`, `kind`,
`metadata`, `created_at`, and these methods:

| Method | Intended kinds | Behavior |
|---|---|---|
| `record(value, occurred_at=None)` | all | Record according to the declared type; time series return `RecordedSeries` |
| `set(value, occurred_at=None)` | JSON, text, scalar | Record one value observation |
| `append(value, occurred_at=None)` | log | Record one event |
| `attach(path)` | file | Copy a file into artifact storage |
| `add(rows)` | time series | Write timestamp/value rows to a run-scoped stream |
| `use(ref_uri, interval=None)` | all | Record a reference and optional datetime interval |
| `observations(experiments=None)` | all | Select persisted observations, optionally within an Experiment collection |

`record()` uploads files, writes time-series samples, or stores other values.
JSON, text, scalar, and log records accept `occurred_at`. Files use receipt time;
time-series rows carry sample times.

`record(path)` sends the file's declared `media_type`. `attach(path)` uses the
client's default media type.

Time-series rows may be an iterable of `(timestamp, value)` pairs or an Arrow
or Polars table containing `time` (or `ts`) and `value`. Timestamp strings must
be ISO 8601. Naive datetimes are interpreted as UTC. A successful write returns
a `RecordedSeries` with `.dataframe()` and `.ref_uri`. Empty input returns
`None` and writes nothing.

`use()` accepts a reference URI string or `Point` and an optional pair of
datetimes. It stores the reference without copying samples.

Writing requires an active Experiment on the variable's Study object.

## Experiment

`study.start(metadata=None)` returns an Experiment with:

| Member | Meaning |
|---|---|
| `run_id` | Persistent identifier for this execution |
| `experiment_id` | Alias for `run_id` used by analysis tables |
| `study` | Owning Study |
| `status`, `metadata`, timestamps, `error` | Saved run details |
| `output` | Run-bound output mapping |
| `observations(variable=None)` | Select all observations or those for one variable |
| `annotate_stream(ref_uri, start=..., end=None, metadata=None)` | Tag an existing stream interval |
| `stream_annotations(ref_uri=None, start=None, end=None)` | Find this Experiment's stream intervals |
| `finish()` | End the run with status `succeeded` |
| `fail(error)` | End the run with status `failed` and save the error type and message |

Observations are saved immediately. Finished and failed runs reject further
variable writes. Stream annotations can still be added or closed.

### Stream annotations

`annotate_stream()` requires a stream reference URI and a start time. Its
optional end must be at or after the start. Both accept datetimes or ISO 8601
strings; timezone-less values use UTC. Metadata is a JSON object stored on the
annotation. Intervals can overlap; samples are not copied.

The returned `StreamAnnotation` has `annotation_id`, `run_id`, `template_id`,
`ref_uri`, `range_start`, `range_end`, `metadata`, and `created_at`. Its
`.dataframe(**kwargs)` reads current samples with inclusive bounds and forwards
query options to the client. Open intervals have `range_end=None`. `.close(end)`
sets the end once, including after the Experiment finishes. `created_at` records
when the annotation was added.

The Study service, Study, and Experiment each provide `stream_annotations()`.
Filter by stream URI and overlapping time window. Omitted bounds are unbounded.
Results include all run statuses and are ordered by start time, run ID, and
recording sequence.

### Output assignment

`experiment.output[label]` returns a declared Study output. Assignment records
a value:

```python
# Store another observation for the active run's cost output.
experiment.output["cost"] = 12.5
```

If the label is new, assignment infers these declarations:

| Python value | Inferred kind |
|---|---|
| `int` or `float`, excluding `bool` | Scalar without a unit |
| `str` | Text |
| Any other JSON-compatible value, including `bool`, `list`, `dict`, and `None` | JSON |

Other values and non-finite floats are rejected. Declare files and time series
explicitly. Assignment requires this Experiment to be active on its Study.

The mapping iterates over labels. Its values are Study variable handles;
direct `record()` calls write to the Study's active run.

## Observation identity and timestamps

The server assigns observations an increasing sequence number
within each run and a UTC `recorded_at` receipt time. Caller-provided
`occurred_at` is stored separately.

A time-series variable writes under source `experiment/<run_id>` with the
variable label as its reference name. The ledger observation contains the
resulting reference URI and the minimum and maximum sample times.

Files are identified by SHA-256 digest and deduplicated by content. Artifact
metadata uses the filename and media type from the first attachment with that
digest.

## Reading recorded data

Reopen a Study without repeating its declarations:

```python
# Reopen a Study to inspect its recorded variables.
study = ac.study.get("operating-scenarios")
variables = study.variables.frame()
```

`study.experiments` is a lazy selection. `.where()` accepts `status`,
`started_after`, `started_before`, and an exact-match subset of run metadata.
Use `.all()` for `Experiment` objects, `.frame()` for one Polars row per
Experiment, and `.get(experiment_id)` for an individual Experiment:

```python
# Filter runs by status and metadata.
runs = study.experiments.where(
    status="succeeded",
    metadata={"batch": "september-sweep"},
)
run_table = runs.frame()

# Select one run from that table.
selected_run_id = run_table["experiment_id"][0]
experiment = study.experiments.get(selected_run_id)
```

Select observations from a run collection, Experiment, or variable. `.all()`
returns `Observation` objects, `.frame()` a Polars table, and `.latest()` the
last matching observation:

```python
cost = study.variables["total operating cost"]

# Read this output across the selected runs.
cost_observations = runs.observations(cost)
cost_history = cost_observations.frame()

# Read all inputs, outputs, and events for one run.
run_observations = experiment.observations()
one_run = run_observations.frame()

# Read the last cost recorded within that run.
run_costs = experiment.observations(cost)
latest_cost_observation = run_costs.latest()
latest_cost = latest_cost_observation.value
```

`cost.observations(runs)` also selects that output across the given runs.

The `value` column preserves the Python values returned by `.all()`. Selections
containing structured values or mixed scalar types use Polars `Object` dtype.
Uniform scalar types retain native columns for calculations; normalize mixed
numeric values to a common type before recording if you need numeric operations.
Observation reads refresh missing variable declarations when another session
has added them, preserving existing variable handles.

Use Polars to sort, group, join, and analyze the returned tables.

Read samples from the `RecordedSeries` returned by `record()`:

```python
# Record the rows produced by your calculation.
recorded_power = electrical_power.record(power_rows)
# Fetch the samples from this result's stream.
frame = recorded_power.dataframe()
```

`RecordedSeries.dataframe()` accepts the same `start`, `end`, `limit`, `order`,
`timeout`, and `value_mode` keyword arguments as the low-level time-series
client. Its `ref_uri` attribute is the persistent stream reference.

A saved time-series observation contains the stream URI and recorded range:

```python
# Find the saved stream reference for this run's output.
power_observations = experiment.observations("electrical power")
power = power_observations.latest()
range_start = power.range_start.isoformat()
range_end = power.range_end.isoformat()

# Read only the interval recorded by this observation.
frame = ac.client.timeseries_df(
    power.ref_uri,
    start=range_start,
    end=range_end,
)
```

`Observation.dataframe()` makes the same call using the recorded range.

## HTTP API

The HTTP API calls a Study a `template`. `template_id`, `variable_id`, and
`run_id` identify declarations and executions.

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/experiments/templates` | Define or reuse a Study |
| `GET` | `/experiments/templates` | List Studies; optionally filter by exact name |
| `GET` | `/experiments/templates/lookup?identifier=...` | Retrieve a Study by name or ID |
| `POST` | `/experiments/templates/{template_id}/variables` | Declare a variable |
| `GET` | `/experiments/templates/{template_id}/variables` | List declarations and metadata |
| `POST` | `/experiments/templates/{template_id}/runs` | Start an Experiment |
| `GET` | `/experiments/templates/{template_id}/runs` | Filter and list Experiments |
| `GET` | `/experiments/runs/{run_id}` | Retrieve one Experiment |
| `GET` | `/experiments/templates/{template_id}/observations` | Filter and list observations |
| `POST` | `/experiments/runs/{run_id}/variables/{variable_id}/observations` | Record a value or reference |
| `POST` | `/experiments/runs/{run_id}/stream-annotations` | Tag a stream interval |
| `GET` | `/experiments/stream-annotations` | Find annotations by Study, run, stream, and overlapping time |
| `POST` | `/experiments/stream-annotations/{annotation_id}/close` | Close an open interval |
| `POST` | `/experiments/runs/{run_id}/variables/{variable_id}/file` | Attach a file |
| `POST` | `/experiments/runs/{run_id}/finish` | Mark the run succeeded |
| `POST` | `/experiments/runs/{run_id}/fail` | Mark the run failed |

See [Record experiments](../how-to/record-experiments.md) for examples and
[Experiments](../explanation/experiments.md) for storage and run behavior.
