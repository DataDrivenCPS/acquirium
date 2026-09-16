# Experiment reference

The experiment interface records a reusable set of variable declarations and
the observations produced by individual executions.

## Entry points

| Interface | Returns | Meaning |
|---|---|---|
| `ac.study.define(name)` | `Study` | Define or reuse a Study by name |
| `ac.study.get(name_or_id)` | `Study` | Reopen a persisted Study and hydrate its variables |
| `ac.study.all()` | `list[Study]` | List persisted Studies |
| `ac.point(uri)` | `Point` | Wrap a property URI for a time-series declaration |
| `ac.resolve(text, kind=None, min_score=0.6)` | `Point` | Resolve text to one graph resource |
| `aq.timestamp(value)` | `datetime` | Parse a string or normalize a datetime to UTC |

Study names must contain a non-whitespace character. `define()` is idempotent
by name.

## Study

A `Study` exposes:

| Member | Meaning |
|---|---|
| `study_id` | Persistent template identifier |
| `name` | Persistent Study name |
| `input` | Callable collection of input variables |
| `output` | Callable collection of output variables |
| `log` | Callable collection of log variables |
| `variables` | All persisted declarations, across roles |
| `experiments` | Queryable collection of current and historical Experiments |
| `start(metadata=None)` | Start and return an `Experiment` |

One Study object can have only one active Experiment. Calling `start()` before
the active Experiment is finished or failed raises `RuntimeError`.

### Variable collections

Calling `study.input(label)` or `study.output(label)` returns a builder. Select
the value type with one of these methods:

| Builder method | Kind | Metadata |
|---|---|---|
| `.json(**metadata)` | `json` | Arbitrary keyword metadata |
| `.text(**metadata)` | `text` | Arbitrary keyword metadata |
| `.scalar(unit=None, **metadata)` | `scalar` | Optional unit plus arbitrary metadata |
| `.file(media_type=None, **metadata)` | `file` | Optional media type plus arbitrary metadata |
| `.timeseries(observed=..., unit=None, **metadata)` | `timeseries` | Required property URI and optional unit; registered as numeric |

`study.log(label, **metadata)` declares a `log` variable directly.

A label must be nonempty and unique across the entire Study. Repeating an
identical declaration reuses the persistent variable and, on the same Study
object, returns the same handle. Changing its role, kind, or metadata raises
`ValueError`.

The role-specific collections support indexing by label, label membership,
`len()`, iteration, `keys()`, `values()`, and `items()`. Iteration yields
handles in declaration order. Both `define()` and `get()` load declarations
that were persisted by earlier processes, so post-hoc analysis does not need
to repeat the declarations.

`study.variables` combines the three roles. Index it by label or persistent
variable ID, call `.all()` for handles, `.where(role=..., kind=...)` to select
them, or `.frame()` for a Polars table containing IDs, labels, roles, kinds,
creation times, and complete metadata. The frame also exposes metadata keys
with at least one non-null value as columns, with nulls for variables that do
not define them. Keys conflicting with a built-in column stay only in the
`metadata` column; the complete metadata is retained there in all cases:

```python
study = ac.study.get("operating-scenarios")
study.variables.frame()

cost = study.variables["total operating cost"]
series_variables = study.variables.where(kind="timeseries").all()
```

Declaring a new variable during an active Experiment emits `UserWarning`.

## ExperimentVariable

Every variable handle exposes `variable_id`, `label`, `role`, `kind`,
`metadata`, `created_at`, and these methods:

| Method | Intended kinds | Behavior |
|---|---|---|
| `record(value, occurred_at=None)` | all | Dispatch to the operation for the declared kind; time series return `RecordedSeries` |
| `set(value, occurred_at=None)` | JSON, text, scalar | Record one value observation |
| `append(value, occurred_at=None)` | log | Record one event |
| `attach(path)` | file | Copy a file into artifact storage |
| `add(rows)` | time series | Write timestamp/value rows to a run-scoped stream |
| `use(ref_uri, interval=None)` | all | Record a reference and optional datetime interval |
| `observations(experiments=None)` | all | Select persisted observations, optionally within an Experiment collection |

`record()` dispatches files to the attachment operation, time series to
`add()`, and other kinds to `set()`. `occurred_at` is supported for JSON, text,
scalar, and log observations; files use receipt time and time-series rows
supply their own sample times.

For a file declaration with `media_type`, prefer `record(path)`: it passes the
declared media type to the upload. The lower-level `attach(path)` method does
not pass declaration metadata and therefore uses the client's default media
type.

Time-series rows may be an iterable of `(timestamp, value)` pairs or an Arrow
or Polars table containing `time` (or `ts`) and `value`. Timestamp strings must
be ISO 8601. Naive datetimes are interpreted as UTC. A successful write returns
a `RecordedSeries`; call `.dataframe()` on it to fetch the stored samples, or
read its `.ref_uri`. An empty row collection does not write data or a ledger
observation and returns `None`.

`use()` accepts a reference URI string or `Point`. Its optional interval is a
two-datetime tuple. It records provenance only and does not copy the referenced
data.

Mutation requires an active Experiment on the variable's Study.

## Experiment

`study.start(metadata=None)` returns an Experiment with:

| Member | Meaning |
|---|---|
| `run_id` | Persistent identifier for this execution |
| `experiment_id` | Alias for `run_id` used by analysis tables |
| `study` | Owning Study |
| `status`, `metadata`, timestamps, `error` | Persisted execution context |
| `output` | Run-bound output mapping |
| `observations(variable=None)` | Select all observations or those for one variable |
| `finish()` | Set status to `succeeded` and make the run terminal |
| `fail(error)` | Set status to `failed`, store its type and message, and make the run terminal |

Each observation is written immediately. `finish()` is not a transaction that
commits all observations at once. A terminal run rejects further observations.

### Output assignment

`experiment.output[label]` returns a declared Study output. Assignment records
a value:

```python
experiment.output["cost"] = 12.5
```

If the label is new, assignment infers these declarations:

| Python value | Inferred kind |
|---|---|
| `int` or `float`, excluding `bool` | Scalar without a unit |
| `str` | Text |
| Any other JSON-compatible value, including `bool`, `list`, `dict`, and `None` | JSON |

Other values and non-finite floats are rejected. File and time-series outputs
must be declared explicitly. A run-bound mapping rejects assignment after its
Experiment becomes inactive, even if the Study has since started another run.

The mapping iterates over labels. Its values are the Study's variable handles,
which continue to route direct `record()` calls to the Study's active run.

## Observation identity and timestamps

The server assigns observations a monotonically increasing sequence number
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
study = ac.study.get("operating-scenarios")
variables = study.variables.frame()
```

`study.experiments` is a lazy selection. `.where()` accepts `status`,
`started_after`, `started_before`, and an exact-match subset of run metadata.
Use `.all()` for `Experiment` objects, `.frame()` for one Polars row per
Experiment, and `.get(experiment_id)` for an individual Experiment:

```python
runs = study.experiments.where(
    status="succeeded",
    metadata={"batch": "september-sweep"},
)
run_table = runs.frame()
experiment = study.experiments.get(run_table["experiment_id"][0])
```

An Experiment collection, individual Experiment, or variable can select
observations. `.all()` returns `Observation` objects, `.frame()` returns a tidy
Polars table, and `.latest()` returns the last matching observation:

```python
cost = study.variables["total operating cost"]
cost_history = runs.observations(cost).frame()
same_history = cost.observations(runs).frame()

one_run = experiment.observations().frame()
latest_cost = experiment.observations(cost).latest().value
```

These are retrieval primitives, not an analysis language. Sort, group, join,
plot, or calculate extrema with Polars and the plotting library of your choice.

Time-series variables are ordinary Acquirium streams. Retain the handle returned
by `record()` and fetch its samples with:

```python
recorded_power = electrical_power.record(power_rows)
frame = recorded_power.dataframe()
```

`RecordedSeries.dataframe()` accepts the same `start`, `end`, `limit`, `order`,
`timeout`, and `value_mode` keyword arguments as the low-level time-series
client. Its `ref_uri` attribute is the persistent stream reference.

A historical time-series `Observation` exposes the same `ref_uri` and recorded
range. Pass that URI to the normal time-series client; experiment storage does
not duplicate samples or implement a separate time-series query path:

```python
power = experiment.observations("electrical power").latest()
frame = ac.client.timeseries_df(
    power.ref_uri,
    start=power.range_start.isoformat(),
    end=power.range_end.isoformat(),
)
```

`Observation.dataframe()` is a convenience that delegates this same call and
defaults to the observation's recorded range.

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
| `POST` | `/experiments/runs/{run_id}/variables/{variable_id}/file` | Attach a file |
| `POST` | `/experiments/runs/{run_id}/finish` | Mark the run succeeded |
| `POST` | `/experiments/runs/{run_id}/fail` | Mark the run failed |

See [Record experiments](../how-to/record-experiments.md) for task-oriented
examples and [How experiments preserve context](../explanation/experiments.md)
for the storage model behind these interfaces.
