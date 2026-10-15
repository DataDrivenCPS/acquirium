# Experiment reference

A **Study** holds variable declarations. An **Experiment** is one run of that
Study. Each call to `record()` adds an **Observation** to the active run.

The tables below describe the implemented Python API. The
[proposed read API](#proposed-read-api) is not implemented yet.

## Studies and run lifecycle

| Method | Returns / behavior |
|---|---|
| `ac.study.define(name)` | Create or reopen a `Study` and load its declarations |
| `ac.study.get(name_or_id)` | Reopen a saved `Study` |
| `ac.study.all()` | Return `list[Study]` |
| `study.start(metadata=None)` | Start and return an `Experiment` |
| `study.resume(run_id)` | Bind a saved running Experiment to this Study's variable handles |
| `run.finish()` | Mark the run `succeeded` |
| `run.fail(error)` | Mark it `failed` and save the error type and message |
| `run.as_dict()` | Return the run's ID, status, metadata, timestamps, and error |

Study names must contain a non-whitespace character. `define()` reuses an
existing Study with that name.

Each Study object allows one active run. `start()` and `resume()` raise
`RuntimeError` if a run is active. `resume()` requires a running Experiment
from the same Study. Observations are saved immediately. Finished and failed
runs reject further variable writes; their stream annotations can still change.

| Object | Members |
|---|---|
| Study | `study_id`, `uri`, `name`, `created_at`, `input`, `output`, `log`, `variables`, `experiments` |
| Experiment | `run_id`, `experiment_id`, `uri`, `study`, `status`, `metadata`, `started_at`, `finished_at`, `error`, `output` |

`experiment_id` is an alias for `run_id`, used in analysis tables.

## Declaring variables

Inputs and outputs support the same five value types. The role describes how
the calculation uses a value; the type selects its storage.

| Declaration | Records |
|---|---|
| `study.input("configuration").json(**metadata)` | Structured JSON |
| `study.input("note").text(**metadata)` | Text |
| `study.output("cost").scalar(unit=None, **metadata)` | Scalar values with an optional unit |
| `study.input("model").file(media_type=None, **metadata)` | File copies |
| `study.output("power").timeseries(observed=None, unit=None, **metadata)` | Numeric timestamp/value samples |
| `study.log("solver events", **metadata)` | Events and diagnostics |

Calling `study.input(label)` or `study.output(label)` returns a builder;
choosing a type returns an `ExperimentVariable`. `study.log()` declares a log
variable directly.

Labels must be nonempty and unique across the Study. Matching redeclarations
reuse the handle on that Study object. Changing its role, kind, or metadata
raises `ValueError`. New declarations during an active run emit `UserWarning`
and remain available to future runs.

Unit strings must resolve to known units; full unit URIs also work. For example,
`"kW"` and `"KiloW"` match the same unit. Saved metadata retains the supplied
string. A scalar can carry a unit without registering a stream. `observed`
accepts a plant property URI or a `Point` wrapper.

Variable handles expose `variable_id`, `uri`, `label`, `role`, `kind`, `metadata`,
and `created_at`. New IDs are reference URIs derived from the Study ID and label.
Older UUID IDs remain unchanged; `uri` provides their absolute graph URI.

## Recording

Use `record()` for every variable type. Writes go to the Study object's active run.

| Method | Behavior |
|---|---|
| `variable.record(value)` | Store a value or event, upload a file, or write samples according to the declared type |
| `variable.record(value, occurred_at=...)` | Supply an event time for JSON, text, scalar, or log data |
| `variable.use(ref_uri, interval=None)` | Record an existing stream reference and optional pair of datetimes without copying samples |
| `run.output[label] = value` | Record an output on this active run; infer a declaration if the label is new |

The type-specific write methods remain available:

| Method | Intended kind | Behavior |
|---|---|---|
| `variable.set(value, occurred_at=None)` | JSON, text, scalar | Add a value observation |
| `variable.append(event, occurred_at=None)` | Log | Add an event observation |
| `variable.attach(path)` | File | Copy a file into artifact storage |
| `variable.add(rows)` | Time series | Write samples to the run's stream |

`record(path)` sends the file's declared `media_type`; `attach(path)` uses the
client's default. Time-series rows may be `(timestamp, value)` pairs or an
Arrow or Polars table with `time` (or `ts`) and `value` columns. Strings must be
ISO 8601 timestamps; naive datetimes use UTC. A sample write returns
`RecordedSeries`, or `None` for empty input. Files use server receipt time;
time-series rows carry sample times and do not accept `occurred_at`.

### Output assignment

Assignment requires that Experiment to be active on its Study:

```python
run.output["cost"] = 12.5
```

| New Python value | Inferred declaration |
|---|---|
| `int` or `float`, excluding `bool` | Unitless scalar |
| `str` | Text |
| Other JSON-compatible values, including `bool`, `list`, `dict`, and `None` | JSON |

Other values and non-finite floats are rejected. Declare files and time series
explicitly. `run.output[label]` returns the Study's variable handle; it does
not read that run's recorded value. The mapping iterates over labels.

## Finding variables and runs

| Operation | Variables | Experiments |
|---|---|---|
| Filter | `study.variables.where(*expressions, role=None, kind=None, **metadata)` | `study.experiments.where(*expressions, status=None, started_after=None, started_before=None, **metadata)` |
| Get objects | `selection.all()` → `list[ExperimentVariable]` | `selection.all()` → `list[Experiment]` |
| Get a table | `selection.frame()` — one row per declaration | `selection.frame()` — one row per run |
| Look up one | `selection[label_or_id]` or `selection.get(label_or_id, default=None)` | `study.experiments.get(run_id)` |
| Shared metadata query | `selection.query()` | `selection.query()` |

Variable indexing respects the selection; a missing indexed variable raises
`KeyError`. Experiment `.get()` looks up an ID within the Study, independent of
collection filters. Study-specific variable handles and selections must come
from the same Study object.

The role collections support `study.input[label]`, `study.output[label]`, and
`study.log[label]`, plus membership, `len()`, iteration, `keys()`, `values()`,
and `items()`. Iteration yields handles in declaration order. `study.variables`
includes every role and supports iteration, membership, and `len()` too.

```python
power_variables = study.variables.where(role="output", unit="kW")
power = power_variables["electrical power"]

baseline_runs = study.experiments.where(status="succeeded", scenario="baseline")
baseline_history = baseline_runs.frame()
```

Metadata keywords in one call are combined with AND. List metadata matches
any element. Nested fields use attribute paths:

```python
high_flow_runs = study.experiments.where(ac.attr.settings.flow_m3_per_hour >= 10)
```

Expressions support comparisons and `&`, `|`, and `~`. The older
`where(metadata={...})` run filter remains available for exact-match compatibility.

## Reading recorded data

`.frame()` lists declarations, runs, or recordings. `.dataframe()` fetches
time-series samples.

### Observations

| Method / member | Returns |
|---|---|
| `run.observations(variable=None)` | An `ObservationCollection` for one run; omit the variable to include every variable |
| `runs.observations(variable=None)` | An `ObservationCollection` across selected runs |
| `variable.observations(experiments=None)` | The variable's observations across the given run collection, or the whole Study |
| `observations.all()` | `list[Observation]` |
| `observations.frame()` | A Polars table with one row per recording |
| `observations.latest()` | The last matching recording; raises `LookupError` if none match |
| `observation.value` | The recorded JSON, text, scalar, or event value |
| `observation.dataframe(**kwargs)` | Samples within a time-series recording's interval |
| `observation.as_dict()` | The observation's value, metadata, identifiers, timestamps, and references |

Run observation selectors accept a variable handle, label, or ID. They return
recordings in run-start, run-ID, and recording-sequence order. `.latest()` picks
the last recording in that order. For incremental time-series writes, it reads
only the last batch's interval, not every batch in the run.

```python
# Read a scalar's recorded history across selected runs.
cost = study.variables["cost"]
cost_observations = baseline_runs.observations(cost)
cost_history = cost_observations.frame()

# Read one run's last power recording.
power_observations = run.observations(power)
power_recording = power_observations.latest()
power_samples = power_recording.dataframe()
```

An Observation exposes `experiment_id`, `variable`, `sequence`, `recorded_at`,
`occurred_at`, `status`, `experiment_metadata`, `started_at`, and `finished_at`.
Time-series records also carry `ref_uri`, `range_start`, and `range_end`.
Files carry `artifact_digest`, `filename`, `media_type`, and `byte_length`.

Observation tables preserve structured and mixed values with Polars `Object`
columns. Uniform scalar types retain native columns; integers mixed with floats
widen to `Float64`. Reads hydrate missing declarations added by another session
while preserving existing handles.

### RecordedSeries

| Member | Behavior |
|---|---|
| `recorded_series.ref_uri` | Persistent reference to the stream returned by a sample write |
| `recorded_series.dataframe(**kwargs)` | Read the stream's samples |

```python
recorded_power = power.record(power_rows)
power_samples = recorded_power.dataframe()
```

Sample reads accept the low-level time-series client's `start`, `end`, `limit`,
`order`, `timeout`, and `value_mode` options. `Observation.dataframe()` defaults
to its recorded range; `RecordedSeries.dataframe()` reads its stream without
those default bounds.

## Metadata

### Saved objects and metadata tables

| Method / member | Returns |
|---|---|
| `variable.metadata` | Saved declaration metadata |
| `run.metadata` | Saved run metadata |
| `annotation.metadata` | Saved annotation metadata |
| `selection.query().include(...).metadata()` | A Polars graph metadata table |
| `selection.query().include(...).metadata_objects()` | Python dictionaries grouped by query alias |
| `query.schema()` | Metadata column types |
| `query.options(attribute)` | Available values and counts |
| `query.facets()` | Attribute summaries |

Queries stay within the Study and collection filters. Graph table columns use
`experiment.*` or `variable.*` names. See the [query reference](client-api.md#query).

Saved metadata retains supplied semantic strings. Native queries expose the
resolved value and supplied text separately: `unit="unit:PSI"` and
`unit.raw="PSI"`. Objects contain each node's own fields, with its CURIE under
`id`. Tables use a shared schema, so missing struct fields appear as nulls.
Collection `.frame()` methods keep saved metadata dictionaries in `Object` columns;
variable frames also expose descriptive metadata keys as columns.

Saved metadata reads use the shared object API, with lifecycle fields removed
and supplied strings restored from `.raw`. SQL snapshots cover recovery and
shapes the shared reader cannot yet reproduce. See the
[integration plan](../plans/experiment-metadata-integration.md).

### Metadata rules

Keys contain letters, digits, underscores, and hyphens, starting with a letter
or underscore. Nested objects and lists use dotted paths and list indices.
`None`, `[]`, and `{}` remove metadata; an empty string is retained. Recorded
JSON and log data retain nulls and empty containers.

Built-in keys such as `unit`, `quantity_kind`, and `type` have semantic meanings.
Relation keys such as `entity` and `upstream` are rejected. These lifecycle keys
are reserved at the top level:

`study_id`, `run_id`, `variable_id`, `annotation_id`, `role`, `kind`, `status`,
`created_at`, `started_at`, `finished_at`, `range_start`, `range_end`, `ref_uri`,
and `label`.

Put similarly named descriptive fields inside a context object, such as
`{"context": {"type": "baseline"}}`. See the
[shared metadata rules](../how-to/attach-metadata.md) for details.

## Stream annotations and file artifacts

### Stream annotations

| Method | Behavior |
|---|---|
| `run.annotate_stream(ref_uri, start=..., end=None, metadata=None)` | Associate an existing stream interval with a run; return `StreamAnnotation` |
| `run.stream_annotations(ref_uri=None, start=None, end=None)` | Find that run's annotations |
| `study.stream_annotations(run_id=None, ref_uri=None, start=None, end=None)` | Find annotations within the Study |
| `ac.study.stream_annotations(ref_uri=None, start=None, end=None)` | Find annotations across Studies |
| `annotation.dataframe(**kwargs)` | Read current samples within the interval, with inclusive bounds |
| `annotation.close(end)` | Set the end of an open interval once |

Annotations require a stream reference URI and start time. The end must be at
or after the start; omitted ends stay open. Times accept datetimes or ISO 8601
strings, with naive values treated as UTC. Metadata is a JSON object.
Intervals may overlap. Samples remain live; annotating does not copy them.

Annotations expose `annotation_id`, `run_id`, `template_id`, `ref_uri`,
`range_start`, `range_end`, `metadata`, and `created_at`. Annotation searches
match overlapping intervals across all run statuses. Results are ordered by
start time, run ID, and sequence; omitted search bounds are unbounded.

### File artifacts

File bytes are copied into artifact storage and deduplicated by SHA-256 digest.
Later local edits do not change the saved copy. Filename and media type come
from the first attachment with that digest. File observations identify the
artifact; there is currently no high-level method for downloading its bytes.

## Identity, timestamps, and shared helpers

| Helper | Returns / behavior |
|---|---|
| `ac.point(uri)` | Wrap a property URI as a `Point` for declarations |
| `ac.resolve(text, kind=None, min_score=0.6)` | Resolve an ontology concept to a `Point` |
| `aq.timestamp(value)` | Parse or normalize a datetime to UTC |

`ac.resolve()` searches vocabulary concepts. Find deployment property instances
with [plant queries](../how-to/record-experiments.md#connect-a-time-series-result-to-the-facility),
then pass their URIs as `observed`.

The server assigns each observation a run-local sequence and UTC `recorded_at`
receipt time. Caller-supplied `occurred_at` is separate. Time-series writes use
source `experiment/<run_id>` and the variable label as the reference name.
Each observation records the stream URI and minimum and maximum sample times.

## Proposed read API

These methods are proposed and are **not implemented**:

| Proposed method | Contract |
|---|---|
| `variables.one()` | Return exactly one variable; error on zero or multiple matches |
| `runs.one()` | Return exactly one Experiment; error on zero or multiple matches |
| `run.dataframe(variable)` | Read all recorded sample intervals for that variable in that run |

The intended workflow is:

```python
# Proposed API; this code does not run on the current branch.
power_variables = study.variables.where(role="output", unit="kW")
power = power_variables.one()

baseline_runs = study.experiments.where(scenario="baseline")
baseline = baseline_runs.one()

power_samples = baseline.dataframe(power)
```

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
| `POST` | `/experiments/templates/{template_id}/runs/query` | Retrieve runs selected by graph IDs |
| `GET` | `/experiments/runs/{run_id}` | Retrieve one Experiment |
| `GET` | `/experiments/templates/{template_id}/observations` | Filter and list observations |
| `POST` | `/experiments/templates/{template_id}/observations/query` | Retrieve observations for graph-selected runs |
| `POST` | `/experiments/metadata/sync` | Backfill or repair the experiment metadata graph |
| `POST` | `/experiments/runs/{run_id}/variables/{variable_id}/observations` | Record a value or reference |
| `POST` | `/experiments/runs/{run_id}/stream-annotations` | Tag a stream interval |
| `GET` | `/experiments/stream-annotations` | Find annotations by Study, run, stream, and overlapping time |
| `POST` | `/experiments/stream-annotations/{annotation_id}/close` | Close an open interval |
| `POST` | `/experiments/runs/{run_id}/variables/{variable_id}/file` | Attach a file |
| `POST` | `/experiments/runs/{run_id}/finish` | Mark the run succeeded |
| `POST` | `/experiments/runs/{run_id}/fail` | Mark the run failed |

See [Record experiments](../how-to/record-experiments.md) for examples and
[Experiments](../explanation/experiments.md) for storage and run behavior.
