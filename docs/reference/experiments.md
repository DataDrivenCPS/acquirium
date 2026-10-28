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
| `run.delete()` | Delete a finished run and what it recorded |
| `study.experiments.where(...).delete()` | Delete the selected runs |
| `study.delete()` | Delete the Study, its declarations, and every run |
| `ac.study.clear(confirm=True)` | Delete every Study and run; raises `ValueError` without `confirm=True` |
| `study.usage()`, `ac.study.usage()`, `study.experiments.where(...).usage()` | Count and size what the Study, every Study, or the selected runs store |

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

Deletion removes a run's observations, stream annotations, the samples and
stream points under `experiment/<run_id>`, and files no remaining run uses.
It returns counts: `runs`, `observations`, `annotations`, `samples`, and
`files` removed, plus `studies` and `variables` for `study.delete()` and
`clear()`. Nothing is removed when a selected run is still running, or when a
run outside the selection uses a stream a selected run recorded. The server
rejects both with HTTP 409.

`usage()` returns `studies`, `variables`, `runs`, `observations`,
`annotations`, `samples`, `files`, and the sizes `sample_bytes`, `file_bytes`,
`ledger_bytes`, and `total_bytes`. File sizes are exact, with a shared file
counted once. Sample and ledger sizes estimate stored values. A run selection
reports zero `studies` and `variables`.

## Declaring variables

Inputs and outputs support the same seven value types. The role describes how
the calculation uses a value; the type selects its storage.

| Declaration | Records |
|---|---|
| `study.input("configuration").json(**metadata)` | Structured JSON |
| `study.input("note").text(**metadata)` | Text |
| `study.output("cost").scalar(unit=None, **metadata)` | Scalar values with an optional unit |
| `study.input("model").file(media_type=None, **metadata)` | File copies |
| `study.output("schedule").table(**metadata)` | Tables, saved as Parquet files |
| `study.output("model").object(codec=Codec, **metadata)` | Python objects, saved as bytes by a codec and rebuilt by `observation.load()` |
| `study.output("power").timeseries(observed=None, unit=None, keys=(), **metadata)` | Numeric timestamp/value samples, optionally one stream per key |
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

Variable user metadata has the same meaning for hand and model declarations.
For example, `study.output("cost").scalar(unit="USD", objective="minimize")`
exposes `cost.unit == "USD"` and `cost.metadata == {"objective": "minimize"}`.
An explicit `metadata={...}` accepts arbitrary string-keyed JSON; legacy
extra descriptive keywords merge into that object. Equal duplicate values
merge; conflicting values raise an error naming the fields to repair.

| Legacy access or query | Current access or query |
|---|---|
| `cost.metadata["unit"]` | `cost.unit` |
| `cost.metadata["observed"]` | `cost.observed` |
| `study.variables.where(objective="minimize")` | `study.variables.where(**{"metadata.objective": "minimize"})` |
| `study.variables.query().include("objective")` | `study.variables.query().include("metadata.objective")` |

Run metadata keeps its existing contract, including `where(scenario=...)`.
Exact `metadata={...}` run selection compares saved document fields; ordinary
attribute expressions filter RDF projections. These are separate operations.

### Explicit model declarations

`aq.adapters.define()` creates a general adapter builder. `aq.adapters.pyomo(model)`
creates an explicit Pyomo builder, initially empty. Optional Pyomo dependencies
are imported lazily; install `acquirium[pyomo]`.

Both builders have `input` and `output` namespaces. Unqualified declarations
are outputs. All labels are unique across namespaces. Arbitrary metadata uses
an explicit `metadata=` JSON object; unknown keyword arguments are rejected.

| General builder declaration | Reader result |
|---|---|
| `scalar(label, *, read, unit=None, quantity_kind=None, metadata=None)` | Finite number or boolean |
| `text(label, *, read, metadata=None)` | String |
| `json(label, *, read, metadata=None)` | Any JSON-convertible value |
| `table(label, *, read, metadata=None)` | Supported Polars, pandas, or Arrow table |
| `object(label, *, read, codec, metadata=None)` | Any value the codec can save |
| `timeseries(label, *, read=None, read_at=None, unit=None, quantity_kind=None, observed=None, metadata=None)` | One finite numeric value |

Time series require exactly one of zero-argument `read()` and one-argument
`read_at(model_time)`. Other kinds require `read()`. Pyomo declarations accept a
source component as a second positional argument. Unindexed sources supply a
current reader by default; indexed sources require an explicit reader. Pyomo
extracts units, documentation, and the source path; it does not infer kinds
or select a time axis. A unit override must denote the same scale and dimension
as known source units. Explicitly converted values can use a general reader
and their resulting unit.

`adapter.frame()` lists declarations and reader modes without evaluating them.
`adapter.preview(*, model_time=UNSET)` returns a label-to-value dictionary after
local validation, with no server requests.

`study.declare_from(adapter, *, include=None, exclude=None)` declares a fixed
selection in a batch and returns `DeclaredVariables`, a mapping from label to
`ExperimentVariable`. Filters accept a glob string or sequence; exclusion wins.
Empty adapters and empty selections raise `ValueError`. Repeated declarations
reuse handles and reject changed roles, kinds, units, or metadata. A fresh
adapter binds readers to a fresh model while reusing the saved declarations.

| `DeclaredVariables` member | Behavior |
|---|---|
| `input`, `output` | Role-specific mapping views with the same operations |
| `record_values()` | Read and batch scalar, text, JSON, and table values in the active run |
| `sample(*, at, model_time=UNSET)` | Read time series and record each value at the supplied timestamp |
| `preview(*, model_time=UNSET)` | Locally read and validate the selected values without an active run |
| `frame()` | List the selected declarations |

`model_time` is passed unchanged to indexed readers; `at` supplies the UTC
observation timestamp and never selects model indices. Current-state readers
receive no arguments. Omitted model time is distinct from `0` and `None`.
Readers for other kinds are not invoked by `sample()`, and time-series readers
are not invoked by `record_values()`. Reader/validation failures precede any
recording requests and include the label, source path, and model-time context.
Full histories are recorded explicitly with `variable.record(rows)`.

Variable `metadata` preserves the user's JSON object, including arbitrary
string keys, nulls, and empty containers. Semantic fields are exposed separately
as `unit`, `quantity_kind`, `keys`, `observed`, `source_path`, and `description`.
Metadata cannot override these fields or the role. Valid attribute paths get a
queryable graph projection under `metadata`; all other keys remain in the
complete saved JSON. JSON values and metadata pass through registered value
handlers and reject NaN and infinity.

The low-level `ModelAdapter` protocol defines `components()`, `read(component)`,
and `sample(component, *, model_time=UNSET)`. `Component` carries the declaration
fields, including its role. `AdapterDefinition.add(component, *, read=None,
read_at=None)` supports advanced explicitly keyed declarations: a keyed reader
returns a mapping from key tuples to numeric values. The declaration's `keys`
determines the expected shape. Ordinary single-stream readers return numbers.

The prototype automatic Pyomo options, `record_all()`, and
`declare_from(role=...)` have been removed. Use explicit declarations,
`sample()`, `record_values()`, and the adapter input/output namespaces. See [Record a whole model](../how-to/record-a-whole-model.md).

### Keyed time series

Declare `study.output("flow").timeseries(keys=("pump",))` and record a mapping
such as `{("pump1",): rows, ("pump2",): rows}`. Each nonempty entry records one
stream observation whose `key` is a tuple. Unkeyed observations have `key=None`.
Index names must be distinct metadata names and cannot override built-in stream
fields or `study_id`, `run_id`, `variable_id`, or `observed`. Key values are
finite numbers, booleans, or strings.

Stream references use source `experiment/<run_id>` and stable storage names
created by the normal reference URI helper from the variable ID and its JSON key.
Labels, delimiters, and key types cannot alias another variable's stream.
The readable variable label stays on the stream point. Each point carries the study, run, and variable IDs,
its named key attributes, and `observed` when supplied. Query a key across runs
with `ac.query().measurement(variable_id=flow.variable_id, pump="pump1")`.
The `observed` argument also accepts per-key mappings on hand declarations.

### Batch transport

Batch endpoints accept a JSON array and return results in request order:

| Endpoint | Item fields |
|---|---|
| `POST /experiments/templates/{template_id}/variables/batch` | `label`, `role`, `kind`, optional `metadata` |
| `POST /experiments/runs/{run_id}/record` | `variable_id`, `form`, and that form’s payload (below) |
| `POST /experiments/runs/{run_id}/observations/batch` (compatibility) | `variable_id`, optional `value`, `occurred_at`, `ref_uri`, `start`, `end`, `key`, `artifact_digest`, `artifact` |

Declarations and observations each commit in a single transaction using the
same validation as individual writes. Unknown runs or observation variables
return 404; invalid or conflicting items return 400 and record none of the
batch. Stream intervals require both `start` and `end` with `start <= end`.
`key` is a JSON array, required for keyed time series and omitted otherwise.
`artifact` contains `filename`, optional `media_type`, and base64-encoded
`data`; tables must contain Parquet bytes. This lets a model's final tables
and scalar observations share one request. Alternatively reference an existing
`artifact_digest`. Artifact variables cannot also carry `value` or `ref_uri`.

The recording endpoint accepts one form per entry:

| `form` | Payload |
|---|---|
| `value` | `value`, optional `occurred_at` |
| `artifact` | `artifact` with `filename`, optional `media_type`, base64 `data` |
| `artifact_reference` | Existing `artifact_digest` |
| `samples` | `samples`: timestamp/number pairs, optional typed `key` |
| `reference` | `ref_uri`, optional `interval: [start, end]`, optional typed `key` |

The server derives sample source and stream identities from the run, variable
ID, and typed key. A nonempty `sample()` or `record_values()` uses one recording
request, including first samples. Ordinary `record()` and convenience methods
use the same execution path. Readers are all checked before sending; table
bytes are prepared once. Invalid batches leave no new SQL samples, stream
registrations, artifacts, observations, revisions, or outbox entries.

SQL commits recording state, artifact bytes, and resolved graph intent
together. Graph delivery remains a separate store: graph failures retain
replayable intent.
Old orphan samples remain covered by deletion and clearing. A retried HTTP
request can append another observation; receipts do not promise exactly-once
HTTP retries. See [migration and recovery](storage-migrations.md).

## Recording

Use `record()` for every variable type. Writes go to the Study object's active run.

| Method | Behavior |
|---|---|
| `variable.record(value)` | Store a value or event, upload a file, or write samples according to the declared type |
| `variable.record(value, occurred_at=...)` | Supply an event time for JSON, text, scalar, or log data |
| `variable.record(stream.between(start, end))` | Record a reference to a stream range without copying its samples |
| `run.output[label] = value` | Record an output on this active run; infer a declaration if the label is new |

The type-specific write methods remain available:

| Method | Intended kind | Behavior |
|---|---|---|
| `variable.set(value, occurred_at=None)` | JSON, text, scalar | Add a value observation |
| `variable.append(event, occurred_at=None)` | Log | Add an event observation |
| `variable.attach(path)` | File | Copy a file into artifact storage |
| `variable.add(rows)` | Time series | Write samples to the run's stream |

Each type accepts one kind of value, checked by the client before sending and
again by the server:

| Type | `record()` accepts |
|---|---|
| Scalar | A finite number or boolean |
| Text | A string |
| JSON, log | Any JSON value: dicts with string keys, lists, strings, numbers, booleans, or `None` |
| File | A file path |
| Table | A Polars, pandas, or Arrow table |
| Object | Any value its codec's `dumps()` accepts |
| Time series | Timestamp/value rows |

Values are converted to JSON first. NumPy scalars become Python numbers,
NumPy arrays and tuples become lists, and dates and times become ISO 8601
strings; a naive datetime is UTC. NaN and infinity are rejected anywhere in a
value. Run, variable, and annotation metadata use the same conversion. Teach
recording about another type with `aq.register_value_handler(cls, handler)`.
The handler returns a JSON value, which may still contain NumPy or date values;
it also applies to subclasses of `cls`.

Assigning a new label through `run.output[label]` creates a text variable for a
string, a scalar for a number or boolean, and a JSON variable otherwise.

`record(path)` sends the file's declared `media_type`; `attach(path)` uses the
client's default. Time-series rows may be `(timestamp, value)` pairs or an
Arrow or Polars table with `time` (or `ts`) and `value` (or `value_numeric`)
columns. A wide query frame with `time` and one stream column also works;
rows from several streams raise `ValueError`. Every value must be a number.
Strings must be ISO 8601 timestamps; naive datetimes use UTC. A sample write
returns the run's `Stream`, or `None` for empty input. Files use server
receipt time; time-series rows carry sample times and do not accept
`occurred_at`.

A time-series variable also records a `StreamInterval`, from
`stream.between(start, end)`. That stores only the stream and range: reads
return the stream's current samples, including later corrections and data
that arrives afterwards. Record `interval.dataframe()` instead to store a
fixed copy. Recording a `Stream` without a range raises `TypeError`, as does
recording a stream to a variable of another type.

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
| `runs.scalars(*variables, metadata=())` | A Polars table with one row per selected run and one column per scalar variable |
| `observation.value` | The recorded JSON, text, scalar, or event value |
| `observation.dataframe(**kwargs)` | Samples within a time-series recording's interval, or a table recording as a Polars DataFrame |
| `observation.read_bytes()` | The saved bytes of a file, table, or object recording |
| `observation.load(codec=None)` | The object an object recording saved, rebuilt by its codec; `codec=` decodes any artifact's bytes |
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
Files and tables carry `artifact_digest`, `filename`, `media_type`, and
`byte_length`. A table's file is `<label>.parquet` with media type
`application/vnd.apache.parquet`.

`runs.scalars()` columns are `experiment_id`, the requested run metadata keys
(dotted paths read nested fields), then each scalar variable by label: those
passed as labels or handles, or every scalar in the Study. A cell holds the
run's last recording of that variable, or null. A column no selected run
recorded is `Float64`. Passing a non-scalar variable raises `TypeError`; a
metadata key that repeats a column name raises `ValueError`.

Observation tables preserve structured and mixed values with Polars `Object`
columns. Uniform scalar types retain native columns; integers mixed with floats
widen to `Float64`. Reads hydrate missing declarations added by another session
while preserving existing handles.

### Streams

| Method / member | Returns |
|---|---|
| `ac.stream(source_id, name)` | A `Stream` for the stream a driver writes as `(source_id, name)` |
| `ac.stream(ref_uri)` | A `Stream` for a reference URI |
| `stream.ref_uri` | The stream's reference URI; `str(stream)` returns it too |
| `stream.dataframe(start=None, end=None, **options)` | The stream's samples as `time` and `value` columns |
| `stream.between(start, end)` | A `StreamInterval`; the end must not precede the start |
| `interval.stream`, `interval.start`, `interval.end` | The interval's stream and inclusive bounds |
| `interval.dataframe(**options)` | The samples in the interval |
| `observation.stream` | The stream a time-series observation recorded or referenced |
| `observation.interval` | That stream and the observation's range, or `None` |

```python
recorded_power = power.record(power_rows)
power_samples = recorded_power.dataframe()
```

Sample reads accept the low-level time-series client's `limit`, `order`,
`timeout`, and `value_mode` options. Every experiment sample read returns
`time` and `value` columns: `Stream`, `StreamInterval`, time-series
`Observation`, and `StreamAnnotation` alike. `Observation.dataframe()` defaults
to its recorded range. Streams compare equal when their reference URIs do.

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
`unit.raw="PSI"`. Objects contain each node's own fields, with its full URI under
`id`. Tables use a shared schema, so missing struct fields appear as nulls.
Collection `.frame()` methods keep saved metadata dictionaries in `Object` columns;
variable frames also expose descriptive metadata keys as columns.

Saved metadata comes from complete documents in the graph, preserving supplied
semantic strings, list order, null positions, and nested empty containers.
Queryable attributes are written alongside each document. Lifecycle and recorded
values remain in SQL; metadata writes enter a durable outbox in the same SQL
transaction. Reads deliver pending updates before returning metadata. Successful
graph writes are acknowledged by removing their outbox entries, and interrupted
delivery is safely replayed after restart. There are no permanent SQL metadata
snapshots or field-level SQL fallbacks.

Opening an existing ledger migrates its metadata snapshots and pinned resolutions
to resolved outbox writes before removing the old columns. See
[Storage migrations](storage-migrations.md). Backups must include both SQL and the
graph: the outbox recovers pending writes, while acknowledged metadata is owned by
the graph. A missing authoritative graph raises an error requiring restoration.

### Metadata rules

These rules apply to hand declarations, run metadata, and stream annotations.
Model adapters' explicit `metadata=` objects preserve arbitrary string keys,
nulls, and empty containers as described in [Explicit model declarations](#explicit-model-declarations).

Keys contain letters, digits, underscores, and hyphens, starting with a letter
or underscore. Nested objects and lists use dotted paths and list indices.
At the top level, `None`, `[]`, and `{}` remove ordinary descriptive keys;
an empty string is retained. Nested nulls and empty containers are preserved in
saved documents. An explicit `metadata` namespace preserves its complete JSON
value, including arbitrary string keys, nulls, and empty containers. Recorded
JSON and log data also retain those values.

Built-in keys such as `unit`, `quantity_kind`, and `type` have semantic meanings.
Relation keys such as `entity` and `upstream` are rejected. These lifecycle keys
are reserved at the top level:

`study_id`, `run_id`, `variable_id`, `annotation_id`, `role`, `kind`, `status`,
`created_at`, `started_at`, `finished_at`, `range_start`, `range_end`, `ref_uri`,
and `label`.

Put similarly named descriptive fields inside a context object, such as
`{"context": {"type": "baseline"}}`. See the
[shared metadata rules](../how-to/attach-metadata.md) for details.

## Stream annotations and artifacts

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
from the first attachment with that digest. `observation.read_bytes()`
downloads the saved bytes. Tables are stored the same way, as Parquet files;
the server rejects a table attachment that is not Parquet. Artifact bytes are
stored in the SQL store, beside the observations that reference them.

### Object variables and codecs

An object variable saves a Python object you want back later but never search
inside, such as a model's full state. Its codec turns the object into bytes
and back:

```python
class ModelSpecCodec:
    media_type = "application/json"
    suffix = ".json"          # optional; appended to the saved filename

    def dumps(self, model) -> bytes:
        return json.dumps(emit_spec(model)).encode()

    def loads(self, data: bytes):
        return build_model(json.loads(data))

model = study.output("model").object(codec=ModelSpecCodec)
model.record(m)
restored = run.observations(model).latest().load()
```

Pass the codec class, not an instance. The declaration saves its import path,
and `load()` imports it and builds it with no arguments, so a codec must be
defined at module level; configure one by subclassing. Redeclaring a variable
with a different codec raises `ValueError`. Where the declared codec cannot be
imported, pass `load(codec=...)`.

| Codec | Saves |
|---|---|
| `aq.PickleCodec` | Picklable objects |
| `aq.CloudpickleCodec` | Also lambdas and classes defined in a notebook; requires `cloudpickle` |
| `aq.JsonCodec` | A JSON document, kept as an opaque file instead of a queryable value |
| `aq.BytesCodec` | Bytes, unchanged |

The server stores and serves the bytes without decoding them; codecs run only
in the client that records or loads. Loading a pickle runs code named in its
bytes, so load pickles only from people you trust. A pickle loads only where
the classes it names still import, usually with the same library versions
that wrote it. A format the library itself defines, such as a model spec,
outlasts upgrades better. [`examples/flexpse-codec`](../../examples/flexpse-codec/README.md)
saves flex-pse models through their spec this way.

`aq.register_value_handler()` is the other extension point: it converts a type
to JSON for a JSON, text, or log value, which is stored in the observation and
can be queried, but is not converted back.

## Identity, timestamps, and shared helpers

| Helper | Returns / behavior |
|---|---|
| `ac.point(uri)` | Wrap a property URI as a `Point` for declarations |
| `ac.resolve(text, kind=None, min_score=0.6)` | Resolve an ontology concept to a `Point` |
| `aq.timestamp(value)` | Parse or normalize a datetime to UTC |
| `aq.register_value_handler(cls, handler)` | Record instances of `cls` as `handler(value)` |

`ac.resolve()` searches vocabulary concepts. Find deployment property instances
with [plant queries](../how-to/record-experiments.md#connect-a-time-series-result-to-the-facility),
then pass their URIs as `observed`.

The server assigns each observation a run-local sequence and UTC `recorded_at`
receipt time. Caller-supplied `occurred_at` is separate. Time-series writes use
source `experiment/<run_id>` and a stable reference name derived from the variable ID and key.
Each observation records the stream URI and minimum and maximum sample times.
Observations written by older versions keep their original stream references.
After upgrading, new writes use the new identity, including when resuming an
older run; observation queries continue to include its earlier samples.
Each run's stream has its own point, labeled with the variable name, with the
attributes `study_id`, `run_id`, `variable_id`, and `observed` when declared.
`ac.query().measurement(run_id=...)` or `measurement(observed=...)` selects
these points. Plant queries on an `observed` property do not include them.
Only time-series values are streams; other kinds stay out of `find_all_data()`.

Printing a handle shows what it already holds and never reads from the server.
A Study lists its variable counts and active run; a run, its status and
metadata; an Observation, its value or stream range. Collections are lazy, so
they print their selection; call `.all()` or `.frame()` to read it:

```text
<Study 'pumps': 1 input, 1 output, 1 log>
<Experiment 3f2a… succeeded {'scenario': 'baseline'}>
<ObservationCollection study='pumps' variable='cost'>
<Observation 'cost' = 500 (run 3f2a…, #1)>
```

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
| `POST` | `/experiments/templates/{template_id}/variables/batch` | Declare an ordered batch atomically; see [Batch transport](#batch-transport) |
| `GET` | `/experiments/templates/{template_id}/variables` | List declarations and metadata |
| `POST` | `/experiments/templates/{template_id}/runs` | Start an Experiment |
| `GET` | `/experiments/templates/{template_id}/runs` | Filter and list Experiments |
| `POST` | `/experiments/templates/{template_id}/runs/query` | Retrieve runs selected by graph IDs |
| `GET` | `/experiments/runs/{run_id}` | Retrieve one Experiment |
| `GET` | `/experiments/templates/{template_id}/observations` | Filter and list observations |
| `POST` | `/experiments/templates/{template_id}/observations/query` | Retrieve observations for graph-selected runs |
| `POST` | `/experiments/metadata/sync` | Backfill or repair the experiment metadata graph |
| `POST` | `/experiments/runs/{run_id}/variables/{variable_id}/observations` | Record a value or reference |
| `POST` | `/experiments/runs/{run_id}/record` | Record values, artifacts, and durable graph intent in one SQL transaction, with samples published to Escapement as its last step; see [Batch transport](#batch-transport) |
| `POST` | `/experiments/runs/{run_id}/observations/batch` | Record an ordered batch atomically, including inline artifacts; see [Batch transport](#batch-transport) |
| `POST` | `/experiments/runs/{run_id}/stream-annotations` | Tag a stream interval |
| `GET` | `/experiments/stream-annotations` | Find annotations by Study, run, stream, and overlapping time |
| `POST` | `/experiments/stream-annotations/{annotation_id}/close` | Close an open interval |
| `POST` | `/experiments/runs/{run_id}/variables/{variable_id}/file` | Attach a file or Parquet table; the body is the bytes and `X-Filename` the percent-encoded name |
| `GET` | `/experiments/artifacts/{digest}` | Download a file or table by digest |
| `POST` | `/experiments/runs/{run_id}/finish` | Mark the run succeeded |
| `POST` | `/experiments/runs/{run_id}/fail` | Mark the run failed |
| `POST` | `/experiments/runs/delete` | Delete finished runs by `run_ids` |
| `POST` | `/experiments/templates/{template_id}/delete` | Delete a Study and its runs |
| `POST` | `/experiments/clear` | Delete every Study; requires `{"confirm": true}` |
| `POST` | `/experiments/usage` | Count and size by optional `template_id` or `run_ids` |

See [Record experiments](../how-to/record-experiments.md) for examples and
[Experiments](../explanation/experiments.md) for storage and run behavior.
