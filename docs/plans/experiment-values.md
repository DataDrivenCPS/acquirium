# Recording other Python values

This note records how two planned additions would fit the value handling that
exists now. Neither is implemented.

## What exists

- `record()` converts a value to JSON on the client (`ExperimentValues.to_json`).
  Lists, dicts, strings, numbers, booleans, and `None` pass through. NumPy
  scalars and arrays, tuples, and dates are converted by built-in handlers.
  NaN and infinity are rejected.
- `aq.register_value_handler(cls, handler)` adds a handler for another type.
  The handler's result is converted again, so it may return containers that
  still hold NumPy or date values.
- Each kind accepts one kind of value (`ExperimentValues.check_kind`). The
  client checks before sending; the server checks again and is authoritative.
- A `table` variable stores a Parquet file as an artifact and reads it back as
  Polars. Artifacts are content addressed, so identical tables are stored once.

## Pickled objects

An `object` kind would save a Python object that has no JSON or table form,
such as a solved model's state:

```python
state = study.output("model state").object()
state.record(model)
restored = run.observations(state).latest().load()
```

How it would plug in:

- Add `object` to `ARTIFACT_KINDS`. The server stores the bytes like a file and
  never unpickles them, so it needs no other change.
- `record()` calls `cloudpickle.dumps(value)` and attaches the bytes as
  `<label>.pkl` with media type `application/x-python-pickle`.
- `observation.load()` downloads the bytes with `read_bytes()` and calls
  `cloudpickle.loads()`.
- cloudpickle becomes an optional extra, `acquirium[objects]`, imported only
  when an object variable records or loads.
- Save the Python and cloudpickle versions with the observation, so a load
  that fails after an upgrade can say why. Artifact observations carry no value
  today; the object kind would allow this one small JSON value.

Risks to document with it:

- Loading a pickle runs code. Load only objects from a server you trust.
- Pickles often fail to load under different library versions. They are for
  restarting work, not for long-term records.
- An object is opaque: it cannot be filtered, compared, or put in a wide frame.
  Record the values you will analyze as scalars or tables as well.

## Declaring variables from a model

Status: planned, not implemented. This section is written to be implemented
as-is; open questions are listed at the end.

### Goal

Lift every value in a model into its own experiment variable, instead of
serializing the model as one opaque value. A builder walks a structure once
per Study, declares one variable per component, and then records the
components' values in each run, either once or repeatedly while a
simulation runs. Every component then behaves like a hand-declared variable:
it has a kind, a unit, and a label, appears in `scalars()` or stream queries,
and can be linked to a plant property.

### Public API

```python
pump_model = aq.adapters.pyomo(model)              # an adapter over one structure
variables = study.declare_from(pump_model)         # declare once; idempotent
experiment = study.start({"scenario": "baseline"})
for step in simulation:
    advance(model)
    variables.record_all(at=start + step * dt)     # append a snapshot
variables.record_all()                             # final scalars and tables
experiment.finish()
```

- `study.declare_from(adapter, *, role="output", include=None, exclude=None)`
  declares every component the adapter yields and returns a
  `DeclaredVariables` collection. It is idempotent: a second call with the
  same model reuses the same declarations, and a component whose kind or unit
  changed raises the existing "declared differently" `ValueError`.
  `include` and `exclude` take glob patterns on component labels.
- `DeclaredVariables` is a mapping from label to `ExperimentVariable`, plus:
  - `record_all(at=None)` reads every component's current value through the
    adapter and records it in the active run, in batched requests (see
    Transport). With `at`, time-series components append one sample at
    that time and scalar, JSON, and table components are skipped. Without
    `at`, scalar, JSON, and table components are recorded and time-series
    components are skipped unless the adapter supplies their full history.
  - `frame()` lists the declarations, like `study.variables.frame()`.

### Adapter interface

The core defines the interface only. Library adapters live in
`acquirium.adapters` (Pyomo first) and are imported lazily, so the core has
no Pyomo dependency.

```python
class ModelAdapter(Protocol):
    def components(self) -> Iterable[Component]: ...
    def read(self, component: Component) -> Any: ...

@dataclass(frozen=True)
class Component:
    label: str                        # stable across runs; from the component name
    kind: str                         # scalar, text, json, table, timeseries
    unit: str | None = None           # QUDT spelling or URI
    keys: tuple[str, ...] = ()        # non-time index names, e.g. ("pump",)
    observed: Mapping[tuple, str] | str | None = None  # property URI, per key or for all
    metadata: Mapping[str, Any] = {}  # extra declaration metadata
```

`read()` returns the value in the shape its kind records: a number or boolean
for a scalar; a string; a JSON value; a table; or, for a time series, a
mapping from key tuple to the current value (with `at`) or to rows (without
`at`). Leaf values still pass through `to_json()` and the registered value
handlers.

The Pyomo adapter maps components as follows:

| Pyomo component | Declared as |
|---|---|
| Scalar `Var`, `Param`, `Expression`, `Objective` | `scalar`, unit from `pyo.units.get_units()` |
| Indexed by a time set only | `timeseries`, one stream |
| Indexed by time and other sets, e.g. `flow[pump, t]` | `timeseries` with `keys=("pump",)`, one stream per key |
| Indexed by non-time sets only | `table` until the `indexed` kind exists (see below) |
| Solver results (status, termination condition) | `text` |

The time set is the model's `ContinuousSet`, or a set the caller names. Pyomo
times are numbers; the adapter maps them to datetimes from a `start` and a
time unit the caller passes.

### Keyed time series

A time-series variable with `keys` records one stream per key under the run's
source, instead of one stream per variable:

- Source `experiment/<run_id>`, as now. Reference name `<label>[<key>]`, for
  example `flow[pump1]`, joined with commas for several keys.
- Each stream's point carries the existing `study_id`, `run_id`, and
  `variable_id`, plus one attribute per key (`pump="pump1"`), and `observed`
  when the adapter maps that key to a plant property. So
  `ac.query().measurement(variable_id=..., pump="pump1")` selects one key
  across runs.
- Each batch writes one observation per key. The observation needs the key,
  so add a nullable `key_json` column to `experiment_observations`
  (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, on both DuckDB and Postgres) and
  return it as `observation.key`. `ExperimentVariable.record()` for a keyed
  variable takes a mapping `{key_tuple: rows}`.
- The per-run registration cache (`Experiment._registered_streams`) is keyed
  by `(variable_id, key)` instead of `variable_id`.
- Deletion needs no change: it already removes every stream under the run's
  source, and the "other runs reference this stream" check works per stream.

### Transport

Recording every component one request at a time does not scale, so add:

- `POST /experiments/templates/{template_id}/variables/batch`: declare many
  variables in one transaction, with the same rules as one declaration.
  Returns them in request order.
- `POST /experiments/runs/{run_id}/observations/batch`: record many value and
  stream-reference observations in one transaction, with the same kind checks
  as one observation. Either all are recorded or none.
- Time-series samples use the existing `insert_timeseries_batch(source_id,
  {ref_name: rows})`, one call per snapshot, after one `register_streams()`
  call for that run's new streams.

`record_all()` should make at most three requests per snapshot: register new
streams, insert samples, record observations. These are separate requests, so
a snapshot is not atomic as a whole. If the observation batch fails, none of
that snapshot's observations are recorded; samples already written stay under
the run's source, unreferenced, and go when the run is deleted or cleared.

### Later: the `indexed` kind

Purely keyed data with no time index, such as `x[i]` or a matrix, is not a
time series. Until it is needed, the Pyomo adapter records it as a `table`.
If cross-run comparison of such values is needed, add an `indexed` kind
stored as rows `(run_id, variable_id, key_json, value)` in a new ledger
table, read with `runs.indexed(variable)` as one long frame across runs. That
is the only new storage this design could require.

### Tests

- Adapter interface: a fake adapter covering every kind, keys, `observed` per
  key, include/exclude, and idempotent redeclaration.
- `record_all()` with and without `at`: values, request count per snapshot,
  and a failed observation batch recording none of its observations.
- Keyed streams: reference names, point attributes, `observation.key`,
  `measurement(variable_id=..., <key>=...)`, and deletion of a run with keyed
  streams.
- Batch endpoints: order, kind enforcement, all-or-nothing, on DuckDB and
  Postgres (`tests/test_experiment_store_contract.py`).
- Pyomo adapter: a small model with a scalar, a time-indexed var, a
  time-and-pump-indexed var, and a non-time-indexed var. Skip when Pyomo is
  absent.
- A real-server check that records a sweep of a few runs with snapshots and
  reads them back with `scalars()` and stream queries.

### Documentation

- A how-to, "Record a whole model", using the Pyomo adapter on a small model.
- Reference entries for `declare_from()`, `DeclaredVariables`, the adapter
  protocol, keyed time series, and the batch endpoints.
- A changelog entry under Unreleased.

### Open questions

- Default role: should `declare_from()` declare outputs only, or let the
  adapter mark inputs (Pyomo `Param`s and fixed `Var`s are natural inputs)?
- Whether `record_all()` should also record the solver log through a
  declared `log` variable.
- Whether keyed time series should be available to hand-declared variables
  too, as `study.output("flow").timeseries(keys=("pump",))`. The design
  allows it.

### Implementation notes

- Build on the `experiment-interface` branch. The recording code is in
  `src/acquirium/Experiments.py` (`ExperimentVariable.record()` and `add()`
  write samples, register streams, and record observations); value
  conversion and kind checks are in `src/acquirium/ExperimentValues.py`;
  routes are in `src/acquirium/Server/app.py`; transport methods are in
  `src/acquirium/Client/client.py`.
- Keep the server authoritative: the batch endpoints run the same
  `_check_observation()` and declaration checks as the single ones.
- Use `uv run` for Python and tests. Run the full suite (`uv run pytest
  tests/unit tests/test_experiment_store_contract.py`) and, with
  `ACQUIRIUM_TEST_PG_DSN` set from `compose.testing.yaml`, the Postgres
  contract tests.
- Run every documentation snippet as written against a real server
  (`aq.init(data_dir=...)` in a temporary directory) before committing.
- Commit messages and PR text carry no AI attribution.

## Pickled objects as a fallback

For state with no table or component form, the `object` kind described above
stays the fallback. A model walk should prefer declared components and only
pickle what it cannot represent.
