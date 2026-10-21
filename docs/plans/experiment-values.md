# Recording other Python values

This note records two additions to value handling. Model declarations are
implemented; pickled objects remain planned.

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

The initial automatic model-walking design has been superseded by
[Explicit model adapters](explicit-model-adapters.md), which is implemented.

Adapters explicitly declare scalar, text, JSON, table, or time-series values
and attach readers. Input/output namespaces choose roles, with output as the
default. `record_values()` records static values; `sample(at=..., model_time=...)`
records time-series samples without inferring model indices from timestamps.
The Pyomo wrapper extracts units and descriptions and evaluates explicit
component selections. Arbitrary nested JSON values and declaration metadata
are preserved.

See [Record a whole model](../how-to/record-a-whole-model.md) for a runnable
example and the [experiment reference](../reference/experiments.md#explicit-model-declarations)
for the API.

## Pickled objects as a fallback

For state with no table or component form, the `object` kind described above
stays the fallback. A model walk should prefer declared components and only
pickle what it cannot represent.
