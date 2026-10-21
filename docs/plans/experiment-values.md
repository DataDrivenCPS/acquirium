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

## Recording a whole model

A helper would record a model's values in one call, choosing each variable's
kind from its value:

```python
aq.record_model(study, model)  # proposed
```

How it would plug in:

- An adapter per model library lists `(label, value, unit)` items. For Pyomo
  and WaterTAP, iterate `component_data_objects(Var)` and read `value()` and the
  Pyomo unit. For SciPy, read `OptimizeResult` fields such as `x`, `fun`,
  `status`, and `nit`.
- The helper picks a kind for each item: a number is a scalar with its unit, a
  string is text, a small array or dict is JSON, a DataFrame is a table, and a
  time-indexed series is a time series. Other values are skipped unless the
  caller asks for `object`.
- It declares each variable through the usual builders and calls `record()`.
  Leaf values still go through the same handlers, so registered types work.
- Labels come from component names, so every run of a Study declares the same
  variables. A component whose kind changes between runs raises the usual
  "declared differently" error.

Before it is practical for large models:

- Recording sends one HTTP request per value. A model with thousands of
  variables needs a batch endpoint that records many observations in one
  request and one transaction.
- Recording every WaterTAP variable in large sweeps still needs a performance
  evaluation, as the explanation's limits section notes.
