---
title: Declaring variables
---

A variable is a named input, output or log of a study. It is declared once and then records one value per call in each run. This page covers the declaration methods for each kind of value, the variable object, and the ways to list and filter declared variables.

| Item | Description |
|---|---|
| [`Study.input`](#studyinput) | Start declaring an input |
| [`Study.output`](#studyoutput) | Start declaring an output |
| [`Study.log`](#studylog) | Declare a log variable for events |
| [`builder.scalar`](#builderscalar) | A number, with an optional unit |
| [`builder.text`](#buildertext) | A string |
| [`builder.json`](#builderjson) | Structured JSON |
| [`builder.file`](#builderfile) | A copy of a file |
| [`builder.table`](#buildertable) | A table |
| [`builder.object`](#builderobject) | A Python object saved through a codec |
| [`builder.timeseries`](#buildertimeseries) | Numeric samples over time |
| [`ExperimentVariable`](#experimentvariable) | Attributes of a declared variable |
| [`Study.variables`](#studyvariables) | Every declared variable of the study |
| [`Study.variables.where`](#studyvariableswhere) | Filter variables |
| [`Study.variables.all`](#studyvariablesall) | Return the selected variables as a list |
| [`Study.variables.frame`](#studyvariablesframe) | Return the selected variables as a table |
| [`Study.variables.get`](#studyvariablesget) | Look up one variable with a default |
| [`Study.variables.query`](#studyvariablesquery) | Query variable metadata |

A declaration has three parts. The role (`input`, `output` or `log`) says how the study uses the value. The label names it. The kind says how it is stored.

```python
import acquirium as aq

acq = aq.init()
study = acq.study.define("pump tuning")
```

Declaring the same label again with the same role, kind and options returns the existing variable. Declaring it differently raises `requests.HTTPError` (400). Labels are unique across the whole study, so an input and an output cannot share a label.

To declare many variables from a model in one call, see [`Study.declare_from`](model-adapters.md#studydeclare_from).

## `Study.input`

```python
Study.input(label)
```

Starts the declaration of an input. Call one of the `builder` methods below on the result to choose the kind.

`study.input` is also a collection of the declared inputs. It supports `study.input[label]`, `label in study.input`, `len()`, iteration over the variables, `keys()`, `values()` and `items()`.

**Parameters**

- `label` (`str`): the variable name. It must not be empty.

**Returns**

A builder with the methods `scalar`, `text`, `json`, `file`, `table`, `object` and `timeseries`.

**Raises**

- `TypeError`: when extra keyword arguments are given. Pass options to the kind method, for example `scalar(unit="Hz")`.

**Example**

```python
speed = study.input("speed").scalar(unit="Hz")
print(speed)
print(study.input["speed"] is speed)
```

```text
<ExperimentVariable input 'speed' scalar>
True
```

## `Study.output`

```python
Study.output(label)
```

Starts the declaration of an output. It works like [`Study.input`](#studyinput), and `study.output` is the collection of declared outputs.

**Parameters**

- `label` (`str`): the variable name.

**Returns**

A builder with the methods `scalar`, `text`, `json`, `file`, `table`, `object` and `timeseries`.

**Raises**

- `TypeError`: when extra keyword arguments are given.

**Example**

```python
cost = study.output("cost").scalar(unit="USD", objective="minimize")
print(cost)
```

```text
<ExperimentVariable output 'cost' scalar {'objective': 'minimize'}>
```

## `Study.log`

```python
Study.log(label, **metadata)
```

Declares a log variable. A log records events and diagnostics as JSON values, each with an optional event time. `study.log` is also the collection of declared logs.

**Parameters**

- `label` (`str`): the variable name.
- `**metadata`: descriptive fields saved in `variable.metadata`.

**Returns**

An [`ExperimentVariable`](#experimentvariable) of kind `"log"`.

**Raises**

- `ValueError`: when `label` is empty.
- `requests.HTTPError` (400): when the label is already declared differently.

**Example**

```python
events = study.log("solver events")
print(events)
```

```text
<ExperimentVariable log 'solver events'>
```

## `builder.scalar`

```python
builder.scalar(*, unit=None, **metadata)
```

Declares a variable that records a finite number or a boolean. Here and below, `builder` is the result of `study.input(label)` or `study.output(label)`.

**Parameters**

- `unit` (`str | None`, default `None`): the unit of the value, such as `"kW"`. It must be a unit the server can resolve. The text you give is kept and returned by `variable.unit`.
- `**metadata`: descriptive fields saved in `variable.metadata`. You can also pass one `metadata={...}` dictionary, which keeps any string keys and `None` values as given. The keyword `quantity_kind` is not metadata. It sets `variable.quantity_kind`.

**Returns**

An [`ExperimentVariable`](#experimentvariable) of kind `"scalar"`.

**Raises**

- `ValueError`: when the label is empty.
- `requests.HTTPError` (400): when the unit cannot be resolved, or the label is already declared differently.

A `UserWarning` is emitted when a new variable is declared while a run is active. The variable is saved and is also available to later runs.

**Example**

```python
cost = study.output("cost").scalar(unit="USD", objective="minimize")
print(cost.unit, cost.metadata)
```

```text
USD {'objective': 'minimize'}
```

## `builder.text`

```python
builder.text(**metadata)
```

Declares a variable that records a string.

**Parameters**

- `**metadata`: descriptive fields saved in `variable.metadata`.

**Returns**

An [`ExperimentVariable`](#experimentvariable) of kind `"text"`.

**Raises**

- `requests.HTTPError` (400): when the label is already declared differently.

**Example**

```python
note = study.input("note").text(author="ana")
print(note)
```

```text
<ExperimentVariable input 'note' text {'author': 'ana'}>
```

## `builder.json`

```python
builder.json(**metadata)
```

Declares a variable that records any JSON value: a dictionary with string keys, a list, a string, a number, a boolean or `None`.

**Parameters**

- `**metadata`: descriptive fields saved in `variable.metadata`.

**Returns**

An [`ExperimentVariable`](#experimentvariable) of kind `"json"`.

**Raises**

- `requests.HTTPError` (400): when the label is already declared differently.

**Example**

```python
configuration = study.input("configuration").json()
print(configuration)
```

```text
<ExperimentVariable input 'configuration' json>
```

## `builder.file`

```python
builder.file(*, media_type=None, **metadata)
```

Declares a variable that records a copy of a file. The bytes are stored on the server, so later edits to the local file do not change the record.

**Parameters**

- `media_type` (`str | None`, default `None`): the content type saved with each file, such as `"text/plain"`.
- `**metadata`: descriptive fields saved in `variable.metadata`.

**Returns**

An [`ExperimentVariable`](#experimentvariable) of kind `"file"`.

**Raises**

- `requests.HTTPError` (400): when the label is already declared differently.

**Example**

```python
report = study.input("report").file(media_type="text/plain")
print(report, report.media_type)
```

```text
<ExperimentVariable input 'report' file> text/plain
```

## `builder.table`

```python
builder.table(**metadata)
```

Declares a variable that records a Polars, pandas or Arrow table. The table is stored as a Parquet file.

**Parameters**

- `**metadata`: descriptive fields saved in `variable.metadata`.

**Returns**

An [`ExperimentVariable`](#experimentvariable) of kind `"table"`.

**Raises**

- `requests.HTTPError` (400): when the label is already declared differently.

**Example**

```python
schedule = study.output("schedule").table()
print(schedule)
```

```text
<ExperimentVariable output 'schedule' table>
```

## `builder.object`

```python
builder.object(*, codec, **metadata)
```

Declares a variable that records a Python object as bytes. A codec turns the object into bytes when it is recorded, and back into an object with [`Observation.load`](reading.md#observationload). See [Codecs and values](codecs-and-values.md).

**Parameters**

- `codec` (`type`): the codec class, not an instance. The class must be defined at the top level of a module, because the declaration saves its import path.
- `**metadata`: descriptive fields saved in `variable.metadata`.

**Returns**

An [`ExperimentVariable`](#experimentvariable) of kind `"object"`. Its `codec` attribute holds the import path and its `media_type` comes from the codec.

**Raises**

- `TypeError`: when `codec` is an instance, has no `media_type` string, or lacks `dumps()` or `loads()`.
- `ValueError`: when the codec class is defined inside a function.
- `requests.HTTPError` (400): when the label is already declared with another codec.

**Example**

```python
model = study.output("model").object(codec=aq.PickleCodec)
print(model.codec, model.media_type)
```

```text
acquirium.ObjectCodecs:PickleCodec application/x-python-pickle
```

## `builder.timeseries`

```python
builder.timeseries(*, observed=None, unit=None, keys=(), **metadata)
```

Declares a variable that records numeric samples over time. Each run stores its samples in its own stream. With `keys`, one variable stores several streams per run, one for each key value.

**Parameters**

- `observed` (`str | dict | None`, default `None`): the URI of the plant property this variable describes. It is saved on the streams the runs create. For a keyed variable, a dictionary from key tuples to property URIs sets one property per key.
- `unit` (`str | None`, default `None`): the unit of the values. It must be a unit the server can resolve.
- `keys` (`tuple[str, ...]`, default `()`): names of the key parts, for example `("pump",)`. Names must be unique, follow the metadata key rules, and not be reserved names such as `unit`, `study_id`, `run_id`, `variable_id` or `observed`.
- `**metadata`: descriptive fields saved in `variable.metadata`.

**Returns**

An [`ExperimentVariable`](#experimentvariable) of kind `"timeseries"`.

**Raises**

- `ValueError`: when a key name is reserved, not valid, or repeated.
- `requests.HTTPError` (400): when the unit cannot be resolved, or the label is already declared differently.

**Example**

```python
power = study.output("power").timeseries(unit="kW")
flow = study.output("flow").timeseries(keys=("pump",))
print(power.unit, flow.keys)
```

```text
kW ('pump',)
```

## `ExperimentVariable`

```python
variable = study.output(label).scalar()
```

A declared variable. Get it from a declaration, from `study.input[label]`, `study.output[label]` or `study.log[label]`, or from [`Study.variables`](#studyvariables).

| Attribute | Type | Meaning |
|---|---|---|
| `label` | `str` | The variable name |
| `role` | `str` | `"input"`, `"output"` or `"annotation"` (the role of a log variable) |
| `kind` | `str` | `"scalar"`, `"text"`, `"json"`, `"file"`, `"table"`, `"object"`, `"timeseries"` or `"log"` |
| `variable_id` | `str` | The variable ID, derived from the study ID and the label |
| `uri` | `str` | The URI of the variable in the metadata graph |
| `study` | `Study` | The study the variable belongs to |
| `metadata` | `dict` | The descriptive fields given at declaration |
| `unit` | `str \| None` | The unit text given at declaration |
| `quantity_kind` | `str \| None` | The quantity kind, when given |
| `keys` | `tuple[str, ...]` | The key names of a keyed time series |
| `observed` | `str \| None` | The property URI of a time series, when one URI was given |
| `media_type` | `str \| None` | The content type of a file or object variable |
| `codec` | `str \| None` | The import path of an object variable's codec |
| `source_path`, `description`, `source_unit` | `str \| None` | Set for variables declared from a model. See [Model adapters](model-adapters.md) |
| `created_at` | `datetime` | When the variable was declared, in UTC |
| `run_id` | `str` | The ID of the study's active run. Raises `RuntimeError` when no run is active |

The methods of a variable are on other pages: [`record`](recording.md#experimentvariablerecord), [`set`](recording.md#experimentvariableset), [`append`](recording.md#experimentvariableappend), [`attach`](recording.md#experimentvariableattach), [`add`](recording.md#experimentvariableadd) and [`observations`](reading.md#experimentvariableobservations).

**Example**

```python
print(cost.label, cost.role, cost.kind, cost.variable_id)
```

```text
cost output scalar urn:acquirium#02d544fe-8184-5b5a-8962-ac1bcfc7a583
```

## `Study.variables`

```python
study.variables
```

The catalog of every declared variable of the study, in all roles. It is ordered by declaration time.

The catalog supports `study.variables[label_or_id]`, `label_or_id in study.variables`, `len()` and iteration. Indexing a missing variable raises `KeyError`.

**Example**

```python
print(study.variables["cost"])
print("cost" in study.variables)
```

```text
<ExperimentVariable output 'cost' scalar {'objective': 'minimize'}>
True
```

## `Study.variables.where`

```python
Study.variables.where(*expressions, role=None, kind=None, **attrs)
```

Returns a narrower catalog. Conditions are combined with AND, and the result can be filtered again. Nothing is read until you call `all()`, `frame()` or index the result.

**Parameters**

- `*expressions`: attribute expressions built with `acq.attr`, such as `acq.attr.unit == "USD"`. See [filtering](../query/filtering.md).
- `role` (`str | None`, default `None`): `"input"`, `"output"` or `"annotation"`.
- `kind` (`str | None`, default `None`): a variable kind, such as `"scalar"`.
- `**attrs`: attribute values to match, such as `unit="kW"`. A metadata field uses its dotted name: `**{"metadata.objective": "minimize"}`.

**Returns**

A catalog with the same methods as `study.variables`.

**Raises**

- `ValueError`: when an attribute name is not known to the metadata graph.

**Example**

```python
print(study.variables.where(role="output", kind="scalar").all())
print(study.variables.where(**{"metadata.objective": "minimize"}).all())
```

```text
[<ExperimentVariable output 'cost' scalar {'objective': 'minimize'}>]
[<ExperimentVariable output 'cost' scalar {'objective': 'minimize'}>]
```

## `Study.variables.all`

```python
Study.variables.all()
```

Returns the selected variables.

**Returns**

`list[ExperimentVariable]`, ordered by declaration time.

**Example**

```python
print(study.variables.where(unit="USD").all())
```

```text
[<ExperimentVariable output 'cost' scalar {'objective': 'minimize'}>]
```

## `Study.variables.frame`

```python
Study.variables.frame()
```

Returns the selected variables as a table with one row per variable.

**Returns**

A Polars `DataFrame`. The columns are `variable_id`, `label`, `role`, `kind`, `metadata` and `created_at`, followed by one column for each declaration field in use, such as `unit`, `keys`, `media_type` or `codec`.

**Example**

```python
print(study.variables.where(role="output").frame().select("label", "kind", "unit"))
```

```text
shape: (2, 3)
┌───────┬────────────┬──────┐
│ label ┆ kind       ┆ unit │
│ ---   ┆ ---        ┆ ---  │
│ str   ┆ str        ┆ str  │
╞═══════╪════════════╪══════╡
│ cost  ┆ scalar     ┆ USD  │
│ power ┆ timeseries ┆ kW   │
└───────┴────────────┴──────┘
```

## `Study.variables.get`

```python
Study.variables.get(label_or_id, default=None)
```

Looks up one variable in the selection without raising when it is missing.

**Parameters**

- `label_or_id` (`str`): the label or the `variable_id`.
- `default` (any, default `None`): the value returned when the variable is not in the selection.

**Returns**

The [`ExperimentVariable`](#experimentvariable), or `default`.

**Example**

```python
print(study.variables.get("nope"))
```

```text
None
```

## `Study.variables.query`

```python
Study.variables.query()
```

Returns a metadata query limited to the selected variables of this study. This is useful for reading variable metadata as a table. See [building queries](../query/building.md) and [reading query results](../query/reading.md).

**Returns**

A query object.

**Example**

```python
print(study.variables.query().metadata())
```

```text
shape: (1, 1)
┌─────────────────────────────────┐
│ variable                        │
│ ---                             │
│ str                             │
╞═════════════════════════════════╡
│ acq:b0fda67e-a67f-5ce3-a8f3-ca… │
└─────────────────────────────────┘
```
