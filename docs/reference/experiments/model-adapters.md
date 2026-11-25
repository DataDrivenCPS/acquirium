---
title: Model adapters
---

A model adapter lists the values of a model that a study should record, with a reader function for each. The study declares all of them in one call, and then records them from the live model with one call per step. This page covers building an adapter, the Pyomo adapter, declaring from an adapter, and the low-level protocol.

| Item | Description |
|---|---|
| [`adapters.define`](#adaptersdefine) | Create an empty adapter for any Python model |
| [`adapters.pyomo`](#adapterspyomo) | Create an empty adapter for a Pyomo model |
| [`AdapterDefinition`](#adapterdefinition) | The adapter builder and its `input` and `output` namespaces |
| [`AdapterDefinition.scalar`](#adapterdefinitionscalar) | Declare a number |
| [`AdapterDefinition.text`](#adapterdefinitiontext) | Declare a string |
| [`AdapterDefinition.json`](#adapterdefinitionjson) | Declare a JSON value |
| [`AdapterDefinition.table`](#adapterdefinitiontable) | Declare a table |
| [`AdapterDefinition.object`](#adapterdefinitionobject) | Declare an object saved through a codec |
| [`AdapterDefinition.timeseries`](#adapterdefinitiontimeseries) | Declare a value sampled over time |
| [`AdapterDefinition.add`](#adapterdefinitionadd) | Register a prepared `Component` |
| [`AdapterDefinition.frame`](#adapterdefinitionframe) | List the declarations as a table |
| [`AdapterDefinition.preview`](#adapterdefinitionpreview) | Read every value locally |
| [`AdapterDefinition.freeze`](#adapterdefinitionfreeze) | Return a copy that accepts no more declarations |
| [`Study.declare_from`](#studydeclare_from) | Declare an adapter's variables in a study |
| [`DeclaredVariables`](#declaredvariables) | The variables declared from an adapter |
| [`DeclaredVariables.record_values`](#declaredvariablesrecord_values) | Read and record every value that is not a time series |
| [`DeclaredVariables.sample`](#declaredvariablessample) | Read and record one sample of every time series |
| [`DeclaredVariables.preview`](#declaredvariablespreview) | Read the selected values locally |
| [`DeclaredVariables.frame`](#declaredvariablesframe) | List the selected variables as a table |
| [`Component`](#component) | The description of one declared value |
| [`ModelAdapter`](#modeladapter) | The protocol a custom adapter implements |

`adapters`, `AdapterDefinition`, `Component`, `ModelAdapter` and `DeclaredVariables` are exported from `acquirium`.

The examples continue from this setup:

```python
import acquirium as aq
import polars as pl

class Tank:
    def __init__(self):
        self.speed = 45.0
        self.cost = 12.5
        self.levels = [1.0, 1.5, 2.0]

tank = Tank()
acq = aq.init()
```

Several signatures below show `UNSET` as a default. `UNSET` means the argument was not given. It is distinct from `None` and `0`, which are valid values.

## `adapters.define`

```python
aq.adapters.define()
```

Creates an empty adapter for a model from any Python library. Every declaration takes a reader function.

**Returns**

An [`AdapterDefinition`](#adapterdefinition).

**Example**

```python
adapter = aq.adapters.define()
adapter.input.scalar("speed", read=lambda: tank.speed, unit="Hz")
```

The examples below add more declarations to this `adapter`.

## `adapters.pyomo`

```python
aq.adapters.pyomo(model)
```

Creates an empty adapter for a Pyomo model. It has the same methods as [`AdapterDefinition`](#adapterdefinition). In addition, each declaration except `object` accepts a Pyomo component as the second positional argument, `source`. Pyomo is imported only when this function is called. Install it with `acquirium[pyomo]`.

A source supplies three things:

- A reader. A source that is not indexed is read with `pyomo.environ.value(source)`. An indexed source needs an explicit `read` or `read_at`.
- The unit, for `scalar` and `timeseries`, taken from the source's Pyomo units. A dimensionless source has no unit.
- `source_path` (the component name) and `description` (the component `doc`).

Readers of scalar, text and time-series declarations may return Pyomo expressions. The result is passed through `pyomo.environ.value()`.

**Parameters**

- `model`: the Pyomo model.

**Returns**

A Pyomo adapter.

**Raises**

- `ImportError`: when Pyomo is not installed.

Declarations on a Pyomo adapter also raise:

- `TypeError`: when `source` is not a Pyomo component.
- `ValueError`: when `source` belongs to another model, when an indexed source has no reader, or when `unit=None` is given for a source that has units.

**Example**

```python
import pyomo.environ as pyo

m = pyo.ConcreteModel()
m.t = pyo.Set(initialize=[0, 1, 2])
m.speed = pyo.Param(initialize=45.0, mutable=True, units=pyo.units.Hz, doc="Pump speed")
m.level = pyo.Var(m.t, initialize={0: 1.0, 1: 1.5, 2: 2.0}, units=pyo.units.m)
m.cost = pyo.Var(initialize=12.5)

pyomo_adapter = aq.adapters.pyomo(m)
pyomo_adapter.input.scalar("speed", m.speed)
pyomo_adapter.scalar("cost", m.cost, unit="USD")
pyomo_adapter.timeseries("level", m.level, read_at=lambda step: m.level[step])
print(pyomo_adapter.preview(model_time=1))
```

```text
{'speed': 45.0, 'cost': 12.5, 'level': 1.5}
```

## `AdapterDefinition`

```python
adapter = aq.adapters.define()
```

The adapter builder. It holds declarations in the order they were made. Labels must be unique across the whole adapter.

| Attribute | Meaning |
|---|---|
| `input` | Namespace with the same declaration methods. Its declarations get the role `"input"` |
| `output` | Namespace with the same declaration methods. Its declarations get the role `"output"` |

Calling a declaration method on the adapter itself, such as `adapter.scalar(...)`, declares an output.

An adapter also implements the [`ModelAdapter`](#modeladapter) protocol through `components()`, `read(component)` and `sample(component, *, model_time=UNSET)`.

**Example**

```python
adapter.output.scalar("cost", read=lambda: tank.cost, unit="USD", metadata={"objective": "minimize"})
print([component.label for component in adapter.components()])
```

```text
['speed', 'cost']
```

## `AdapterDefinition.scalar`

```python
AdapterDefinition.scalar(label, source=UNSET, *, read=None, unit=UNSET, quantity_kind=None, metadata=None)
```

Declares a value that is a finite number or a boolean.

**Parameters**

- `label` (`str`): the variable label.
- `source`: a Pyomo component. Only an adapter from [`adapters.pyomo`](#adapterspyomo) accepts it.
- `read` (callable, default `None`): a function with no arguments that returns the value. It is required unless a Pyomo source supplies it.
- `unit` (`str | None`): the unit of the value. When not given, a Pyomo adapter uses the unit of the source and a general adapter uses no unit.
- `quantity_kind` (`str | None`, default `None`): the quantity kind of the value.
- `metadata` (`dict | None`, default `None`): descriptive fields saved in `variable.metadata`. Keys are strings and values must be convertible to JSON.

**Returns**

A [`Component`](#component) that describes the declaration.

**Raises**

- `ValueError`: when the label is empty or already used, or when no reader is given.
- `TypeError`: when `read` is not callable, when `source` is given to a general adapter, or when an unknown keyword is given.
- `RuntimeError`: when the adapter is frozen.

**Example**

```python
component = adapter.scalar("efficiency", read=lambda: 0.82)
print(component.label, component.kind, component.role, component.unit)
```

```text
efficiency scalar output None
```

## `AdapterDefinition.text`

```python
AdapterDefinition.text(label, source=UNSET, *, read=None, metadata=None)
```

Declares a value that is a string.

**Parameters**

- `label` (`str`): the variable label.
- `source`: a Pyomo component, on a Pyomo adapter only.
- `read` (callable, default `None`): a function with no arguments that returns the string.
- `metadata` (`dict | None`, default `None`): descriptive fields.

**Returns**

A [`Component`](#component).

**Raises**

The same errors as [`scalar`](#adapterdefinitionscalar).

**Example**

```python
adapter.text("status", read=lambda: "optimal")
```

## `AdapterDefinition.json`

```python
AdapterDefinition.json(label, source=UNSET, *, read=None, metadata=None)
```

Declares a value that is any JSON value.

**Parameters**

- `label` (`str`): the variable label.
- `source`: a Pyomo component, on a Pyomo adapter only.
- `read` (callable, default `None`): a function with no arguments that returns the value.
- `metadata` (`dict | None`, default `None`): descriptive fields.

**Returns**

A [`Component`](#component).

**Raises**

The same errors as [`scalar`](#adapterdefinitionscalar).

**Example**

```python
adapter.json("settings", read=lambda: {"levels": tank.levels})
```

## `AdapterDefinition.table`

```python
AdapterDefinition.table(label, source=UNSET, *, read=None, metadata=None)
```

Declares a value that is a Polars, pandas or Arrow table.

**Parameters**

- `label` (`str`): the variable label.
- `source`: a Pyomo component, on a Pyomo adapter only.
- `read` (callable, default `None`): a function with no arguments that returns the table.
- `metadata` (`dict | None`, default `None`): descriptive fields.

**Returns**

A [`Component`](#component).

**Raises**

The same errors as [`scalar`](#adapterdefinitionscalar).

**Example**

```python
adapter.table("levels", read=lambda: pl.DataFrame({"step": [0, 1, 2], "level": tank.levels}))
```

## `AdapterDefinition.object`

```python
AdapterDefinition.object(label, *, read, codec, metadata=None)
```

Declares a Python object that is saved as bytes through a codec. See [Codecs and values](codecs-and-values.md).

**Parameters**

- `label` (`str`): the variable label.
- `read` (callable): a function with no arguments that returns the object.
- `codec` (`type`): the codec class, defined at the top level of a module.
- `metadata` (`dict | None`, default `None`): descriptive fields.

**Returns**

A [`Component`](#component).

**Raises**

- `TypeError`: when `codec` is an instance or is not a valid codec.
- `ValueError`: when the label is empty or already used, or the codec class is defined inside a function.

**Example**

```python
adapter.object("state", read=lambda: vars(tank), codec=aq.PickleCodec)
```

## `AdapterDefinition.timeseries`

```python
AdapterDefinition.timeseries(label, source=UNSET, *, read=None, read_at=None, unit=UNSET, quantity_kind=None, observed=None, metadata=None)
```

Declares a number that changes over time. Each call to [`DeclaredVariables.sample`](#declaredvariablessample) reads one value and stores it as one sample. Give exactly one of `read` and `read_at`.

**Parameters**

- `label` (`str`): the variable label.
- `source`: a Pyomo component, on a Pyomo adapter only.
- `read` (callable, default `None`): a function with no arguments that returns the current value.
- `read_at` (callable, default `None`): a function with one argument, the model time, that returns the value at that model time. Use it for a model that holds a whole time horizon.
- `unit` (`str | None`): the unit of the values.
- `quantity_kind` (`str | None`, default `None`): the quantity kind of the values.
- `observed` (`str | None`, default `None`): the URI of the plant property the values describe.
- `metadata` (`dict | None`, default `None`): descriptive fields.

**Returns**

A [`Component`](#component).

**Raises**

- `ValueError`: when both or neither of `read` and `read_at` are given, or the label is empty or already used.
- `TypeError`: when a reader is not callable.

**Example**

```python
adapter.timeseries("level", read_at=lambda step: tank.levels[step], unit="m")
adapter.timeseries("speed now", read=lambda: tank.speed)
```

## `AdapterDefinition.add`

```python
AdapterDefinition.add(component, *, read=None, read_at=None)
```

Registers a [`Component`](#component) you built yourself. This is useful for a keyed time series, which the declaration methods above do not create. The reader of a keyed time series returns a `dict` from key tuples to numbers.

**Parameters**

- `component` (`Component`): the declaration. Its kind must be `"scalar"`, `"text"`, `"json"`, `"table"`, `"object"` or `"timeseries"`, and its role `"input"` or `"output"`.
- `read` (callable, default `None`): a function with no arguments.
- `read_at` (callable, default `None`): a function of the model time. Only a time series accepts it.

**Returns**

A copy of the validated `Component`.

**Raises**

- `ValueError`: when the kind or role is not supported, the label is already used, both or neither readers are given, or `read_at` is given for a kind other than a time series.
- `TypeError`: when the reader is not callable.
- `RuntimeError`: when the adapter is frozen.

**Example**

```python
flows = {"p1": 3.0, "p2": 4.0}

keyed = aq.adapters.define()
keyed.add(
    aq.Component("flow", "timeseries", keys=("pump",), unit="m3/h"),
    read=lambda: {("p1",): flows["p1"], ("p2",): flows["p2"]},
)
print(keyed.preview())
```

```text
{'flow': {('p1',): 3.0, ('p2',): 4.0}}
```

## `AdapterDefinition.frame`

```python
AdapterDefinition.frame()
```

Lists the declarations as a table. It does not call the readers and does not contact the server.

**Returns**

A Polars `DataFrame` with the columns `label`, `role`, `kind`, `unit`, `quantity_kind`, `source_path`, `description`, `reader` and `metadata`. The `reader` column is `"indexed"` for a `read_at` reader and `"current"` for a `read` reader.

**Example**

```python
print(adapter.frame().select("label", "role", "kind", "unit", "reader"))
```

```text
shape: (9, 5)
┌────────────┬────────┬────────────┬──────┬─────────┐
│ label      ┆ role   ┆ kind       ┆ unit ┆ reader  │
│ ---        ┆ ---    ┆ ---        ┆ ---  ┆ ---     │
│ str        ┆ str    ┆ str        ┆ str  ┆ str     │
╞════════════╪════════╪════════════╪══════╪═════════╡
│ speed      ┆ input  ┆ scalar     ┆ Hz   ┆ current │
│ cost       ┆ output ┆ scalar     ┆ USD  ┆ current │
│ efficiency ┆ output ┆ scalar     ┆ null ┆ current │
│ status     ┆ output ┆ text       ┆ null ┆ current │
│ settings   ┆ output ┆ json       ┆ null ┆ current │
│ levels     ┆ output ┆ table      ┆ null ┆ current │
│ state      ┆ output ┆ object     ┆ null ┆ current │
│ level      ┆ output ┆ timeseries ┆ m    ┆ indexed │
│ speed now  ┆ output ┆ timeseries ┆ null ┆ current │
└────────────┴────────┴────────────┴──────┴─────────┘
```

## `AdapterDefinition.preview`

```python
AdapterDefinition.preview(*, model_time=UNSET)
```

Calls every reader and checks every value, without a study, a run or the server. This is useful for testing an adapter before recording.

**Parameters**

- `model_time` (any): the model time passed to `read_at` readers. It is required when the adapter has a `read_at` reader.

**Returns**

A `dict` from label to the value read, in declaration order.

**Raises**

- `ValueError`: when a reader fails or returns a value that does not fit its kind. The message names the label, the source path and the model time.

**Example**

```python
print(keyed.preview())
adapter.preview()
```

```text
{'flow': {('p1',): 3.0, ('p2',): 4.0}}
ValueError: could not read 'level': this reader requires model_time
```

## `AdapterDefinition.freeze`

```python
AdapterDefinition.freeze()
```

Returns a copy of the adapter that accepts no more declarations. The copy keeps the same reader functions, so it still reads the live model. [`Study.declare_from`](#studydeclare_from) freezes the adapter it is given, so declarations added to the original afterwards do not change what was declared.

**Returns**

An `AdapterDefinition`. Adding a declaration to it raises `RuntimeError`.

**Example**

```python
frozen = adapter.freeze()
frozen.scalar("extra", read=lambda: 1)
```

```text
RuntimeError: this declared adapter is frozen; build a new adapter to change declarations
```

## `Study.declare_from`

```python
Study.declare_from(adapter, *, include=None, exclude=None)
```

Declares the adapter's variables in the study in one request. Variables that are already declared the same way are reused. The saved declarations stay with the study, so a later script can build a fresh adapter on a fresh model and call `declare_from()` again.

**Parameters**

- `adapter` ([`AdapterDefinition`](#adapterdefinition) or [`ModelAdapter`](#modeladapter)): the adapter.
- `include` (`str | list[str] | None`, default `None`): label patterns to keep, such as `"level*"`. Patterns use shell wildcards and are case-sensitive. With `None`, every label is kept.
- `exclude` (`str | list[str] | None`, default `None`): label patterns to leave out. A label that matches both `include` and `exclude` is left out.

**Returns**

A [`DeclaredVariables`](#declaredvariables) mapping.

**Raises**

- `ValueError`: when the adapter is empty, when no label passes `include` and `exclude`, or when a unit override on a Pyomo source would need a conversion (for example `kHz` for a source in `Hz`).
- `requests.HTTPError` (400): when a label is already declared in the study with a different role, kind, unit or metadata.

A `UserWarning` is emitted when new variables are declared while a run is active.

**Example**

```python
study = acq.study.define("tank")
declared = study.declare_from(adapter, exclude="speed now")
print(list(declared))
print(declared["cost"])
```

```text
['speed', 'cost', 'efficiency', 'status', 'settings', 'levels', 'state', 'level']
<ExperimentVariable output 'cost' scalar {'objective': 'minimize'}>
```

## `DeclaredVariables`

```python
declared = study.declare_from(adapter)
```

The variables declared from an adapter, together with their readers. It is a read-only mapping from label to [`ExperimentVariable`](declaring-variables.md#experimentvariable) and supports indexing, iteration over labels, `len()`, `in`, `keys()`, `values()` and `items()`.

| Attribute | Meaning |
|---|---|
| `input` | A `DeclaredVariables` with only the inputs |
| `output` | A `DeclaredVariables` with only the outputs |
| `study` | The study |
| `adapter` | The frozen adapter |

The `input` and `output` views have the same methods, so `declared.input.record_values()` records only the inputs.

**Example**

```python
print(list(declared.input), list(declared.output))
```

```text
['speed'] ['cost', 'efficiency', 'status', 'settings', 'levels', 'state', 'level']
```

## `DeclaredVariables.record_values`

```python
DeclaredVariables.record_values()
```

Reads every selected value that is not a time series (scalar, text, JSON, table and object) and records them in the active run with one request. All readers run and all values are checked before anything is sent.

**Returns**

A `list` of receipt dictionaries with `sequence` and `recorded_at`, one per value.

**Raises**

- `RuntimeError`: when the study has no active run.
- `ValueError`: when a reader fails or returns a value that does not fit its kind. Nothing is recorded.

**Example**

```python
run = study.start({"case": "a"})
print(len(declared.record_values()))
```

```text
7
```

## `DeclaredVariables.sample`

```python
DeclaredVariables.sample(*, at, model_time=UNSET)
```

Reads one value of every selected time series and records it as a sample in the active run with one request. Other kinds are not read.

**Parameters**

- `at` (`datetime | str`): the timestamp stored with the samples. A time without a timezone is read as UTC. It does not select anything in the model.
- `model_time` (any): the model time passed unchanged to `read_at` readers. It is required when a selected time series has a `read_at` reader. Readers declared with `read` take no argument and return the current value.

**Returns**

A `list` of receipt dictionaries with `sequence`, `recorded_at` and `stream_uri`, one per stream.

**Raises**

- `RuntimeError`: when the study has no active run.
- `ValueError`: when a reader fails, needs a `model_time` that was not given, or returns a value that is not a finite number. Nothing is recorded.

**Example**

```python
from datetime import datetime, timezone

for step in range(3):
    declared.sample(at=datetime(2026, 1, 1, 0, step, tzinfo=timezone.utc), model_time=step)
run.finish()

print(run.observations("level").latest().stream.dataframe())
```

```text
shape: (3, 2)
┌─────────────────────────┬───────┐
│ time                    ┆ value │
│ ---                     ┆ ---   │
│ datetime[μs, UTC]       ┆ f64   │
╞═════════════════════════╪═══════╡
│ 2026-01-01 00:00:00 UTC ┆ 1.0   │
│ 2026-01-01 00:01:00 UTC ┆ 1.5   │
│ 2026-01-01 00:02:00 UTC ┆ 2.0   │
└─────────────────────────┴───────┘
```

Each call to `sample()` records one observation per stream. Reading the stream returns all samples of the run.

To record a whole history in one call, use [`ExperimentVariable.record`](recording.md#experimentvariablerecord) with rows: `declared["level"].record(rows)`.

## `DeclaredVariables.preview`

```python
DeclaredVariables.preview(*, model_time=UNSET)
```

Reads and checks the selected values without recording them. It needs no active run.

**Parameters**

- `model_time` (any): the model time passed to `read_at` readers.

**Returns**

A `dict` from label to the value read.

**Raises**

- `ValueError`: when a reader fails or returns a value that does not fit its kind.

**Example**

```python
print(declared.input.preview())
```

```text
{'speed': 45.0}
```

## `DeclaredVariables.frame`

```python
DeclaredVariables.frame()
```

Lists the selected variables as a table, in the format of [`Study.variables.frame`](declaring-variables.md#studyvariablesframe).

**Returns**

A Polars `DataFrame`.

**Example**

```python
print(declared.frame().select("label", "role", "kind", "unit"))
```

```text
shape: (8, 4)
┌────────────┬────────┬────────────┬──────┐
│ label      ┆ role   ┆ kind       ┆ unit │
│ ---        ┆ ---    ┆ ---        ┆ ---  │
│ str        ┆ str    ┆ str        ┆ str  │
╞════════════╪════════╪════════════╪══════╡
│ speed      ┆ input  ┆ scalar     ┆ Hz   │
│ cost       ┆ output ┆ scalar     ┆ USD  │
│ efficiency ┆ output ┆ scalar     ┆ null │
│ status     ┆ output ┆ text       ┆ null │
│ settings   ┆ output ┆ json       ┆ null │
│ levels     ┆ output ┆ table      ┆ null │
│ state      ┆ output ┆ object     ┆ null │
│ level      ┆ output ┆ timeseries ┆ m    │
└────────────┴────────┴────────────┴──────┘
```

## `Component`

```python
aq.Component(label, kind, unit=None, keys=(), observed=None, metadata={}, role='output', quantity_kind=None, source_path=None, description=None, source_unit=None, media_type=None, codec=None)
```

The description of one declared value. The declaration methods of an adapter return it, and [`AdapterDefinition.add`](#adapterdefinitionadd) accepts it. A `Component` is immutable. The class has a few more fields (`variable_id`, `resolved`, `observed_by_key`, `semantic_fields`) that the library fills in.

**Parameters**

- `label` (`str`): the variable label.
- `kind` (`str`): `"scalar"`, `"text"`, `"json"`, `"table"`, `"object"` or `"timeseries"`.
- `unit` (`str | None`, default `None`): the unit of the value.
- `keys` (`tuple[str, ...]`, default `()`): the key names of a keyed time series.
- `observed` (`str | dict | None`, default `None`): the URI of the plant property a time series describes, or a `dict` from key tuples to URIs.
- `metadata` (`dict`, default `{}`): descriptive fields.
- `role` (`str`, default `"output"`): `"input"` or `"output"`.
- `quantity_kind` (`str | None`, default `None`): the quantity kind.
- `source_path` (`str | None`, default `None`): where the value lives in the model. The Pyomo adapter sets it to the component name.
- `description` (`str | None`, default `None`): a description. The Pyomo adapter sets it to the component `doc`.
- `source_unit` (`str | None`, default `None`): the unit found on the model source.
- `media_type` (`str | None`, default `None`): the content type of an object.
- `codec` (`str | None`, default `None`): the import path of an object's codec, as `module:Class`.

**Example**

```python
component = aq.Component("flow", "timeseries", keys=("pump",), unit="m3/h")
print(component.label, component.kind, component.keys, component.role)
```

```text
flow timeseries ('pump',) output
```

## `ModelAdapter`

```python
class ModelAdapter(Protocol):
    def components(self) -> Iterable[Component]: ...
    def read(self, component: Component) -> Any: ...
    def sample(self, component: Component, *, model_time: Any = UNSET) -> Any: ...
```

The protocol [`Study.declare_from`](#studydeclare_from) expects. Any object with these three methods is an adapter. Most code uses [`adapters.define`](#adaptersdefine) and does not implement it.

| Method | Purpose |
|---|---|
| `components()` | Return the declarations as [`Component`](#component) objects |
| `read(component)` | Return the current value of a component that is not a time series |
| `sample(component, *, model_time=UNSET)` | Return one value of a time-series component. A keyed component returns a `dict` from key tuples to numbers |

**Example**

```python
class TankAdapter:
    def __init__(self, tank):
        self.tank = tank

    def components(self):
        return [aq.Component("total cost", "scalar", unit="USD")]

    def read(self, component):
        return self.tank.cost

    def sample(self, component, *, model_time=None):
        raise TypeError("this adapter has no time series")

declared = study.declare_from(TankAdapter(tank))
print(declared.preview())
```

```text
{'total cost': 12.5}
```
