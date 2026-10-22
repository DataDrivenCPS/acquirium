# Record a whole model

An adapter declares experiment variables and binds functions that read their
values. Choose scalar, text, JSON, table, or time series explicitly. The Pyomo
adapter evaluates selected components and extracts units and descriptions.
Install the optional `acquirium[pyomo]` extra; no solver is needed here.

This complete example inspects its adapter locally, starts a temporary server,
and records three scenarios. It includes a scalar model state sampled through
time and a component explicitly indexed by model time.

```python
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory

import acquirium as aq
import polars as pl
import pyomo.environ as pyo

model = pyo.ConcreteModel()
model.time = pyo.Set(initialize=[0, 1, 2])
model.pump = pyo.Set(initialize=["pump1", "pump2"])
model.cost = pyo.Var(initialize=0)
model.pressure = pyo.Var(initialize=0, units=pyo.units.Pa)
model.flow = pyo.Var(model.pump, model.time, initialize=0, units=pyo.units.L / pyo.units.s)
model.capacity = pyo.Param(model.pump, initialize={"pump1": 10, "pump2": 20})
start = datetime(2026, 1, 1, tzinfo=timezone.utc)
configuration = {"solver options": {"notes": [None, {}, []]}, "scenario.names": ["demo"]}
annotations = {"experiment notes": {"a.b": [None, {}, [], {"": "arbitrary string keys"}]}}

adapter = aq.adapters.pyomo(model)
adapter.input.json("configuration", read=lambda: configuration, metadata=annotations)
adapter.scalar("cost", model.cost)  # Unqualified declarations are outputs.
adapter.timeseries("pressure", model.pressure)
adapter.table(
    "capacity",
    model.capacity,
    read=lambda: pl.DataFrame({
        "pump": list(model.pump),
        "value": [pyo.value(model.capacity[pump]) for pump in model.pump],
    }),
)
for pump in model.pump:
    adapter.timeseries(
        f"flow/{pump}",
        model.flow,
        read_at=lambda t, pump=pump: model.flow[pump, t],
    )

print(adapter.frame())  # Declarations only; no readers or server requests.
assert adapter.preview(model_time=0)["pressure"] == 0

with TemporaryDirectory() as data_dir:
    ac = aq.init(data_dir=data_dir)
    try:
        study = ac.study.define("pump scenarios")
        variables = study.declare_from(adapter)
        assert study.declare_from(adapter)["cost"] is variables["cost"]

        for scenario in range(3):
            run = study.start({"scenario": scenario})
            variables.input.record_values()
            for step in model.time:
                model.pressure.set_value(10 + scenario + step)
                for pump in model.pump:
                    model.flow[pump, step].set_value(scenario + step + 1)
                variables.sample(model_time=step, at=start + timedelta(hours=step))
            model.cost.set_value(100 + scenario)
            variables.output.record_values()
            run.finish()

        costs = study.experiments.scalars(variables["cost"])
        assert sorted(costs["cost"]) == [100, 101, 102]
        assert run.observations(variables["capacity"]).latest().dataframe().height == 2
        assert run.observations(variables["configuration"]).latest().value == configuration
        reopened = ac.study.get("pump scenarios")
        assert reopened.variables["configuration"].metadata == annotations
        pump1 = ac.query().measurement(
            variable_id=variables["flow/pump1"].variable_id
        ).dataframe(shape="narrow")
        assert pump1.height == 9
        print(costs)
        print(pump1)
    finally:
        aq.shutdown()
```

`adapter.input` and `adapter.output` expose the same declaration methods.
`adapter.scalar(...)` is shorthand for `adapter.output.scalar(...)`, and the
same default applies to text, JSON, table, and time-series declarations.
Labels are unique across both namespaces. `variables.input` and
`variables.output` select matching declarations for previewing and recording.

`sample(at=when, model_time=index)` reads only time series. `model_time` is
passed unchanged to `read_at(index)`; `at` is the timestamp stored with the
sample. Acquirium does not convert between them. A current-state `read()` is
called without arguments, so an adapter with only current-state readers needs
just `sample(at=when)`.

`record_values()` reads scalar, text, JSON, and table declarations. It does not
read time series or infer histories. Record inputs before running the model
and outputs afterward using the namespace views. To record an already
available history explicitly, use `variables[label].record(rows)`.

For unindexed Pyomo sources, the wrapper supplies a reader that evaluates the
current value. Indexed sources require an explicit `read` or `read_at`
function. The source supplies its model path, description, and known units.
A `unit=` override asserts the same unit using another spelling or URI;
changing scale or physical dimension is rejected before declaration. For
converted or derived values, use the general builder with a reader performing
that conversion and an explicit resulting unit.

The JSON declaration records the reader's value. The separate optional
`metadata=` argument attaches descriptive JSON to a declaration. Both support
arbitrary string keys and nested objects, arrays, nulls, and empty containers.
Metadata is available as `variable.metadata` after reopening. Semantic fields
such as `variable.unit`, `quantity_kind`, and `source_path` are separate; a
user metadata key named `unit` or `role` cannot override those fields.

Metadata is preserved exactly as JSON. Paths composed of valid attribute names
also get a graph projection under `metadata`, on declarations and stream
points, for queries such as `measurement(**{"metadata.location": "tank"})`
when you deliberately supplied `metadata={"location": "tank"}`. Other keys
remain available in the complete JSON object. Unit and quantity-kind fields
are resolved at declaration, so sampling needs no semantic-resolution requests.
Quantity kinds and optional `observed=property_uri` links are supplied explicitly.

For another library, use `aq.adapters.define()`. Its helpers take Python
readers instead of Pyomo source arguments: `scalar("cost", read=lambda:
model.cost)` or `timeseries("pressure", read=lambda: model.pressure)`. No
arbitrary keyword arguments or inferred index mappings are needed.

Construction and `adapter.frame()` never read model values. `adapter.preview()`
and `variables.preview()` read and validate locally without recording or an
active run; indexed readers still need `model_time`. Reader failures identify
the label, source path, and model-time argument. A missing/uninitialized value
fails before any recording requests.

`declare_from()` freezes the selected descriptors and reader bindings. Build
an adapter for each new model instance and redeclare it on the study to reuse
saved definitions with fresh readers. Declaring an empty adapter or selecting
no components raises an explanatory error.

Each nonempty `sample()` and `record_values()` call sends one recording request.
All readers and values are checked first; a reader failure sends no recording
request. Stream registration, revisioned samples, artifact ownership, ordered
observations, and durable graph intent share one SQL transaction. A rejected
batch leaves no new samples or observations. Graph delivery retries from the
outbox after commit; HTTP retries can still append another observation.

Hand declarations use the same user metadata namespace and semantic properties.
See the [declaration conversion table](../reference/experiments.md#declaring-variables)
and [storage boundary and recovery](../reference/storage-migrations.md).
