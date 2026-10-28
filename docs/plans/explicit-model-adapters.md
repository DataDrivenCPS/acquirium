# Explicit model adapters

Status: implemented; this supersedes the automatic model-walking design in
[experiment-values.md](experiment-values.md#declaring-variables-from-a-model).
The runnable guide is [Record a whole model](../how-to/record-a-whole-model.md).

## Decisions

Model adapters declare experiment variables and bind ordinary Python readers.
The application chooses scalar, text, JSON, table, or time series explicitly.
The Pyomo wrapper extracts known metadata and evaluates selected values.

- `aq.adapters.define()` is the general builder.
- `aq.adapters.pyomo(model)` is an initially empty Pyomo builder using the same
  declaration and reader machinery.
- Both have `input` and `output` namespaces. Unqualified methods declare outputs.
- `metadata=` accepts arbitrary string-keyed nested JSON. Declaration methods
  have explicit arguments rather than interpreting arbitrary keywords.
- `sample(at=when, model_time=index)` reads time-series declarations.
- `record_values()` reads scalar, text, JSON, and table declarations.
- Namespace views on `DeclaredVariables` select inputs or outputs for recording.
- Inspection and preview are available without recording or an active run.

## Public shape

```python
adapter = aq.adapters.pyomo(model)
adapter.input.scalar("tariff", model.tariff)
adapter.scalar("cost", model.cost)
adapter.input.json(
    "configuration",
    read=lambda: configuration,
    metadata={"experiment notes": {"a.b": [None, {}, []]}},
)
adapter.timeseries("pressure", model.pressure)
for pump in model.pump:
    adapter.timeseries(
        f"flow/{pump}",
        model.flow,
        read_at=lambda t, pump=pump: model.flow[pump, t],
    )

print(adapter.frame())
preview = adapter.preview(model_time=0)
variables = study.declare_from(adapter)
run = study.start()
variables.input.record_values()
variables.sample(model_time=2, at=start + timedelta(hours=2))
variables.output.record_values()
run.finish()
```

The snippet sketches the API; the how-to supplies the complete runnable model.
No pump/family metadata conventions are required. Optional user annotations
are ordinary JSON and do not affect how the adapter indexes or records values.

For another library, use readers on the general builder:

```python
adapter = aq.adapters.define()
adapter.input.json("configuration", read=lambda: configuration)
adapter.scalar("cost", read=lambda: model.current_cost)
adapter.timeseries("pressure", read=lambda: model.current_pressure)
adapter.timeseries("flow", read_at=lambda t: model.flow_at(t))
```

## Reader contract and clocks

Every declaration has a zero-argument `read` function, except a time series
may instead have a one-argument `read_at` function. Exactly one reader is
required. Missing/conflicting/non-callable readers and duplicate labels fail
while building the adapter. Callable signatures are not inspected.

`model_time` is forwarded unchanged to indexed readers. `at` supplies the UTC
sample timestamp and never selects model indices. Omission uses a private
sentinel; `0` and `None` remain valid supplied arguments. Mixed adapters can
have both current-state and indexed readers. Current readers are always called
without arguments.

`sample()` never invokes final-value readers. `record_values()` never invokes
time-series readers and never infers full histories. Existing variable
`record(rows)` remains available for explicit histories. Recording methods
require an active run and return ordered observation receipts.

All selected values are read and validated before any recording requests.
Errors preserve their cause and identify the declaration label, source path,
and supplied model time. Missing/uninitialized values fail rather than being
silently skipped or replaced.

## Roles and lifecycle

Roles are part of declarations, chosen by namespaces. They cannot be overridden
by a user JSON metadata key named `role`. There are no paired boolean role
flags or methods such as `scalar_input`.

`declare_from()` freezes the selection's descriptors and reader bindings while
keeping their live references to model values. Adding declarations to the
original builder afterward does not change a bound selection. A new model
instance should have a new adapter; declaring it on the existing study reuses
saved experiment definitions and installs fresh local readers.

The role views are mappings supporting `frame()`, `preview()`, `sample()`, and
`record_values()`. Labels remain unique across input/output namespaces. Empty
builders and filters matching nothing fail with explanatory errors.

## JSON values and metadata

`.json()` records any JSON-convertible reader value. `metadata=` attaches a
JSON object to a declaration. Both use the registered value handlers and
preserve arbitrary string keys, nested arrays/objects, nulls, and empty
containers. NaN and infinity are rejected.

User metadata is separate from semantic fields such as unit, quantity kind,
role, and source path. `variable.metadata` exposes the exact user object after
reopening; semantic fields have their own properties.

Declarations use complete graph-owned documents with a projection under
`metadata` on variables and stream points. Arbitrary unprojectable keys remain
in the document. See the current [storage boundary](experiment-metadata-integration.md#storage-boundary)
and [migration/recovery reference](../reference/storage-migrations.md).

## Pyomo wrapper

The source component supplies units, documentation, and the original model
path. Numeric readers are evaluated with `pyo.value()`. Unindexed sources
supply a current-state reader by default; indexed sources require an explicit
reader. The application chooses index values and ordering in normal Python.

`.timeseries()` explicitly declares a sampled quantity, even if its source is
a scalar Pyomo component. There is no automatic ContinuousSet discovery,
recording-kind inference, or conversion between datetimes and model indices.

A unit override asserts the same known unit with another spelling or URI.
Scale/dimension changes are rejected before declaration. Derived or converted
values can use the general builder with an explicit conversion reader and
resulting unit. Quantity kinds and observed-property links are explicit;
ambiguous physical meaning is not guessed from names or units.

The simple per-pump loop declares one variable per pump. Low-level
`AdapterDefinition.add(Component(..., keys=...), read=.../read_at=...)` retains
advanced keyed-stream support. Keyed readers explicitly return a mapping from
key tuple to current numeric value. The declaration determines the expected
shape; no reader-result shape is used to infer recording mode.

## Implementation

- `ModelAdapters.py`: declaration namespaces, ordered reader bindings, JSON
  metadata preservation/projection, inspection, preview, and labeled validation.
- `adapters/_pyomo.py`: explicit Pyomo wrapper over the general builder.
- `Experiments.py`: fixed declarations, role views, `sample()`, `record_values()`,
  unit assertions, and existing batching/storage.
- Tests cover generic readers, Pyomo helpers, both clock roles, binding lifecycle,
  invalid values, namespaces, JSON round trips, querying, and batch failures.

Unit and quantity-kind identifiers are resolved during declaration and carried
with the normalized declaration. Samples and final values each use one recording
request. Registration, samples, artifact ownership, observations, and durable
graph intent commit in one SQL transaction.

## Migration and compatibility

Prototype automatic options, `record_all()`, and `declare_from(role=...)` were
removed. Use explicit input/output declarations and recording methods. Saved
identifiers and observations remain readable. All variables expose user JSON
through `.metadata` and semantic/source fields through explicit properties.
Legacy descriptive fields move under `metadata`; conflicting duplicates require
repair. See the [conversion table](../reference/experiments.md#declaring-variables).

## Verification

Run the full unit suite and experiment contracts on DuckDB and Postgres. Run
the how-to example exactly as written and the local-server regression. Verify
that arbitrary JSON metadata survives graph reads and a server restart, and
that request counts and deletion include unreferenced samples from rejected
observation batches.
