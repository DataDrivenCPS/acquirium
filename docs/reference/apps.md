---
title: App reference
---

This is the complete reference for authoring a materialization app. An app is
an importable `App` class: its query selects stored input streams, its class
attributes say when and how much data to read, and its `transform` method
publishes one or more derived streams.

For a shorter walkthrough and deployment examples, see [Apps](../apps.md). For
the revision frontier, transaction, and recovery model, see
[Incremental materialization implementation](../materialization-implementation.md).

## Complete class shape

```python
import polars as pl
import acquirium as aq


class TemperatureNormalizer(aq.App):
    name = "temperature-normalizer"
    lookback = "5m"
    backfill = True
    coalesce = "250ms"
    max_delay = "5s"
    outputs = {
        "normalized": aq.output.per_input(
            value_kind="numeric",
            label="Normalized temperature",
            unit="http://qudt.org/vocab/unit/DEG_C",
        ),
    }

    def __init__(self, offset: float = 0.0):
        self.offset = offset

    def build_query(self, plant):
        return plant.query().measurement(alias="temperature", quantity_kind="temperature")

    def transform(self, inputs, output, context):
        temperature = inputs["temperature"].in_unit("DEG_C").df()
        if temperature.is_empty():
            return
        output["normalized"] = temperature.select(
            "time", (pl.col("value") - self.offset).alias("value")
        )
```

The public declaration API is available directly from `acquirium` and from
`acquirium.Materialization`.

## Class contract

Acquirium calls `transform` once for each concrete group of input streams. The
runtime calls one such input group a *binding*. The term matters when reading
diagnostics, but app authors can usually read “binding” as “the inputs for one
call.” Acquirium creates these groups by running `build_query` against the
current plant model.

Every scheduling and windowing knob is a plain attribute holding a duration
string (`"250ms"`, `"30s"`, `"5m"`, `"2h"`), a bool, or `"all"`. Durations
also accept `datetime.timedelta` and cannot be negative.

| member | default | meaning |
|---|---|---|
| `name` | the Python class name | Durable app name and namespace for derived streams. |
| `lookback` | `"0s"` | How much stored context precedes the new data in each call's window; `"all"` reads the whole retained extent. |
| `lookback_after` | `"0s"` | Context after the changed range, for corrections landing mid-history. |
| `backfill` | `False` | Whether a new input group's first run processes retained history. |
| `coalesce` | `"0s"` | Wait for this long a quiet gap in a burst of writes before running. |
| `max_delay` | `None` | Cap on the coalesce wait: run once the oldest pending change is this old. |
| `min_interval` | `None` | At most one run per interval for an input group. |
| `outputs` | none | Mapping of local output-port names to output declarations. At least one is required; the flavors also decide how query matches group into calls. Port names must be non-empty strings, each value must come from `aq.output.per_input(...)` or `aq.output.named(...)`, and two named outputs cannot claim the same stream name. These are checked when the app is deployed or checked, before it runs. |
| `build_query(plant)` | required | Return one Acquirium `Query` that selects inputs and names them with aliases. |
| `transform(inputs, output, context)` | required | Compute tables and assign them to declared output ports. |

Constructor arguments are deployment parameters. For example,
`parameters={"offset": 273.15}` constructs the class as
`TemperatureNormalizer(offset=273.15)`. An instance may use those values in
`transform` or set instance knobs such as `self.lookback`. The `name` and
`outputs` declarations are read from the class, so declare those as class
attributes rather than constructing them in `__init__`.

### `name`

```python
name: str | None = None
```

When `name` is `None`, the Python class name is used. The name identifies the
durable deployment and forms the output source ID `derived:<name>`. Keep it
stable across code revisions if the class should continue writing the same
derived streams.

### How query matches become calls

`build_query` can find one stream, many streams, or rows containing several
related streams. The `outputs` declaration decides whether the complete set
is processed together or each matched row separately — there is no separate
attribute or subclass for this.

Suppose a query with one alias, `temperature`, finds three streams: A, B, and C.

| outputs declared | calls to `transform` | derived streams | use it when |
|---|---|---|---|
| `per_input` | Three calls: one for A, one for B, and one for C. | Three streams, one beside each matched input. | The same calculation should be applied independently to every match, such as normalizing every sensor. |
| `named` | One call whose `inputs["temperature"]` contains A, B, and C. | One stream with the declared name. | The calculation combines or compares all matches, such as a fleet average. |

Declaring both flavors together is valid only when the query resolves to
exactly one input group — then both describe the same single call. When the
query fans out into several groups, a named output alongside `per_input`
fan-out is a planning error: an absolute stream has one owner, so compute
the aggregate in a second app whose query selects this app's derived
streams (the scheduler orders chained apps automatically).

A query row can contain several aliases. For example, each row might contain
a related `supply` and `return` temperature. A `per_input` app then gets one
pair per call and can create one `delta_t` output for that pair. “Per row”
does not necessarily mean “one input stream.”

If the query has no matches, it creates no input groups, `transform` is not
called, and no output streams exist. The query is compiled again when the
semantic graph changes, so input groups can appear or disappear as the model
changes.

### `lookback` and `lookback_after`

The lookback controls the rows read for a call; it does not change which
streams the query binds. The read window is the changed extent — the
timestamps affected by the unconsumed writes — padded by `lookback` before
and `lookback_after` after. `lookback = "all"` reads the complete retained
extent instead. A windowed calculation (rolling average, rate of change)
needs `lookback` at least as long as its window.

`context.changed_window` always records the narrower extent that caused the
call. `context.read_window` records the padded range actually read.
Every `StreamSet.window` equals `context.read_window`, and
`StreamSet.changes` contains only rows written in the revisions being consumed.

Because a re-emitted (stream, time) value overwrites the previous one, the
simplest correct habit is to recompute the entire read window and emit all of
it.

### `backfill`

`backfill = False` starts a new input group at the current storage revision,
so it processes only later writes. `backfill = True` starts it at revision
zero, so retained input history is eligible for its first run. This matters
only the first time Acquirium sees that input group; its saved progress is
reused after a restart, and survives edits to the app's code and parameters.

### `coalesce`, `max_delay`, `min_interval`

Three composable throttles decide when pending input actually triggers a run;
there are no trigger modes. With all three unset, an input group runs as soon
as it has unconsumed input. `coalesce` waits for a quiet gap in a burst of
writes; `max_delay` bounds that wait so a steady trickle cannot postpone a
run forever; `min_interval` enforces at most one run per interval regardless
of write rate. The transform still runs only when there is new input —
`min_interval` is a throttle, not a wall-clock schedule.

## `build_query(plant)`

```python
def build_query(self, plant) -> Query
```

`build_query` must return an Acquirium `Query`. It runs during planning against
a pinned semantic graph, not once per data batch. Use query aliases to define
the keys later passed in `inputs`:

```python
def build_query(self, plant):
    return (
        plant.query()
        .entity("AirHandlingUnit", alias="ahu")
        .measurement(frm="ahu", alias="supply", quantity_kind="temperature")
        .measurement(frm="ahu", alias="return", quantity_kind="temperature")
    )
```

This produces `inputs["supply"]` and `inputs["return"]`. Entity aliases such as
`ahu` help express the semantic match, but only measurement/data-node aliases
that resolve to stream references become `StreamSet` inputs.

The `plant` argument is deliberately a query facade over the plant model. It
supports `plant.query()` and the graph and text-resolution operations needed
while building and executing a query; do not use it as the runtime client or
fetch timeseries inside `build_query`.

## `transform(inputs, output, context)`

```python
def transform(
    self,
    inputs: Mapping[str, StreamSet],
    output: OutputBuilder,
    context: InputBatch,
) -> None
```

The runtime calls `transform` once for each input group that has new work. It
receives a fixed, internally consistent batch and expects results through
`output`; its return value is ignored. Given the same `InputBatch`, a transform
should be deterministic.

### Inputs

`inputs` maps each data alias from `build_query` to a `StreamSet`:

| member | type | meaning |
|---|---|---|
| `.df()` | `polars.DataFrame` | All rows in the read window, as Polars. |
| `.df("pandas")` | `pandas.DataFrame` | The same rows as pandas. |
| `.collect()` | `pyarrow.Table` | The same rows as one Arrow table. |
| `.batches()` | iterator of `pyarrow.RecordBatch` | The same rows in record batches, for large windows. |
| `.changes` | `pyarrow.Table` | Only rows from the revisions this invocation is consuming. |
| `.in_unit(unit)` | `StreamSet` | The same stream set with every value converted into `unit`. |
| `.alias` | `str` | The query alias. |
| `.streams` | `tuple[StreamDescriptor, ...]` | Metadata for every stream bound under the alias. |
| `.window` | `TimeWindow` | The read range used for this batch. |

Input tables contain `ref_uri`, `time`, and `value`. Multiple physical streams
under one alias are distinguished by `ref_uri`.

Each `StreamDescriptor` carries the metadata the compiled query exposes
today: `ref_uri`, `point_uri`, `unit`, and `label`. The remaining fields
(`quantity_kind`, `medium`, `substance`, `properties`) are reserved and
currently unpopulated.

### `in_unit`: convert values where you use them

```python
temperature = inputs["temperature"].in_unit("DEG_C")
```

`in_unit` returns a new `StreamSet` whose values — the read window and
`.changes` alike — are converted into the requested unit, each stream from its
own recorded unit, so an alias mixing Fahrenheit and Celsius sensors comes out
uniform. The unit may be a QUDT URI, symbol, or label (`"DEG_C"`,
`"http://qudt.org/vocab/unit/DEG_C"`, `"mg/L"`). Because the result is an
ordinary `StreamSet`, everything composes: `.df()`, `.batches()`, and
`aq.align` all work on it unchanged.

Conversion fails immediately — rather than feeding mis-scaled values into
the calculation — when a stream has no recorded unit, the units are
dimensionally incompatible, or the stream set carries no converter (the
server's runtime injects one; an embedded scheduler passes `unit_converter=`
to `RevisionStore`).

### Context: the match this call is bound to

`context` describes the semantic match behind the call:

| member | meaning |
|---|---|
| `.entities` | The query's entity aliases resolved to model URIs — with `per_input` outputs, this call's row, e.g. `{"hx": "urn:plant/hx-1"}`. A `named` output's combined group has no single row, so its mapping is empty. |
| `.changed_window` | The timestamp extent of the unconsumed writes: why this call happened. |
| `.read_window` | The lookback-padded range actually read. |
| `.from_revision`, `.to_revision`, `.graph_revision`, `.binding_signature` | Runtime diagnostics. |

Data selection should use the supplied tables rather than querying storage
again.

### `aq.align`: one clock for many streams

```python
frame = aq.align(inputs, every="1m", aggregate="mean")
```

`align` resamples every input stream onto shared time buckets and returns a
wide Polars dataframe: a `time` column plus one column per stream. An alias
bound to one stream contributes a column named after the alias; an alias bound
to several contributes `alias[label-or-ref]` columns. Buckets a stream never
reported in hold nulls. `aggregate` is one of `mean`, `min`, `max`, `sum`,
`first`, `last`, `median`, or `count`.

### Publishing results

Assign each result by its declared port name:

```python
output["normalized"] = result
```

The value may be a Polars dataframe, a pandas dataframe, a PyArrow table or
record batch, or a sequence of Arrow record batches. After conversion it must
have exactly these columns:

| column | requirement |
|---|---|
| `time` | Non-null, timezone-aware timestamps, unique within this output assignment. Values are normalized to UTC and sorted ascending. |
| `value` | Non-null numbers for a numeric output or strings for a text output. Numeric values are stored as `float64`. |

Assignment is validated against the declaration, inside `transform`:

| assignment | result |
|---|---|
| a port the app declared | accepted after schema validation |
| a name not in `outputs` | `KeyError`, naming the declared ports |
| the same port twice in one call | `ValueError` |
| a table with the wrong columns, types, or duplicate timestamps | `TypeError`/`ValueError`, prefixed with the port name |

A declared port that is not assigned publishes no rows for that call. Empty
tables also publish no rows. All accepted outputs and the saved input
progress commit in one database transaction.

## Outputs

```python
outputs: Mapping[str, OutputSpec] = {
    "port_name": aq.output.per_input(...),   # or aq.output.named("...", ...)
}
```

An output mapping declares named *ports*: names used inside the class, such as
`"normalized"` in both the `outputs` mapping and `output["normalized"]`. Each
port becomes one or more derived streams, and the two declaration flavors
choose how those streams are identified.

### `aq.output.per_input(...)`: relative identity

Each input group gets one derived stream for every `per_input` port, and the
stream's identity is derived from that group's inputs:

```text
app name + output port + sorted (input alias, input ref_uri) pairs
                              |
                              v
                  one stable derived stream reference
```

For example, an app with `per_input` port `normalized` and three matched sensors
creates three derived streams. All three have source ID
`derived:temperature-normalizer`, but each has a different generated reference
name because each is bound to a different input. This is the flavor for
calculations that fan out across many streams: nothing needs to be named by
hand, and recompiling the same app name, port, aliases, and input references
reuses the same derived stream identity.

The generated storage identity has:

- `source_id`: `derived:<app name>`;
- `ref_name`: `<port name>:<deterministic hash of the port and bound inputs>`;
- `ref_uri`: the normal Acquirium reference URI computed from that source ID
  and reference name.

### `aq.output.named(stream_name, ...)`: absolute identity

A named output is one stream whose reference name you choose:

- `source_id`: `derived:<app name>`;
- `ref_name`: exactly `stream_name`;
- `ref_uri`: computed from those two.

Use it for any single result the plant refers to as a thing in itself — an
aggregate across a fleet, an index, a KPI, a compliance figure — so that other
queries, dashboards, and people can find it directly by name rather than
discovering it relative to its inputs.

Because an absolute stream has exactly one owner, a named output is valid
only when the app's query produces exactly one input group — which is always
the case when every output is `named`, since they aggregate the whole result.
Declaring one alongside `per_input` fan-out over several groups is a planning
error pointing you to a second, chained app.

### Attachment to the plant model

Either flavor may set `point_uri` to attach the derived reference to a known
semantic point. Otherwise Acquirium creates a derived point for that stream.
Internally, the runtime records an `isCalculatedFrom` relationship from the
binding to every input and records which reference the binding produces.
Derived streams can therefore be selected by later apps' queries, and those
dependencies form the materialization DAG.

### Declaration arguments

`aq.output.per_input(value_kind, **kwargs)` and
`aq.output.named(stream_name, value_kind, **kwargs)` return an `OutputSpec`:

| argument | meaning |
|---|---|
| `value_kind` | **Required.** `"numeric"` or `"text"`. Fixed at declaration, like a driver's stream registration — never inferred from published data. |
| `point_uri` | Semantic point to which this derived reference is attached. When omitted, generate a point for the stream. |
| `label` | RDF label placed on the output point. |
| `unit` | Unit URI placed on the output point. |
| `quantity_kind` | Quantity-kind URI placed on the output point. |
| `medium` | Medium URI placed on the output point. |
| `substance` | Substance URI placed on the output point. |
| `data_source` | Literal data-source tag placed on the output reference. |
| `properties` | Mapping of predicate URIs to tuples of object URIs, added to the output point. |

Every field is explicit: an output's metadata is exactly what its declaration
says, never copied from its inputs.

## Checking an app

```python
client.check_app(TemperatureNormalizer, parameters={"offset": 273.15})
```

```bash
uv run acquirium app check ./temperature_normalizer.py:TemperatureNormalizer
```

A check compiles the app's query against the live graph, reads every
retained input row for each resulting input group, runs `transform`, and
returns what it computed. It writes nothing: the app is not registered, its
derived streams are not created, no progress row is written or advanced, and
no revision is allocated. A deployed app of the same name is unaffected.

The app runs on the server, which imports it by module name. `search_path`
names a directory on the server's filesystem to look in first, so a file
that is not otherwise importable there can still be checked: the CLI sends
the directory of any file spec it was given, and `Acquirium.check_app` sends
the directory of the class's own module (pass `search_path=""` to send
none). It applies to checks only — a deployed app must be importable by the
server on its own, since it is loaded again long after the request that
created it.

A module whose file has changed since the server imported it is reloaded, so
editing an app and re-checking runs the new code rather than the code the
process first loaded.

### `aq.console()`

```python
aq.console(banner=None, *, depth=1)
```

Opens an interactive console holding the calling frame's variables — its
locals merged over its globals, locals winning. The default banner names the
calling function, its file and line, and the local variables in scope.
Ctrl-D or `exit()` closes it and execution resumes.

The namespace is a snapshot, so rebinding a name in the console does not
change the variable in the running function; mutating an object does. Pass
`banner` to replace the default, and `depth` to show an outer frame instead
(`depth=2` from inside a helper shows that helper's caller).

Without an interactive terminal — a deployed app, a server-side check, a
test — it logs a warning naming the call site and returns immediately, so a
forgotten console never blocks a server.

### Running the check locally

```bash
uv run acquirium app check ./temperature_normalizer.py:TemperatureNormalizer --local
```

```python
from acquirium.Materialization import local
result = local.check_app(client, TemperatureNormalizer, parameters={"offset": 273.15})
```

The app is compiled and run in the caller's process, with its inputs fetched
over the client API, and returns the same result document. Three things
follow from running here rather than on the server:

- `breakpoint()` opens a console in the calling terminal, and debuggers and
  profilers attach to the app normally.
- A failing `transform` raises where it was called, with its traceback,
  instead of being captured in that binding's `error` field. One broken
  binding therefore stops the check.
- The server never imports the app, so nothing needs to be importable there
  and `search_path` is irrelevant.

Everything else matches a server-side check, including the derived stream
identities each output would be published under. Input rows travel over HTTP,
so a check reading a large extent is slower this way.

The result document is:

```text
{
  "app": "temperature-normalizer",
  "graph_revision": 12,
  "bindings": [
    {
      "inputs": {"temperature": [{"ref_uri": ..., "label": ..., "unit": ...}]},
      "entities": {"hx": "urn:plant/hx-1"},
      "input_rows": {"temperature": 288},
      "read_window": ["2026-09-01T00:00:00+00:00", "2026-09-02T00:00:00+00:00"],
      "outputs": {
        "normalized": {"stream": ..., "ref_name": ..., "value_kind": "numeric",
                       "rows": 288, "truncated": false,
                       "values": [{"time": ..., "value": ...}, ...]}
      },
      "error": null
    }
  ]
}
```

One entry per input group the query produced, so an empty `bindings` list
means the query matched nothing. `values` holds every row the transform
computed for that output; `rows` is that count. Passing `limit` keeps only
the first `limit` rows of each output and sets `truncated` — `rows` still
reports the full count. A transform that raises is reported in that group's
`error` rather than propagating, so one broken group still shows the others;
the CLI exits non-zero when any group has an error.

The CLI prints the first five rows of each output by default; `-n N` heads
it at a different count and `-n 0` prints every row. The Python and HTTP
forms return every row unless `limit` is given.

Because each group reads its whole retained extent rather than an
incremental window, the values shown are what a `backfill = True` deployment
would publish.

## Deployment

Deploy an importable class through a connected client:

```python
client.deploy_app(
    TemperatureNormalizer,
    parameters={"offset": 273.15},
)
```

Or load it when the server starts:

```toml
[[apps]]
spec = "./temperature_normalizer.py:TemperatureNormalizer"
offset = 273.15
```

For a class spec, every key other than `spec` is passed to the constructor. A
`spec` may instead name a registrar function for a group of related apps; see
[Apps](../apps.md#deploying-and-inspecting-apps).

`deploy_app` ships a *reference* to the code — module path, qualified name,
and a source digest — not the code itself. The server must be able to import
the identical module. A class defined in a script's `__main__` module is
rejected at deploy time for this reason; move it into an importable module.

Progress is keyed by what a binding reads and writes, so editing the app's
source or parameters neither resets its place nor skips data. To reprocess
history with changed code, redeploy under a new name with `backfill = True`
(the old streams remain until their app is removed), or remove and redeploy
the same name and backfill.
