# Apps

Apps are pieces of code that process data stored in Acquirium. You write a
small Python class; the server runs it whenever its input data changes and
stores what it produces as new streams. Those derived streams behave like any
other stream: you can query them, plot them, and feed them into further apps.

Apps implement the recurring patterns of plant analytics:

- **Soft sensors** — derived measurements that update as fast as their inputs:
  a delta-T from a supply/return pair, an estimated dose from flow and
  concentration.
- **Unit normalization and cleaning** — one stream per sensor in a known unit,
  despiked or range-checked.
- **Fault detection and alarms** — streams that carry a value only when
  something is wrong: a threshold exceeded, a sensor gone quiet.
- **KPIs and rollups** — plant-wide totals, fleet averages, compliance
  figures, published under a name everyone can find.

```text
sensors ──► raw streams ──► app ──► derived streams ──► another app ──► …
                 │                         │
                 └───────────┬─────────────┘
                             ▼
                    queries, dashboards
```

Drivers are the other half of the data path: a driver pulls data *into* the
server from a source, an app derives new values *from* stored data. Drivers
are configured in `[[drivers]]`, apps in `[[apps]]`.

## A first app

This app finds every temperature stream in the plant and publishes a
Celsius copy beside each one:

```python
import acquirium as aq


class NormalizeTemperatures(aq.App):
    name = "normalize-temperatures"
    backfill = True
    outputs = {
        "celsius": aq.output.per_input(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/DEG_C",
        ),
    }

    def build_query(self, plant):
        return plant.query().measurement(alias="temperature", quantity_kind="temperature")

    def transform(self, inputs, output, context):
        celsius = inputs["temperature"].in_unit("DEG_C")
        output["celsius"] = celsius.df().select("time", "value")
```

Deploy it by listing it in `acquirium.toml`:

```toml
[[apps]]
spec = "./normalize_temperatures.py:NormalizeTemperatures"
```

Reading the class top to bottom:

- `build_query` selects the inputs *semantically*: every measurement whose
  quantity kind is temperature, whatever its source or unit. The alias
  `temperature` names that selection.
- `outputs` declares the app's whole shape. `per_input` means "run me once
  per match, and put one derived stream beside each match's inputs" — fifty
  matched sensors mean fifty independent calls to `transform` and fifty
  Celsius streams, none of them named by hand.
- `transform` is the calculation. `in_unit("DEG_C")` converts the input from
  whatever unit the sensor records, and the result is written as `time` and
  `value` rows.
- `backfill = True` processes the history that is already stored. From
  then on, every new write to a temperature stream triggers a conversion of
  just the new rows.

## The mental model

An app is a function from a window of input data to the matching window of
output data. Three rules cover everything:

1. **The server hands you a window.** It watches your inputs, notices what
   changed, and calls `transform` with the relevant time range already
   loaded. You never poll, subscribe, or track positions.
2. **You emit the output rows for that window.** Just the rows your inputs
   justify — the server stores them and remembers how far you have processed,
   in one transaction.
3. **Re-emitting is overwriting.** An output value is identified by its
   stream and timestamp. If you compute the same window twice, the rows
   overwrite themselves. Recomputing is always safe.

Rule 3 is what keeps apps simple. There is no checkpoint to manage, no
duplicate to suppress, no partial failure to reason about: if the server
restarts mid-computation, it re-runs the window and the result is identical.
The one obligation this places on you is determinism — given the same input
window, `transform` should produce the same output.

The same three rules support very different processing styles, chosen with
one attribute. Suppose new rows arrive with timestamps between 10:02 and
10:04:

```text
time ────────────────────────────────────────────────▓▓▓▓▓▓▓▓──►
                   stored history                    new data
                                                   10:02  10:04

                   what transform() receives

lookback = "0s"                                      ├──────┤
lookback = "5m"                             ├────────┼──────┤
                                          09:57
lookback = "all"  ├──────────────────────────────────────────┤
```

| style | `lookback =` | what `transform` sees |
|---|---|---|
| Streaming | `"0s"` (default) | Only the time range of new data. Right for row-at-a-time math: conversions, thresholds. |
| Windowed | `"10m"` | New data plus context before it. Right for rolling averages, rates of change — set it to your calculation's window so the edge is computed correctly. |
| Whole-stream rewriting | `"all"` | Everything ever stored. The entire derived stream is recomputed on each change. Right when any input point can affect any output point — a normalization against the historical median, a cumulative total. |

The code does not change between styles; only the window you are handed does —
one `lookback` attribute, holding a duration or `"all"`. Recompute all of it
and emit all of it.

## The parts of an app

### The query: what to read

`build_query` returns an Acquirium query, and its aliases become the keys of
`inputs`:

```python
def build_query(self, plant):
    return (
        plant.query()
        .entity("HeatExchanger", alias="hx")
        .measurement(frm="hx", alias="supply", quantity_kind="temperature")
        .measurement(frm="hx", alias="return", quantity_kind="temperature")
    )
```

The query runs against the plant model, not against a list of point IDs. When
the model gains a heat exchanger, this app starts processing it; when one is
removed, its processing stops. An app with `per_input` outputs receives one
query-result row per call — here, a related supply/return pair — so one query
can express "for every X, take its Y and Z."

The output flavor decides how the query's matches become calls:

```text
build_query() matches three sensors: A, B, C

per_input outputs — one call per match

  A ──► transform(A) ──► derived A
  B ──► transform(B) ──► derived B
  C ──► transform(C) ──► derived C

named outputs — one call for the complete set

  A ─┐
  B ─┼──► transform(A, B, C) ──► one result stream
  C ─┘
```

To read another app's results, give its output a handle and select on it like
any other stream. Declare a tag on the upstream output —
`aq.output.per_input(data_source="normalized-temperatures", ...)` — and query
it downstream with
`measurement(alias="celsius", data_source="normalized-temperatures")`;
semantic metadata (`unit`, `quantity_kind`, `point_uri`) works the same way.
The server orders chained apps so downstream work always sees upstream
results.

### The outputs: what to write

An output declaration pins down three things before the app ever runs: which
derived streams exist, how each one is identified, and what rows it accepts.
Nothing about an output's identity is decided inside `transform`.

**Which streams exist.** Each entry in `outputs` is a *port* — the name used
inside the class, as in `output["celsius"] = ...` — declared with one of two
identities:

- `aq.output.per_input(...)` — one derived stream per call, identified by the
  inputs it came from. Use it whenever the calculation fans out: a thousand
  matched sensors become a thousand derived streams automatically.
- `aq.output.named("plant-average-flow", ...)` — one stream with exactly the
  name you give it. Use it whenever the result is a thing the plant refers to
  directly — a total, an index, an alarm roll-up — so dashboards and queries
  find it by name. A named output requires the query to resolve to a single
  input group, because an absolute stream has exactly one owner.

Every derived stream is registered under its app, with this identity:

```text
app "normalize-temperatures", port "celsius"

source id    derived:normalize-temperatures        always derived:<app name>
ref name     celsius:<digest of port + inputs>     per_input — generated
             plant-average-flow                    named — exactly your string
value kind   numeric | text                        always declared, like a
                                                   driver's stream registration
point        created in the model, carrying the    or the existing point you
             declared unit, label, quantity kind   pass as point_uri=
```

The identity is deterministic: recompiling the same app name, port, and bound
inputs writes the same stream, across restarts and code edits. If a
`per_input` port's bound inputs change — a sensor joins the group — that
group's derived stream is a new one; the old stream keeps its history.

**What rows a port accepts.** A call may only assign a port the app
declared — `output["typo"] = ...` raises immediately, listing the declared
ports, because every published stream is declared up front and a transform
cannot invent one. Each port takes at most one assignment. The value may be
a Polars or pandas dataframe, an Arrow table, or Arrow record batches;
whatever the container, its schema must be exactly two columns:

| column | type | rules |
|---|---|---|
| `time` | timezone-aware timestamp | non-null; unique within the assignment; normalized to UTC microseconds and sorted on write |
| `value` | number for a `"numeric"` port, string for a `"text"` port | non-null; numeric values are stored as `float64` |

No other columns are accepted — in particular, drop `ref_uri` from an input
frame before emitting it. Violations raise at assignment, inside `transform`,
naming the port.

**Write semantics.** A stored row is identified by (stream, time): emitting a
timestamp that already exists overwrites its value, which is what makes
recomputation safe. A port left unassigned — or assigned an empty table —
publishes nothing for that call. Everything assigned in one call, across all
ports, commits in one transaction together with the app's saved progress.

The full declaration argument list (`unit`, `label`, `quantity_kind`,
`medium`, `substance`, `point_uri`, `data_source`) is in the
[app reference](reference/apps.md#outputs).

### The transform: the calculation

`transform(inputs, output, context)` receives the window and emits results.

`context` is the *match* this call is bound to: `context.entities` maps the
query's entity aliases to model URIs — for the heat-exchanger query above,
`context.entities["hx"]` says which exchanger this call is about — and
`context.changed_window` / `context.read_window` record why the call happened
and what range it was handed.

`inputs[alias]` holds the loaded data:

```python
frame = inputs["supply"].df()            # Polars dataframe: ref_uri, time, value
frame = inputs["supply"].df("pandas")    # pandas, if you prefer
table = inputs["supply"].collect()       # Arrow table
cool  = inputs["supply"].in_unit("DEG_C")  # same data, values converted
```

Two helpers cover the awkward parts of real sensor data:

- `.in_unit("DEG_C")` converts every stream in the set from its own recorded
  unit. Mixed Fahrenheit and Celsius sensors come out uniform; a stream with
  no unit, or an incompatible one, raises instead of polluting the result.
- `aq.align(inputs, every="1m")` resamples all inputs onto shared time
  buckets and returns one wide dataframe — a `time` column plus one column
  per stream — so combining sensors on different clocks is a column
  expression, not a hand-written join:

```text
temperature   ●────●───●────●───●            (a sample every ~40 s)
flow             ●─────────●─────────●       (a sample every ~2 min)

                align(inputs, every="1m")
                          │
                          ▼
                time    temperature   flow
                10:00   20.1          1.02
                10:01   20.4          null
                10:02   20.9          1.05
```

Emit by assigning dataframes to declared outputs:

```python
output["delta_t"] = frame.select("time", "value")
```

The assigned table must match the output schema — exactly `time` and `value`
columns, specified under [The outputs](#the-outputs-what-to-write). Assigning
nothing publishes nothing for that call — normal for alarm apps with nothing
to report.

### The knobs: when to run, where to begin

Every knob is a plain attribute holding a duration string, a bool, or
`"all"` — there are no policy objects:

| attribute | default | meaning |
|---|---|---|
| `lookback` | `"0s"` | How much stored context precedes the new data in each call — the processing style, above. `"all"` reads the whole stream. |
| `lookback_after` | `"0s"` | Context after the changed range, for corrections landing mid-history. |
| `backfill` | `False` | Whether the first run processes already-stored history. |
| `coalesce` | `"0s"` | Wait for a quiet gap in a burst of writes before running. |
| `max_delay` | none | Cap on the coalesce wait. |
| `min_interval` | none | At most one run per interval. |

The app's progress is durable and follows what it reads and writes: server
restarts, code edits, and parameter changes all resume where processing left
off.

## Check it before you deploy

A check runs the app for real — same query, same stored data, same
`transform` — and prints what it computed instead of saving it. Nothing is
deployed, no derived stream is created, no progress is recorded, so there is
nothing to clean up afterwards.

```bash
uv run acquirium app check ./normalize_temperatures.py:NormalizeTemperatures
```

```text
normalize-temperatures: 3 input group(s) matched

[1] inputs
      temperature: 1 stream(s), 288 rows read
        - Basin 1 inlet temperature
    output 'celsius' -> celsius:6f21c0… (numeric, 288 rows)
        2026-09-01T00:00:00+00:00  18.4
        2026-09-01T00:05:00+00:00  18.6
        … 286 more row(s); pass -n 0 for all of them
```

Read it as three questions answered at once: did the query match the streams
you expected (and only those), did the transform run without raising, and are
the values right. Each group reads every retained input row, so the numbers
you see are what a `backfill = True` deployment would publish.

Every row the transform computed is available; the command prints the first
five of each output by default. `-n N` (or `--limit N`) heads it at a
different count and `-n 0` prints every row. `--params '{"threshold": 3}'`
passes constructor parameters, and `--json` prints the whole result
document. A failing transform is reported per group and the command exits
non-zero, so a check works in CI.

The same thing from Python, where every computed row comes back unless you
ask for fewer:

```python
result = client.check_app(NormalizeTemperatures, parameters={"offset": 273.15})
rows = result["bindings"][0]["outputs"]["celsius"]["values"]
```

A check runs the app *on the server*, so the file has to exist there. When
you name a file (`./my_app.py:MyApp`, or any relative path), the CLI sends
its directory along, so a server on the same machine can import it wherever
it sits — no need to install it or move it next to the config first. Against
a server on another machine or in a container, copy the file somewhere that
server imports from. Deployment is stricter: it stores only a module path,
so a deployed app must be importable by the server on its own.

Editing an app and re-checking always runs the new code — a check reloads a
module whose file has changed rather than reusing what it imported before.

### Debugging a check

By default the app runs on the server, so a `breakpoint()` in `transform`
opens a console on the server's stdin, where you cannot reach it. `--local`
runs the app in your own process instead, reading its inputs over the API:

```bash
uv run acquirium app check ./normalize_temperatures.py:NormalizeTemperatures --local
```

Now `breakpoint()` stops in the terminal you ran the command from, and a
failing `transform` raises a full traceback there rather than being reported
as a per-binding error. The results are otherwise identical, down to the
derived stream identities. Running locally also sidesteps importing
entirely: the server never loads your app, so nothing needs to be
importable there.

## Pattern: fault detection

An alarm is an app whose output stream has values only when something is
wrong. Emit violations; silence is an empty output.

```python
import polars as pl
import acquirium as aq


class HighTurbidityAlarm(aq.App):
    name = "high-turbidity-alarm"
    outputs = {"alarm": aq.output.per_input(value_kind="text")}

    def __init__(self, threshold: float = 5.0):
        self.threshold = threshold

    def build_query(self, plant):
        return plant.query().measurement(alias="turbidity", quantity_kind="turbidity")

    def transform(self, inputs, output, context):
        ntu = inputs["turbidity"].df()
        exceeded = ntu.filter(pl.col("value") > self.threshold)
        output["alarm"] = exceeded.select(
            "time", pl.format("turbidity {} NTU over limit", pl.col("value")).alias("value")
        )
```

The threshold is a constructor argument, so it is set at deployment rather
than edited in code:

```toml
[[apps]]
spec = "./high_turbidity_alarm.py:HighTurbidityAlarm"
threshold = 3.0
```

Each turbidity sensor gets its own alarm stream; querying it back gives the
full alarm history with timestamps.

## Pattern: filling holes in a stream

An imputation app publishes a gap-free copy of a stream: resample the raw
data onto its expected cadence, interpolate across the holes, and let
downstream apps and dashboards read the filled stream instead.

```python
import polars as pl
import acquirium as aq


class FillGaps(aq.App):
    name = "fill-gaps"
    lookback = "2h"
    lookback_after = "2h"
    outputs = {"filled": aq.output.per_input(value_kind="numeric")}

    def __init__(self, cadence: str = "1m"):
        self.cadence = cadence

    def build_query(self, plant):
        return plant.query().measurement(alias="raw", quantity_kind="temperature")

    def transform(self, inputs, output, context):
        raw = inputs["raw"].df().select("time", "value").sort("time")
        if raw.is_empty():
            return
        output["filled"] = (
            raw.upsample(time_column="time", every=self.cadence)
               .with_columns(pl.col("value").interpolate())
               .drop_nulls()
        )
```

The lookback is doing the real work here. A hole can only be interpolated
once the sample on its far side exists, and that sample is what eventually
triggers the run. `lookback = "2h"` hands that run the two hours before the
new data, so the app sees both edges of the hole, recomputes the stretch, and
overwrites the filled stream — the hole closes the moment the outage ends.
`lookback` is therefore the longest outage the app can fill;
`lookback_after` gives the same context when a late correction lands in the
middle of history. Boundary holes stay open (`interpolate` leaves leading and trailing
nulls, and `drop_nulls` removes them) until data arrives to close them, which
keeps the app deterministic: it never guesses beyond its inputs.

Swap the `quantity_kind` selector to target other streams, or drop it and
select by `data_source` to fill one troublesome source.

## Pattern: a plant-wide KPI

A KPI is what a `named` output is for: it aggregates every match into one
call and publishes one stream everyone can find:

```python
import polars as pl
import acquirium as aq


class PlantAverageFlow(aq.App):
    name = "plant-average-flow"
    lookback = "5m"
    outputs = {"average": aq.output.named("plant-average-flow", value_kind="numeric")}

    def build_query(self, plant):
        return plant.query().measurement(alias="flow", quantity_kind="flow")

    def transform(self, inputs, output, context):
        frame = aq.align(inputs, every="1m", aggregate="mean")
        output["average"] = frame.select(
            "time", pl.mean_horizontal(pl.exclude("time")).alias("value")
        ).drop_nulls()
```

`align` puts every flow meter on a one-minute clock; `mean_horizontal`
averages across them; the result is one stream named `plant-average-flow`
under this app, regardless of how many meters exist this month.

## Deploying and inspecting

From configuration, at server start — every key besides `spec` becomes a
constructor argument:

```toml
[[apps]]
spec = "./high_turbidity_alarm.py:HighTurbidityAlarm"
threshold = 3.0
```

Or from Python, against a running server:

```python
client = aq.Acquirium(server_url="localhost", server_port=8000)
client.check_app(HighTurbidityAlarm, parameters={"threshold": 3.0})   # saves nothing
client.deploy_app(HighTurbidityAlarm, parameters={"threshold": 3.0})
client.app_dag()                      # the compiled plan as a NetworkX DiGraph
client.remove_app("high-turbidity-alarm")
```

`deploy_app` ships a reference to the class — module path plus a source
digest — so the server must be able to import the same module. Deployments
are durable; a removed app stops running and forgets its progress, so
redeploying it starts fresh under its `backfill` setting.

## Going further

- [Your first app](tutorials/first-app.md) — a runnable end-to-end walkthrough.
- [App reference](reference/apps.md) — the complete class contract: every
  policy, accessor, and output rule.
- [Incremental materialization implementation](materialization-implementation.md)
  — how the server tracks progress, transactions, and recovery underneath.
