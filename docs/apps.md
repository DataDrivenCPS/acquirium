# Apps

An **app** is a calculation that Acquirium keeps up to date for you. You write
a small Python class that says which measurements it reads and what to compute
from them; the server runs it whenever those measurements change and stores the
results as new streams. Those **derived streams** behave like any other stream
in the plant: you can query them, plot them, export them, and feed them into
further apps.

Most of a plant's recurring calculations take one of four shapes:

- **Soft sensors** — a value nobody measures directly, computed from ones that
  are: pressure drop across a membrane, specific flux, an estimated chemical
  dose from flow and concentration.
- **Cleaned-up copies of raw data** — one stream per sensor in a known unit,
  with spikes removed, out-of-range values dropped, and gaps filled.
- **Fault detection and alarms** — a stream that carries a value only when
  something is wrong: a threshold exceeded, a sensor gone quiet.
- **KPIs and rollups** — plant-wide totals, fleet averages, compliance figures,
  published under a name everyone can find.

```text
   the outside world              acquirium server                   you

  SCADA, historian,   driver    ┌───────────────────┐   query    notebooks,
  CSV, MQTT, lab   ──────────►  │   raw streams     │ ─────────► dashboards,
  sheets                        │        │          │            reports
                                │        ▼          │
                                │      [ app ]      │
                                │        │          │
                                │        ▼          │
                                │  derived streams  │ ─────────►
                                │        │          │
                                │        ▼          │
                                │     [ app ]  …    │
                                └───────────────────┘
```

Drivers and apps are the two halves of the data path, and it is worth keeping
them straight: a **driver** brings data *into* the server from somewhere
outside it, while an **app** derives new values *from* data the server already
has. Drivers are configured in `[[drivers]]`, apps in `[[apps]]`. If you are
reading a file, a database, or an OPC-UA server, you want a
[driver](tutorials/first-driver.md). If you are computing something from
measurements already stored, you want an app.

## A first app

This app finds every temperature measurement in the plant and publishes a
Celsius copy of each one:

```python
import acquirium as aq


class NormalizeTemperatures(aq.App):
    name = "normalize-temperatures"
    backfill = True
    outputs = {
        "celsius": aq.output.per_row(
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

That is a complete, deployable app. Reading it top to bottom:

- **`build_query` says what to read.** It selects measurements *semantically* —
  every point whose quantity kind is temperature, whatever its equipment,
  vendor or unit — rather than listing sensor IDs. The word `temperature` in
  `alias="temperature"` is a name you choose, and you use that same name to
  reach the data inside `transform`.
- **`outputs` says what to write.** `per_row` means "run me once for each
  measurement you matched, and publish one derived stream beside each of
  them". Fifty matched sensors mean fifty Celsius streams, and you name none
  of them.
- **`transform` is the calculation.** `in_unit("DEG_C")` converts each sensor
  from whatever unit it records into Celsius, and the result — a table of
  `time` and `value` — is assigned to the `celsius` output.
- **`backfill = True`** asks Acquirium to process the history that is already
  stored the first time this app runs. From then on, each new temperature
  reading triggers a conversion of just the new rows.

Deploy it by listing the file in `acquirium.toml`:

```toml
[[apps]]
spec = "./normalize_temperatures.py:NormalizeTemperatures"
```

Notice what is *not* in the class: no schedule, no loop, no connection
handling, no record of what has already been processed, no retry logic. That
is the point of the platform, and the next section explains the model that
makes it possible.

## The mental model

An app takes a **window** of input data — a stretch of time on the streams it
reads — and produces the matching stretch of output data. Three rules cover
the whole system.

### 1. The server hands you a window

Acquirium tracks every write to every stream. When rows arrive on a stream one
of your apps reads, it works out which rows are new, loads the relevant time
range, and calls your `transform` with the data already in memory. You never
poll, subscribe, or record a position.

```text
  one sensor's stored rows                              ── time ──►

  ●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●   ○○○○○○
  └────────────── already processed ──────────────┘   └ new ┘
                                                  ▲
                                      how far this app has read

                                                      ├──────┤
                                              transform() is called
                                              with this window of data
```

That mark — how far the app has read — is durable. It survives server
restarts, code edits, and configuration changes, so an app that is stopped
for a day picks up exactly where it left off rather than skipping or
reprocessing that day.

### 2. You produce the output rows for that window

Your job is to compute what those inputs call for: a table of timestamps and
values. Acquirium stores that table and moves the mark forward in the same
database transaction, so it can never record progress for work it did not
actually store.

### 3. Writing the same timestamp again replaces it

A stored value is identified by its stream and its timestamp, so computing the
same window twice does not produce two sets of values — the second set
replaces the first. There is nothing to de-duplicate and no half-finished
state to repair: if the server stops halfway through a computation, it runs
that window again and gets the same answer.

That third rule is what removes the bookkeeping most streaming systems ask an
author to do, and it asks one thing in return: **`transform` should be
deterministic** — given the same window of input data, it should produce the
same output. A calculation that reads the current time, draws random numbers,
or calls an outside service breaks that promise, because re-running it quietly
changes values that were already stored.

Everything else in this guide is detail on those three rules: how the query
decides which windows exist, how wide a window you get, and how to look at
what your code computed before you deploy it.

## From query to calls

Three steps get you from a plant model to a running calculation. The query
finds the streams; what you declare as outputs decides how those streams are
grouped into *calls* — one call being one run of your `transform`; and each
call receives three arguments.

### 1. The query finds your streams

`build_query` returns an Acquirium [query](tutorials/querying.md). It runs
against the plant model, so it keeps matching as the model changes — a sensor
added next month is picked up without touching the app.

```python
def build_query(self, plant):
    return (
        plant.query()
        .entity("ReverseOsmosis", alias="ro")
        .measurement(frm="ro", alias="feed", direction="upstream",
                     nearest=True, quantity_kind="pressure")
        .measurement(frm="ro", alias="permeate", direction="downstream",
                     nearest=True, quantity_kind="pressure")
    )
```

Every `alias=` you write becomes a name you can use later. `direction` walks
the piping topology, so `feed` and `permeate` name genuinely different points —
the nearest matching pressure before and after each unit — rather than two
labels on the same sensor.

What comes back is a table of **matches**: one row for each combination the
plant model contains.

```text
  ro                      feed                    permeate
  ────────────────────────────────────────────────────────────────────
  urn:plant/ro-1          PT-101 "RO-1 feed"      PT-102 "RO-1 permeate"
  urn:plant/ro-2          PT-201 "RO-2 feed"      PT-202 "RO-2 permeate"
  urn:plant/ro-3          PT-301 "RO-3 feed"      PT-302 "RO-3 permeate"
                          └──────────────────────────────────────┘
                          these two columns are sensors, so their data
                          arrives as inputs["feed"] and inputs["permeate"]
```

The table describes what was found; it holds no measurements. It reaches your
code as `context.result`, and the aliases that name sensors also arrive in
`inputs`, carrying their data. Aliases that name equipment — `ro` above — are
there to read, group by, or ignore.

If you are unsure what your query matches, build it interactively first with
[`options()` and `facets()`](how-to/explore-a-model.md), or run
`acquirium app check` (below), which prints the matches before running
anything.

### 2. The outputs decide how matches become calls

Each row of that table is one piece of work. What you declare in `outputs`
decides whether the rows are processed one at a time or all together; there is
no separate setting for it:

```text
three matched rows

per_row outputs — one call per row

  row 1 ──► transform(…) ──► one derived stream
  row 2 ──► transform(…) ──► one derived stream
  row 3 ──► transform(…) ──► one derived stream

named outputs — one call for the whole table

  row 1 ─┐
  row 2 ─┼──► transform(…) ──► one derived stream, under the name you chose
  row 3 ─┘
```

| declared output | calls to `transform` | derived streams | use it for |
|---|---|---|---|
| `aq.output.per_row(...)` | one per match | one per match | the same calculation applied to every match: a conversion, a smoother, an alarm |
| `aq.output.named("plant-total", ...)` | one call, holding every match | one, named exactly as you say | a calculation over all matches: a fleet average, a plant total |

**One row is not the same thing as one sensor.** A row is a combination the
query found, and a combination can involve several sensors: the query above
pairs the feed and permeate pressures of the same RO unit, so a single
`per_row` call receives both of them and publishes one `delta_p` stream for
that unit. Three RO units mean three calls and three delta streams — not
six.

A named stream has exactly one app writing it, so you cannot declare both
kinds of output in an app whose query matches more than one row. Acquirium
says so when you check or deploy it, and tells you the fix: compute the
aggregate in a second app that reads the first app's derived streams.

### 3. What each call receives

```python
def transform(self, inputs, output, context):
```

| argument | what it is | holds |
|---|---|---|
| `inputs` | mapping of alias → `StreamSet` | **the data**, one entry per measurement alias |
| `output` | `OutputBuilder` | **where results go**, assigned by port name |
| `context` | `InputBatch` | **the match**: which equipment, which time range, and why the call happened |

For the first row of the match table above, in a `per_row` app:

```python
inputs.keys()               # dict_keys(['feed', 'permeate'])
inputs["feed"].stream       # StreamDescriptor(ref_uri='urn:…/PT-101',
                            #   label='RO-1 feed', unit='…/PSI', point_uri=…)
inputs["feed"].df()         # polars: ref_uri | time | value      (288 rows)

context.row                 # {'ro': 'urn:plant/ro-1',
                            #  'feed': 'urn:…/point-101', 'feed_ref': 'urn:…/PT-101',
                            #  'feed.label': 'RO-1 feed', 'feed.unit': '…/PSI', …}
context.result              # every matched row — all three units — as a dataframe
context.read_window         # TimeWindow(start=…09:57+00:00, end=…10:04+00:00)
context.changed_window      # TimeWindow(start=…10:02+00:00, end=…10:04+00:00)

output["delta_p"] = frame   # a table of time and value
```

Each question has exactly one place to look:

| question | where |
|---|---|
| what are the values? | `inputs[alias].df()` |
| which sensor is this call for? | `inputs[alias].stream` |
| which row is this call computing? | `context.row` |
| what did the query match overall? | `context.result` |
| what time range, and why? | `context.read_window`, `context.changed_window` |

The next three sections take each argument in turn.

## `inputs`: the data

`inputs` maps each measurement alias from your query to a **stream set** — the
rows for that alias inside this call's window, together with a description of
the streams they came from.

```python
frame = inputs["feed"].df()              # Polars dataframe: ref_uri, time, value
frame = inputs["feed"].df("pandas")      # the same rows as pandas
table = inputs["feed"].collect()         # the same rows as an Arrow table
psi   = inputs["feed"].in_unit("PSI")    # the same data, values converted
```

| member | type | what it is |
|---|---|---|
| `.df()` | [`polars.DataFrame`](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html) | every row in this call's window: `ref_uri`, `time`, `value` |
| `.df("pandas")` | [`pandas.DataFrame`](https://pandas.pydata.org/docs/reference/frame.html) | the same rows, if you prefer pandas |
| `.collect()` | [`pyarrow.Table`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) | the same rows as Arrow, with no dataframe library involved |
| `.batches()` | iterator of Arrow record batches | the same rows in chunks, for windows too large to hold at once |
| `.changes` | `pyarrow.Table` | only the rows that are *new* in this call — the ones that triggered it |
| `.stream` | `StreamDescriptor` | which sensor this call is about (see below) |
| `.streams` | tuple of `StreamDescriptor` | every sensor bound to this alias |
| `.in_unit(unit)` | `StreamSet` | the same set with every value converted into `unit` |
| `.window` | `TimeWindow` | the time range these rows cover |
| `.alias` | `str` | the alias from your query |

The frame always has the same three columns — `ref_uri`, `time`, `value` —
whether one stream is bound or fifty, so your code never has to change shape
with the match. `time` is a timezone-aware UTC timestamp;
`value` is a float for numeric streams and a string for text streams.
`.changes` is an Arrow table rather than a dataframe, so reach for
`pl.from_arrow(inputs["feed"].changes)` when you want to work with it the same
way.

**Which sensor am I computing for?** `.stream` answers that, and returns a
`StreamDescriptor`:

```python
sensor = inputs["feed"].stream
sensor.ref_uri      # the stream's identity in storage
sensor.point_uri    # the point in the plant model it measures
sensor.label        # human-readable name, e.g. "RO-1 feed"
sensor.unit         # the unit URI the sensor records in
```

In a `per_row` app every alias holds exactly one stream per call, so `.stream`
always has an answer. A `named` app sees every match at once, so its aliases
can hold many streams: there `.stream` raises an error and `.streams` gives
you the whole tuple. The error is deliberate — an app changed from one kind of
output to the other stops loudly, instead of quietly computing on the first
sensor and ignoring the rest.

### Two helpers for real sensor data

`in_unit` converts a whole set, each stream from its own recorded unit, so an
alias holding a mix of Fahrenheit and Celsius sensors comes out uniform:

```python
celsius = inputs["temperature"].in_unit("DEG_C")   # a StreamSet, so .df() etc. still work
```

The unit may be a QUDT URI, symbol, or label. A stream with no recorded unit,
or one that is dimensionally incompatible with the target, raises immediately
rather than feeding mis-scaled numbers into your calculation. See
[Units](explanation/units.md) for how Acquirium records and compares them.

`aq.align` puts several inputs on one clock. Sensors rarely sample at the same
instants, and combining them otherwise means a hand-written resample and join
per stream:

```text
temperature   ●────●───●────●───●            (a sample every ~40 s)
flow             ●─────────●─────────●       (a sample every ~2 min)

                aq.align(inputs, every="1m")
                          │
                          ▼
                time    temperature   flow
                10:00   20.1          1.02
                10:01   20.4          null
                10:02   20.9          1.05
```

```python
frame = aq.align(inputs, every="1m", aggregate="mean")
```

The result is one wide Polars frame: a `time` column plus one column per
stream, named after the alias (or `alias[label]` when an alias holds several
streams). Where a stream reported nothing in a bucket, its column holds a
null, which
[Polars' null handling](https://docs.pola.rs/user-guide/expressions/missing-data/)
lets you fill, drop, or carry through as your calculation requires. `aggregate`
may be `mean`, `min`, `max`, `sum`, `first`, `last`, `median`, or `count`.

## `output`: where results go

Each key of the `outputs` declaration is a **port** — the name you use to
refer to that output inside the class. Assign a table to a port and its rows
become that port's derived stream:

```python
output["celsius"] = frame.select("time", "value")
```

The table may be a Polars dataframe, a pandas dataframe, an Arrow table, or
Arrow record batches. Whichever you use, it must have exactly two columns and
no others:

| column | type | rules |
|---|---|---|
| `time` | timezone-aware timestamp | non-null; no duplicates within one assignment; normalized to UTC and sorted for you |
| `value` | number for a `"numeric"` port, string for a `"text"` port | non-null |

Two things follow in practice: drop the `ref_uri` column before publishing a
frame that came from `inputs` — `select("time", "value")` is the usual way —
and drop or fill nulls rather than publishing them.

A few rules keep published streams predictable:

- **You can only assign a port you declared.** `output["typo"] = …` raises
  immediately and lists the declared ports. Every published stream is
  declared up front; a transform cannot invent one.
- **Each port takes at most one assignment per call.** Build the whole table,
  then assign it.
- **Assigning nothing publishes nothing.** That is the normal case for an
  alarm app with nothing to report; it is not an error.
- **Writing a timestamp that already has a value replaces it.** This is the
  third rule of the mental model above, and it is what makes recomputing a
  window safe.
- **Everything one call publishes is saved together**, along with the app's
  record of how far it has read, so the two can never disagree.

The declaration itself is where a derived stream's metadata lives — unit,
label, quantity kind, the point it attaches to:

```python
outputs = {
    "celsius": aq.output.per_row(
        value_kind="numeric",                          # required: numeric or text
        label="Feed temperature (normalized)",
        unit="http://qudt.org/vocab/unit/DEG_C",
        quantity_kind="http://qudt.org/vocab/quantitykind/Temperature",
    ),
}
```

None of this is guessed from the data you publish: `value_kind` is required,
and the rest is exactly what you declare, in the same spirit as a driver
registering a stream. The full argument list is in the
[app reference](reference/apps.md#declaration-arguments).

Derived streams are registered under the app that produced them, with source
`derived:<app name>`. A `per_row` port's stream name is generated from the
port name and the sensors that call reads, so it stays the same across
restarts and code edits; a `named` port's stream name is exactly the string
you gave. Either way, the
derived stream is queryable like any other measurement — including by the
next app.

## `context`: the match this call is for

`context` holds no data. It tells you what this call is *about*: which match,
which time range, and why it happened.

| member | what it is |
|---|---|
| `.row` | the one matched row this call is computing, as a dictionary — `per_row` apps only |
| `.result` | every row the query matched, as a Polars dataframe — the same table in every call |
| `.changed_window` | the timestamps of the new data: *why* this call happened |
| `.read_window` | the range actually loaded, once `lookback` and `lookahead` have widened it |
| `.from_revision`, `.to_revision`, `.graph_revision`, `.binding_signature` | runtime diagnostics, useful in logs |

`context.row` and `context.result` use the same column names as
[`Query.metadata()`](reference/client-api.md): a column named for each alias
holds the URI that matched it, and where that alias is a sensor, three more
columns sit beside it — `<alias>_ref`, `<alias>.label` and `<alias>.unit`. So
a `per_row` app can read which equipment its sensors belong to:

```python
unit_uri = context.row["ro"]                 # which RO unit this call is for
label    = context.row["feed.label"]         # "RO-1 feed"
```

`context.result` is the whole match table, and every call gets it, whichever
kind of output the app declares. A `named` app uses it to aggregate over the
fleet. A `per_row` app uses it to see the fleet it is part of — to rank
against its siblings, count them, or group them:

```python
siblings = context.result.height - 1
by_unit  = context.result.group_by("ro").agg(pl.col("feed_ref"))
```

The two windows explain the call. `changed_window` is the timestamp range of
the rows that triggered it; `read_window` is what you were actually handed,
which is wider whenever `lookback` is set. Every `inputs[alias].window` equals
`context.read_window`, and `inputs[alias].changes` holds exactly the rows
inside `changed_window`.

## Windows: how much data each call sees

By default a call sees only the new data. One attribute widens that:

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

| style | `lookback =` | what `transform` sees | right for |
|---|---|---|---|
| Streaming | `"0s"` (default) | only the range of new data | arithmetic done row by row: conversions, thresholds, unit changes |
| Windowed | `"10m"` | new data plus context before it | rolling averages, rates of change, gap filling — set it to at least your calculation's window |
| Whole-stream | `"all"` | everything ever stored | when any input point can affect any output point: normalization against a historical median, a cumulative total |

Your code does not change between these styles; only the window you are
handed does. Recompute everything you were given and publish all of it —
since re-writing a timestamp replaces it, that is always safe.

`lookahead` is the mirror image: context *after* the changed range. You need
it when corrections land in the middle of history — a lab result backdated to
yesterday, a historian replaying a gap — and your calculation needs the rows
that follow.

### The scheduling attributes

Each of these is a plain class attribute holding a duration string
(`"250ms"`, `"30s"`, `"5m"`, `"2h"`, `"7d"`), true or false, or `"all"`:

| attribute | default | meaning |
|---|---|---|
| `lookback` | `"0s"` | stored context before the new data; `"all"` reads the whole stream |
| `lookahead` | `"0s"` | context after the changed range, for corrections landing mid-history |
| `backfill` | `False` | whether the first run processes history that is already stored |
| `coalesce` | `"0s"` | wait for a quiet gap in a burst of writes before running |
| `max_delay` | none | cap on the `coalesce` wait, so a steady trickle cannot postpone a run forever |
| `min_interval` | none | at most one run per interval |

`backfill` matters only the first time Acquirium sees a given set of inputs.
After that the app's saved position takes over, and it survives restarts, code
edits, and parameter changes.

The last three attributes are about *cost*, not correctness. An app with none of
them runs as soon as it has new input, which is usually what you want. Reach
for `coalesce = "2s"` when a driver writes in bursts and you would rather
compute once at the end of each burst, and `min_interval = "5m"` when the
calculation is expensive and near-real-time results are not worth it.

One honest limitation: these are throttles, not a clock. An app runs when its
inputs change, so there is no way to say "run at midnight regardless". A daily
figure is computed as a rolling window over incoming data
(`lookback = "24h"`), which converges to the same answer as the day's data
arrives.

## Check it before you deploy

A check runs your app for real — same query, same stored data, same
`transform` — and prints what it computed instead of saving it. Nothing is
deployed, no derived stream is created, no progress is recorded, so there is
nothing to undo afterwards:

```bash
uv run acquirium app check ./normalize_temperatures.py:NormalizeTemperatures
```

```text
normalize-temperatures: 3 input group(s) matched

[1] inputs
      temperature: 1 stream(s), 288 rows read
        - Basin 1 inlet temperature
    output 'celsius' -> celsius:5d717e… (numeric, 288 rows)
        2026-01-01T00:00:00+00:00  17.777777777777878
        2026-01-01T00:05:00+00:00  17.833333333333428
        … 286 more row(s); pass -n 0 for all of them

[2] inputs
      temperature: 1 stream(s), 288 rows read
        - Basin 2 inlet temperature
    output 'celsius' -> celsius:f06dd2… (numeric, 288 rows)
        2026-01-01T00:00:00+00:00  18.0
        2026-01-01T00:05:00+00:00  18.1
        … 286 more row(s); pass -n 0 for all of them
```

Read it as three questions answered at once:

1. **Did the query match what I expected?** Three groups, each one sensor,
   named the way you know them. A missing sensor, an extra one, or `0 input
   group(s) matched` is a query problem — see
   [debugging an empty query](how-to/debug-an-empty-query.md).
2. **Did the transform run?** A group whose transform raised prints
   `error:` instead of outputs, and the command exits non-zero, so a check
   works as a test in CI.
3. **Are the values right?** Group 1 is a Fahrenheit sensor converted to
   17.8 °C; group 2 was already Celsius and comes through unchanged. This is
   where a wrong unit, a sign error, or an off-by-one window shows up.

Each group reads every retained input row, so what you see is what a
`backfill = True` deployment would publish. The command prints the first five
rows of each output; `-n 20` shows more, `-n 0` shows all of them,
`--params '{"threshold": 3}'` sets constructor parameters, and `--json`
prints the whole result document for scripting.

The same thing from Python, where every computed row comes back unless you
ask for fewer:

```python
result = client.check_app(NormalizeTemperatures, limit=10)
rows = result["bindings"][0]["outputs"]["celsius"]["values"]
```

## Debugging: `--local` and `aq.console()`

By default a check runs your app **on the server**, because that is where the
data is. That is also what makes it awkward to debug: a `breakpoint()` opens
on the server's standard input, where nobody can type, and a traceback lands
in the server log rather than in front of you.

`--local` moves the app into your own process and pulls its inputs over the
API instead:

```bash
uv run acquirium app check ./normalize_temperatures.py:NormalizeTemperatures --local
```

```text
  acquirium app check                     acquirium app check --local

  your terminal                           your terminal
      │ sends the app class                   │ asks for the input rows
      ▼                                       ▼
  ┌────────────┐                          ┌────────────┐
  │   server   │  runs transform()        │   server   │  just serves data
  └────────────┘                          └────────────┘
                                              │ rows come back
  console()  → skipped, logged                ▼
  traceback  → reported per group          transform() runs here
                                           console()  → opens in your terminal
                                           traceback  → lands in your terminal
```

Everything else about the two is identical, down to the exact derived stream
each output would be written to. Input rows travel over HTTP, so a check that
reads a lot of history is slower this way — and because the server never
imports your app, `--local` also works when the file is not importable there
at all.

With the app running in your terminal, two tools work normally. Python's
`breakpoint()` drops you into the debugger. And `aq.console()` opens an
interactive prompt holding whatever is in scope where you put it:

```python
def transform(self, inputs, output, context):
    frame = inputs["temperature"].df()
    aq.console()          # look at `frame` before deciding what to emit
    output["celsius"] = frame.select("time", "value")
```

```text
acquirium console — transform at ./normalize_temperatures.py:14
in scope: context, frame, inputs, output, self
Ctrl-D (or exit()) resumes.
>>> frame.height
288
>>> frame["value"].mean()
64.3
>>> inputs["temperature"].stream.unit
'http://qudt.org/vocab/unit/DEG_F'
>>> context.read_window
TimeWindow(start=…, end=…)
```

Ctrl-D closes the prompt and the app carries on to the next group. The console
sees a snapshot of your variables: rebinding a name inside it does not change
the running function, though mutating an object does.

A console left in an app that runs somewhere without a terminal — a deployed
app, or a check without `--local` — logs that it was skipped and carries on,
rather than stalling on input that will never come. It is still worth taking
out when you are done.

### Common problems

| what you see | usual cause | what to do |
|---|---|---|
| `0 input group(s) matched` | the query matches nothing in this plant model | build the query interactively with [`options()`](how-to/explore-a-model.md); check spelling of a `quantity_kind` or class |
| more groups than expected | the query is under-constrained, or two aliases resolve to the same points | add a `direction`, `frm`, or attribute filter; see [querying](tutorials/querying.md) |
| `no stored data for these inputs` | the query matched a point that has never been written to | confirm the driver is running and writing that stream |
| `output 'x': an output must be … exactly time and value columns` | `ref_uri` is still on the frame | `select("time", "value")` before assigning |
| `output 'x': output value must be non-null` | nulls survived a resample or join | `drop_nulls()` or fill them explicitly |
| `stream … has no recorded unit to convert from` | `in_unit` on a stream whose point has no unit | declare the unit on the point, or skip the conversion for that stream |
| values look right but nothing updates after deploying | the app's window is too narrow, or nothing has written since | check `lookback`, and whether `backfill` was needed |

## Worked example: filling gaps in a sensor stream

Real plant data has holes. A logger reboots, a network link drops, a probe is
pulled for calibration. Downstream calculations — daily averages, mass
balances, model training sets — either have to cope with those holes
individually or read a stream where the holes are already filled. An
imputation app publishes the second kind of stream.

The interesting part is that not every hole should be filled the same way:

```text
  one sensor, 5-minute readings                            ── time ──►

  ●●●●●●●●●●●●●●●●●  ○○○○○○  ●●●●●●●●●●●●  ○○○○○○○○○○○○○○○○○○○○○○  ●●●●●●●●●
                     └ 30m ┘               └────── 6 hours ──────┘

                 a short hole:                a long hole:
                 the readings on either       a straight line across a third
                 side are still relevant      of a day is a fabrication; the
                 → draw a straight line       daily pattern is better evidence
                                              → use the typical value for
                                                that time of day
```

So the app has two strategies and a threshold between them, and it tells you
which one it used for every value it invented.

```python
from datetime import timedelta

import polars as pl
import acquirium as aq


class FillGaps(aq.App):
    """Publish a gap-free copy of every temperature stream."""

    name = "fill-gaps"
    backfill = True
    lookback = "7d"
    lookahead = "7d"
    outputs = {
        "filled": aq.output.per_row(
            value_kind="numeric",
            label="Gap-filled temperature",
            unit="http://qudt.org/vocab/unit/DEG_C",
        ),
        "method": aq.output.per_row(value_kind="text", label="How each gap was filled"),
    }

    def __init__(self, cadence: str = "5m", max_interpolate_hours: float = 2.0):
        self.cadence = cadence
        self.max_interpolate = timedelta(hours=max_interpolate_hours)

    def build_query(self, plant):
        return plant.query().measurement(alias="reading", quantity_kind="temperature")

    def transform(self, inputs, output, context):
        measured = inputs["reading"].in_unit("DEG_C").df()
        if measured.is_empty():
            return

        # 1. put the readings on a regular grid; missing buckets become nulls
        grid = (
            measured.sort("time")
            .group_by_dynamic("time", every=self.cadence)
            .agg(pl.col("value").mean())
            .upsample(time_column="time", every=self.cadence)
        )

        # 2. measure each hole: how far apart are the readings on either side?
        seen = pl.when(pl.col("value").is_not_null()).then(pl.col("time"))
        grid = grid.with_columns(
            gap=seen.backward_fill() - seen.forward_fill(),
            clock=pl.col("time").dt.time(),
        )

        # 3. a typical day, from the readings in this window
        profile = (
            grid.drop_nulls("value")
            .group_by("clock")
            .agg(pl.col("value").median().alias("typical"))
        )

        # 4. short holes are interpolated, long ones follow the typical day
        short = pl.col("gap") <= self.max_interpolate
        filled = (
            grid.join(profile, on="clock", how="left")
            .sort("time")
            .with_columns(straight_line=pl.col("value").interpolate_by("time"))
            .with_columns(
                method=pl.when(pl.col("value").is_not_null()).then(pl.lit("measured"))
                .when(short).then(pl.lit("interpolated"))
                .otherwise(pl.lit("profile")),
                value=pl.when(pl.col("value").is_not_null()).then(pl.col("value"))
                .when(short).then(pl.col("straight_line"))
                .otherwise(pl.col("typical")),
            )
            .drop_nulls("value")
        )

        output["filled"] = filled.select("time", "value")
        output["method"] = filled.filter(pl.col("method") != "measured").select(
            "time", pl.col("method").alias("value")
        )
```

Checking it against three days of readings with a 30-minute hole and a
six-hour hole:

```bash
uv run acquirium app check ./fill_gaps.py:FillGaps -n 3
```

```text
fill-gaps: 1 input group(s) matched

[1] inputs
      reading: 1 stream(s), 786 rows read
        - Basin 1 inlet temperature
    output 'filled' -> filled:9e6cf9… (numeric, 864 rows)
        2026-01-01T00:00:00+00:00  18.0
        2026-01-01T00:05:00+00:00  18.09
        2026-01-01T00:10:00+00:00  18.17
        … 861 more row(s); pass -n 0 for all of them
    output 'method' -> method:ca16c4… (text, 78 rows)
        2026-01-01T10:00:00+00:00  interpolated
        2026-01-01T10:05:00+00:00  interpolated
        2026-01-01T10:10:00+00:00  interpolated
        … 75 more row(s); pass -n 0 for all of them
```

786 measured rows in, 864 gap-free rows out: 6 interpolated across the short
hole and 72 taken from the daily profile across the long one. The counts are
worth checking — 864 is exactly three days of five-minute buckets, which says
the grid is right, and 78 imputed values is the size of the two holes.

Several things in that app generalize beyond gap filling.

**Choosing the window is the real design decision.** A hole can only be
filled once the reading on its far side exists — and that reading is what
eventually triggers the run. So `lookback` has to be long enough to reach back
across the hole from there, and `lookahead` gives the same context when a
historian later backfills data into the middle of history. The seven days here
also feed the daily profile, which is built from the readings in the window
rather than from a stored model, so the app depends on nothing but the data it
was handed. Widen the window and the profile is steadier but each run does
more work; narrow it and the app is cheaper but can no longer close long
outages. `lookback = "all"` is the far end of that trade: always correct,
always self-healing, and steadily more expensive as the stream grows.

**Re-running the app is what closes a hole.** During an outage the app keeps
running on whatever data does arrive, and the hole stays open at its trailing
edge: the grid is only built between the first and last reading the app can
see, so it never invents values past the end of its inputs. When the sensor
comes back, the run triggered by that new data reaches back over the outage,
recomputes the whole stretch, and replaces what it published before. Nothing
in the app handles the outage as a special case — replacing values by
timestamp handles it.

**Two outputs, for two different readers.** `filled` is what dashboards and
downstream apps read. `method` is a text stream with one row per invented
value, so anyone can see which points were measured and which were
manufactured, and a compliance report can leave the manufactured ones out. A
port assigned an empty table publishes nothing, so a sensor with no holes
produces no `method` rows at all — that stream appears only once there is
something to say.

**Settings belong in configuration, not in the code.** `cadence` and
`max_interpolate_hours` are constructor arguments, so changing either one is a
config edit rather than a change to a calculation someone has already
reviewed:

```toml
[[apps]]
spec = "./fill_gaps.py:FillGaps"
cadence = "5m"
max_interpolate_hours = 2.0
```

An app's name identifies one deployment, so the same class cannot be deployed
twice with different settings. A second configuration — an hourly cadence for
lab measurements, say — is a small subclass with its own `name` and its own
query:

```python
class FillLabGaps(FillGaps):
    name = "fill-lab-gaps"

    def build_query(self, plant):
        return plant.query().measurement(alias="reading", data_source="lab-sheets")
```

**Know what you are publishing.** Some of a filled stream is model output
rather than measurement. The `method` output records which parts, and a
`label` that says the stream is gap-filled costs nothing — anyone who finds it
in a dashboard should be able to tell it apart from the raw sensor.

## More patterns

### Fault detection

An alarm is an app whose output stream carries a value only when something is
wrong. Emit a row for each violation and nothing at all the rest of the
time.

```python
class HighTurbidityAlarm(aq.App):
    name = "high-turbidity-alarm"
    outputs = {"alarm": aq.output.per_row(value_kind="text")}

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

Each turbidity sensor gets its own alarm stream, and querying it back gives
the full alarm history with timestamps — an alarm log you can plot, count, and
join against operator notes.

### A plant-wide KPI

A KPI is what a `named` output is for. Every match arrives in one call, and
the result is one stream under a name people can look up:

```python
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

`align` puts every flow meter on a one-minute clock, `mean_horizontal`
averages across them, and the result is one stream named `plant-average-flow`
however many meters the plant has this month.

### Chaining apps

Apps read each other's output, which is how a fleet aggregate over a
fanned-out calculation is written: one `per_row` app, then one `named` app
over its results.

Give the upstream output a handle the downstream query can select on — a
`data_source` tag is the simplest:

```python
# upstream
outputs = {"clean": aq.output.per_row(value_kind="numeric", data_source="cleaned-flow")}

# downstream
def build_query(self, plant):
    return plant.query().measurement(alias="flow", data_source="cleaned-flow")
```

Semantic metadata works the same way: a `quantity_kind`, `unit`, or
`point_uri` declared on the upstream output can be selected on downstream. The
server works out the dependency order and always runs upstream work first, so
the downstream app never reads a half-finished result.

## Deploying and managing apps

From configuration, at server start — every key other than `spec` (and an
optional `name` label) becomes a constructor argument:

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
client.app_dag()                       # the running plan as a NetworkX DiGraph
client.remove_app("high-turbidity-alarm")
```

Three consequences of how deployment works are worth knowing up front:

- **The server runs your code, so it must be able to import it.** `deploy_app`
  ships a reference to the class — module path, name, and a digest of the
  source — not the code itself. A class defined in a script's `__main__` is
  rejected for that reason; put it in a file the server can import. (A check
  is more forgiving: it sends the file's directory along, and `--local` skips
  the question entirely.)
- **Editing a deployed app's file is not deploying it.** The server goes on
  running the version it imported, and pins that version by a digest of the
  source — so deploy again after an edit, rather than waiting for the change
  to be noticed.
- **An app's position is durable and follows what it reads and writes.**
  Editing the code or changing a parameter resumes where processing left off
  rather than starting over — which is what you want, since otherwise fixing a
  comment would silently skip everything written in the meantime. To reprocess
  history with new code, remove the app and redeploy it with
  `backfill = True`, or deploy the new version under a new name.

Removing an app stops it and forgets its position. Its derived streams and
their history stay in the store — removal does not delete data.

## Where to go next

- [Your first app](tutorials/first-app.md) — a runnable end-to-end walkthrough
  with a server and a publishing script.
- [App reference](reference/apps.md) — the complete contract: every attribute,
  method, and output rule, plus the algorithms and design decisions behind
  them.
- [Querying](tutorials/querying.md) and
  [Explore a model](how-to/explore-a-model.md) — building the query that
  selects your inputs.
- [Working with data](tutorials/data.md) — the same dataframes, outside an app.
- [Why apps look like this](explanation/apps.md) — the reasoning behind
  recomputed windows.

### The names in one place

| name | what it is | reference |
|---|---|---|
| `aq.App` | the class you subclass; its attributes set timing and windows | [Class contract](reference/apps.md#aqapp) |
| `aq.output.per_row` / `.named` | the output declarations, which also decide how matches group into calls | [Outputs](reference/apps.md#aqoutput) |
| `StreamSet` | one alias's data: `.df()`, `.collect()`, `.changes`, `.stream`, `.in_unit()` | [StreamSet](reference/apps.md#streamset) |
| `StreamDescriptor` | one stream's identity: `ref_uri`, `point_uri`, `label`, `unit` | [StreamDescriptor](reference/apps.md#streamdescriptor) |
| `InputBatch` | the `context` argument: the match, the windows, diagnostics | [InputBatch](reference/apps.md#inputbatch) |
| `OutputBuilder` | the `output` argument: assign a table per declared port | [OutputBuilder](reference/apps.md#outputbuilder) |
| `aq.align` | resample several inputs onto one clock | [align](reference/apps.md#aqalign) |
| `aq.console` | an interactive prompt inside a running transform | [console](reference/apps.md#aqconsole) |

The authoring names all come from the `acquirium` package (`aq.App`,
`aq.output`, `aq.align`, `aq.console`). The runtime types underneath them live
in `acquirium.Materialization`, and you only need those if you are embedding
the scheduler in your own program.
