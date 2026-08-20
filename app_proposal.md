# Acquirium application interface — proposal and review outcome

A design proposal for how an application talks to acquirium, worked out against a
real app: the eFlex pump + tank load-shifting example in this directory. This
document summarises what the proposal argues, what got settled during review, and
what still needs a decision.

Full write-up with code sketches:
<https://claude.ai/code/artifact/2797a47e-6c13-422c-b2d9-c2bca18f2b72>
(source: `acquirium-app-api.html` in this directory)

---

## The problem

`helpers/acquirium_store.py` is ~200 lines. Roughly 10 of them are about load
shifting. The rest is bookkeeping that every acquirium app will have to write,
and will write differently: connection policy and fail-open, graph re-insertion,
minting run ids and URIs, a `runs/<id>/<name>` naming scheme invented so repeated
horizons don't overwrite each other, hand-written `isCalculatedFrom` /
`produces` / `hasRunStream` / `hasObservationLocation` triples, a ref-node typing
workaround, and two `insert_log` loops.

On the read side, `experiments.py` hand-writes a 15-line SPARQL query that takes
apart exactly what the writer just assembled, then rebuilds stream references
with `f"runs/{run_id}/{quantity}"`. The two files are coupled by a string
convention rather than an API — which is why the notebook already carries a
`// run predates this quantity` branch for silently-missing series.

**The smell:** every acquirium app has to invent, and version, its own schema for
run provenance, then keep the writer and the reader in sync by convention.

## The organising principle

The app declares what it is a function of and what it produces; the framework
owns identity, provenance, and retrieval; the app writes values through a handle
that is already provenance-aware.

Two rules follow. **No implicit outputs** — writing an undeclared name is an
error, because implicit outputs are where string conventions creep back in. And
**one clearly-labelled escape hatch** — `run.graph` writes app-specific triples
into the run's own source, which the framework stores and never interprets.

---

## Settled during review

### 1. There is one canonical instance graph, and apps read it

Acquirium maintains a single canonical instance graph — the product of unions and
inference — and every app bases its reads on that same shared picture. An app
does not load the plant model; it points at the graph and names the slice it
uses.

An app *may* contribute a graph of its own, tagged with a **source id** so
acquirium knows where those triples came from. In this example the eFlex binding
layer (`acq:hasPyomoVar` on each observable point) is the app's contribution, not
the plant's.

Today's example re-inserting `watr_model.ttl` with `replace=True` on every store
is the anti-pattern this replaces.

### 2. `Experiment` is a separate base class, not a flag on `App`

The original draft made boundedness a `shape` enum (`Stream` / `Scenario` /
`Batch`) on `App`. That was wrong twice: it put the special case inside the
general case, and it bundled two unrelated decisions into one word. The real
question is whether the work has a beginning and an end, and that changes what
provenance even means.

|                          | `App`                                                        | `Experiment`                                        |
| ------------------------ | ------------------------------------------------------------ | --------------------------------------------------- |
| Runs when                | a batch of readings arrives — continuously, forever            | someone asks a question                              |
| Boundaries               | none; a standing transformation                                | explicit: you start it and you stop it               |
| Provenance attaches to   | the **stream** — "calculated from those, by `app@version`"     | the **run** — params, inputs, metrics, lineage       |
| Per-run records          | off; available if a specific app wants them                    | on; the whole point                                  |
| Stream identity          | one stream per point, appended                                 | one stream per point *per run*                       |
| Example                  | a soft sensor, a unit conversion, a gap-filler                 | load shifting; any what-if study                     |

The failure mode this avoids: a stream transform fires every time a batch of
readings lands — thousands of times a month, each uninteresting alone. Recording
a separately attributable run for each is provenance you pay to store and never
query. The question you actually ask of such a stream is *"what is this derived
from, and by what?"* — one statement about the stream, not thousands about its
runs.

This also retires the `runs/<id>/<name>` scheme, the per-run stream nodes, and
the `efx:hasRunStream` back-links. They were all one idea — repeated horizons
must not overwrite each other — hand-built because nothing in the framework knew
this app was bounded. Inheriting `Experiment` says it once.

The beginning and the end are the context manager: entering `with app.run(...)`
is the start, leaving it is the stop, including when you leave by raising.

### 3. Run records live in SQL; provenance stays in the graph

`Run` becomes a first-class record the server owns — id, app name and version,
start and end, status, params, metrics, pointers to streams and attachments —
kept in an internal relational store rather than as triples, so run bookkeeping
does not bloat the graph. That is what lets the server answer "the last fifty
runs of this app where `total_cost < 4000`" without reading anyone's triples, and
enforce retention, atomic commit, and failure records once for everyone.

Provenance stays in the graph, because the questions worth asking about it are
graph-shaped: which run produced this stream, which app produced that run, what
was it calculated from, what else did the same inputs feed.

### 4. Most runs are garbage, and the framework should say so

Apps run many, many times. A parameter sweep produces hundreds of runs and you
care about four. So retention is declared, not improvised:

- `retain = Keep.last(5)` on the app — the recent window is always there, so you
  can look at what just happened.
- Anything older survives only if something marked it: `run.keep(reason)` during
  the run, or `r.keep()` / a tag afterwards from the reader. You usually only
  know a run mattered once you have looked at it, which is what the recent
  window is for.
- Everything else is collected.

Without this, "record every run" is a promise to fill a disk.

### 5. Params are pydantic, and that is the schema of record

The app points `params` at its own `BaseModel`. Three things follow: the
framework stores `model_json_schema()` with the app version, which is what makes
params indexable and filterable server-side; `r.params` hands the model back on
read, so the notebook stops doing
`cfg["model"]["plant"]["units"]["tank"]["construction_options"]`; and `rerun` can
do a one-field `model_copy(update=...)` of the config that actually ran.

---

## What the app author is left with

For the load-shifting example: the declaration (name, version, params model,
retention, what it reads, what it produces) plus the `with` block — roughly 45
lines, every one of them about load shifting. Deleted outright: `SOURCE_ID`, the
`EFX`/`ACQ`/`S223` namespace juggling, the `StoreReport` dataclass, the
`acquirium.toml` parsing and connect try/except, the registration loop, the
payload dict, the naming convention, every hand-written triple, the ref-node
typing workaround, both `insert_log` loops — and in `experiments.py`, the SPARQL
query, the `json.loads` config walking, the `format_solved` id parser, and the
`reference_uri` string building.

That ratio — ~10 lines of app logic currently dragging ~190 lines of bookkeeping
— is the measure of whether the boundary landed in the right place.

---

## Open asks

Five decisions, each with a recommendation.

### 1. Where exactly is the line between the run table and the graph?

Proposal: the graph carries only the edges you would traverse — stream → run →
app, plus `isCalculatedFrom` — and every literal (timings, status, params, metric
values) lives in the table, joined on the run id.

**To decide:** whether metric values are an exception. `produced_by()`-style
traversals get considerably more useful if you can filter on a metric
mid-traversal rather than joining out to the table and back.

### 2. Which revision of the canonical graph did a run read?

The canonical graph moves as sources are added and inference reruns. A run has to
record the revision it saw, and an app-contributed graph needs a content hash
under its source id. Without both, a rerun silently reinterprets an old run.

**To decide:** whether the canonical graph is versioned in a way a run can cite.

### 3. When a run is collected, what is actually deleted?

Retention has to drop the expensive part — series and attachments, nearly all the
bytes. Less obvious whether the cheap part goes with it.

Proposal: collect series and attachments, keep the row (id, params, metrics,
status) as a tombstone marked `collected`. It costs almost nothing and preserves
the sweep — you can still plot cost against pump size across four hundred runs
long after their timeseries are gone, and a lineage link from a surviving rerun
does not dangle.

**To decide:** whether retention is a disk-space policy or actual forgetting.

### 4. What happens to the bounded-but-not-an-experiment case?

A nightly backfill or reconciliation job has real boundaries, and you do want to
know whether last night's went wrong — but you emphatically do not want a
permanently attributable record of each of the last four hundred nights.

Proposal: `App` plus retention (per-run records switched on, `retain` keeping a
short window) rather than a third base class.

The principle either way: a small closed set of named kinds beats a configurable
policy object. A policy object lets you request combinations that are meaningless
or quietly broken — a simulated time axis with shared stream identity is just
runs overwriting each other — and makes every app author responsible for picking
a valid one. Two named kinds, and a third only when a real job cannot be
expressed as either.

**To decide:** check this against a real backfill before the base classes are
fixed.

### 5. How do params schemas version?

Pydantic params are the big usability win and the big compatibility risk in the
same move.

Proposal: `r.params` returns the model when the stored values still validate, and
a plain dict plus a drift warning when they do not. Never raise on read — an old
run you can no longer parse is still a run that happened.

**To decide:** whether "never raise on read" is the right call, and whether the
stored JSON schema is per app version or content-addressed.

---

## Implementation split

Roughly half is a client-side wrapper that could be prototyped against the
running server now; the other half needs server work.

| Piece | Where | Notes |
| --- | --- | --- |
| `App` / `Experiment` / `Reads` / `Produces` declarations | wrapper | Sits on `register_streams`, `insert_graph`, `resolve_point_metadata`. Today's `App` ABC already is the unbounded half; `Experiment` is the new one. |
| Run handle, atomic commit, per-run naming | wrapper | Buildable on `insert_timeseries_batch` + one graph insert. Atomicity needs a server-side transaction to be real. |
| `realizes=` → unit / observation-location inheritance | wrapper | The graph already carries `qudt:hasUnit` and `s223:hasProperty`; today the app copies them by hand. |
| Ref nodes discoverable by the query layer | fix | `register_streams` should type its own refs. The example works around this. |
| Params stored **and indexed** | server | Today `AppSpec.params` is a JSON literal. Filtering by params needs a column store, plus the JSON schema per app version. |
| Run status, failure capture, attachments | server | No run record type exists; `list_app_runs` tracks only in-actor state. |
| Run records in SQL, provenance in the graph | server | Two stores, one write. |
| Annotations as typed rows | server | Logs are close — point URI, message, observation window — but have no author, tags, or series-span target. |
| `retain` and `keep()` | server | Needs a collector that drops a run's streams and attachments, and a keep flag the reader can set after the fact. |
| Offline buffer and `sync()` | server | Needs an on-disk spool format and idempotent replay. |
