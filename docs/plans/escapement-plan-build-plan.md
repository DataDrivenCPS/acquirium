# Escapement build plan: the plan

Written 2026-11-22 for whoever implements it. It covers step 3 of the
[implementation order](data-processing-model.md#implementation-order) in the
design: the plan, and its tables in storage. Steps 1 and 2 were built from
[the first build plan](escapement-build-plan.md) and are on the Escapement
branch `gtf-new-engine`.

The signatures below state intent. None of this code has been compiled.
Change a signature where the compiler or a database requires it, and say what
you changed in your report.

## What you are building

A plan is the complete description of what the engine computes: templates,
instances and bindings. You build the crate that holds plans, checks them and
compares them, and you add the plan to storage, so that an epoch can publish
a new plan in the same transaction as its readings and any past plan can be
read back.

| Crate | Path | Package | Contents |
| --- | --- | --- | --- |
| `plan` | `crates/plan` | `escapement-plan` | Templates, instances, bindings, validation, the DAG, the definition digest, plan comparison, the plan's rows. No I/O. |
| `series` | `crates/series` | `escapement-series` | Gains the plan in the storage interface, the reference store and the contract suite. |
| `store` | `crates/store` | `escapement-store` | Gains the plan tables on DuckDB and PostgreSQL. |

Work in that order. Finish the `plan` crate, with its tests passing, before
changing `series` and `store`.

Not in this step: the request log, activation requests and receipts (step 6),
evaluating anything (steps 4 and 5), the Python bindings (step 6), and
anything in Acquirium. A plan reaches storage here only as part of a
publication that a test or the benchmark makes directly.

## Read first

The design is `docs/plans/data-processing-model.md` in the Acquirium
repository. Read these sections before writing code:

- Vocabulary
- Plans, all of it
- Windows, all of it, for the shape of a role
- Functions: The law, and Calling modes are adapters
- Freshness and failure: Synchronous and asynchronous templates
- Storage: Tables, for the plan tables
- The Acquirium boundary
- Decisions

Then read the code of steps 1 and 2 in `crates/`, especially
`escapement-geometry`'s `Window` and `Role`, and `escapement-series`'s
`Store`, `Changes`, `MemoryStore` and contract suite.

Where the design and this plan disagree, the design decides what the system
means and this plan decides crate layout, names and tests. If they conflict,
or the design is silent on something that changes behaviour, stop and ask
the user. Do not settle it yourself.

## Where things are

- The Escapement repository is `/home/gabe/src/acquirium/escapement`, on
  branch `gtf-new-engine`. It is its own git repository inside the Acquirium
  checkout. The design is at `../docs/plans/` from there.
- The current engine is the root package `escapement-core`, in `src/`. Its
  plan is `Plan` and `Operator` in `src/model.rs`, checked by
  `src/plan.rs`. It is your reference, not your starting point. An operator
  there is an instance here, and its inputs are bindings.

## Ground rules

These are the first build plan's rules, with its dependency list extended.

- **Leave the current engine alone.** Do not edit `src/`, `bindings/`,
  `python/`, `tests/`, `examples/` or `benchmarks/`. Outside `crates/` you
  may edit only the root `Cargo.toml` (to add the workspace member),
  `Cargo.lock`, `.github/workflows/ci.yml` and the `Makefile`.
- **Toolchain.** Rust edition 2024, `rust-version` 1.89, as the workspace
  sets them.
- **Dependencies.** Everything the first build plan allowed, and also, as
  the root package pins them:
  - `serde_json = "1"` in `plan` and `series`, for parameters, annotations
    and the stored definition;
  - `sha2 = "0.10"` in `plan`, for the definition digest;
  - the `json` feature of `duckdb` in `store`, so that DuckDB's plan tables
    hold JSON that SQL can read. Features are shared across the workspace,
    so this adds DuckDB's JSON extension to the root package's build too.

  `plan` uses no `serde` derives: it writes and reads its JSON by hand (see
  [Encoding](#encoding)). Ask before adding anything else.
- **Checks.** These must pass, as in CI:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  - `cargo clippy --all-targets --no-default-features --locked -- -D warnings`
  - the tests of the four crates, in release, with TimescaleDB as before
- **Safety.** `#![forbid(unsafe_code)]` in the new crate.
- **Workspace.** Add `crates/plan` to `members` and `default-members`, with
  `publish = false`, and to the CI step and the Makefile target that run the
  engine crates' tests.
- **Commits.** Commit only when the user asks. No pushes and no pull
  requests unless asked.
- **Honesty.** Report what you ran and what it showed. If something was not
  measured or not tested, say so.

## Milestone 1: `plan`

The crate holds plans and nothing that needs data or a database. It depends
on `geometry` for the window shape.

### Types

```rust
pub use escapement_geometry::{Interval, Time, Window};
pub type Json = serde_json::Value;
pub type Object = serde_json::Map<String, Json>;

/// A complete plan. Order does not matter: two plans with the same
/// templates, instances and bindings are equal.
pub struct Plan {
    pub templates: BTreeMap<String, Template>,
    pub instances: BTreeMap<InstanceId, Instance>,
    pub bindings: BTreeSet<Binding>,
}

pub struct Template {
    pub function: Function,
    pub window: Window,
    /// By role name.
    pub roles: BTreeMap<String, RoleSpec>,
    pub ports: BTreeSet<String>,
    pub freshness: Freshness,
    /// Opaque to the engine. Not part of the definition.
    pub annotations: Json,
}

/// The function's identity, supplied by the caller and opaque to the
/// engine: which code, and its content digest.
pub struct Function {
    pub module: String,
    pub name: String,
    /// The caller's digest of the code, such as a SHA-256 in hex.
    pub digest: String,
    pub mode: Mode,
}
pub enum Mode { PerWindow, Vectorized }
pub enum Freshness { Synchronous, Asynchronous }

/// A role: how a window reads it, and what it reads.
pub struct RoleSpec {
    pub reach: Reach,          // from geometry
    pub driving: bool,
    pub reads: Reads,
}
pub enum Reads {
    /// Streams bound to the role.
    Bound,
    /// The instance's own output on these ports, from before each window.
    OwnPorts(BTreeSet<String>),
}

pub struct InstanceId { pub template: String, pub key: String }

pub struct Instance {
    /// A JSON object the function receives.
    pub parameters: Object,
    /// Each of the template's ports, to the name of the stream it owns.
    pub outputs: BTreeMap<String, String>,
    /// Opaque to the engine. Not part of the definition.
    pub annotations: Json,
}

/// A stream feeds an instance in a role over a span of event time.
pub struct Binding {
    pub instance: InstanceId,
    pub role: String,
    /// A stream name.
    pub stream: String,
    /// `[valid_from, valid_to)`; the unbounded ends mean no limit.
    pub span: Interval,
    /// Free-form text the function receives with this input.
    pub context: String,
}
```

`RoleSpec` converts to geometry's `Role`. With `Reads::OwnPorts` the role's
`Source` is `OwnPorts`. Give `RoleSpec` a constructor for each common case,
with the design's defaults: a `Span` role drives, a `Latest` role does not.

Streams are named, not numbered, everywhere in this crate. Storage maps names
to its integer ids.

### Validation

`Plan::validate(&self) -> Result<Dag>` refuses a plan the design does not
allow, naming what is wrong. It refuses:

- an empty template id, instance key, role name, port name, function module,
  name or digest, or stream name;
- a template with no roles, or no ports;
- a template whose window and roles `Window::validate` refuses: no driving
  role, a driving own-ports role, a `Span` on own ports with an `after` or
  no `before`, and the rest of geometry's rules;
- an own-ports role naming a port the template does not have;
- a `Vectorized` function on a template with an own-ports role, since each
  window's input includes the output of the one before;
- an instance whose template is not in the plan;
- an instance whose `outputs` does not map exactly the template's ports;
- a parameter with an empty name;
- two outputs, of one instance or of two, naming the same stream: each
  derived series has exactly one owning instance;
- a binding whose instance is not in the plan, or whose role is not a
  `Bound` role of the instance's template;
- a binding with an empty span;
- two bindings of the same stream to the same instance and role whose spans
  overlap. Identical rows are one row, because the bindings are a set;
- a cycle among the instances, as below.

An instance with no bindings is allowed. Its driving roles hold nothing, so
it has no windows until a later plan binds it.

A stream may feed several roles of one instance, and several instances.

Before writing this, read `src/plan.rs` in the current engine for the rules
it enforces. Some no longer apply: there are no versions, no native
functions and no cached operators.

### The DAG

An instance *reads* a stream when some binding names it. Instance `B` reads
from instance `A` when `B` reads a stream that `A` outputs. Those edges must
form a DAG. Reading a stream it outputs itself, through a binding, is a cycle
of one; an instance reads its own earlier output only through an own-ports
role. Asynchronous instances are edges like any other: the design treats
their output as a source for evaluation, but a plan with a cycle through one
is still refused.

```rust
pub struct Dag { /* ... */ }
impl Dag {
    /// Every instance, each after every instance it reads from. Ties in
    /// (template, key) order, so the order is deterministic.
    pub fn order(&self) -> &[InstanceId];
    /// The instances that read from `id`.
    pub fn readers(&self, id: &InstanceId) -> &[InstanceId];
    /// The instances `id` reads from.
    pub fn sources(&self, id: &InstanceId) -> &[InstanceId];
    /// The instance and port that output `stream`, if any.
    pub fn owner(&self, stream: &str) -> Option<(&InstanceId, &str)>;
}
```

A refused cycle names the instances on it.

### The definition and its digest

An instance's *definition* is its template's function, window, roles and
ports, plus its own parameters and outputs. Bindings, annotations,
freshness, the template's id and the instance's key are not part of it.

`Plan::digest(&self, id: &InstanceId) -> String` is the lowercase hex
SHA-256 of the canonical encoding of the definition, below. Two instances
with equal definitions have equal digests, whatever order their maps were
built in, and any change to a definitional field changes the digest.

### Encoding

The definition and the stored rows are JSON, written by this crate. Write
one `canonical(&Json) -> String`: objects with their keys sorted by bytes,
no whitespace, strings and numbers as `serde_json` writes them. Do not rely
on `serde_json`'s map order. Its order depends on a cargo feature any crate
in the workspace can turn on.

The window and roles are written as:

```text
window   {"anchors": {"instant": {}}, "body": "point"}
         {"anchors": {"grid": {"origin": 0, "step": 60000000000}}, "body": "bucket"}
role     {"reach": {"span": {"before": 0, "after": 0}}, "driving": true, "reads": "bound"}
         {"reach": {"latest": {"max_age": null}}, "driving": false,
          "reads": {"own_ports": ["total"]}}
function {"module": "tanks", "name": "mean_temperature", "digest": "…", "mode": "per_window"}
```

Times are JSON integers, which `serde_json` holds exactly for every `i64`.
Decoding refuses anything else, naming the field.

### Rows

The plan is stored as rows of three tables, the design's (Storage: Tables):

```rust
pub struct TemplateRow { pub id: String, pub definition: Canonical, pub annotations: Canonical }
pub struct InstanceRow { pub template: String, pub key: String,
                         pub parameters: Canonical, pub outputs: BTreeMap<String, String>,
                         pub annotations: Canonical }
pub struct BindingRow  { /* the fields of Binding, flattened */ }
pub struct Rows { pub templates: BTreeSet<TemplateRow>, pub instances: …, pub bindings: … }

impl Plan {
    pub fn rows(&self) -> Rows;
    pub fn from_rows(rows: &Rows) -> Result<Plan>;
}
```

A template row's `definition` holds its function, window, roles, ports and
freshness. Freshness is stored there but left out of the digest.

`Canonical` is JSON text in canonical form, made from a value or parsed
from text in any form, such as a database returns. Equal values have equal
text, so rows can be compared, ordered and kept in sets.

### Comparison

```rust
pub enum Class { New, Redefined, Rebound, Unchanged, Removed }

pub struct Comparison {
    pub instances: BTreeMap<InstanceId, Class>,
    /// Rows of the active plan that the new one ends.
    pub ended: Rows,
    /// Rows of the new plan that the active one lacks.
    pub added: Rows,
}

pub fn compare(active: &Plan, new: &Plan) -> Comparison;
```

- **New**: the instance is only in the new plan.
- **Removed**: only in the active plan.
- **Redefined**: in both, with different digests.
- **Rebound**: in both, with equal digests and different bindings.
- **Unchanged**: in both, with equal digests and equal bindings. A change
  to annotations or freshness alone leaves an instance unchanged, though its
  rows differ.

`ended` and `added` are the differences between `active.rows()` and
`new.rows()`. A template row is identified by its id, an instance row by its
template and key, and a binding row by all its fields. A row whose identity
is in both plans but whose fields differ is ended and added again.
Activating the same plan again compares to no rows at all.

A rebound instance recomputes only the windows in the spans its bindings
changed. Give the comparison that answer:

```rust
impl Comparison {
    /// For a rebound instance, by role: the event times at which its
    /// bound streams or their contexts differ between the plans.
    pub fn rebound_spans(&self, id: &InstanceId) -> BTreeMap<String, IntervalSet>;
}
```

For each (role, stream, context), it is the symmetric difference of the
spans of the instance's ended rows and of its added rows, and a role's
spans are the union of its pairs'. Not the plain union of those spans: when
probe 2 moves on 1 March, tank A's ended row spans all time, but only times
from 1 March change. The rows the plans share do not matter, because a
valid plan never binds one stream to one role at overlapping times.

### Tests

Unit tests:

- one test for each validation rule above, each with a plan that breaks
  only that rule;
- the design's example plan, `tank_mean` with two tanks and three probes
  (Plans), is valid, and moving probe 2 to tank B on 1 March classifies both
  tanks as rebound, with spans starting on 1 March;
- the `running_total` template (Reading its own output) is valid, and is
  refused when vectorized;
- the setpoint example (Driving roles), with a `Span` role and a `Latest`
  role, is valid.

Translate the plan cases of the current engine's tests, where they still
mean something:

| Current test | What it becomes |
| --- | --- |
| `rejects_cycles_conflicting_outputs_and_ambiguous_context` (`tests/contracts.rs`) | Two ports owning one stream, and an instance bound to its own output, are refused. A stream bound twice to one role at overlapping times is refused. |
| `operator_versions_name_definitions_not_inputs` | New bindings under the same definition are `Rebound`; a changed definition is `Redefined`. There are no versions to refuse. |
| `operator_parameters_are_part_of_its_immutable_definition` | A changed parameter is `Redefined`. |
| `roles_name_input_sets_and_a_stream_may_take_two_roles` | A stream may feed two roles; the same binding listed twice is one row. |
| `plan_rebuild_clears_old_geometry_and_removed_outputs` | An instance left out of the new plan is `Removed`. |
| `operator_without_inputs_can_gain_them_at_next_plan_boundary` | An instance with no bindings is valid, and gaining them is `Rebound`. |
| `plans_reject_sequential_functions_they_cannot_order` (`tests/sequential.rs`) | A vectorized function with an own-ports role is refused. |

Property tests, with `proptest`. Write a generator of valid plans: several
templates of every window kind with `Span` and `Latest` roles, some reading
their own ports; instances with JSON parameters; bindings with bounded and
unbounded spans; chains of instances reading each other's outputs. Then:

| Law | Statement |
| --- | --- |
| Encoding round trip | `from_rows(&p.rows()) == p`, and decoding any row's canonical JSON gives the row back. |
| Digest | Equal definitions give equal digests however their maps were built. Changing any definitional field changes the digest. Changing bindings, annotations or freshness does not. |
| Identity | `compare(p, p)` classes every instance `Unchanged`, with no rows. |
| Classes | The classes cover every instance of either plan once, and each class matches its definition above, checked directly. |
| Rows | Removing `ended` from `p.rows()` and adding `added` gives `q.rows()`. Nothing in `added` is in `p.rows()`. |
| Rebound spans | At a time inside a role's `rebound_spans`, and only there, the role's streams and contexts differ between the plans. Check by sampling times. |
| Order | In `Dag::order`, every instance comes after every instance it reads from. |
| Cycles | Adding a random binding is refused for a cycle exactly when a brute-force search finds one. |

### Done when

- Every test above passes.
- The public items are documented.
- The crate depends only on `geometry`, `serde_json` and `sha2`, apart from
  dev-dependencies.

## Milestone 2: the plan in storage

The plan joins publication. One transaction publishes an epoch's readings
and, when the epoch activates a plan, its plan rows. Every past plan stays
readable by epoch.

### The interface

`series` now depends on `plan`. Change the storage interface:

```rust
pub struct Publication<'a> {
    pub streams: &'a [NewStream],
    pub changes: &'a Changes,
    /// The plan the epoch activates. `None` keeps the active plan.
    pub plan: Option<&'a Plan>,
}

pub trait Store {
    // ...as before, except:
    fn publish(&self, epoch: Epoch, publication: &Publication) -> Result<()>;
    /// The plan active at `epoch`. Every published epoch can be read, even
    /// before the retention point: pruning never removes plan rows.
    fn plan(&self, epoch: Epoch) -> Result<Plan>;
}
```

Update every caller: `view::apply_and_publish`, the contract suite, the
store's tests and the benchmark. Epoch 0 has the empty plan.

The publication carries the whole new plan, not a comparison. The store
compares it with its own active plan, read inside the publishing
transaction, so no caller can hand it a comparison worked out against
another plan. (An earlier draft passed a `Comparison`, and a stale one
could not be detected whenever its ended rows happened to still be
active.) What `publish` does with a plan, after adding the new streams and
before committing:

1. Check the plan: it validates, every output names a derived stream,
   every bound stream exists, and every bound derived stream is an output
   of the plan. Refuse otherwise with `Invalid`, naming the stream.
2. Read the plan active at the base epoch and compare it with the new one.
3. End each row in `ended`: set its `until` to the epoch. Each update must
   touch exactly one row; if one does not, the tables moved under the
   transaction: abort with `Conflict`.
4. Insert the rows in `added`, with `since` set to the epoch. Activating
   the active plan writes no row.

Plan rows name streams by name. Storage keeps their integer ids:
`bindings.stream` is the id, and `instances.outputs` maps each port to an
id. `plan(epoch)` turns them back into names.

The caller supplies the new streams a plan needs, as it does for readings:
the outputs that do not exist yet, as derived streams, and the bound streams
that do not exist yet, as source streams. Write that once, in `series`:

```rust
/// The streams `plan` needs that `known` lacks, numbered from `next`.
/// Refuses an output that names a source stream, and a bound stream that
/// is derived but that no instance of `plan` outputs.
pub fn plan_streams(plan: &Plan, known: &[StreamInfo], next: StreamId)
    -> Result<Vec<NewStream>>;
```

Validation is the plan crate's job. A store checks only what needs its
tables: whether the rows match, and the streams' kinds.

### Tables

On both backends, beside the tables of the first build plan:

```sql
CREATE TABLE templates (id TEXT NOT NULL, definition JSON NOT NULL,
                        annotations JSON NOT NULL,
                        since BIGINT NOT NULL, until BIGINT);
CREATE TABLE instances (template TEXT NOT NULL, key TEXT NOT NULL,
                        parameters JSON NOT NULL, outputs JSON NOT NULL,
                        annotations JSON NOT NULL,
                        since BIGINT NOT NULL, until BIGINT);
CREATE TABLE bindings  (template TEXT NOT NULL, instance TEXT NOT NULL,
                        role TEXT NOT NULL, stream INTEGER NOT NULL,
                        valid_from BIGINT, valid_to BIGINT,
                        context TEXT NOT NULL,
                        since BIGINT NOT NULL, until BIGINT);
```

- `JSON` is DuckDB's JSON type and PostgreSQL's `json`, not `jsonb`: `jsonb`
  rewrites numbers (`1e16` comes back as `10000000000000000`), which would
  change a template's digest. Write the canonical encoding and parse what
  is read back into canonical form. PostgreSQL text columns of the plan
  tables are `COLLATE "C"`, so they sort by bytes as the plan crate does.
- `until` is null while a row is active. The plan as of epoch `E` is the
  rows with `since <= E` whose `until` is null or after `E`.
- A null `valid_from` or `valid_to` is unbounded. Ending a binding row
  matches every column, and nulls must match nulls (`IS NOT DISTINCT FROM`
  on PostgreSQL).
- On TimescaleDB these are ordinary tables, not hypertables.
- `prune` does not touch them.

Raise `LAYOUT` to 2. A store of layout 1 is refused, as any other layout
is. No layout-1 store is in use.

These tables make the design's promise that "which streams feed which
outputs is a SQL join of `bindings`, `instances` and `streams`". Test that
promise with this query, or one like it, on both backends:

```sql
-- PostgreSQL; DuckDB has json_each in place of json_each_text.
SELECT i.template, i.key, s_in.name AS input, b.role, o.key AS port,
       s_out.name AS output
FROM bindings b
JOIN instances i ON i.template = b.template AND i.key = b.instance
JOIN streams s_in ON s_in.id = b.stream
CROSS JOIN LATERAL json_each_text(i.outputs) o
JOIN streams s_out ON s_out.id = o.value::int
WHERE b.until IS NULL AND i.until IS NULL;
```

### The reference store

`MemoryStore` keeps every plan row with its `since` and `until`, in a
`PlanHistory`, and answers `plan(epoch)` from them. It makes the same
checks as the backends, and `plan_history()` gives the rows the tables
test compares with.

### Tests

- **The contract suite gains properties**, run on the reference store,
  DuckDB, PostgreSQL and TimescaleDB as before. Its random histories now
  interleave reading epochs with activations of random valid plans (use the
  generator from Milestone 1) and random edits of the active plan.

  | Property | Check |
  | --- | --- |
  | Plans agree with the reference | At every epoch, `plan(e)` equals the reference's and equals the plan activated at or before `e`. |
  | Activation is idempotent | Activating the active plan again publishes no plan rows: the plan tables are unchanged. |
  | Plans fit the streams | Outputs exist as derived streams. An output naming a source stream is refused, and so are a bound stream that does not exist and a bound derived stream the plan does not output; each refusal changes nothing. |

  Extend the existing properties too. *Publication is atomic* covers plan
  rows, *Reopening* covers `plan`, and *Retention* checks that pruning
  leaves every past plan readable.
- **The tables are the snapshot** extends to the plan tables. After each
  epoch, read the three tables with plain SQL and compare their rows, with
  `since` and `until`, with the reference's. Idempotence is checked here,
  on the tables themselves: after each activation, activate the same plan
  again and check the tables are unchanged.
- **The join promise.** The query above returns, at each epoch, exactly the
  inputs and outputs of the reference's active plan.
- **Stopping around a commit.** The second epoch of the stopping test also
  activates a plan. After reopening, the plan is the old one entirely or the
  new one entirely.
- **Changes tests** are unaffected. Run them.

### Measurements

Add plan activation to the benchmark, on each backend. Use a plan shaped
like Benicia: about 300 instances of 10 templates, 3 bindings each.

| Operation | Report |
| --- | --- |
| Validate and compare, in memory | Time |
| Activate the whole plan into an empty store | Time, rows written |
| Activate the same plan again | Time; rows written must be 0 |
| Rebind one instance | Time, rows written |
| `plan(epoch)` at the latest epoch, and at the first | Time |

There are no design figures to compare with. Report what you find.

### Done when

- The contract suite, with its new properties, passes on the reference
  store, DuckDB, PostgreSQL and TimescaleDB.
- The other tests in this milestone pass.
- The benchmark reports plan activation on all three backends.
- CI runs the four crates' tests.

## Open points

Decide these by measurement or by reading, and say what you found:

- **Indexes on the plan tables.** The tables are small. Add none unless
  `plan(epoch)` or the checks in `publish` are slow at the benchmark's size.
- **DuckDB's JSON type.** Checked on 2026-11-22 with DuckDB 2.0.0 from
  `duckdb =1.20000.2`. JSON is not in DuckDB's core. Without the `json`
  feature, the first JSON query downloads the extension, about 51 MB,
  and fails without a network. With the feature it is linked in, and the
  type, `json_extract_string` and `json_each` work with no download. Keep
  the feature, and add a store test that runs with
  `autoinstall_known_extensions` and `autoload_known_extensions` off, so
  a lost feature fails the tests rather than downloading.

Stop and ask the user about anything else that changes what the system
means, including:

- any plan rule that this plan and the design do not settle;
- adding a dependency not listed above;
- changing a table's columns.

## What to report

At the end of each milestone, write a short report:

- what was built, and which tests pass, with the command that ran them;
- the benchmark table, for Milestone 2;
- every place you departed from this plan, and why;
- what you verified and what you only inferred;
- anything you could not test, and what it would take.

## Not in this plan

The request log, activation requests and receipts, admission, the epoch
driver, the run protocol, Python workers, the dirty rule, maintenance
records, refresh, the query interface, the Python bindings, and anything in
Acquirium. Do not start them.
