---
name: acquirium-querying
description: Write Acquirium queries — find equipment, topology and measurements, and pull their timeseries.
load_when: The task involves reading anything out of an Acquirium server (entities, relationships, measurements, values).
human_doc: ../tutorials/querying.md
---

# Acquirium querying

Entry point: `acq.query()` (`acq.explore()` is an alias). Returns an immutable
`Query`; every verb returns a new one. Nothing executes until a terminal.

## Signatures

```python
entity(cls=None, *, uri=None, alias=None, **attrs)
related(cls=None, *, uri=None, alias=None, frm=None, via="any",
        direction=None, max_depth=None, nearest=None, **attrs)
        # max_depth default: 3 for via="any"/single predicate/direction, 1 for a predicate list
        # nearest default: True only for via="any" with no direction
measurement(*, frm=None, alias=None, direction=None, max_depth=3,
            nearest=False, include_connection_points=True, **attrs)
        # frm: alias | None (pointer) | list of aliases | "*" (one data node per entity)
        # nearest=True requires direction; include_connection_points=False requires no direction

alias(name)                              # add a name for the current node; the old alias keeps working
refocus(alias)                           # move pointer to an existing node
where(target=None, **attrs)              # target=None -> pointer; "*" -> all measurement nodes
include(*names, of=None, required=False) # attr columns; also un-drops a node
drop(*names)                             # hide node; also un-includes an attr; no args -> pointer
with_columns(*specs, of=None, required=False)   # "name" includes, "-name" drops

options(attr_name, *, of=None) -> pl.DataFrame          # runs now; [attr, count]
facets(*, of=None) -> FacetSummary                      # runs now; f["quantity_kind"]
metadata(*, include_internals=False) -> pl.DataFrame    # runs now
# options/facets/metadata/data/dataframe/execute also take include_dependencies=True
# (ontology/shape triples; False = deployment data only, faster, breaks subclass matching)
data(*, start=None, end=None, limit=None, order="asc",
     cast_value="float", value_mode="default") -> DataObject
dataframe(shape="wide", *, start=None, end=None, limit=None, order="asc",
          cast_value="str", value_mode="default",
          include_ref=False, compact=True) -> pl.DataFrame
to_sparql() -> str                       # compile only, no execution
execute() -> {"columns": [...], "rows": [[...]]}
```

Column specs (all three column verbs): `"unit"` (attr of pointer or `of=`),
`"m.unit"` (attr of node `m`), `"m"` (node alias). Attribute names beat
aliases on collision.

Default aliases: `entity("pump")` -> `pump`; a class URI -> its CURIE;
`measurement()` -> `<source alias>_data`; root `acq.query().measurement()`
-> `data`. Derived aliases are uniquified (`pump_2`); explicit ones are not.

`metadata()` columns: one per node (CURIEs), `alias.attr` per `include()`,
and `alias.label` after each measurement node that has an `rdfs:label`
(all-null label columns are dropped). `dataframe(compact=True)` names
auto-aliased data columns by that label when present, else by the point's
compacted local name. `shape="wide"` is `[time, <one column per point>]`;
`shape="narrow"` is `[data_alias, point_id, time, value_numeric, value_text]`.

## Attributes (the one filter vocabulary)

| name | entity | measurement |
|---|---|---|
| `type` | yes | yes |
| `medium` | yes | yes |
| `process` | yes | no |
| `cp_type` | yes | no |
| `substance` | no | yes |
| `quantity_kind` | no | yes |
| `unit` | no | yes |
| `enumeration_kind` | no | yes |
| `data_source` | no | yes (literal, verbatim) |

Usable as inline kwargs on `entity`/`related`/`measurement`, in `where()`, and
as names in `include`/`options`/`facets`. Values: URI, free text (resolved),
list (OR), `Not(v)` (exclude). `from acquirium.Client.explore import Not`.

## Decision rules

- One specific item → `entity(uri="wbs:RO")` (CURIEs expand). A class of
  items → `entity("pump")`.
- Topology (what feeds what) → `related(..., direction="upstream"|"downstream")`.
  Generic proximity → `related(...)` with default `via="any"`.
  Known predicate → `via="has member"`, faster and unambiguous.
- Measurements of the pointer → `measurement()`. Of every entity in the
  pattern → `measurement(frm="*")`. Every stream in the plant →
  `acq.query().measurement()` on an empty query.
- Closest measurement along the flow → `measurement(direction=..., nearest=True)`.
- Need attribute values as columns → `include()`. Need a node for filtering
  but not in output → `drop()`.
- Don't know the vocabulary of this plant → `options(attr)` / `facets()`
  before guessing filter values.
- Metadata only → `metadata()`. Values → `dataframe()` (wide by default) for
  plotting/joining, `data()` when you may not need all of it.

## Hard rules

- NEVER assume free text resolved correctly. Below match score 0.4 it raises
  `Could not resolve ...`; above it returns the closest match, which can be
  wrong, including for `via=` predicates. Verify surprising results with
  `acq.client.resolve(text, kind, top_k=3)`, or pass a URI.
- Free text resolves against ONTOLOGIES ONLY (water, s223, QUDT), never
  instance labels. `entity("P1")` will not find the item named P1; use `uri=`.
- `where()` / `include()` act on the pointer. After `measurement()` the
  pointer is the measurement node. Use `target=` / `of=` / `frm=` to reach
  another node instead of assuming.
- `max_depth=0` means UNBOUNDED, not zero hops. Only pass it deliberately.
- `related()` defaults to nearest-only for `via="any"` with no direction;
  `via="all"`, an explicit `via=`, or `direction=` defaults to all matches.
  Set `nearest=` explicitly when it matters. `nearest=True` does not combine
  with `direction=`; pass `via=UPSTREAM_EQUIPMENT` / `via=DOWNSTREAM_EQUIPMENT`
  (from `acquirium.Client.explore`) with `nearest=True` instead.
- `direction=` only combines with `via="any"`.
- Aliases are unique per query; reusing one explicitly raises.
- A `Query` is immutable. `q.related(...)` returns a new query; assign it.
- The server is one process (`workers` must be 1). Prefer bounded walks and
  `limit=`/`start=` over pulling everything.

## Error → cause

| message | cause |
|---|---|
| `attribute 'X' does not apply to data node 'Y'` | entity-only attr used on a measurement node (pointer moved) |
| `attribute 'X' does not apply to entity node 'Y'` | measurement-only attr used on an entity (also prefixed `include:`/`options:`) |
| `unknown attribute(s) ['X']; known: [...]` | attr name not in the table above |
| `Could not resolve 'X' as <kind>` | free text scored below 0.4 for that kind; check `resolve(..., top_k=3)` or pass a URI |
| `related: could not resolve via predicate 'X'` | same, for `via=` text |
| `where: unknown target alias 'X'` | alias never defined (renaming with `alias()` keeps the old name valid) |
| `at: unknown alias 'X'` | `refocus()` with an unknown alias |
| `unknown column 'X': not an attribute (...) or a node alias (...)` | typo in an include/drop spec |
| `alias 'X' is already used by another node` | explicit alias collision; omit `alias=` for auto-unique |
| `related: provide cls, uri, or attribute filters` | empty verb call (`entity:` likewise) |
| `where: provide at least one attribute filter` | `where()` with no kwargs |
| `related: direction only combines with via='any'` | `direction=` with an explicit `via=` |
| `related: nearest=True with direction is not supported` | use `via=UPSTREAM_EQUIPMENT` etc. |
| `measurement: nearest=True requires direction` | `nearest=True` without `direction=` |
| `via chain needs at least N step(s) but max_depth is M` | predicate-list `via=` longer than `max_depth` |
| `drop(): every node is dropped — nothing left to select` | dropped the last visible node |
| empty frame, no error | wrong resolution, filter on wrong node, out of `max_depth`, or `required=True` |

## Canonical snippets

```python
from acquirium import Acquirium
from acquirium.Client.explore import Not

acq = Acquirium(server_url="localhost", server_port=8000)

# entity + topology + measurements, with attribute columns
(acq.query()
 .entity(uri="wbs:RO", alias="ro").include("process")
 .measurement(alias="m", quantity_kind="mass flow rate")
 .include("unit", required=True)
 .metadata())

# every equipment's points in one result (M+N rows, nulls where a node has none)
acq.query().entity("Equipment").measurement(frm="*").metadata()

# filter an earlier node without moving the pointer
(acq.query().entity("Equipment").measurement(alias="m")
 .where(target="Equipment", process="reverse osmosis")
 .where(quantity_kind="mass flow rate", medium=Not("brine")))

# branch: two different relations from the same source
q = acq.query().entity("pump")
tanks = q.related("tank")
px = q.related("Pressure Exchanger")

# nearest pressure reading downstream of each pump
q.measurement(direction="downstream", nearest=True, quantity_kind="pressure").metadata()

# discover before filtering
acq.query().measurement().options("quantity_kind")

# values
df = q.measurement(quantity_kind="pressure").dataframe(limit=1000)          # wide
df = q.measurement(quantity_kind="pressure").dataframe("narrow", limit=1000)
```

## Anti-patterns

```python
q.related("tank")                    # WRONG: result discarded (immutable)
q = q.related("tank")                # right

q.measurement().where(process="ro")  # WRONG: process is entity-only, pointer is the measurement
q.measurement().where(target="Equipment", process="ro")

.related("pump", max_depth=0)        # WRONG unless unbounded is intended
.related("pump", max_depth=5)

.entity("P1")                        # WRONG: instance label, not a class
.entity(uri="wbs:P1")

.related("tank", direction="upstream", nearest=True)   # WRONG: raises
.related("tank", via=UPSTREAM_EQUIPMENT, nearest=True) # nearest along the flow

acq.client.sparql_query(...)         # avoid hand-written SPARQL for pattern queries;
                                     # to inspect what a query does use q.to_sparql()
```
