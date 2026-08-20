---
name: acquirium-querying
description: Write Acquirium queries — find equipment, topology and measurements, and pull their timeseries.
load_when: The task involves reading anything out of an Acquirium server (entities, relationships, measurements, values).
human_doc: ../querying.md
---

# Acquirium querying

Entry point: `acq.query()` (`acq.explore()` is an alias). Returns an immutable
`Query`; every verb returns a new one. Nothing executes until a terminal.

## Signatures

```python
entity(cls=None, *, uri=None, alias=None, **attrs)
related(cls=None, *, uri=None, alias=None, frm=None, via="any",
        direction=None, max_depth=None, nearest=None, **attrs)
measurement(*, frm=None, alias=None, direction=None, max_depth=3,
            nearest=False, include_connection_points=True, **attrs)

alias(name)                              # rename current node
refocus(alias)                           # move pointer to an existing node
where(target=None, **attrs)              # target=None -> pointer; "*" -> all measurement nodes
include(*names, of=None, required=False) # attr columns; also un-drops a node
drop(*names)                             # hide node; also un-includes an attr; no args -> pointer
with_columns(*specs, of=None, required=False)   # "name" includes, "-name" drops

options(attr_name, *, of=None) -> pl.DataFrame          # runs now
facets(*, of=None) -> FacetSummary                      # runs now
metadata(*, include_internals=False) -> pl.DataFrame    # runs now
# every terminal also takes include_dependencies=True (ontology/shape triples;
# False = deployment data only, faster, breaks subclass matching)
data(*, start=None, end=None, limit=None, order="asc",
     cast_value="float", value_mode="default") -> DataObject
dataframe(*, start=None, end=None, limit=None, order="asc",
          shape="narrow", cast_value="str", value_mode="default",
          include_ref=False) -> pl.DataFrame
to_sparql() -> str                       # compile only, no execution
execute() -> {"columns": [...], "rows": [[...]]}
```

Column specs (all three column verbs): `"unit"` (attr of pointer),
`"m.unit"` (attr of node `m`), `"m"` (node alias). Attribute names beat
aliases on collision.

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

- One specific item → `entity(uri="wbs:RO")`. A class of items →
  `entity("pump")`.
- Topology (what feeds what) → `related(..., direction="upstream"|"downstream")`.
  Generic proximity → `related(...)` with default `via="any"`.
  Known predicate → `via="has member"`, faster and unambiguous.
- Measurements of the pointer → `measurement()`. Of every entity in the
  pattern → `measurement(frm="*")`. Every stream in the plant →
  `acq.query().measurement()` on an empty query.
- Need attribute values as columns → `include()`. Need a node for filtering
  but not in output → `drop()`.
- Don't know the vocabulary of this plant → `options(attr)` / `facets()`
  before guessing filter values.
- Metadata only → `metadata()`. Values → `dataframe(shape="wide")` for
  plotting/joining, `data()` when you may not need all of it.

## Hard rules

- NEVER assume free text resolved correctly. It never errors; it returns the
  closest match, including for `via=` predicates. Verify surprising results
  with `acq.client.resolve(text, kind, top_k=3)`, or pass a URI.
- Free text resolves against ONTOLOGIES ONLY, never instance labels.
  `entity("P1")` will not find the item named P1; use `uri=`.
- `where()` / `include()` act on the pointer. After `measurement()` the
  pointer is the measurement node. Use `target=` / `of=` / `frm=` to reach
  another node instead of assuming.
- `max_depth=0` means UNBOUNDED, not zero hops. Only pass it deliberately.
- `related()` defaults to nearest-only (`via="any"`, no direction); an
  explicit `via=` or `direction=` defaults to all matches. Set `nearest=`
  explicitly when it matters.
- Aliases are unique per query; reusing one explicitly raises.
- A `Query` is immutable. `q.related(...)` returns a new query; assign it.
- The server is single-process. Prefer bounded walks and `limit=`/`start=`
  over pulling everything.

## Error → cause

| message | cause |
|---|---|
| `attribute 'X' does not apply to data node 'Y'` | entity-only attr used on a measurement node (pointer moved) |
| `include: attribute 'X' does not apply to entity node 'Y'` | measurement-only attr used on an entity |
| `where: unknown target alias 'X'` | alias never defined, or renamed by `alias()` |
| `unknown column 'X': not an attribute (...) or a node alias (...)` | typo in an include/drop spec |
| `alias 'X' is already used by another node` | explicit alias collision; omit `alias=` for auto-unique |
| `related: provide cls, uri, or attribute filters` | empty verb call |
| `where: provide at least one attribute filter` | `where()` with no kwargs |
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

# discover before filtering
acq.query().measurement().options("quantity_kind")

# values
df = q.measurement(quantity_kind="pressure").dataframe(shape="wide", limit=1000)
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

acq.client.sparql_query(...)         # avoid hand-written SPARQL for pattern queries;
                                     # to inspect what a query does use q.to_sparql()
```
