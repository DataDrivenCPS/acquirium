# Graframe: a faceted query interface for RDF graphs

Graframe ("graph frame") is a fluent Python API for exploring graph-shaped
metadata — [ASHRAE 223](https://data.ashrae.org/standard223), Brick, the NAWI
water ontology — **without writing SPARQL and without being an expert in the
ontology**. You start from a set of nodes, look at what is reachable, and step
through the graph one relationship at a time. Every step compiles to a
well-defined SPARQL query, so the convenience never costs you correctness.

This document has four parts:

- **[1. Motivation](#1-motivation)** — why the interface is shaped this way.
- **[2. Formalism](#2-formalism)** — the mathematical model underneath, for
  readers who want to know *why* it is correct.
- **[3. Patterns & examples](#3-patterns--examples)** — a cookbook.
- **[4. API reference](#4-api-reference)** — every method, what it does, and how
  it transforms the current selection.

It is written for a computer-science student: comfortable with sets, relations,
and a little relational algebra, but not necessarily with RDF.

---

## 1. Motivation

### The problem

RDF metadata is a labelled directed graph. A model of a water-treatment plant
might say:

```
:P1        a                s223:Pump .
:P1        s223:hasProperty  :P1-power .
:P1-power  qudt:hasQuantityKind  qudtqk:Power .
:S1        s223:observes     :P1-power .
:S1        s223:hasObservationLocation  :P1 .
```

To answer "which sensors measure a power reading?" you would normally write
SPARQL:

```sparql
SELECT DISTINCT ?s WHERE {
  ?s a/rdfs:subClassOf* s223:Sensor .
  ?s s223:observes ?p .
  ?p qudt:hasQuantityKind qudtqk:Power .
}
```

That requires you to know (a) SPARQL, (b) the exact class and predicate URIs, and
(c) the shape of the graph. Domain engineers usually have none of these. Worse,
the *hard part* is not writing the query — it is **discovering** that `observes`
and `hasQuantityKind` are the relevant predicates in the first place.

### The idea

Graframe reframes querying as **navigation**. At every moment you hold a set of
nodes (a *selection*). You can:

1. **Ask what is around you** — `facets()` lists the predicates leaving your
   current nodes and how many nodes each one applies to. This is exploratory:
   you *see* that `observes` exists before you use it.
2. **Move** — `follow("observes")` walks along that edge to the nodes on the
   other end.
3. **Narrow** — `having("observes", is_a="Power")` keeps only the nodes that
   *have* such an edge, without moving.

So the walkthrough above becomes:

```python
g.instances("s223:Sensor").having("observes",
    matching=g.instances("Property").having("hasQuantityKind", value="Power"))
```

or, exploring interactively:

```python
sensors = g.instances("s223:Sensor")
sensors.facets().show()                 # oh, there's an "observes" predicate
props = sensors.follow("observes")       # step to the properties
props.facets().show()                   # ...which have "hasQuantityKind"
```

The design goals, in priority order:

- **Discoverable.** The neighbourhood is always one call away (`facets()`).
- **Composable.** Selections are immutable; every operator returns a new
  selection, so you can branch and reuse freely.
- **Correct.** Everything denotes a SPARQL query you can print (`to_sparql()`).
- **Ergonomic for non-experts.** Natural-language names resolve to URIs in
  *every* slot — class, predicate, *and* object value (`instances("pump")`,
  `having("of substance", value="salt")`); facet rows are actionable
  (`having(f.row(...))`) so you never retype a URI you just saw; and a *profile*
  hides ontology noise and names common multi-hop paths.

---

## 2. Formalism

The whole API is a thin, fluent surface over a small algebra. Understanding the
algebra is the fastest way to understand (and trust) the API.

### 2.1 The graph

Let

- `I` = IRIs, `B` = blank nodes, `L` = literals,
- `T = I ∪ B ∪ L` be the set of **terms**, and `N = I ∪ B` the **nodes**.

An RDF graph is a set of triples

```
G ⊆ N × I × T
```

A triple `(s, p, o)` reads "subject `s` has predicate `p` with object `o`".

### 2.2 The central object: a Selection

The one thing the API manipulates is a **selection**. In the minimal model a
selection is just a set of *focus* nodes

```
F ⊆ T
```

Everything is a function that takes a selection and returns another selection
(`Selection → Selection`) or summarises it (`Selection → Facets`). Because these
compose, a query is a pipeline of such functions.

> **Implementation note.** The real selection is slightly richer than a bare set:
> it is a *bindings table with a cursor* — a relation over named columns plus one
> distinguished "focus" column. In the common case there is exactly one column
> and it behaves exactly like a set of nodes. The extra columns (`mark`) let you
> remember waypoints and correlate; see §2.7. For the semantics below, read `F`
> as the projection onto the focus column.

### 2.3 Steps (edges and property paths)

A **step** is a binary relation on terms:

```
π ⊆ T × T
```

The simplest step is a single predicate `p`, giving the relation
`{(s, o) | (s, p, o) ∈ G}`. The general step is a **property path** — the same
regular-expression-like language SPARQL uses:

| path            | meaning                                  |
|-----------------|------------------------------------------|
| `p`             | one `p` edge                             |
| `π₁ / π₂`       | `π₁` then `π₂` (composition)             |
| `π₁ \| π₂`      | `π₁` or `π₂` (alternation)               |
| `^π`            | `π` reversed (inverse)                   |
| `π+`            | one or more `π` (transitive)             |
| `π*`            | zero or more `π`                         |
| `π?`            | zero or one `π`                          |

A **virtual edge** in the design vocabulary is simply a named property path.
There is no special machinery for it: `downstream = connectedTo+` is a step like
any other.

### 2.4 The two operators

Almost the entire API is two operators, and — this is the elegant part — they
take the *same* two arguments: a step `π` and an **object filter** `φ` (a
predicate on the target term, e.g. "is an IRI", "= 5", "has type C").

**`having` — stay and narrow (a semijoin):**

```
having(F, π, φ)  =  { s ∈ F | ∃ o . (s, o) ∈ π ∧ φ(o) }        ⊆ F
```

"Keep the current nodes that *have* such an edge." The result is a subset of `F`;
you are still looking at the same kind of thing, just fewer of them. This is
**existential** — one matching edge is enough — and therefore never duplicates a
node.

**`follow` — move to the neighbours (an image):**

```
follow(F, π, φ)  =  { o | ∃ s ∈ F . (s, o) ∈ π ∧ φ(o) }
```

"Walk along the edge to the things on the other end." The cursor moves; you are
now looking at a *new* set of nodes.

The symmetry — same `π`, same `φ`; one stays, one moves — is what makes the
surface small enough to learn in one sitting.

### 2.5 Facets: the available moves

Given a selection `F`, a **direction** `d ∈ {out, in}`, and a **key function** `κ`
on edges, a *facet* is a histogram. For each key `k`,

```
support(k)  =  | { s ∈ F | ∃ edge e leaving s with κ(e) = k } |
```

We count **distinct focus nodes**, not edges, because the useful question during
exploration is "*how many of the things I am holding can take this step?*"

Three key functions cover the common cases:

- `κ(s,p,o) = p` — group by predicate;
- `κ(s,p,o) = (p, o)` — group by (predicate, object value);
- `κ(s,p,o) = (p, type(o))` — group by (predicate, class of object). Literal
  objects have no `rdf:type`, so for them `type(o)` is read as the literal's
  datatype instead — otherwise every literal-valued edge (e.g. `s223:hasValue`)
  would vanish from the histogram.

Facets are the formal statement of "what can I query next": they enumerate the
steps available from `F`, ranked by how widely they apply.

### 2.6 Denotational semantics: everything is a SPARQL query

This is the correctness anchor. Every selection **denotes** a SPARQL query, and
the fluent operators are defined so that they build exactly that query:

- a one-column selection compiles to `SELECT DISTINCT ?focus WHERE { … }`;
- `having` adds a `FILTER EXISTS { ?focus <path> ?o . φ(?o) }` — an existential
  test, which is why it cannot multiply rows;
- `follow` adds a triple `?focus <path> ?o .` and rebinds the focus to `?o`;
- a step's property path compiles to SPARQL property-path syntax verbatim.

Because the target is a small, well-understood fragment (conjunctive queries +
property paths + filters), you can always check what a pipeline *means* by
printing `to_sparql()`, and the implementation is testable against handwritten
SPARQL. The fluent API is sugar; the SPARQL is the meaning.

### 2.7 Correlation, waypoints, and the relational view

A bare set of nodes cannot answer "sensors that measure power **and** sit in
room 5", because you must hold the sensor fixed while checking two independent
branches. For that, a selection carries a small **bindings table** — a relation
over named columns — with one column marked as the focus.

- `mark("x")` names the current focus column.
- `to("x")` moves the cursor back to a named column (the bindings are kept).
- `where(fn)` imposes a *correlated existential* constraint: the sub-pipeline
  `fn` runs from the current focus and must be satisfiable, but the cursor does
  not move and no columns are added. Denotes `FILTER EXISTS { … }`.
- `any_of(f₁, …)` is disjunction: `FILTER(EXISTS{…} || EXISTS{…})`.
- `select("x", "y")` projects the marked columns into a table.

This gives a clean rule that keeps the semantics predictable:

> **`having` / `where` / `any_of` never multiply rows** (they are existential
> filters). **`follow` followed by `select` on several columns is the only place a
> join — and hence row multiplication — happens.**

Under the hood this is exactly the conjunctive-query fragment of relational
algebra: `follow` is a projection of a join, `having` is a semijoin, `where` is a
semijoin against a sub-query, `select` is a projection.

### 2.8 Laws

Because the operators denote relational-algebra expressions, they obey laws that
justify both intuition and (future) query optimisation:

- **`having` is decreasing, monotone, idempotent, and commutes.** Narrowing can
  only shrink the set, order does not matter, and repeating a refinement changes
  nothing: `having(having(F,c),c) = having(F,c)`.
- **Composed pivots = composed paths (the virtual-edge law).** With no filter in
  between, `follow(follow(F, π₁), π₂) = follow(F, π₁ / π₂)`. Naming
  `π₁ / π₂` as a virtual edge is therefore just memoising a `follow` chain.
- **Filtering commutes with the image.** `follow(F, π, φ) = σ_φ(follow(F, π, ⊤))`.
- **Monotone in the graph.** If `G ⊆ G'` then every operator's result on `G` is a
  subset of its result on `G'`.

### 2.9 The lattice picture (optional)

Faceted browsing is navigation of a *concept lattice*. A selection is a pair
`(extent, intent)`: the extent is the set of matching nodes `F`; the intent is
the accumulated pipeline of refinements. Refinements conjoin — they commute and
are idempotent — so intents form a meet-semilattice, and a facet enumerates the
atoms you can add to the intent to move down the lattice. This is the same
structure that Formal Concept Analysis studies; you do not need it to *use*
Graframe, but it explains why "narrow, look, narrow again" always terminates in a
well-defined place.

---

## 3. Patterns & examples

Set-up (assumes a running Acquirium server with a graph loaded):

```python
from acquirium import Acquirium
from acquirium.Graframe import Profile, P, like

aq = Acquirium(server_url="localhost", server_port=8000)
g = aq.graph()          # a Graframe "root" bound to the client
```

### 3.1 Explore from a class

```python
sensors = g.instances("s223:Sensor")   # every Sensor (and subclasses)
sensors.count()
sensors.facets().show()                 # what predicates leave a sensor?
```

`facets()` prints a table of predicates with `support` (how many sensors have
the edge) and `edges` (total matches), in **both** directions by default.

### 3.2 Move vs. narrow

```python
# MOVE: hop from sensors to the properties they observe
props = sensors.follow("s223:observes")

# NARROW: keep only sensors that observe *something* (cursor stays on sensors)
observing = sensors.having("s223:observes")

# NARROW with an object filter: sensors observing a Temperature property
temp = sensors.having("s223:observes", is_a="qudtqk:Temperature")
```

Object filters (`φ`) work identically on `having` and `follow`:

| keyword          | meaning                                             |
|------------------|-----------------------------------------------------|
| `value=`         | object equals a URI/CURIE/literal                   |
| `in_=[...]`      | object is one of several values                     |
| `is_a=`          | object has an rdf:type (or one of a list)           |
| `datatype=`      | literal object has a given datatype (e.g. `xsd:double`) |
| `min=`, `max=`   | numeric range on a literal object                   |
| `matching=sel`   | object is a member of another selection (a join)    |
| `direction="in"` | follow the edge backwards (inverse)                 |

**Facet rows are moves.** The whole point of `facets()` is to *show you* the next
step — so a facet row can be handed straight back to `follow` / `having` /
`without` instead of retyping the predicate and object you just saw:

```python
f = sensors.facets(by="pred-obj")          # predicate + object histogram
row = f.row("s223:observes", key="qudtqk:Temperature")  # pick one (or f.row(0))

sensors.having(row)     # narrow to sensors with that exact edge+object
sensors.follow(row)     # ...or move along it
```

A row carries its predicate, its direction (`in`/`out`), and — for `pred-obj` /
`pred-obj-type` facets — its object value or type, so `follow`/`having` apply the
right filter automatically. This closes the explore→act loop: `facets()` to see,
`f.row(...)` to act, no URI ever typed twice. Explicit keyword filters passed
alongside a row still combine (AND) with it.

> **Round-trip caveat for typed-literal objects.** The `pred-obj` key is the
> object *value*, which the server converts to a native Python type before it
> reaches the row. For IRIs and plain strings the round-trip is exact; for a
> numeric literal like `5^^xsd:decimal` the value arrives as `5.0` (float), and
> re-filtering reconstructs `5.0^^xsd:double`, which is not term-equal to the
> original — so `having(row)` can silently match nothing for decimal-valued
> objects. Filter numeric objects with `min=`/`max=`/`datatype=` (or an explicit
> `value=` number) instead of a `pred-obj` row.

### 3.3 Correlated constraints — and which operator to reach for

There are four ways to constrain the current focus; they differ only in *how many
edges* and *how many branches* are involved. The rule of thumb:

| use…       | when…                                                              | denotes            |
|------------|--------------------------------------------------------------------|--------------------|
| `having`   | **one edge, or one linear path**, reaching something of type/value X (`having("p", …)` or `having("p/q/r", …)`) | `FILTER EXISTS` |
| `without`  | the *negation* of such a condition                                 | `FILTER NOT EXISTS`|
| `where`    | conditions a single linear path **can't** express — two or more independent branches that must stay correlated on the same node | `FILTER EXISTS { … }` per branch |
| `any_of`   | a **disjunction** of such branches                                 | `FILTER(EXISTS{…} \|\| …)` |
| `matching=`| the object must lie in **another, pre-built selection** (a set join)| `FILTER EXISTS` against the inlined sub-query |

The key distinction: **`having` takes a `step` (a predicate *or* a property path)
and an object filter**, so any *linear* condition — however many hops — is just
`having("a/b/c", …)`; the filter applies to the far end, and each segment resolves
by name (§3.5). Reach for `where` only when the branch isn't a single linear path:
two or more independent branches that must stay correlated on the same node.

```python
# ONE hop, one condition -> having (not where):
sensors.having("s223:observes", is_a="qudtqk:Temperature")

# TWO independent conditions on the same sensor -> where (holds the focus):
result = (g.instances("s223:Sensor")
    .where(lambda s: s.follow("s223:observes").is_a("qudtqk:Temperature"))
    .where(lambda s: s.follow("s223:hasLocation").is_one_of("bldg:room_5")))

# A LINEAR multi-hop reaching one filter is just a path step -> having, NO lambda:
sensors.having("observes/has quantity kind", value="temperature")
#   ^ segments resolve by name too; the object filter applies to the FAR end of
#     the path. Reach for `where` only when the branch is NOT a single linear
#     path (e.g. two independent branches, below).

# DISJUNCTION -> any_of; NEGATION -> without:
sensors.any_of(
    lambda s: s.follow("observes").is_a("qudtqk:Temperature"),
    lambda s: s.follow("observes").is_a("qudtqk:Pressure"),
)
sensors.without("s223:hasLocation")     # sensors with no location

# JOIN against a pre-built set -> matching=:
rooms = g.instances("s223:DomainSpace").having("s223:hasProperty")
sensors.having("s223:hasLocation", matching=rooms)
```

`where` / `any_of` / `matching` are all existential — like `having`, they *narrow*
and never multiply rows. The only place rows multiply is `follow` + a multi-column
`select` (§3.4).

### 3.4 Build a table with waypoints

```python
table = (g.instances("s223:Sensor").mark("sensor")
    .follow("s223:observes").mark("property")
    .follow("qudt:hasQuantityKind").mark("quantity")
    .to("sensor")
    .follow("s223:hasObservationLocation").mark("location"))

table.select("sensor", "property", "quantity", "location")   # -> polars DataFrame
```

`select` with several columns is the one place a join (and row multiplication)
happens — exactly when you want it.

### 3.5 Property paths (virtual edges)

Any `step` — in `follow`, `having`, or `without` — may be an **inline
property-path string** in SPARQL syntax, so a multi-hop traversal needs no
builder and no lambda:

```python
g.instances("nawi:Pump").follow("s223:connectedTo+").count()          # transitive
g.instances("s223:DomainSpace").having(                                # linear multi-hop
    "s223:hasProperty/qudt:hasQuantityKind", value="qk:Temperature")
```

The supported operators are the SPARQL ones — `/` (sequence), `|` (alternation),
`^` (inverse), `+ * ?` (modifiers), and `()` grouping. A string is treated as a
path only when it contains one of these; a plain predicate or natural-language
name (`"observes"`, `"has property"`) is still a single-predicate step.

**Each segment resolves like any predicate slot** — URI, CURIE, or (when fuzzy is
on) a natural-language name — so names work mid-path too, including multi-word
ones:

```python
g.instances("s223:Sensor").follow("observes/has quantity kind")
g.instances("s223:DomainSpace").having("has property/has quantity kind", value="temperature")
```

When you need to build a path programmatically, `P(...)` and its combinators do
the same thing:

```python
connected = aq.client.expand_uri("s223:connectedTo")
downstream = P(connected).plus()                 # connectedTo, one or more hops
g.instances("nawi:Pump").follow(downstream).count()
```

But you rarely need to — an inline string or a profile-named edge is usually
clearer.

### 3.6 Query by name (fuzzy resolution)

You do not have to know the URIs. **Every slot resolves by the same rule** — the
class in `instances(...)`, the predicate in `follow`/`having`, *and* the object in
`value=` / `in_=`:

1. a full URI (or `rdflib.URIRef`) is used as-is;
2. a CURIE `prefix:local` with a **bound** prefix is expanded;
3. a CURIE with an **unknown** prefix raises a `UserWarning` and falls back to
   fuzzy resolution of the local part (a typo'd prefix degrades gracefully rather
   than failing hard);
4. a colon-less string is treated as natural language and embedding-resolved.

```python
g.instances("pump")                        # class slot   -> nawi:Pump
g.instances("sensor").follow("connected to")  # predicate slot -> s223:connectedTo
props.having("has quantity kind", value="pressure")  # value slot -> qk:Pressure

g.suggest("pump", kind="class")            # preview matches when a term is ambiguous
```

So filtering by an object no longer means knowing its URI — `value="salt"`
resolves just like `instances("pump")` does. Two knobs remain:

- **`like(text, kind=...)`** pins the concept *kind* when a bare word is
  ambiguous (e.g. force `"salt"` to resolve as a substance, not a class):
  ```python
  props.having("of substance", value=like("salt", "substance"))
  ```
- **To force a real literal** (bypassing all resolution), pass a number or an
  explicit `Lit(...)` / `rdflib.Literal` — a bare string is *always* resolved:
  ```python
  from acquirium.Graframe import Lit
  readings.having("watr:value", value=5)             # numeric literal
  things.having("rdfs:label", value=Lit("Pump 1"))   # string literal
  ```

`aq.graph(fuzzy=False)` turns resolution off entirely: only full URIs and bound
CURIEs are accepted (an unknown prefix or a bare word then raises).

### 3.7 Curate the view with a profile

An ontology exposes far more predicates than any task needs. A `Profile` (a)
hides noise from facets, and (b) names virtual edges so you can traverse them by
name.

```python
water = Profile.base().with_(              # base() hides rdf/rdfs/owl/sh noise
    allow=["s223:", "nawi:", "qudt:hasQuantityKind", "qudt:hasUnit"],
    deny=["s223:cnx", "s223:hasConnectionPoint"],       # connection plumbing
    edges={
        "downstream": "s223:connectedTo+",
        "measures":   "s223:hasProperty",
        "quantity":   "s223:hasProperty/qudt:hasQuantityKind",
    },
)
g = aq.graph(profile=water)

g.instances("nawi:Pump").facets().show()   # curated; named edges appear on top
g.nodes("wbs:P1").follow("downstream")       # traverse the named path
```

Named virtual edges surface as extra facet rows (tagged `is_virtual`) so you can
see and traverse them. Profiles shape *discovery only* — a hidden predicate can
still be pivoted explicitly, and `facets(raw=True)` bypasses the bound profile
entirely (explicit `only=`/`hide=` overrides still apply).

### 3.8 From graph to data

When the focus nodes are *data points* (they carry an external reference to a
timeseries), fetch the values:

```python
pressures = g.instances("Property").having("has quantity kind", value=like("pressure","quantity_kind"))
pressures.dataframe(shape="wide")           # one column per point, per-unit
pressures.latest_data()                     # most recent per series

# marks become grouping keys:
(g.instances("nawi:Pump").mark("pump")
   .follow("measures")
   .data().by("pump"))                       # iterate (pump_uri, DataObject)
```

`.data()` returns a `DataObject` (see [data-api.md](data-api.md)); `.dataframe()`
and `.latest_data()` are convenience wrappers. This is the bridge from the
metadata plane (which points?) to the data plane (what values?).

### 3.9 Always inspect the query

```python
print(result.to_sparql())
```

Any selection prints the SPARQL it denotes. Use this to learn the mapping, debug
a surprising result, or hand a query to someone who prefers raw SPARQL.

---

## 4. API reference

Import surface:

```python
from acquirium.Graframe import (
    Graframe, Selection, Reasoning, Profile, Facets, FacetRow,
    Path, P, Lit, Iri, like, Fuzzy, parse_path, to_path,
)
```

Terminology used below: **F** = the current focus set; a "concept slot" accepts a
URI, CURIE, `URIRef`, natural-language name, or `like(...)`; a "term" accepts a
URI/CURIE or a literal.

### 4.1 `Acquirium.graph(...)` → `Graframe`

```python
aq.graph(reasoning=None, profile=None, *, fuzzy=True, min_score=0.5)
```

Creates a Graframe *root* bound to the client. Parameters:

- `reasoning` — a `Reasoning` profile controlling entailments (default: transitive
  subclass, see §4.9).
- `profile` — a `Profile` curating discovery (§4.8).
- `fuzzy` — resolve natural-language names via embeddings (§4.7). `False` requires
  exact URIs/CURIEs.
- `min_score` — confidence threshold for fuzzy resolution.

### 4.2 Seeds: `Graframe → Selection`

These create the initial selection.

| method | result set `F` | notes |
|--------|----------------|-------|
| `instances(cls)` | all instances of `cls` | `cls` is a concept slot; includes subclasses under the default reasoning profile |
| `nodes(*uris)`   | exactly the given nodes | seeds from specific nodes (URI/CURIE/name) |
| `everything()`   | every node that is a subject in the graph | rarely needed; useful as a starting universe |

`Graframe.suggest(text, kind=None, top_k=5)` returns ranked embedding matches
`[{curie, score, kind}, …]` for disambiguation (does not create a selection).

### 4.3 Navigation: `Selection → Selection`

All of these return a **new** selection; the original is unchanged.

**`having(step, *, direction="out", value=None, is_a=None, datatype=None, min=None, max=None, in_=None, matching=None)`**
Existential semijoin. Keeps `s ∈ F` such that some edge `step` from `s` satisfies
the object filter. **`F` shrinks; the cursor does not move; rows never multiply.**
Compiles to `FILTER EXISTS`.

**`follow(step, *, direction="out", value=…, is_a=…, datatype=…, min=…, max=…, in_=…, matching=…)`**
Image. Moves the cursor to the neighbours reached along `step` that satisfy the
object filter. **The cursor moves and a column is added.** Compiles to a joined
triple.

**`without(step, *, direction="out", **filters)`**
Negated existential — keeps `s ∈ F` with **no** matching edge. Compiles to
`FILTER NOT EXISTS`. Accepts the same object-filter keywords (`value=`, `is_a=`,
`datatype=`, `min=`/`max=`, `in_=`, `matching=`) as `having`.

**`where(fn)`**
Correlated existential constraint. `fn` is `Selection → Selection`; it runs from
the current focus and must be satisfiable. **`F` shrinks; cursor unchanged.**

**`any_of(*fns)`**
Disjunction of `where`-style branches. Keeps `s ∈ F` satisfying at least one.
Compiles to `FILTER(EXISTS{…} || …)`.

Shared parameters:

- `step` — a predicate (concept slot), an **inline property-path string**
  (`"a/b"`, `"p+"`, `"^a|b"`; segments resolve by name — §3.5), a `Path`, a list
  of predicates (alternation), a named edge from the profile, `like(...)`, or a
  **`FacetRow`** from `facets().row(...)` (which supplies its own predicate,
  direction, and object filter). A leading `^` (or `~`) on a single predicate
  means inverse.
- `direction` — `"out"` (default) or `"in"` (reverse the step).
- **object filter `φ`** — `value=` / `in_=` (equality/membership; each value is a
  concept slot, so it resolves by name — §4.7), `is_a=` (type, or list),
  `datatype=` (literal datatype; a CURIE/URI expanded without fuzzy, e.g.
  `"xsd:double"` — compiles to `FILTER(DATATYPE(?o) = <…>)`),
  `min=` / `max=` (numeric range), `matching=other_selection` (membership in
  another selection — a join). Pass a number or `Lit(...)` in `value=`/`in_=` to
  force a plain literal instead of resolving it.

When `step` is a `FacetRow`, its direction and (for `pred-obj`/`pred-obj-type`
facets) its object key are applied automatically; any explicit keyword filters
combine with the row's own.

### 4.4 Focus filters: `Selection → Selection`

Constraints on the *current* node rather than on an edge target.

| method | keeps `s ∈ F` such that | compiles to |
|--------|-------------------------|-------------|
| `is_a(cls)`            | `s` has type `cls` (or a subclass) | `FILTER EXISTS { s a/subClassOf* cls }` |
| `is_one_of(*uris)`           | `s` is one of the given nodes      | `VALUES`   |
| `in_range(min=, max=)` | the (literal) `s` is within range  | `FILTER`   |

### 4.5 Waypoints: `Selection → Selection`

| method | effect |
|--------|--------|
| `mark(name)` | label the current focus column `name` (for later `to`/`select`) |
| `to(name)`   | move the cursor back to a previously marked column (bindings kept) |

### 4.6 Introspection: `Selection → Facets`

```python
facets(by="predicate", *, direction="both", limit=50,
       only=None, hide=None, raw=False, virtual=True)
```

Summarises the neighbourhood of `F`.

- `by` — `"predicate"`, `"pred-obj"` (group by object value), or
  `"pred-obj-type"` (group by the object's `rdf:type`, or — for literal objects,
  which have no type — by their datatype, so literal-valued edges like
  `s223:hasValue` are not silently dropped). For `direction="in"` the "object"
  is the incoming neighbour (the node pointing at the focus), so the key is its
  type/datatype — useful for "what kinds of things point at me".
- `direction` — `"out"`, `"in"`, or `"both"`.
- `only=` / `hide=` — per-call allow / deny lists. These are **overrides
  independent of the profile**: they apply even when `raw=True` (which only drops
  the *bound* profile and its virtual edges), so you never choose between seeing
  raw facets and trimming noise.
- `raw=True` — ignore the active profile entirely (no virtual edges, no
  allow/deny from the profile); explicit `only=`/`hide=` still apply.
- `virtual=` — include the profile's named edges as facet rows.

A **`Facets`** object holds `rows: list[FacetRow]` and offers:

- `.show(limit=25)` — pretty-print a table (returns self);
- `.to_polars()` — the facets as a DataFrame;
- `.predicates(direction=None)` — distinct predicates, most-supported first;
- `.row(selector=None, *, key=None, direction=None)` — pick a **single**
  `FacetRow` to feed to `follow`/`having`/`without`. `selector` is an integer
  index into `rows`, or a predicate / virtual-edge name (matched compacted *or*
  full). Disambiguate collisions with `key=` (object value/type) and/or
  `direction=`. Raises `KeyError` if nothing matches, `ValueError` if ambiguous.

A **`FacetRow`** has `direction` (`"out"`/`"in"`/`"virtual"`), `predicate`
(predicate URI, or the virtual-edge name), `support` (distinct focus nodes),
`edges` (total matches), `key` (object value/type/datatype for the
non-`predicate` modes), `key_kind` (`"value"` for `pred-obj`; `"type"` for an
IRI object of a `pred-obj-type` facet; `"datatype"` for a literal object of a
`pred-obj-type` facet; else `None`), and `is_virtual` (True for named-edge rows).
A `"type"`/`"datatype"` row is actionable: handing it to `follow`/`having`
re-applies the filter as an `is_a=` / `datatype=` constraint respectively. A row
is a first-class *move*: pass it to `follow`/`having`/`without` (§4.3), or traverse
a named edge by name with `follow("<name>")`.

### 4.7 Fuzzy resolution

- `like(text, kind=None)` → a `Fuzzy` marker forcing embedding resolution in any
  slot. `kind` pins the concept kind (`"class"`, `"predicate"`, `"quantity_kind"`,
  `"unit"`, `"substance"`) when a bare word would otherwise be ambiguous.
- `Selection.suggest(text, kind=None, top_k=5)` / `Graframe.suggest(...)` — preview
  matches without resolving.

Resolution order for **any** slot value (class, predicate, or object) — the rule
is uniform:

1. `Fuzzy` (from `like`) → embedding;
2. full URI / `URIRef` → itself;
3. `Lit(...)` / `rdflib.Literal` / a non-string (number, bool) → a plain literal,
   never resolved (the escape hatch for real literals in `value=`);
4. bound CURIE `prefix:local` → prefix expansion;
5. **unknown** prefix → a `UserWarning`, then embedding resolution of the local
   part (a typo'd prefix does not fail hard);
6. colon-less string → embedding when fuzzy is on (else raises).

The chosen URI is logged at INFO. `aq.graph(fuzzy=False)` disables steps 5–6:
only full URIs and bound CURIEs are accepted.

### 4.8 `Profile` — curating discovery

```python
Profile(allow=(), deny=(), allow_types=(), deny_types=(), edges={})
Profile.base()                      # hides rdf/rdfs/owl/sh + class/shape objects
profile.with_(allow=…, deny=…, allow_types=…, deny_types=…, edges=…)   # layer on top
```

- `allow` / `deny` — predicate visibility in facets. Entries are exact
  CURIEs/URIs or namespace globs (`"s223:"`). Rule: visible iff
  `(allow empty OR matches allow) AND (matches no deny)`.
- `allow_types` / `deny_types` — same, for object rdf:types in `pred-obj-type`
  facets (drops schema/shape objects).
- `edges` — `{name: path}` named virtual edges, where `path` is a property-path
  string (`"s223:connectedTo+"`), a list of predicates, or a `Path`. Named edges
  are usable in `follow`/`having` by name and surface as facet rows.

Profiles shape **discovery only** — they never prevent an explicit follow/having.

### 4.9 `Reasoning` — entailments

```python
Reasoning(subclass=True, subproperty=False, inverse=False)
```

- `subclass` — treat `rdf:type` as `rdf:type/rdfs:subClassOf*`, so
  `instances(Sensor)` also matches instances of subclasses. On by default.
- `subproperty` / `inverse` — reserved; not yet implemented (setting either
  raises, so behaviour is never silently wrong).

### 4.10 Property paths (`Path`)

Build steps explicitly when you need a path the profile has not named:

- `P(uri)` → an atomic predicate step (pass the **full URI**; combinators do not
  call the server — use `aq.client.expand_uri("curie")` for a CURIE).
- Combinators: `.then(q)` (`/`), `.or_(q)` (`|`), `.inverse()` (`^`), `.plus()`
  (`+`), `.star()` (`*`), `.opt()` (`?`).
- `parse_path("s223:connectedTo+", expand)` / `to_path(value, expand)` — parse a
  path string or coerce a `str`/list/`Path` (used internally for profile edges).

### 4.11 Terminals: `Selection → results`

These execute the query.

| method | returns | notes |
|--------|---------|-------|
| `to_sparql(*columns)` | `str` | the compiled SPARQL; no columns = the focus, else the named marks |
| `count()`             | `int` | `COUNT(DISTINCT focus)` |
| `nodes()`             | `list[str]` | focus node URIs, sorted, deduplicated |
| `frame(*, compact=True)` | polars DataFrame | one column of focus nodes (CURIE-compacted) |
| `select(*columns, compact=True)` | polars DataFrame | project marked columns; **the join / row-multiplication point** |
| `suggest(text, kind=None, top_k=5)` | `list[dict]` | embedding-match preview |

### 4.12 Terminals: the data plane

Valid when the focus nodes carry `ref:hasExternalReference` (i.e. are data
points). Each mark becomes a context column — grouped with `.data().by("<mark>")`
and present (under the **bare mark name**) in the narrow `dataframe` / `metadata`
frames. A mark may not reuse a reserved data-column name (`time`,
`value_numeric`, `value_text`, `data_alias`, `point_uri`, `ref_uri`); doing so
raises at `.data()` time.

| method | returns |
|--------|---------|
| `data(*, start=None, end=None, limit=None, order="asc", cast_value="float", value_mode="default")` | `DataObject` — lazy, alias-driven timeseries access (see [data-api.md](data-api.md)) |
| `dataframe(*, start=…, end=…, limit=…, order="asc", shape="wide", cast_value="float")` | polars DataFrame (`"wide"` = one column per point) |
| `latest_data(*, shape="wide", cast_value="float")` | polars DataFrame of the most recent point per series |

---

## Where the code lives

`src/acquirium/Graframe/`:

- `algebra.py` — terms, property paths, the pattern AST, and SPARQL rendering.
  The denotational core (§2.6).
- `selection.py` — `Graframe` (root/seeds), `Selection` (operators + terminals),
  `Reasoning`.
- `facets.py` — facet computation and the `Facets` / `FacetRow` result types.
- `profile.py` — `Profile` (predicate/type visibility + named edges).
- `resolve.py` — fuzzy term resolution (`like`, `Fuzzy`, `suggest`).
- `data.py` — the bridge from a selection to `DataObject` timeseries.

Tests in `tests/unit/test_graframe.py` are *denotational*: they assert on the
compiled SPARQL, so the formalism above is what is actually checked. A worked
tour is in `notebooks/watertap/watertap-facets.ipynb`.
