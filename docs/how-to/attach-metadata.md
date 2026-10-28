---
title: Attach metadata to a node
---

Any node of the plant model, a piece of equipment or a measurement point,
can carry values of your own: a maintenance date, a manufacturer, a set of
tags, a service contract number.
Edges of the plant model, which equipment a point belongs to for instance,
are not written this way; they come from the model or from
`sparql_update()`.
`insert_metadata()` writes them as a plain `dict`, and from then on they are
attributes like `unit` or `medium`: `where()` filters on them, `include()`
returns them as columns, `options()` and `facets()` count them, and
`acq.attr` completes their names.

This page uses two pumps, `swro:P1` and `swro:P2`, each with one outlet
pressure point.

Run metadata uses these attribute rules. Variable declarations keep semantic
and source fields explicit and store all user JSON under `metadata`, whether
created by hand or through a model adapter. For example, a legacy variable
`category="simulation"` is queried as `metadata.category` after upgrade;
`variable.metadata` contains `{"category": "simulation"}`. Units remain
available as `variable.unit` and the semantic query attribute `unit`.
See [experiment declaration conversion](../reference/experiments.md#declaring-variables).
Create them through the [experiment interface](record-experiments.md), which
creates their nodes and handles their lifecycle.

## Attach values

```python
from datetime import date

acq.insert_metadata("swro:P1", {
    "last_cleaned": date(2024, 3, 12),
    "product_info": {"manufacturer": "Grundfos", "year": 2019},
    "tags": ["critical", "spare-available"],
})
acq.insert_metadata("swro:P2", {
    "product_info": {"manufacturer": "Grundfos", "year": 2015},
    "tags": ["critical"],
})
```
```text
{'message': 'update applied', 'changed': True, 'nodes': 1}
```

The first argument is the node, as a URI or a CURIE the server knows.
The node must already exist in the model; a URI no graph mentions raises
`ValueError`.
Values may be strings, numbers, booleans, dates, datetimes, dicts and lists,
nested to any depth.
A string that is a URI under a namespace the model binds, such as
`"urn:example:tank"` with `@prefix ex: <urn:example:>`, is stored as a link;
any other string is text, a web link included. Wrap a value in
`rdflib.URIRef` to store it as a URI regardless.
A nested dict becomes dotted attributes, `product_info.manufacturer` and
`product_info.year`; a list keeps its order, `tags.0` and `tags.1`, and is
also addressable as a whole under `tags`.
At the top level, an empty list or dict, like `None`, removes an ordinary key.
Nested nulls and empty containers remain in the saved document and are returned
by `metadata_objects()`. They have no scalar attribute projection.
Values keep their type: `metadata()` returns `year` as an `Int64` column
and `last_cleaned` as a `Date` column, because that is how they were
written.

Keys are letters, digits, underscore and hyphen, starting with a letter or
underscore.
For an arbitrary JSON object, use an explicit `metadata` namespace:

```python
acq.insert_metadata("swro:P1", {
    "metadata": {"any.key": [None, {}, [], {"": "arbitrary string keys"}]},
})
```

The shared backend preserves that complete object, including nulls and empty
containers. Valid paths also become queryable attributes under `metadata`;
other keys remain available from `include("metadata").metadata_objects()`.
Keys such as `unit` and `type` inside this namespace are ordinary user values.
The encoding and query projection are written together and replaced together.

Anything else raises before the write:

```python
acq.insert_metadata("swro:P1", {"last cleaned": "x"})
```
```text
ValueError: invalid metadata key 'last cleaned': letters, digits, underscore and hyphen, starting with a letter or underscore
```

## Query them

The new attributes show up in `acq.attr`, which is bound to your server:

```python
dir(acq.attr)
dir(acq.attr.product_info)
```
```text
['app', 'cp_type', 'data_source', 'enumeration_kind', 'id', 'label', 'last_cleaned', 'medium', 'process', 'product_info', 'quantity_kind', 'substance', 'tags', 'type', 'unit']
['manufacturer', 'year']
```

A key is a keyword argument to `where()`, like any built-in attribute.
On a list, the filter holds when any element matches:

```python
pumps = acq.query().entity("pump", alias="pump")
pumps.where(tags="critical").metadata()
```
```text
shape: (2, 1)
┌─────────┐
│ pump    │
╞═════════╡
│ swro:P2 │
│ swro:P1 │
└─────────┘
```

A nested key is not a Python keyword, so it is spelled through `acq.attr`,
which also gives you comparisons and boolean logic:

```python
pumps.where(acq.attr.product_info.year >= 2018).metadata()
```
```text
shape: (1, 1)
┌─────────┐
│ pump    │
╞═════════╡
│ swro:P1 │
└─────────┘
```

```python
pumps.where((acq.attr.product_info.year >= 2018) | ~acq.attr.last_cleaned.exists()).metadata()
```
```text
shape: (2, 1)
┌─────────┐
│ pump    │
╞═════════╡
│ swro:P2 │
│ swro:P1 │
└─────────┘
```

`==`, `!=`, `<`, `<=`, `>`, `>=`, `.is_in([...])` and `.exists()` build a
condition on a path; `&`, `|` and `~` combine conditions, each side in
parentheses.
`acq.attr.tags[0]` addresses one list element.
Numbers compare as numbers: the value was stored typed, and so is the
`2018` in the filter.
A misspelled name fails on the `acq.attr` line with the keys that do exist.

`include()` returns the values as columns, `alias.key` and
`alias.nested.key`, `null` where a node has no value:

```python
pumps.include("product_info.manufacturer", acq.attr.last_cleaned).metadata()
```
```text
shape: (2, 3)
┌─────────┬────────────────────────────────┬───────────────────┐
│ pump    ┆ pump.product_info.manufacturer ┆ pump.last_cleaned │
╞═════════╪════════════════════════════════╪═══════════════════╡
│ swro:P2 ┆ Grundfos                       ┆ null              │
│ swro:P1 ┆ Grundfos                       ┆ 2024-03-12        │
└─────────┴────────────────────────────────┴───────────────────┘
```

A parent key comes back as one struct cell holding every sub-key any
matched node has, `null` where a node lacks one, fields sorted by name.
Nested dicts nest the struct, and a list inside stays a list:

```python
pumps.include("product_info").metadata()
```
```text
shape: (2, 2)
┌──────────────────────────┬──────────────────────────────────┐
│ pump                     ┆ pump.product_info                │
│ str                      ┆ struct[4]                        │
╞══════════════════════════╪══════════════════════════════════╡
│ dpr:backwash-dosing-pump ┆ {null,"Grundfos","DDA 7.5",2019} │
│ dpr:chlorine-pump        ┆ {2015,"ProMinent",null,null}     │
└──────────────────────────┴──────────────────────────────────┘
```

Here the first pump has `manufacturer`, `model` and `year`, the second
`build_year` and `manufacturer`, so the struct has the four fields
`{build_year, manufacturer, model, year}`.
Reach a field with polars, `pl.col("pump.product_info").struct.field("year")`,
or include the leaf next to the struct, `include("product_info", "product_info.year")`.
`where()` filters leaves, not parent keys.

For code that wants plain Python values rather than a frame,
`metadata_objects()` assembles saved user values directly from complete documents
and uses RDF results for matches, relationships, and semantic values. It returns rows as dicts, each node's attributes
in the shape they were saved and holding only what was saved on that node:

```python
pumps.include("product_info", "tags").metadata_objects()
```
```text
[{'pump': {'id': 'urn:dpr#backwash-dosing-pump',
           'product_info': {'manufacturer': 'Grundfos', 'model': 'DDA 7.5', 'year': 2019},
           'tags': ['lab', 'critical', 'spare-available']}},
 {'pump': {'id': 'urn:dpr#chlorine-pump',
           'product_info': {'build_year': 2015, 'manufacturer': 'ProMinent'},
           'tags': ['lab']}},
 ...]
```

The table from `metadata()` gives both pumps the same four `product_info`
fields, with nulls; the objects leave out the fields a pump never had. The
table also makes a list column of a key that is a list on any node, so a
pump that saved `"tags": "lab"` shows `["lab"]` there; the object keeps
`"lab"`. A list of dicts or a nested list comes back as saved, element
positions intact. A URI value stays a URI; only the table compacts to CURIEs.

For a list of objects, `include("items.name")` selects each object's `name`
and preserves list positions, including nulls and empty objects.
`include("items.1.name")` selects the second object's name; the other positions
are null. `include("items").drop("items.name")` removes names from every item,
while `drop("items.1.name")` removes the name only from the second item.

`id` is an attribute like the others, with a fallback: it is the id saved on
the node, else the node itself. The objects always carry it, as the URI.
`include("id")` adds it to the table, as the CURIE; `include("all")` leaves
it out, since the node column is already there. A saved `id` is a literal
kept as written, so `"pump-7"` and `"urn:other:c-1"` both come back as
given. `where(id=...)` takes a saved id, a URI or a CURIE, and matches the
saved id where there is one, the node otherwise:

```python
acq.insert_metadata("urn:example:b", {"id": "pump-7"})
pts = acq.query().entity("urn:example:Point", alias="point")
pts.include("id").metadata()["point.id"].to_list()   # ['ex:a', 'pump-7']
pts.metadata_objects()                               # [{'point': {'id': 'urn:example:a'}}, {'point': {'id': 'pump-7'}}]
pts.where(id="ex:a").metadata()                      # the node ex:a
pts.where(id="pump-7").metadata()                    # the node ex:b
```

## Know the schema before you run

`schema()` returns the polars schema `metadata()` will produce, computed
from what the graph holds for each attribute, without running the query:

```python
pumps.include("tags", "product_info", "year").schema()
```
```text
Schema({'pump': String, 'pump.tags': List(String),
        'pump.product_info': Struct({'build_year': Int64, 'manufacturer': String, 'model': String, 'year': Int64}),
        'pump.year': Object})
```

An `Object` entry tells you a column's values do not agree before you see
them (`year` above is an integer on one pump and a string on another), and
a cast asked for with `type=` shows as the type it produces.
A column no matched node has a value for is still typed, from the values
other nodes hold under that key, so the schema does not change with the
filter.

## When values do not agree

Nothing stops two nodes from holding different kinds of value under one
key: `year` as the integer `2019` here and the string `"2019"` there, or
`product_info` as a dict on one pump and `"Grundfos CR32"` on another.
A polars column has one dtype, so such a column comes back as `Object`,
each cell the Python value exactly as written: `2019`, `"2019"`, a `dict`,
a `str`.
Nothing is dropped or re-spelled, and `metadata().schema` shows `Object`
where it happened.
An `Object` column is for looking and for `to_list()`; polars expressions,
sorting, joins and Parquet do not work on it.

`type=` on `include()` casts the named columns to one dtype instead:

```python
pumps.include("product_info.year", type="int")           # "2019" -> 2019
pumps.include("product_info", type="struct")              # the string becomes null
pumps.include("product_info", type="string")              # the dict becomes JSON text
pumps.include("tags", type="float", strict=False)         # "abc" -> null inside the list
```

The types are `"string"`, `"int"`, `"float"`, `"bool"`, `"date"`,
`"datetime"`, `"struct"` and `"object"`, or the matching Python type or
polars dtype.
List cells are cast element by element.
`strict=True`, the default, raises on a value that does not convert and
names the node it sits on; `strict=False` turns it into `null`, the same
pair polars' `cast` has.

`include("all")` adds every attribute of the node, yours included.
A list comes back as one `List` cell per node, elements in the order you
wrote them; a node that holds a single value under the same key gets a
one-element list, and `tags[0]` or `"tags.0"` picks one element as a plain
column.
`options()` and `facets()` count elements across nodes:

```python
pumps.options("tags")
```
```text
shape: (2, 2)
┌─────────────────┬───────┐
│ tags            ┆ count │
╞═════════════════╪═══════╡
│ critical        ┆ 2     │
│ spare-available ┆ 1     │
└─────────────────┴───────┘
```

```python
pumps.facets()
```
```text
FacetSummary('pump')
  type [matched]: s223:Pump (2)
  process: (no values)
  cp_type: (no values)
  medium: (no values)
  label [matched]: Feed pump 1 (1), Feed pump 2 (1)
  product_info.manufacturer [matched]: Grundfos (2)
  tags [matched]: critical (2), spare-available (1)
  product_info.year [matched]: 2015 (1), 2019 (1)
  last_cleaned [matched]: 2024-03-12 (1)
```

Note that a `Query` caches what it has fetched.
After a write, build a new query to see the change.

## Change and remove values

Writing a key replaces the node's previous value for it, including every
leaf under a nested key.
Keys you do not mention stay as they are.

For user fields saved by another source, an explicit metadata edit takes
precedence in filters, query tables, and object reads. The query view replaces
the whole field, so old nested leaves do not remain visible. The source's
original assertions stay intact; removing the edit reveals them again.
Semantic fields such as `unit` retain their additive source semantics described
below.

For example:

```python
acq.insert_metadata("swro:P1", {
    "product_info": {"manufacturer": "Grundfos", "year": 2019, "model": "CR 32"},
})
acq.query().entity("pump", alias="pump").include("product_info.model").metadata()
```
```text
shape: (2, 2)
┌─────────┬─────────────────────────┐
│ pump    ┆ pump.product_info.model │
╞═════════╪═════════════════════════╡
│ swro:P2 ┆ null                    │
│ swro:P1 ┆ CR 32                   │
└─────────┴─────────────────────────┘
```

`None` removes a key:

```python
acq.insert_metadata("swro:P1", {"tags": None})
acq.query().entity("pump", alias="pump").options("tags")
```
```text
shape: (1, 2)
┌──────────┬───────┐
│ tags     ┆ count │
╞══════════╪═══════╡
│ critical ┆ 1     │
└──────────┴───────┘
```

## Write to every node a query matches

`Query.insert_metadata()` runs the pattern and writes the same map on every
node matched at the current pointer, or at `of=alias`:

```python
(acq.query().entity("pump", alias="pump")
 .where(acq.attr.product_info.manufacturer == "Grundfos")
 .insert_metadata({"service_contract": "SC-2026-014"}))
```
```text
{'message': 'update applied', 'changed': True, 'nodes': 2}
```

This is the form for bulk annotation: match the nodes with the query
vocabulary you already use, then tag them in one update.

## Annotate a stream at registration

`register_streams()` takes the same map under a `metadata` key and writes it
on the point right after the stream, with the same update as
`insert_metadata()`. Registering again with a changed map replaces each key
it names, so a driver that registers its streams on every start keeps one
value per key:

```python
acq.register_streams([{
    "source_id": "scada",
    "ref_name": "FT-101",
    "unit": "gal/min",
    "metadata": {"tags": ["scada"], "loop": "FIC-101"},
}])
```

## Built-in attributes

A key that names a built-in attribute, `unit`, `medium`, `label`, `type`,
`process` and the others, is written with that attribute's own predicate,
and free text is resolved to a URI first, as it is in `where()`:

```python
acq.insert_metadata("swro:P2-out-pressure", {"unit": "PSI", "label": "P2 discharge pressure"})
```

The text you wrote is kept next to the resolved URI, so you can see what a
value was matched from:

```python
acq.query().measurement(alias="m").include("unit", "unit.raw").metadata()
```
```text
┌──────────────────────┬──────────┬────────────┐
│ m                    ┆ m.unit   ┆ m.unit.raw │
╞══════════════════════╪══════════╪════════════╡
│ swro:P2-out-pressure ┆ unit:PSI ┆ PSI        │
└──────────────────────┴──────────┴────────────┘
```

`register_streams()` does the same for `unit`, `quantity_kind`, `medium`
and `substance`, and keeps the text even when it did not resolve, so a
failed match stays visible. A value given as a URI has no raw text, and
rewriting the key replaces both. In `metadata_objects()` the text sits
next to the value as `"unit.raw"`.

Be aware that the write lands in its own graph and cannot retract a value the
plant model asserts.
Writing a `unit` on a point that already has one from the model leaves the
point with two units; a later shape validation reports it.
`register_streams()` checks such conflicts for the fields it takes at top
level and raises.

## What the server stores

Every value is a triple on the node itself, in the reserved `metadata`
source graph.
A user key becomes a predicate under `urn:acquirium:attr#`, prefix `attr:`,
named by its dotted path, so the values above are:

```turtle
swro:P1 attr:last_cleaned "2024-03-12"^^xsd:date ;
        attr:product_info.manufacturer "Grundfos" ;
        attr:product_info.year 2019 ;
        attr:product_info.model "CR 32" .
```

The graph is an ordinary source graph.
`insert_graph(..., source_id="metadata", replace=True)` clears it, and
`sparql_update(..., source_id="metadata")` edits it directly.
Metadata is not removed when the node it describes disappears from the
plant or a source graph: the model keeps every graph's past, so a write to
one graph never edits another. Experiment records are the exception:
deleting a study, run or annotation also deletes the metadata of its nodes,
from then on.
Generic traversal (`related(via="any")`) never follows `attr:` predicates,
so annotating a node does not change what its neighbours are.

The [client reference](../reference/client-api.md#metadata-values) lists
every rule, and the [expression table](../reference/client-api.md#attribute-expressions)
every operator.
