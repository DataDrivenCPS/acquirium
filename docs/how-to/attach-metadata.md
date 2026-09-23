---
title: Attach metadata to a node
---

Any node of the plant model, a piece of equipment or a measurement point,
can carry values of your own: a maintenance date, a manufacturer, a set of
tags, a service contract number.
`insert_metadata()` writes them as a plain `dict`, and from then on they are
attributes like `unit` or `medium`: `where()` filters on them, `include()`
returns them as columns, `options()` and `facets()` count them, and
`acq.attr` completes their names.

This page uses two pumps, `swro:P1` and `swro:P2`, each with one outlet
pressure point.

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
A nested dict becomes dotted attributes, `product_info.manufacturer` and
`product_info.year`; a list keeps its order, `tags.0` and `tags.1`, and is
also addressable as a whole under `tags`.

Keys are letters, digits, underscore and hyphen, starting with a letter or
underscore.
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
['app', 'cp_type', 'data_source', 'enumeration_kind', 'label', 'last_cleaned', 'medium', 'process', 'product_info', 'quantity_kind', 'substance', 'tags', 'type', 'unit']
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

`include("all")` adds every attribute of the node, yours included.
A list gives one row per element, as a node with two media does.
`options()` and `facets()` count values the same way:

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
Keys you do not mention stay as they are:

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

## Link a measurement to its equipment

Two keys write edges of the plant model instead of attributes.
`entity` on a measurement point links it to the equipment it belongs to,
and `measurement` on a piece of equipment does the same from the other
side.
Values are node URIs or CURIEs, one or a list:

```python
acq.insert_metadata("swro:P2-out-pressure", {"entity": "swro:P1"})
acq.query().entity(uri="swro:P1", alias="p1").measurement(alias="m").metadata()
```
```text
shape: (2, 3)
┌─────────┬──────────────────────┬────────────────────┐
│ p1      ┆ m                    ┆ m.label            │
╞═════════╪══════════════════════╪════════════════════╡
│ swro:P1 ┆ swro:P1-out-pressure ┆ P1 outlet pressure │
│ swro:P1 ┆ swro:P2-out-pressure ┆ P2 outlet pressure │
└─────────┴──────────────────────┴────────────────────┘
```

This is useful for a stream that was registered without a place in the
model.
A stream can also be linked at registration: `register_streams()` takes the
same map under a `metadata` key, written together with the stream:

```python
acq.register_streams([{
    "source_id": "scada",
    "ref_name": "FT-101",
    "unit": "gal/min",
    "metadata": {"entity": "swro:P1", "tags": ["scada"]},
}])
```

`upstream` and `downstream` are relations of several steps and cannot be
written this way.

## Built-in attributes

A key that names a built-in attribute, `unit`, `medium`, `label`, `type`,
`process` and the others, is written with that attribute's own predicate,
and free text is resolved to a URI first, as it is in `where()`:

```python
acq.insert_metadata("swro:P2-out-pressure", {"unit": "PSI", "label": "P2 discharge pressure"})
```

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
Metadata lives as long as the node it describes: when a plant reload or an
update removes a node, the server drops the metadata about it in the same
write.
A node a stream introduced survives a plant reload, and so does its
metadata.
Generic traversal (`related(via="any")`) never follows `attr:` predicates,
so annotating a node does not change what its neighbours are.

The [client reference](../reference/client-api.md#metadata-values) lists
every rule, and the [expression table](../reference/client-api.md#attribute-expressions)
every operator.
