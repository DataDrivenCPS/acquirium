---
title: Metadata
---

Metadata is a set of keys and values attached to a node of the graph, such as a pump or a property. `insert_metadata` writes it, and `acq.attr` names the keys in a query. The `resolve` methods match plain text such as `"gal/min"` to the URI of a unit or another ontology term.

| Item | Purpose |
|---|---|
| [`Acquirium.insert_metadata`](#acquiriuminsert_metadata) | Attach metadata to one node |
| [`Acquirium.attr`](#acquiriumattr) | Attribute names for `Query.where` |
| [`Acquirium.resolve_property_metadata`](#acquiriumresolve_property_metadata) | Match the metadata text of one property to URIs |
| [`Acquirium.resolve`](#acquiriumresolve) | Match one text to a URI |
| [`Acquirium.point`](#acquiriumpoint) | Wrap a property URI for an experiment variable |

The examples on this page use the pump and the streams from the [Streams](streams.md) page.

Note that text is matched exactly by default: `"gal/min"` and `"degC"` match because they are known symbols, and a loose description such as `"flow rate"` does not. A server started with `exact_only=False` also matches by similarity. See [`acquirium.init`](starting.md#acquiriuminit).

## `Acquirium.insert_metadata`

```python
Acquirium.insert_metadata(uri, values)
```

Attaches metadata to one node. Writing a key replaces its previous value, including every nested value under it. Keys that are not mentioned stay as they are. The metadata is removed when the node is removed from the graph.

**Parameters**

- `uri` (`str` or `URIRef`): the node, as a full URI or as a short form such as `"ex:pump-1"` whose prefix the graph defines. The node must already be in the graph.
- `values` (`dict`): the keys and values to write.
  - A key is made of letters, digits, underscores and hyphens, and starts with a letter or an underscore.
  - A value is a `str`, `int`, `float`, `bool`, `date`, `datetime` or `URIRef`, or a `dict` or `list` of those. A nested value is addressed with dots: `{"product_info": {"year": 2019}}` gives `product_info.year`, and the first item of a list `tags` is `tags.0`.
  - `None`, an empty `dict` or an empty `list` removes the key.
  - A built-in key, such as `unit`, `quantity_kind`, `medium`, `substance`, `label` or `type`, is written as the graph statement that key stands for. Text given for a built-in key that holds a URI is matched to one, so `{"unit": "gal/min"}` stores the unit's URI.
  - On a property, `unit`, `quantity_kind`, `medium` and `substance` follow the same rule as [`register_streams`](streams.md#acquiriumregister_streams):
    - A property that Acquirium generated for a stream holds one value. The new value replaces the old one, also when `register_streams` wrote it. `None` removes it. The same holds for `label`.
    - On a property of the plant model, a value that the model or a stream registration wrote stays. A different value raises `ValueError`, and nothing is written. A key the property does not have yet is added.
  - A string that is a URI is stored as a link when it starts with a namespace the graph knows. Otherwise it is stored as text. Pass a `URIRef` to store a link in every case.

**Returns**

A `dict`. `nodes` is the number of nodes written.

```python
{'ok': True, 'message': 'update applied', 'changed': True, 'nodes': 1}
```

**Raises**

- `ValueError`: when the node is not in the graph.
- `ValueError`: when `uri` is a short form with an unknown prefix.
- `ValueError`: when a key has an invalid name.
- `ValueError`: when a key names a relation of the plant model, such as `entity`, `measurement`, `upstream` or `downstream`. Relations are written with [`insert_graph`](graph.md#acquiriuminsert_graph).
- `ValueError`: when a built-in key does not apply to this kind of node, or its text matches no URI. For example, `unit` and `quantity_kind` apply to a property and not to a piece of equipment.
- `TypeError`: when a value has an unsupported type.

**Example**

```python
acq.insert_metadata("urn:ex:pump-1", {
    "last_cleaned": "2026-03-12",
    "product_info": {"manufacturer": "Siemens", "year": 2019},
    "tags": ["lab", "critical"],
})
```

```text
{'ok': True, 'message': 'update applied', 'changed': True, 'nodes': 1}
```

A node that is not in the graph:

```python
acq.insert_metadata("urn:ex:pump-9", {"tags": ["lab"]})
```

```text
ValueError: unknown node(s), not in any data graph: ['urn:ex:pump-9']
```

## `Acquirium.attr`

```python
Acquirium.attr
```

The names of the attributes that a query can filter on: the built-in ones and every key written with `insert_metadata`. `acq.attr.unit` names an attribute, and `acq.attr.product_info.year` names a nested key. Comparing an attribute with a value gives a condition for `Query.where`. Conditions combine with `&`, `|` and `~`. See the [query reference](../query/_index.md).

`acq.attr("product_info.year")` gives the same attribute from a string. This is useful for keys that are not valid Python names, such as a key with a hyphen.

In a notebook, `acq.attr.` followed by the Tab key lists the known names. `dir(acq.attr)` returns the same list.

**Raises**

- `AttributeError`: when the name is not a known attribute. The message lists the known names.
- `ValueError`: when a name given as a string is not a valid attribute path.

**Example**

```python
print(acq.attr.product_info.year >= 2015)
print(dir(acq.attr))
acq.attr.colour
```

```text
(attr.product_info.year >= 2015)
['app', 'cp_type', 'data_source', 'enumeration_kind', 'generation', 'id', 'is_disconnected', 'label', 'last_cleaned', 'medium', 'process', 'product_info', 'quantity_kind', 'substance', 'tags', 'type', 'unit']
AttributeError: unknown attribute 'colour'; known: ['app', 'cp_type', 'data_source', ...]
```

## `Acquirium.resolve_property_metadata`

```python
Acquirium.resolve_property_metadata(fields, min_score=0.6)
```

Matches the metadata text of one property to URIs. All the fields are matched together, so a quantity kind helps to choose between two units with the same symbol. [`register_streams`](streams.md#acquiriumregister_streams) does this for the text it receives, so calling this method is only needed to see what a text matches before registering.

`resolve_point_metadata` is an older name for the same method.

**Parameters**

- `fields` (`dict`): maps a field name to its text. The names `unit`, `quantity_kind`, `medium` and `substance` are each matched against their own vocabulary. Any other name is matched against all terms. A value that is `None` or already a URI is returned unchanged.
- `min_score` (`float`, default `0.6`): lowest score, from 0 to 1, a match must have to be returned.

**Returns**

A `dict` with the same keys. Each value is the matched URI as a `str`, or `None` when nothing matched. Note that when the server cannot be reached, every value is `None` and no error is raised.

**Example**

```python
acq.resolve_property_metadata({"unit": "gal/min", "medium": "water"})
```

```text
{'unit': 'http://qudt.org/vocab/unit/GAL_US-PER-MIN', 'medium': 'http://data.ashrae.org/standard223#Fluid-Water'}
```

## `Acquirium.resolve`

```python
Acquirium.resolve(text, *, kind=None, min_score=0.6)
```

Matches one text to a term of the loaded ontologies, such as a unit or a class. It does not search the nodes of the plant model. Use a [query](../query/_index.md) to find those.

**Parameters**

- `text` (`str`): the text to match. A URI is returned unchanged.
- `kind` (`str`, optional): the kind of term to look for: `"unit"`, `"quantity_kind"`, `"substance"`, `"class"`, `"predicate"`, `"process"` or `"role"`. When omitted, all kinds are searched. Give `"unit"` for a unit symbol such as `"degC"`, which is only matched as a unit.
- `min_score` (`float`, default `0.6`): lowest score, from 0 to 1, a match must have.

**Returns**

A `Point`, a small object whose `uri` attribute holds the matched URI. It can be passed where an experiment variable expects a term. See the [experiment reference](../experiments/_index.md).

**Raises**

- `ValueError`: when nothing matches.

**Example**

```python
print(acq.resolve("degC", kind="unit"))
print(acq.resolve("volume flow rate").uri)
acq.resolve("Pump 1")
```

```text
Point(uri='http://qudt.org/vocab/unit/DEG_C')
http://qudt.org/vocab/quantitykind/VolumeFlowRate
ValueError: could not resolve graph resource 'Pump 1'
```

## `Acquirium.point`

```python
Acquirium.point(uri)
```

Wraps the URI of a property in a `Point`, the object an experiment variable takes to refer to a property of the plant. The URI is not checked. See the [experiment reference](../experiments/_index.md).

**Parameters**

- `uri` (`str`): the `property_uri`.

**Returns**

A `Point` whose `uri` attribute holds the URI.

**Example**

```python
acq.point("urn:ex:pump-1-flow")
```

```text
Point(uri='urn:ex:pump-1-flow')
```
