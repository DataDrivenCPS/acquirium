---
title: Building a query
---

These methods start a query and add nodes to it. Each one returns a new `Query` whose current node is the node just added.
The examples use the [example setup](_index.md#example-setup).

| Method | Description |
| --- | --- |
| [`Acquirium.query`](#acquiriumquery) | Create an empty query |
| [`Query.entity`](#queryentity) | Add an entity node |
| [`Query.related`](#queryrelated) | Add an entity related to an existing node |
| [`Query.measurement`](#querymeasurement) | Add a measurement node: properties that have a stream |
| [`Query.property`](#queryproperty) | Add a property node, with or without a stream |
| [`Query.context`](#querycontext) | Add the entity a measurement belongs to |
| [`Query.alias`](#queryalias) | Name the current node |
| [`Query.refocus`](#queryrefocus) | Make another node the current node |

Several parameters are shared by these methods:

- A class (`cls`) is a class URI or free text. Text is matched against the class names the instance knows, and the best match is used.
- A node URI (`uri`) is a full URI (`urn:...`, `http://...`, `https://...`) or a CURIE with a known prefix, such as `"wbs:P1"`.
- Extra keyword arguments (`**attrs`) are attribute filters on the new node. They follow the rules of [`Query.where`](filtering.md#querywhere).
- An alias given with `alias=` must not be used by another node. The name `"all"` is reserved.

## `Acquirium.query`

```python
Acquirium.query()
```

Creates an empty query bound to the instance. `Acquirium.explore()` is an alias of this method.

**Parameters**

None.

**Returns**

`Query`: a query with no nodes.

**Example**

```python
q = acq.query()
q
```

```text
Query (empty)
```

## `Query.entity`

```python
Query.entity(cls=None, *, uri=None, alias=None, **attrs)
```

Adds an entity node. The node matches every instance of the class, or one node when `uri` is given.

**Parameters**

- `cls` (`str | URIRef | None`, default `None`): the class of the entity.
- `uri` (`str | URIRef | None`, default `None`): the URI of one node to match.
- `alias` (`str | None`, default `None`): the name of the node. Without it, the alias is the text given as `cls`. A class URI gives its CURIE. With no `cls`, the alias is the number of the node, such as `"0"`. A default alias that is already taken gets a suffix (`Pump_2`).
- `**attrs`: attribute filters on the new node, such as `label="Booster Pump P2"`.

**Returns**

`Query`: a new query with the entity node added.

**Raises**

- `ValueError`: when none of `cls`, `uri` and attribute filters is given.
- `ValueError`: when the class text or an attribute value cannot be matched, when `uri` has an unknown prefix, when an attribute name is unknown, or when the alias is taken or reserved.

**Example**

```python
acq.query().entity("Pump").metadata()
```

```text
shape: (3, 1)
┌────────────┐
│ Pump       │
│ ---        │
│ str        │
╞════════════╡
│ wbs:intake │
│ wbs:P2     │
│ wbs:P1     │
└────────────┘
```

```python
acq.query().entity("Pump", alias="pump", label="High-pressure Pump P1").metadata()
```

```text
shape: (1, 1)
┌────────┐
│ pump   │
│ ---    │
│ str    │
╞════════╡
│ wbs:P1 │
└────────┘
```

## `Query.related`

```python
Query.related(cls=None, *, uri=None, alias=None, frm=None, via="any",
              direction=None, max_depth=None, nearest=None, **attrs)
```

Adds an entity node that is connected to an existing node. The connection is any chain of links in the model, a chain of given predicates, or the flow direction of the plant.

**Parameters**

- `cls` (`str | URIRef | None`, default `None`): the class of the related entity.
- `uri` (`str | URIRef | None`, default `None`): the URI of one node to match.
- `alias` (`str | None`, default `None`): the name of the new node. The default follows the rules of [`Query.entity`](#queryentity).
- `frm` (`str | None`, default `None`): the alias of the node to start from. The default is the current node.
- `via` (`str | list`, default `"any"`): which links to follow.
  - `"any"` or `"all"`: any predicate except the hidden ones. See [Traversal settings](traversal.md).
  - One predicate as a URI or free text. A leading `"^"` follows the predicate backwards. The step can repeat up to `max_depth` times.
  - A list of predicates. Each is an alternative for one step.
- `direction` (`str | None`, default `None`): `"upstream"` or `"downstream"`. Follows the connections of the plant against or along the flow. It can only be used with `via="any"`.
- `max_depth` (`int | None`, default `None`): the largest number of steps. `0` means no limit. When it is `None`, the limit is 3, or 1 for a list of predicates.
- `nearest` (`bool | None`, default `None`): `True` keeps only the closest matches of each start node. Matches at the same distance are all kept. `False` keeps every match within `max_depth`. When it is `None`, it is `True` for `via="any"` without a direction and `False` otherwise.
- `**attrs`: attribute filters on the new node.

**Returns**

`Query`: a new query with the related node added.

**Raises**

- `ValueError`: when none of `cls`, `uri` and attribute filters is given.
- `ValueError`: when the query has no node to start from, or `frm` is not an alias of the query.
- `ValueError`: when `direction` is not `"upstream"` or `"downstream"`, or is combined with a `via` other than `"any"`.
- `ValueError`: when a class, predicate or attribute value cannot be matched, or the alias is taken or reserved.

**Example**

The nearest pump to `P1`, over any link:

```python
acq.query().entity(uri="wbs:P1", alias="p1").related("Pump", alias="other").metadata()
```

```text
shape: (1, 2)
┌────────┬────────┐
│ p1     ┆ other  │
│ ---    ┆ ---    │
│ str    ┆ str    │
╞════════╪════════╡
│ wbs:P1 ┆ wbs:P2 │
└────────┴────────┘
```

Every tank downstream of `P1`, with no depth limit:

```python
(acq.query().entity(uri="wbs:P1", alias="p1")
    .related("Tank", alias="tank", direction="downstream", max_depth=0)
    .metadata())
```

```text
shape: (2, 2)
┌────────┬────────────────────┐
│ p1     ┆ tank               │
│ ---    ┆ ---                │
│ str    ┆ str                │
╞════════╪════════════════════╡
│ wbs:P1 ┆ wbs:storage-tank-3 │
│ wbs:P1 ┆ wbs:storage-tank-2 │
└────────┴────────────────────┘
```

With `nearest=True` the same query returns only `wbs:storage-tank-2`.

## `Query.measurement`

```python
Query.measurement(*, frm=None, alias=None, direction=None, max_depth=3,
                  nearest=None, include_connection_points=True, uri=None,
                  stream=None, **attrs)
```

Adds a measurement node. It matches properties that have a stream connected, so it is the node time series are read from.
On an empty query it matches every such property in the plant. After an entity it matches the properties of that entity.

**Parameters**

- `frm` (`str | list | None`, default `None`): the entity the properties belong to. An alias, a list of aliases, or `"*"` for every node of the query. A list or `"*"` adds one measurement node per entity. The default is the current node.
- `alias` (`str | None`, default `None`): the name of the measurement node. The defaults are `"data"` on an empty query, `<entity alias>_data` after an entity, and `<entity alias>_<direction>_data` with a direction. When several nodes are added, the second and later ones are named `<alias>_1`, `<alias>_2`.
- `direction` (`str | None`, default `None`): `"upstream"` or `"downstream"`. Searches along the flow instead of on the entity itself. Downstream, the search covers the outlet connection points of the entity, then the connection that follows, then the next entity with its connection points, and so on. Upstream mirrors this from the inlet.
- `max_depth` (`int`, default `3`): with a direction, the number of entities the search may pass. `0` means no limit.
- `nearest` (`bool | None`, default `None`): with a direction, `True` keeps the properties of the first place that has a match, and `False` keeps every match within `max_depth`. When it is `None`, it is `True` with a direction. Without a direction it has no effect.
- `include_connection_points` (`bool`, default `True`): also match properties on the connection points of the entity (inlets and outlets). `False` keeps only the properties on the entity itself. It cannot be `False` with a direction.
- `uri` (`str | URIRef | None`, default `None`): the URI of one property to match.
- `stream` (`str | Stream | list | None`, default `None`): keep only the given streams. A stream URI, a `Stream` from `acq.stream(source_id, stream_name)`, or a list of either. An empty list matches nothing. `ref=` is the deprecated name of this parameter.
- `**attrs`: attribute filters on the measurement node, such as `quantity_kind="pressure"`.

With a direction, the method adds a second node named `<entity alias>_<direction>_entity`. It holds the node each property was found on.

**Returns**

`Query`: a new query with the measurement node added.

**Raises**

- `ValueError`: when `direction` is not `"upstream"` or `"downstream"`.
- `ValueError`: when `direction` is combined with a list in `frm` or with `include_connection_points=False`.
- `ValueError`: when `frm` names an unknown alias, is an empty list, or is `"*"` on an empty query.
- `ValueError`: when `stream` is not a stream URI or a `Stream`.
- `ValueError`: when an attribute is unknown, does not apply to a measurement node, or its value cannot be matched.
- `TypeError`: when both `ref` and `stream` are given.

**Example**

Every pressure property in the plant:

```python
acq.query().measurement(quantity_kind="pressure").metadata()
```

```text
shape: (6, 2)
┌─────────────────────────────────────────────────┬─────────────────────────────────────────────┐
│ data                                            ┆ data.label                                  │
│ ---                                             ┆ ---                                         │
│ str                                             ┆ str                                         │
╞═════════════════════════════════════════════════╪═════════════════════════════════════════════╡
│ wbs:P1-out-pressure-pa                          ┆ P1 out pressure pa                          │
│ wbs:PXR-brine-out-pressure-pa                   ┆ PXR brine out pressure pa                   │
│ wbs:RO-out-retentate-pressure-pa                ┆ RO out retentate pressure pa                │
│ wbs:RO-out-pressure-pa                          ┆ RO out pressure pa                          │
│ wbs:conn-cartridge-filtration-to-S1-pressure-pa ┆ conn cartridge filtration to S1 pressure pa │
│ wbs:RO-in-pressure-pa                           ┆ RO in pressure pa                           │
└─────────────────────────────────────────────────┴─────────────────────────────────────────────┘
```

The properties on each pump itself, without its connection points:

```python
(acq.query().entity("Pump", alias="pump")
    .measurement(include_connection_points=False)
    .metadata())
```

```text
shape: (4, 3)
┌────────┬─────────────────────────┬─────────────────────┐
│ pump   ┆ pump_data               ┆ pump_data.label     │
│ ---    ┆ ---                     ┆ ---                 │
│ str    ┆ str                     ┆ str                 │
╞════════╪═════════════════════════╪═════════════════════╡
│ wbs:P2 ┆ wbs:P2-efficiency       ┆ P2 efficiency       │
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power │
│ wbs:P1 ┆ wbs:P1-efficiency       ┆ P1 efficiency       │
└────────┴─────────────────────────┴─────────────────────┘
```

The nearest temperature downstream of `P1`:

```python
(acq.query().entity(uri="wbs:P1", alias="p1")
    .measurement(direction="downstream", quantity_kind="temperature")
    .metadata())
```

```text
shape: (1, 4)
┌────────┬──────────────────────┬─────────────────────────┬──────────────────────────┐
│ p1     ┆ p1_downstream_entity ┆ p1_downstream_data      ┆ p1_downstream_data.label │
│ ---    ┆ ---                  ┆ ---                     ┆ ---                      │
│ str    ┆ str                  ┆ str                     ┆ str                      │
╞════════╪══════════════════════╪═════════════════════════╪══════════════════════════╡
│ wbs:P1 ┆ wbs:RO               ┆ wbs:RO-in-temperature-k ┆ RO in temperature k      │
└────────┴──────────────────────┴─────────────────────────┴──────────────────────────┘
```

One stream by its `source_id` and `stream_name`:

```python
acq.query().measurement(stream=acq.stream("plant-data", "P1-efficiency")).metadata()
```

```text
shape: (1, 2)
┌───────────────────┬───────────────┐
│ data              ┆ data.label    │
│ ---               ┆ ---           │
│ str               ┆ str           │
╞═══════════════════╪═══════════════╡
│ wbs:P1-efficiency ┆ P1 efficiency │
└───────────────────┴───────────────┘
```

## `Query.property`

```python
Query.property(*, frm=None, alias=None, has_stream=None,
               include_connection_points=True, uri=None, **attrs)
```

Adds a property node. A property is one measured quantity of the plant, whether or not a stream is connected to it.
On an empty query it matches every property. After an entity it matches the properties of that entity.
A property node returns no time series. Use [`Query.measurement`](#querymeasurement) to read data.

**Parameters**

- `frm` (`str | None`, default `None`): the alias of the entity the properties belong to. The default is the current node.
- `alias` (`str | None`, default `None`): the name of the node. The defaults are `"property"` on an empty query and `<entity alias>_property` after an entity.
- `has_stream` (`bool | None`, default `None`): `True` keeps properties with a stream connected, `False` keeps properties without one, `None` keeps both.
- `include_connection_points` (`bool`, default `True`): also match properties on the connection points of the entity. `False` keeps only the properties on the entity itself.
- `uri` (`str | URIRef | None`, default `None`): the URI of one property to match.
- `**attrs`: attribute filters on the property node.

**Returns**

`Query`: a new query with the property node added.

**Raises**

- `ValueError`: when `has_stream` is not `True`, `False` or `None`.
- `ValueError`: when the node to start from is a measurement node, or `frm` is not an alias of the query.
- `ValueError`: when an attribute is unknown or its value cannot be matched, or the alias is taken or reserved.

**Example**

```python
(acq.query().entity("Pump", alias="pump")
    .property(include_connection_points=False, quantity_kind="power")
    .metadata())
```

```text
shape: (2, 2)
┌────────┬─────────────────────────┐
│ pump   ┆ pump_property           │
│ ---    ┆ ---                     │
│ str    ┆ str                     │
╞════════╪═════════════════════════╡
│ wbs:P1 ┆ wbs:P1-mechanical-power │
│ wbs:P2 ┆ wbs:P2-mechanical-power │
└────────┴─────────────────────────┘
```

In the example setup every property has a stream, so `has_stream=False` matches nothing:

```python
acq.query().property(has_stream=False).metadata()
```

```text
shape: (0, 1)
┌──────────┐
│ property │
│ ---      │
│ str      │
╞══════════╡
└──────────┘
```

## `Query.context`

```python
Query.context(cls=None, *, uri=None, alias=None, frm=None, via="entity", **attrs)
```

Adds the entity a measurement belongs to. It is the reverse of [`Query.measurement`](#querymeasurement): it starts from a measurement node and adds an entity node.
A property reached from several entities gives several rows.

**Parameters**

- `cls` (`str | URIRef | None`, default `None`): the class of the entity. Without `cls` and `uri` the node matches whatever the relation reaches.
- `uri` (`str | URIRef | None`, default `None`): the URI of one node to match.
- `alias` (`str | None`, default `None`): the name of the new node. The default is `<measurement alias>_<relation>`, such as `data_entity`. With a predicate in `via` the default is `<measurement alias>_context`.
- `frm` (`str | None`, default `None`): the alias of the measurement node to start from. The default is the current node.
- `via` (`str | list | tuple`, default `"entity"`): the relation to follow. It is always one step.
  - `"entity"`: the entity the property is on, directly or through one of its connection points.
  - `"upstream"`: the entity directly upstream of the property, such as the pump that feeds the pipe a pressure is measured on.
  - `"downstream"`: the entity directly downstream of the property.
  - A name added with [`register_relation`](traversal.md#register_relation).
  - One predicate as a URI or free text (a leading `"^"` follows it backwards), or a list of predicates.
- `**attrs`: attribute filters on the new node.

**Returns**

`Query`: a new query with the entity node added.

**Raises**

- `ValueError`: when the node to start from is not a measurement node.
- `ValueError`: when `via` is not a relation name and cannot be matched to a predicate.
- `ValueError`: when a class or attribute value cannot be matched, or the alias is taken or reserved.

**Example**

```python
acq.query().measurement(quantity_kind="pressure").context("Pump", via="upstream").metadata()
```

```text
shape: (1, 3)
┌────────────────────────┬────────────────────┬───────────────┐
│ data                   ┆ data.label         ┆ data_upstream │
│ ---                    ┆ ---                ┆ ---           │
│ str                    ┆ str                ┆ str           │
╞════════════════════════╪════════════════════╪═══════════════╡
│ wbs:P1-out-pressure-pa ┆ P1 out pressure pa ┆ wbs:P1        │
└────────────────────────┴────────────────────┴───────────────┘
```

## `Query.alias`

```python
Query.alias(name)
```

Names the current node. The earlier alias of the node keeps working as a handle. Tables and summaries show the new name.

**Parameters**

- `name` (`str`): the new alias.

**Returns**

`Query`: a new query with the node renamed.

**Raises**

- `ValueError`: when the query has no nodes, when `name` is used by another node, or when `name` is `"all"`.

**Example**

```python
q = acq.query().entity("Pump").alias("pump")
q
```

```text
Query
  pump: entity, Pump
```

## `Query.refocus`

```python
Query.refocus(alias)
```

Makes an existing node the current node. Use it to add a filter or another node to an earlier part of the query.

**Parameters**

- `alias` (`str`): the alias of the node.

**Returns**

`Query`: a new query with the same nodes and a different current node.

**Raises**

- `ValueError`: when `alias` is not an alias of the query.

**Example**

```python
q = acq.query().entity("Pump", alias="pump").measurement(alias="m")
q.refocus("pump").where(label="High-pressure Pump P1").metadata()
```

```text
shape: (3, 3)
┌────────┬─────────────────────────┬─────────────────────┐
│ pump   ┆ m                       ┆ m.label             │
│ ---    ┆ ---                     ┆ ---                 │
│ str    ┆ str                     ┆ str                 │
╞════════╪═════════════════════════╪═════════════════════╡
│ wbs:P1 ┆ wbs:P1-out-pressure-pa  ┆ P1 out pressure pa  │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power │
│ wbs:P1 ┆ wbs:P1-efficiency       ┆ P1 efficiency       │
└────────┴─────────────────────────┴─────────────────────┘
```
