---
title: Filtering
---

`Query.where` keeps the nodes whose attributes meet a condition. A condition is a keyword argument (`unit="Pa"`) or an attribute expression built from `acq.attr` (`acq.attr.product_info.year >= 2015`).
The examples use the [example setup](_index.md#example-setup).

| Name | Description |
| --- | --- |
| [`Query.where`](#querywhere) | Filter a node by its attributes |
| [`Acquirium.attr`](#acquiriumattr) | Start an attribute expression |
| [Comparisons](#comparisons) | `==`, `!=`, `<`, `<=`, `>`, `>=` on an attribute |
| [`is_in`](#is_in) | The attribute has one of several values |
| [`exists`](#exists) | The node has the attribute |
| [`&`, `\|`, `~`](#combining-expressions) | Combine expressions with and, or, not |
| [`Not`](#not) | Negate a keyword filter |
| [Built-in attributes](#built-in-attributes) | The attributes every instance knows |

## `Query.where`

```python
Query.where(target=None, *exprs, **attrs)
```

Filters one node of the query by its attributes. All conditions given in one call must hold. Calling `where` again adds more conditions.

**Parameters**

- `target` (`str | None`, default `None`): the alias of the node to filter. `"*"` filters every measurement node. The default is the current node. An expression may be passed in this position; it then applies to the current node.
- `*exprs`: attribute expressions built from [`acq.attr`](#acquiriumattr).
- `**attrs`: keyword filters, one per attribute name. The value can be:
  - a single value: the node has this value.
  - a list: the node has at least one of the values.
  - [`Not(value)`](#not): the node does not have the value.

How a value is read depends on the attribute. See [Built-in attributes](#built-in-attributes).

- For attributes that hold a concept (`type`, `process`, `cp_type`, `medium`, `substance`, `quantity_kind`, `unit`, `enumeration_kind`), the value is a URI or free text. Text is matched against the names the instance knows, and the best match is used.
- For attributes that hold plain values (`label`, `id`, `data_source`, `app`, `is_disconnected` and every attribute written with `insert_metadata`), the value is compared as given.

A bare `==`, `!=` or `is_in` expression is the same filter as the keyword form. `where(acq.attr.unit == "Pa")` equals `where(unit="Pa")`.

**Returns**

`Query`: a new query with the filter added.

**Raises**

- `ValueError`: when no condition is given.
- `ValueError`: when `target` is not an alias of the query, or is `"*"` and the query has no measurement node.
- `ValueError`: when an attribute name is unknown, or names a group of keys such as `product_info` instead of one key.
- `ValueError`: when an attribute does not apply to a measurement node, such as `process`.
- `ValueError`: when a text value cannot be matched.
- `ValueError`: when `<`, `<=`, `>` or `>=` is used on an attribute that holds a concept.
- `ValueError`: when `type`, `process` or `cp_type` is used with anything other than a bare `==`.
- `TypeError`: when a positional argument after `target` is not an attribute expression.

**Example**

A list of values:

```python
acq.query().measurement().where(quantity_kind=["pressure", "temperature"]).metadata()
```

```text
shape: (8, 2)
┌───────────────────────────────────────────────────┬───────────────────────────────────────────────┐
│ data                                              ┆ data.label                                    │
│ ---                                               ┆ ---                                           │
│ str                                               ┆ str                                           │
╞═══════════════════════════════════════════════════╪═══════════════════════════════════════════════╡
│ wbs:P1-out-pressure-pa                            ┆ P1 out pressure pa                            │
│ wbs:PXR-brine-out-pressure-pa                     ┆ PXR brine out pressure pa                     │
│ wbs:RO-in-temperature-k                           ┆ RO in temperature k                           │
│ wbs:RO-out-retentate-pressure-pa                  ┆ RO out retentate pressure pa                  │
│ wbs:RO-out-pressure-pa                            ┆ RO out pressure pa                            │
│ wbs:conn-cartridge-filtration-to-S1-temperature-k ┆ conn cartridge filtration to S1 temperature k │
│ wbs:conn-cartridge-filtration-to-S1-pressure-pa   ┆ conn cartridge filtration to S1 pressure pa   │
│ wbs:RO-in-pressure-pa                             ┆ RO in pressure pa                             │
└───────────────────────────────────────────────────┴───────────────────────────────────────────────┘
```

A filter on another node of the query:

```python
(acq.query().entity("Pump", alias="pump").measurement(alias="m")
    .where("pump", label="Booster Pump P2")
    .metadata())
```

```text
shape: (2, 3)
┌────────┬─────────────────────────┬─────────────────────┐
│ pump   ┆ m                       ┆ m.label             │
│ ---    ┆ ---                     ┆ ---                 │
│ str    ┆ str                     ┆ str                 │
╞════════╪═════════════════════════╪═════════════════════╡
│ wbs:P2 ┆ wbs:P2-efficiency       ┆ P2 efficiency       │
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power │
└────────┴─────────────────────────┴─────────────────────┘
```

An expression:

```python
acq.query().entity("Pump").where(acq.attr.product_info.year >= 2015).metadata()
```

```text
shape: (1, 1)
┌────────┐
│ Pump   │
│ ---    │
│ str    │
╞════════╡
│ wbs:P1 │
└────────┘
```

## `Acquirium.attr`

```python
Acquirium.attr
```

Starts an attribute expression. Reading a name from it gives an attribute path. A comparison on a path gives an expression for [`Query.where`](#querywhere).
A path can also be passed as a column name to [`Query.include`](columns.md#queryinclude), [`Query.drop`](columns.md#querydrop) and [`Query.options`](facets.md#queryoptions).

A path is written in one of these ways:

- `acq.attr.unit`: an attribute by name.
- `acq.attr.product_info.year`: a key inside a nested value written with `insert_metadata`.
- `acq.attr("product_info.year")`: the same path as a string. Use it for keys that are not valid Python names, such as keys with a hyphen.
- `acq.attr.tags[0]`: one position of a list. `acq.attr.tags` without an index stands for every element.

The names are checked against the built-in attributes and the attributes stored on the instance. `dir(acq.attr)` lists them.

**Returns**

`AttrProxy`. Reading a name from it returns a `Path`.

**Raises**

- `AttributeError`: when the name is not a known attribute or the start of one.
- `ValueError`: when a path given as a string is not made of valid keys separated by dots.
- `TypeError`: when a path given as a string is empty or not a string, or a list index is not a non-negative integer.

**Example**

```python
dir(acq.attr)
```

```text
['app', 'cp_type', 'data_source', 'enumeration_kind', 'generation', 'id', 'is_disconnected', 'label', 'medium', 'process', 'product_info', 'quantity_kind', 'substance', 'tags', 'type', 'unit']
```

```python
acq.attr.product_info.nonsense
```

```text
AttributeError: unknown attribute 'product_info.nonsense'; known under 'product_info': ['manufacturer', 'year']
```

## Comparisons

```python
path == value
path != value
path < value
path <= value
path > value
path >= value
```

Compares an attribute with a value. A node matches when at least one value of the attribute meets the comparison. For a list such as `tags`, `acq.attr.tags == "booster"` matches a node when any element is `"booster"`.
`!=` is the opposite of `==`: the node has no value equal to the given one. A node without the attribute also matches `!=`.

**Parameters**

- `value`: the value to compare with. It cannot be another attribute path.

`<`, `<=`, `>` and `>=` work on attributes that hold plain values. `type`, `process` and `cp_type` accept only `==`.

**Returns**

An expression (`Expr`) for [`Query.where`](#querywhere).

**Raises**

- `TypeError`: when the value is another path or expression.

**Example**

```python
acq.query().entity("Pump").where(acq.attr.label != "Booster Pump P2").metadata()
```

```text
shape: (2, 1)
┌────────────┐
│ Pump       │
│ ---        │
│ str        │
╞════════════╡
│ wbs:intake │
│ wbs:P1     │
└────────────┘
```

## `is_in`

```python
path.is_in(values)
```

The attribute has at least one of the given values.

**Parameters**

- `values` (iterable): the accepted values. `None` entries are ignored.

**Returns**

An expression (`Expr`) for [`Query.where`](#querywhere).

**Raises**

- `ValueError`: when `values` has no entry other than `None`.

**Example**

```python
(acq.query().entity("Pump")
    .where(acq.attr.product_info.year.is_in([2012, 2019]), tags=Not("booster"))
    .metadata())
```

```text
shape: (1, 1)
┌────────┐
│ Pump   │
│ ---    │
│ str    │
╞════════╡
│ wbs:P1 │
└────────┘
```

## `exists`

```python
path.exists()
```

The node has the attribute, with any value. On a group of keys such as `acq.attr.product_info`, the node has at least one key of the group. Put `~` in front to select nodes without the attribute.

**Parameters**

None.

**Returns**

An expression (`Expr`) for [`Query.where`](#querywhere).

**Example**

```python
acq.query().entity("Pump").where(~acq.attr.product_info.exists()).metadata()
```

```text
shape: (1, 1)
┌────────────┐
│ Pump       │
│ ---        │
│ str        │
╞════════════╡
│ wbs:intake │
└────────────┘
```

## Combining expressions

```python
expr & expr
expr | expr
~expr
```

Combines expressions: `&` means and, `|` means or, `~` means not. Put each side in parentheses, because these operators bind more tightly than comparisons.
The Python words `and`, `or` and `not` do not work on expressions and raise `TypeError`.

**Returns**

An expression (`Expr`) for [`Query.where`](#querywhere).

**Raises**

- `TypeError`: when one side is not an expression, or when an expression is used with `and`, `or`, `not` or `if`.

**Example**

```python
(acq.query().entity("Pump")
    .where((acq.attr.product_info.year < 2015) & (acq.attr.tags == "booster"))
    .metadata())
```

```text
shape: (1, 1)
┌────────┐
│ Pump   │
│ ---    │
│ str    │
╞════════╡
│ wbs:P2 │
└────────┘
```

## `Not`

```python
Not(value)
```

Marks a keyword filter as negative: the node does not have the value. A node without the attribute also matches. `value` can be a single value or a list.
`Not` is imported from `acquirium.Client.explore`.

**Parameters**

- `value`: the value, or list of values, the node must not have.

**Returns**

A marker object to pass as a keyword value to [`Query.where`](#querywhere) and to the builder methods.

**Example**

```python
from acquirium.Client.explore import Not

acq.query().entity("Pump").where(tags=Not("booster")).metadata()
```

```text
shape: (2, 1)
┌────────────┐
│ Pump       │
│ ---        │
│ str        │
╞════════════╡
│ wbs:intake │
│ wbs:P1     │
└────────────┘
```

## Built-in attributes

Every instance knows these attributes. Attributes written with `insert_metadata` are added to them; see [metadata](../client/metadata.md).

| Name | Applies to | Value | Meaning |
| --- | --- | --- | --- |
| `type` | entity, measurement | concept | Class of the node. A class also matches its subclasses. |
| `process` | entity | concept | Treatment process the entity performs, such as "ozonation". A process also matches its subclasses. |
| `cp_type` | entity | concept | Class of one of the connection points of the entity, such as "outlet connection point". |
| `medium` | entity, measurement | concept | Medium carried or measured, such as "fluid water" or "air". |
| `substance` | measurement | concept | Substance measured, such as "chlorine". |
| `quantity_kind` | measurement | concept | QUDT quantity kind, such as "pressure" or "volume flow rate". |
| `unit` | measurement | concept | QUDT unit, such as "Pa" or "K". |
| `enumeration_kind` | measurement | concept | Enumeration kind of a state property, such as "on off". |
| `data_source` | measurement | plain value | Origin tag of the data, such as "Lab" or "SCADA". |
| `app` | measurement | plain value | Name of the app that calculated the stream. Absent on streams written by a driver. |
| `is_disconnected` | measurement | plain value | `True` on the property generated for a stream that was registered without a `property_uri`. The attribute is removed when the stream is connected to a plant property. |
| `label` | entity, measurement | plain value | Label of the node. |
| `id` | entity, measurement | plain value | The id saved on the node with `insert_metadata`, or the URI of the node when no id is saved. A filter accepts the saved id, the URI or a CURIE. |

A measurement node accepts only the attributes that apply to a measurement. Using another one, such as `process`, raises `ValueError`.
Entity nodes and property nodes accept every attribute. The column is null where a node does not have it.

`include("all")` leaves out `type`, `cp_type`, `id` and `is_disconnected`, and also `app` and `label` on a measurement node. Include them by name when they are needed.

**Example**

A stream registered without a `property_uri` is found with `is_disconnected`:

```python
acq.register_streams([{"source_id": "lab", "stream_name": "grab-sample-ph"}])

(acq.query().measurement(alias="m")
    .where(is_disconnected=True)
    .include("is_disconnected")
    .metadata())
```

```text
shape: (1, 3)
┌─────────────────────────────────────────────────┬────────────────┬───────────────────┐
│ m                                               ┆ m.label        ┆ m.is_disconnected │
│ ---                                             ┆ ---            ┆ ---               │
│ str                                             ┆ str            ┆ bool              │
╞═════════════════════════════════════════════════╪════════════════╪═══════════════════╡
│ acq:36c1827f-1cc8-5583-ac79-5fae3fdf5ea7__point ┆ grab-sample-ph ┆ true              │
└─────────────────────────────────────────────────┴────────────────┴───────────────────┘
```
