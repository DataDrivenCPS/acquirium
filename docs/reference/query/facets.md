---
title: Options and facets
---

These methods count the attribute values of the nodes a query matches. They show which values a filter can use before the filter is written.
The examples use the [example setup](_index.md#example-setup).

| Name | Description |
| --- | --- |
| [`Query.options`](#queryoptions) | Values of one attribute on the matched nodes, with counts |
| [`Query.facets`](#queryfacets) | Value counts for every attribute of a node |
| [`FacetSummary`](#facetsummary) | The object `facets()` returns |

## `Query.options`

```python
Query.options(attr_name, *, of=None, include_dependencies=True)
```

Returns the values one attribute takes on the matched nodes, with the number of nodes that have each value. The query runs when the method is called.

**Parameters**

- `attr_name` (`str`): the attribute name, such as `"unit"` or `"product_info.manufacturer"`. A path from [`acq.attr`](filtering.md#acquiriumattr) is also accepted.
- `of` (`str | None`, default `None`): the alias of the node to count on. The default is the current node.
- `include_dependencies` (`bool`, default `True`): also read the ontologies the model imports.

**Returns**

`polars.DataFrame` with two columns: one named after the attribute, and `count`. Rows are sorted by count, largest first. A list attribute is counted per element.

**Raises**

- `ValueError`: when the attribute is unknown or does not apply to a measurement node.
- `ValueError`: when `of` is not an alias of the query, or the query has no nodes.

**Example**

```python
acq.query().measurement().options("quantity_kind")
```

```text
shape: (8, 2)
┌────────────────────────────────┬───────┐
│ quantity_kind                  ┆ count │
│ ---                            ┆ ---   │
│ str                            ┆ i64   │
╞════════════════════════════════╪═══════╡
│ quantitykind:MassFlowRate      ┆ 10    │
│ quantitykind:MassConcentration ┆ 6     │
│ quantitykind:Pressure          ┆ 6     │
│ quantitykind:Efficiency        ┆ 3     │
│ quantitykind:Power             ┆ 2     │
│ quantitykind:Temperature       ┆ 2     │
│ quantitykind:VolumeFlowRate    ┆ 2     │
│ quantitykind:Area              ┆ 1     │
└────────────────────────────────┴───────┘
```

```python
acq.query().entity("Pump", alias="pump").measurement(alias="m").options("unit")
```

```text
shape: (5, 2)
┌────────────────────┬───────┐
│ unit               ┆ count │
│ ---                ┆ ---   │
│ str                ┆ i64   │
╞════════════════════╪═══════╡
│ unit:KiloGM-PER-M3 ┆ 2     │
│ unit:UNITLESS      ┆ 2     │
│ unit:W             ┆ 2     │
│ unit:M3-PER-SEC    ┆ 1     │
│ unit:PA            ┆ 1     │
└────────────────────┴───────┘
```

## `Query.facets`

```python
Query.facets(*, of=None, include_dependencies=True)
```

Counts the values of every attribute of one node, except `id`. On a measurement node it covers the attributes that apply to a measurement.

For each attribute the values come from the first of these scopes that has any:

1. `matched`: the values on the nodes the query matches. This is what [`Query.options`](#queryoptions) returns.
2. `model`: the values used anywhere in the model. The counts are for the whole model.
3. `vocabulary`: for `type`, `medium` and `substance`, up to 200 values from the loaded ontologies. The counts are 0.

**Parameters**

- `of` (`str | None`, default `None`): the alias of the node. The default is the current node.
- `include_dependencies` (`bool`, default `True`): also read the ontologies the model imports.

**Returns**

[`FacetSummary`](#facetsummary).

**Raises**

- `ValueError`: when `of` is not an alias of the query, or the query has no nodes.

**Example**

```python
acq.query().entity("Pump").facets()
```

```text
FacetSummary('Pump')
  type [matched]: watr:Pump (3)
  process [model]: watr:Process-Dosing (3), watr:Process-AdvancedOxidation (1), watr:Process-UVIrradiation (1), watr:Process-Filtration (1), watr:Process-Mixing (1), … +4
  cp_type [matched]: wbs:P1-in (1), wbs:P1-out (1), wbs:P2-in (1), wbs:P2-out (1), wbs:intake-in (1), … +1
  medium [model]: watr:Water-Seawater (56), watr:Water-Freshwater (18), s223:Fluid-Water (10), watr:Water-Brine (6), watr:Fluid-Sludge (1)
  label [matched]: Booster Pump P2 (1), High-pressure Pump P1 (1), Seawater Onshore Intake (1)
  generation: (no values)
  substance [model]: watr:Constituent-DissolvedSolids (7), watr:Constituent-SuspendedSolids (1), watr:WastewaterTreatmentChemical (1), watr:pHAdjuster-Lime (1), watr:Coagulant-FerricChloride (1)
  quantity_kind [model]: quantitykind:Voltage (39), quantitykind:MassFlowRate (10), quantitykind:VolumeFraction (10), quantitykind:Pressure (6), quantitykind:MassConcentration (6), … +8
  unit [model]: unit:V (39), unit:PERCENT (10), unit:KiloGM-PER-SEC (10), unit:PA (6), unit:NUM (3), … +9
  enumeration_kind: (no values)
  data_source: (no values)
  app: (no values)
  is_disconnected: (no values)
  product_info
    .manufacturer <String> [matched]: Grundfos (1), Siemens (1)
    .year <Int64> [matched]: 2012 (1), 2019 (1)
  tags <List(String)> [matched]: booster (1), critical (1), high-pressure (1)
```

Each line shows the attribute, the scope in brackets and the first five values with their counts. Attributes written with `insert_metadata` come last, with their column type.

## `FacetSummary`

```python
FacetSummary(node_alias, frames={}, scopes={}, dtypes={})
```

The result of [`Query.facets`](#queryfacets). It prints as an overview and gives the full table of each attribute. It is created by `facets()`, not by hand.

**Attributes**

- `node_alias` (`str`): the alias of the node the summary describes.
- `frames` (`dict[str, polars.DataFrame]`): one table per attribute, with the columns of [`Query.options`](#queryoptions).
- `scopes` (`dict[str, str]`): the scope each table came from: `"matched"`, `"model"` or `"vocabulary"`. An attribute with no values in any scope has an empty table and the scope `"vocabulary"`.
- `dtypes` (`dict[str, str]`): the column type of each attribute written with `insert_metadata`. Built-in attributes are not listed.

**Methods**

- `summary[attr_name]`: the table of one attribute. Raises `KeyError` for a name that is not in the summary.
- `attr_name in summary`: whether the summary has the attribute.
- `summary.attrs()`: the list of attribute names.

**Example**

```python
f = acq.query().entity("Pump").facets()
f["label"]
```

```text
shape: (3, 2)
┌─────────────────────────┬───────┐
│ label                   ┆ count │
│ ---                     ┆ ---   │
│ str                     ┆ i64   │
╞═════════════════════════╪═══════╡
│ Booster Pump P2         ┆ 1     │
│ High-pressure Pump P1   ┆ 1     │
│ Seawater Onshore Intake ┆ 1     │
└─────────────────────────┴───────┘
```

```python
f.scopes["process"], f.scopes["label"]
```

```text
('model', 'matched')
```

```python
f.dtypes
```

```text
{'product_info.manufacturer': 'String', 'tags': 'List(String)', 'product_info.year': 'Int64'}
```
