---
title: 4 - Inserting a Plant Model
---

In this article we'll describe how to insert a plant model, and how to connect streams to it.

This page is also available as a [Python script](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/4_inserting_plant_model.py) and a [Jupyter notebook](https://github.com/DataDrivenCPS/acquirium/blob/main/examples/documentation/4_inserting_plant_model.ipynb).

## The model file

A plant model is an RDF file, normally in Turtle format.
It describes the plant with the ASHRAE 223 ontology and the water ontology built on top of it.

You can insert a plant model file with `insert_graph_file()`:

```python
acq.insert_graph_file("path/to/your/file.ttl", source_id="plant")
```

- `source_id` is required. `"plant"` refers to the plant model of this Acquirium instance.
- `format` is inferred from the file extension (`.ttl`, `.n3`, `.xml`, `.rdf`, `.trix`). Pass `format=` for other extensions.
- `replace` is `True` by default: the model inserted before under the same `source_id` is removed first. With `replace=False`, the file is added to what is already there.

## Inserting a model from text

A model can also be inserted from text, with `insert_graph()`.
Below is a small part of a seawater desalination plant model, which we'll use in the rest of this page.

In this model, the pump `P1` feeds the mixer `M1`, and the mixer feeds the reverse osmosis stage `RO`.
There's a property at the outlet of `P1`, which represents a pressure measurement.

```python
model = """
@prefix owl:    <http://www.w3.org/2002/07/owl#> .
@prefix qudt:   <http://qudt.org/schema/qudt/> .
@prefix qudtqk: <http://qudt.org/vocab/quantitykind/> .
@prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .
@prefix s223:   <http://data.ashrae.org/standard223#> .
@prefix unit:   <http://qudt.org/vocab/unit/> .
@prefix watr:   <https://watermetadata.org/ontology/watr#> .
@prefix wbs:    <urn:swro/> .

wbs: a owl:Ontology ;
    owl:imports <https://watermetadata.org/ontology/watr> .

wbs:P1 a watr:Pump ;
    rdfs:label "High-pressure Pump P1" ;
    s223:hasConnectionPoint wbs:P1-out ;
    s223:connectedTo wbs:M1 .

wbs:M1 a s223:Junction ;
    rdfs:label "Feed Mixer M1" ;
    s223:connectedTo wbs:RO .

wbs:RO a watr:ReverseOsmosisMembrane ;
    rdfs:label "Reverse Osmosis Stage" .

wbs:P1-out a s223:OutletConnectionPoint ;
    s223:hasMedium watr:Water-Seawater ;
    s223:hasProperty wbs:P1-out-pressure-pa .

wbs:P1-out-pressure-pa a s223:QuantifiableObservableProperty ;
    qudt:hasQuantityKind qudtqk:Pressure ;
    qudt:hasUnit unit:PA .
"""

acq.insert_graph(model, source_id="plant")

print(acq.query().entity("Equipment").include("label"))
```
```text
Query
  Equipment: entity, Equipment
2 matches:
┌───────────┬───────────────────────┐
│ Equipment ┆ Equipment.label       │
│ ---       ┆ ---                   │
│ str       ┆ str                   │
╞═══════════╪═══════════════════════╡
│ wbs:P1    ┆ High-pressure Pump P1 │
│ wbs:RO    ┆ Reverse Osmosis Stage │
└───────────┴───────────────────────┘
```

`entity()` selects the parts of the plant by their class.
Note that `Equipment` is a superclass in the ASHRAE 223 and water ontologies, thus the pump and the reverse osmosis stage both show up.
The mixer is a `Junction`, which is not an `Equipment`.

The `owl:imports` line at the top of the model loads the water ontology, which Acquirium needs to know that a `watr:Pump` is an `Equipment`.

`wbs:` is the short form of `urn:swro/`, as declared in the `@prefix` line of the model. So `wbs:P1` and `urn:swro/P1` are the same thing.

## Connecting streams to the plant model

The model is kept in the `.acquirium` folder, so it is still there when you run `aq.init()` again.

To connect a stream to the model, pass the URI of the property as `property_uri` when registering the stream:

```python
from datetime import datetime, timezone

acq.register_streams([{
    "source_id": "scada",
    "stream_name": "PT-101",
    "property_uri": "urn:swro/P1-out-pressure-pa",
}])

acq.insert_timeseries("scada", "PT-101", [
    (datetime(2026, 8, 8, 10, 0, tzinfo=timezone.utc), 5.1e6),
    (datetime(2026, 8, 8, 10, 5, tzinfo=timezone.utc), 5.2e6),
])
```

There are now a few names around this one measurement:

| name | value | where it comes from |
|---|---|---|
| `source_id` | `scada` | you choose it |
| `stream_name` | `PT-101` | you choose it, for example the instrument tag |
| property | `wbs:P1-out-pressure-pa` | the plant model |
| label | `P1 out pressure pa` | made from the name of the property, unless the model or the registration gives one |

The stream now gets its unit and quantity kind from the model.
The values of the stream are not changed, so the data has to be in the unit of the model.

```python
print(acq.query().measurement().where(quantity_kind="pressure").include("unit", "quantity_kind"))
```
```text
Query
  data: measurement, quantity_kind=Pressure
1 match:
┌────────────────────────┬────────────────────┬───────────┬───────────────────────┐
│ data                   ┆ data.label         ┆ data.unit ┆ data.quantity_kind    │
│ ---                    ┆ ---                ┆ ---       ┆ ---                   │
│ str                    ┆ str                ┆ str       ┆ str                   │
╞════════════════════════╪════════════════════╪═══════════╪═══════════════════════╡
│ wbs:P1-out-pressure-pa ┆ P1 out pressure pa ┆ unit:PA   ┆ quantitykind:Pressure │
└────────────────────────┴────────────────────┴───────────┴───────────────────────┘
```

Its data is also reachable through the model.
For instance, the pressure upstream of the mixer:

```python
q = acq.query().entity("Junction").measurement(direction="upstream", quantity_kind="pressure")
q.dataframe()
```
```text
shape: (2, 2)
┌─────────────────────────┬─────────────────────────────────┐
│ time                    ┆ Junction_upstream_data__P1 out… │
│ ---                     ┆ ---                             │
│ datetime[μs, UTC]       ┆ f64                             │
╞═════════════════════════╪═════════════════════════════════╡
│ 2026-08-08 10:00:00 UTC ┆ 5.1e6                           │
│ 2026-08-08 10:05:00 UTC ┆ 5.2e6                           │
└─────────────────────────┴─────────────────────────────────┘
```

```{button-ref} a-full-example
:ref-type: doc
:color: primary
:outline:
:align: right

Next: A Full Example →
```