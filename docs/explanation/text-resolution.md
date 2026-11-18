---
title: Text matcher
---

In this article we'll describe how the text matcher works.
The text matcher is the part of the server that turns plain text, such as "mg/L" or "pump", into the standard term that Acquirium stores.
[0 - Fundamentals](../tutorials/fundamentals.md) introduces it, and this page explains what it searches and how it decides.

## Where text is matched

Text is matched in these places:

- The `unit`, `quantity_kind`, `medium` and `substance` keys of `register_streams()`.
- The class in `entity()` and `related()`, and the values of the built-in filters of `where()`, such as `unit` and `quantity_kind`.
- The target unit of `convert_to()`.

Each place looks for one kind of term: a unit, a quantity kind, a substance, a class, a predicate, a process or a role.
The text is only compared with the terms of that kind.
For instance, "condenser" is a piece of equipment as a class, and it is a role as a role.

A value that is already a URI, such as `http://qudt.org/vocab/unit/PA`, is used as it is.

## What is searched

The text matcher searches the terms of the ontologies that come with Acquirium:

- The water ontology and ASHRAE 223 for classes, predicates, substances, processes and roles.
- QUDT for units and quantity kinds.

The list of terms is built when the server starts.

Note that your plant model is not searched.
The text matcher does not know the label "High-pressure Pump P1" of a pump in your model.
In a query, use `where(label=...)` or `uri=` to select a part of the plant by its name.

## Exact matches

By default only exact matches are found.
A text is an exact match when it is one of the names of a term.
Upper and lower case do not matter, with one exception described below.
The names of a term are:

- Its labels in the ontology, such as "Reverse Osmosis Membrane".
- The words of its identifier. The term `watr:ReverseOsmosisMembrane` has the name "reverse osmosis membrane".
- For a term with a name of three or more words, its initials. "VFD" matches `watr:VariableFrequencyDrive`.
- For a unit, its symbol. A symbol can be written in several ways: "m3/h", "m³/h" and "m^3 per h" are the same unit.

`acq.resolve()` shows what a text is matched to:

```python
acq.resolve("m^3 per h", kind="unit")
```
```text
Point(uri='http://qudt.org/vocab/unit/M3-PER-HR')
```

When a text is the name of several terms, the first one is used.
The lower-level client lists all of them:

```python
for match in acq.client.resolve("pump", "class", top_k=3):
    print(match["uri"], round(match["score"], 3), match["match_stage"])
```
```text
https://watermetadata.org/ontology/watr#Pump 1.0 exact
http://data.ashrae.org/standard223#Pump 1.0 exact
```

The water ontology and ASHRAE 223 both have a class named "Pump".
In this case the term of the water ontology comes first.

The symbols of units are the exception for upper and lower case.
"kg" is the kilogram and "kG" is the kilogauss, so the unit with the same case comes first.

### When nothing matches

A text with no match is handled differently at registration and in a query.
At registration, the key is skipped with a warning and the stream is still registered:

```python
acq.register_streams([{"source_id": "scada", "stream_name": "speed", "unit": "rpm"}])
```
```text
UserWarning: Could not resolve 'unit' value 'rpm' to a QUDT URI; skipping
```

The symbol of this unit in QUDT is "rev/min", which is matched.

In a query, a text with no match raises a `ValueError`:

```python
acq.query().entity("ro membrane")
```
```text
ValueError: Could not resolve 'ro membrane' as class
```

## Close matches

With `aq.init(exact_only=False)`, a text that has no exact match is compared with all the names by meaning.
This is done with a small embedding model: the model turns each name into a list of numbers, and two texts with a similar meaning get similar numbers.
Every candidate gets a score between 0 and 1, and the candidate with the highest score is used.

The first start with `exact_only=False` takes a few minutes, because the model is downloaded and all the names are prepared.
The result is kept in the `.acquirium` folder, so later starts are faster.

With close matches on, "ro membrane" is matched:

```python
for match in acq.client.resolve("ro membrane", "class", top_k=3):
    print(match["uri"], round(match["score"], 3), match["match_stage"])
```
```text
https://watermetadata.org/ontology/watr#ReverseOsmosisMembrane 0.839 semantic
https://watermetadata.org/ontology/watr#ElectricallyConductingMembrane 0.788 semantic
https://watermetadata.org/ontology/watr#MolybdenumSulfideBasedMembrane 0.786 semantic
```

A close match is accepted when its score is at least 0.6 at registration, and at least 0.4 in a query.

Be aware that a close match can be wrong.
The closest candidate is used even when the text means something else, and no warning is given.
For instance, "rpm" is not the name of a unit in QUDT, and these are its closest candidates:

```python
for match in acq.client.resolve("rpm", "unit", top_k=3):
    print(match["uri"], round(match["score"], 3), match["match_stage"])
```
```text
http://qudt.org/vocab/unit/TON_Metric-PER-MIN 0.695 semantic
http://qudt.org/vocab/unit/MilliV-PER-MIN 0.693 semantic
http://qudt.org/vocab/unit/PSI 0.692 semantic
```

The registration above then gives the stream the unit "metric ton per minute":

```python
acq.query().measurement().where(label="speed").include("unit").metadata()
```
```text
shape: (1, 3)
┌─────────────────────────────────┬────────────┬─────────────────────────┐
│ data                            ┆ data.label ┆ data.unit               │
│ ---                             ┆ ---        ┆ ---                     │
│ str                             ┆ str        ┆ str                     │
╞═════════════════════════════════╪════════════╪═════════════════════════╡
│ acq:2bfcf1c4-ddd2-567f-acc9-6f… ┆ speed      ┆ unit:TON_Metric-PER-MIN │
└─────────────────────────────────┴────────────┴─────────────────────────┘
```

This is why exact matching is the default.
When you use close matches, check the result with `include()` after registering, or pass the symbol or the URI of the term.

## Matching a unit together with its quantity kind

At registration, the `unit` and the `quantity_kind` of a stream are matched together.
QUDT says which units belong to which quantity kind, and a pair that belongs together is preferred.
This is useful for a text that fits several units.
For instance, with close matches on, "ounce" alone is matched to a unit of mass, and with the quantity kind "volume" it is matched to a unit of volume:

```python
print(acq.resolve_property_metadata({"unit": "ounce"}))
print(acq.resolve_property_metadata({"unit": "ounce", "quantity_kind": "volume"}))
```
```text
{'unit': 'http://qudt.org/vocab/unit/OZ'}
{'unit': 'http://qudt.org/vocab/unit/OZ_VOL_UK', 'quantity_kind': 'http://qudt.org/vocab/quantitykind/Volume'}
```

Note that an exact match is never replaced this way.

## Further reading

- [`Acquirium.resolve`](../reference/client/metadata.md#acquiriumresolve) and [`Acquirium.resolve_property_metadata`](../reference/client/metadata.md#acquiriumresolve_property_metadata): the methods that show what a text is matched to.
- [`acquirium.init`](../reference/client/starting.md#acquiriuminit): the `exact_only` parameter.
- [Configuration](../reference/server/configuration.md): the `exact_only` setting of a server that is started on its own.
- [The query model](query-model.md): where a query uses the text matcher.
