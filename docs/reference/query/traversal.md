---
title: Traversal settings
---

These functions change how two builder methods walk the model: which predicates [`Query.related`](building.md#queryrelated) follows with `via="any"`, and which named relations [`Query.context`](building.md#querycontext) accepts in `via=`.
They are imported from `acquirium.Client.explore`. The settings apply to every query of the running Python process and are not saved on the instance.

```python
from acquirium.Client import explore
```

| Function | Description |
| --- | --- |
| [`hide`](#hide) | Stop `related()` from following a predicate |
| [`unhide`](#unhide) | Let `related()` follow a predicate again |
| [`hidden_predicates`](#hidden_predicates) | The predicates currently hidden |
| [`hidden_prefixes`](#hidden_prefixes) | The namespaces currently hidden |
| [`register_relation`](#register_relation) | Add a named relation for `context(via=)` |
| [`unregister_relation`](#unregister_relation) | Remove a named relation |
| [`reset_relations`](#reset_relations) | Restore the default relations |

## `hide`

```python
hide(*predicates)
```

Stops `related(via="any")` from following the given predicates. A predicate named in `via=` is always followed, hidden or not.

The predicates behind the [built-in attributes](filtering.md#built-in-attributes) are hidden from the start, together with `rdfs:subClassOf`, `s223:hasProperty`, `s223:cnx` and the link from a property to its stream. Attributes written with `insert_metadata` are hidden as a whole namespace.

**Parameters**

- `*predicates` (`str | URIRef`): full predicate URIs. A string that ends in `#` or `/` hides every predicate in that namespace.

**Returns**

`None`.

**Raises**

- `ValueError`: when an argument is not a full URI.

**Example**

```python
explore.hide("http://data.ashrae.org/standard223#hasObservationLocation")
"http://data.ashrae.org/standard223#hasObservationLocation" in explore.hidden_predicates()
```

```text
True
```

## `unhide`

```python
unhide(*predicates)
```

Lets `related(via="any")` follow the given predicates again. This also works for the predicates hidden from the start. Called with no arguments, it restores the default set.

**Parameters**

- `*predicates` (`str | URIRef`): full predicate URIs, or namespaces that end in `#` or `/`.

**Returns**

`None`.

**Example**

```python
explore.unhide()
"http://data.ashrae.org/standard223#hasObservationLocation" in explore.hidden_predicates()
```

```text
False
```

## `hidden_predicates`

```python
hidden_predicates()
```

Returns the predicates that are hidden now.

**Returns**

`frozenset[str]`: full predicate URIs.

**Example**

```python
sorted(explore.hidden_predicates())
```

```text
['http://data.ashrae.org/standard223#cnx', 'http://data.ashrae.org/standard223#hasConnectionPoint', 'http://data.ashrae.org/standard223#hasMedium', 'http://data.ashrae.org/standard223#hasProperty', 'http://data.ashrae.org/standard223#ofMedium', 'http://data.ashrae.org/standard223#ofSubstance', 'http://qudt.org/schema/qudt/hasEnumerationKind', 'http://qudt.org/schema/qudt/hasQuantityKind', 'http://qudt.org/schema/qudt/hasUnit', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://www.w3.org/2000/01/rdf-schema#label', 'http://www.w3.org/2000/01/rdf-schema#subClassOf', 'https://brickschema.org/schema/Brick/ref#hasExternalReference', 'https://watermetadata.org/ontology/watr#hasProcess', 'urn:acquirium#dataSource', 'urn:acquirium#isDisconnected', 'urn:acquirium#producedBy', 'urn:acquirium:attr#id']
```

## `hidden_prefixes`

```python
hidden_prefixes()
```

Returns the namespaces that are hidden now. Every predicate that starts with one of them is hidden.

**Returns**

`frozenset[str]`.

**Example**

```python
explore.hidden_prefixes()
```

```text
frozenset({'urn:acquirium:attr#'})
```

## `register_relation`

```python
register_relation(name, chains)
```

Adds a named relation that [`Query.context`](building.md#querycontext) accepts in `via=`. A relation says how to get from a property to an entity. Registering a name that exists replaces it.

Three relations exist from the start: `"entity"`, `"upstream"` and `"downstream"`. `explore.RELATIONS` is the dict that holds them all.

**Parameters**

- `name` (`str`): the name of the relation.
- `chains` (`tuple`): one or more alternative chains. The relation matches when any chain does. A chain is a tuple of steps, walked from the property. A step is `(predicate_uri, class_uri)`:
  - `predicate_uri` is a full URI. A leading `"^"` follows the predicate backwards.
  - `class_uri` limits the node the step arrives at to a class. `None` means any node.

**Returns**

`None`.

**Raises**

- `ValueError`: when `name` is empty, when `chains` or a chain is empty, or when a step is not a `(predicate_uri, class_uri)` pair.

**Example**

The sensor that observes a property is one step backwards over `s223:observes`:

```python
explore.register_relation(
    "observed_by",
    ((("^http://data.ashrae.org/standard223#observes", None),),),
)
acq.query().measurement(quantity_kind="power").context(via="observed_by").metadata()
```

```text
shape: (2, 3)
┌─────────────────────────┬─────────────────────┬────────────────────────────────┐
│ data                    ┆ data.label          ┆ data_observed_by               │
│ ---                     ┆ ---                 ┆ ---                            │
│ str                     ┆ str                 ┆ str                            │
╞═════════════════════════╪═════════════════════╪════════════════════════════════╡
│ wbs:P2-mechanical-power ┆ P2 mechanical power ┆ wbs:P2-mechanical-power-sensor │
│ wbs:P1-mechanical-power ┆ P1 mechanical power ┆ wbs:P1-mechanical-power-sensor │
└─────────────────────────┴─────────────────────┴────────────────────────────────┘
```

## `unregister_relation`

```python
unregister_relation(name)
```

Removes a named relation. A name that does not exist is ignored. A default relation that is removed stays removed until [`reset_relations`](#reset_relations) is called.

**Parameters**

- `name` (`str`): the name of the relation.

**Returns**

`None`.

**Example**

```python
explore.unregister_relation("observed_by")
sorted(explore.RELATIONS)
```

```text
['downstream', 'entity', 'upstream']
```

## `reset_relations`

```python
reset_relations()
```

Restores the three default relations and removes every registered one.

**Parameters**

None.

**Returns**

`None`.

**Example**

```python
explore.reset_relations()
sorted(explore.RELATIONS)
```

```text
['downstream', 'entity', 'upstream']
```
