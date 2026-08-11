# Text resolution

This is a guide to how acquirium turns free text into ontology URIs: the
`resolve()` API, how matching works, and unit conversion.
The querying and data guides use this machinery implicitly; this doc is for
using it directly and for debugging a bad match.

## resolve()

`acq.client.resolve()` has three forms.

A single text returns the best matching URI, or `None` when nothing clears
the score threshold:

```python
acq.client.resolve("ro membrane", "class")
# 'urn:nawi-water-ontology#ReverseOsmosisMembrane'

acq.client.resolve("blorp", "class", min_score=0.9)
# None
```

`top_k` returns the ranked candidates with scores.
This is useful for checking a suspicious match:

```python
acq.client.resolve("ro membrane", "class", top_k=2, min_score=0.3)
```
```text
[{'uri': 'urn:nawi-water-ontology#ReverseOsmosisMembrane',
  'label': 'Reverse Osmosis Membrane', 'score': 0.839, 'match_stage': 'semantic', ...},
 {'uri': 'urn:nawi-water-ontology#ElectricallyConductingMembrane',
  'label': 'Electrically Conducting Membrane', 'score': 0.788, 'match_stage': 'semantic', ...}]
```

A dict resolves several fields together:

```python
acq.client.resolve({"u": ("mg/L", "unit"), "qk": ("mass concentration", "quantity_kind")})
# {'u': 'http://qudt.org/vocab/unit/MilliGM-PER-L',
#  'qk': 'http://qudt.org/vocab/quantitykind/MassConcentration'}
```

Note that this is not just a batch call.
Unit and quantity-kind fields are cross-checked against QUDT, so a compatible
pair is preferred over two individually best matches that are incompatible.

The `kind` argument picks the vocabulary: `class`, `predicate`, `process`,
`substance`, `unit`, `quantity_kind`.
Anything that already looks like a URI passes through unchanged.

## How matching works

Each text is matched in two stages.
The first stage is an exact lookup on known labels and symbols.
For instance, `"kg"` and `"mg/L"` resolve without similarity search.
When there is no exact hit, the text is matched by cosine similarity over an
embedding index of the ontology vocabulary.
This is how `"ro membrane"` resolves to `ReverseOsmosisMembrane`.

The indexes are built from the ontologies at server startup: one over the
water and s223 vocabularies, one over QUDT.
Instance data is not indexed; see the note in the
[querying guide](querying.md#free-text-and-what-it-resolves-to).

Semantic matching returns the closest candidate above `min_score` (0.5 by
default).
Note that this means a bad input gives a wrong answer rather than an error.
If a match looks wrong, check the candidates and scores with `top_k=3`.

## Units and conversion

`resolve_unit()` returns the QUDT record behind a unit text, including the
conversion factors to the base unit:

```python
acq.client.resolve_unit("psi")
# {'uri': 'http://qudt.org/vocab/unit/PSI', 'label': 'Psi', 'symbol': 'psi',
#  'multiplier': 6894.757, 'offset': 0.0, ...}
```

`resolve_conversion(from_unit, to_unit)` resolves both sides together and
picks a *convertible* pair: candidates are considered jointly, and a pair
whose quantity kinds are incompatible is skipped in favor of one that
converts.
A request with no convertible pair fails, with both candidate lists in the
message:

```python
acq.client.resolve_conversion("bar", "celsius")
```
```text
ValueError: resolve_conversion failed: no convertible pair among the matches
for 'bar' -> 'celsius' (from candidates: ['.../BAR'], to candidates: ['.../DEG_C'])
```

`get_conversion_factors(from_uri, to_uri)` returns the raw multipliers and
offsets when both URIs are already known.
`DataObject.convert_to()` is built on these calls; see the
[data guide](data.md#units).

## Tuning

- `min_score` sets the floor below which `resolve()` returns nothing.
  Raise it when wrong matches slip through; lower it when valid text comes
  back `None`.
- `top_k` is diagnostic; keep production calls at the default of 1.
- The embedding model is `[server] embedding_model`
  (`BAAI/bge-small-en-v1.5` by default); changing it rebuilds the indexes on
  next start.
- `GET /embedding_status` reports index state when resolution returns
  nothing at all: an index still building resolves nothing until it is done.
