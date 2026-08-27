---
title: Text resolution
---

<!-- TODO: intro -->

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
[querying guide](query-model.md#free-text-and-what-it-resolves-to).

Semantic matching returns the closest candidate above `min_score` (0.5 by
default).
Note that this means a bad input gives a wrong answer rather than an error.
If a match looks wrong, check the candidates and scores with `top_k=3`.
