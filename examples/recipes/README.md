# Recipes

Query shortcuts that are not part of the package. Each file here is a recipe
file: load it with `acq.recipes.add(path)` and its functions appear on
`acq.recipes`, the same way the shipped ones do.

```python
import acquirium as aq

acq = aq.init()
acq.recipes.add("examples/recipes/flow.py")
acq.recipes.nearest_measurement("ultrafiltration unit", quantity_kind="turbidity").metadata()
print(acq.recipes.source("nearest_measurement"))   # the function, to read or copy
```

| file | recipes |
|---|---|
| `plant.py` | `equipment_in`, `measurements_of`, `units_in` |
| `flow.py` | `nearest_measurement`, `across` |
| `quality.py` | `missing_metadata`, `gaps`, `exceedances` |

## Writing a recipe file

A recipe file is a plain Python module. Every public top-level function it
defines whose first parameter is named `acq` is a recipe. Functions with an
underscore name, functions imported from elsewhere, and functions whose first
parameter has another name are helpers and are not registered.

```python
"""Recipes for the Benicia plant."""

ACQUIRIUM = ">=0.4.0a8"


def ro_pressures(acq):
    """Pressure streams on the reverse osmosis units."""
    return acq.query().entity("reverse osmosis membrane").measurement(quantity_kind="pressure")
```

`ACQUIRIUM` is a PEP 440 version specifier. Loading the file under an
acquirium version outside it raises `RecipeError`, so a recipe written
against an old query API fails at `add()` rather than somewhere inside a
notebook. Leave the constant out to skip the check.

A recipe takes the client first and returns whatever is useful: a `Query` to
build on, a dataframe, a tuple. Keep one file self-contained, with its own
imports, so that copying the file is enough to run it. A recipe can import
a shipped one, as `quality.py` does with `catalog`.

The first line of the docstring is what `acq.recipes` lists, so make it the
question the recipe answers.

## Adding a recipe here

Open a pull request with the function in the file whose theme fits, or a new
file for a new theme, and a test under `tests/unit/test_recipes_examples.py`
that checks the query it builds. A recipe here has no stability promise; it
may change or go away between releases.

A recipe moves into `acquirium/recipes/builtins.py` once someone other than
its author has used it for real work and the signature has stopped
changing. Until then it lives here.
