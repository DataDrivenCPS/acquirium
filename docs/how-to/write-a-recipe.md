---
title: Share a query as a recipe
---

A recipe is a function that takes the client and builds a query, so a
question you answer more than once, or hand to someone else, has a name.
Shipped recipes are listed with `acq.recipes`; this page is about writing
your own.

## Write the file

```python
"""Recipes for the pilot plant."""

ACQUIRIUM = ">=0.4.0a8"


def ro_pressures(acq, *, unit=None):
    """Pressure streams on the reverse osmosis membranes."""
    q = acq.query().entity("reverse osmosis membrane").measurement(quantity_kind="pressure")
    return q.where(unit=unit) if unit else q
```

Three things make this a recipe file:

- the function is public and its first parameter is `acq`;
- `ACQUIRIUM` says which versions of acquirium it was written against;
- the first docstring line is the question it answers, which is what the
  listing shows.

Helpers keep an underscore name or a different first parameter and are not
registered. A recipe can return a `Query`, a dataframe, or anything else.

## Load and use it

```python
acq.recipes.add("pilot_recipes.py")
acq.recipes                                   # lists ro_pressures with its description
acq.recipes.ro_pressures(unit="psi").metadata()
print(acq.recipes.source("ro_pressures"))     # the function, to read or copy
```

The client is bound, so the call drops `acq`. `help(acq.recipes.ro_pressures)`
shows the remaining parameters and the docstring.

## What `add()` refuses

- A name that is already registered, from the shipped set or an earlier
  file. Pass `replace=True` to override it on purpose.
- A file whose `ACQUIRIUM` excludes the installed version. Update the
  recipe, then the constant.
- A malformed `ACQUIRIUM`, a name that would shadow `add` or `source`, and
  a path that is not a `.py` file.

Each raises `RecipeError`. A refused file registers nothing, so a collision
in its last function does not leave the first ones half loaded.

## Share it

Send the file. Whoever receives it runs `acq.recipes.add(path)` and has the
same names, and `source()` when they want to see how a query is built.
`add()` takes local paths only; it does not fetch from a URL.

The repository keeps recipes that are useful but not part of the package
under `examples/recipes/`, with the rule for moving one into
`acquirium/recipes/builtins.py`. The [client API reference](../reference/client-api.md#recipes)
lists the shipped ones and their columns.
