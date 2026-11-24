---
title: App inputs
---

## Inputs

`inputs` maps each input's name to a selection, or to a list of selections that
feed the same input:

```python
aq.App(
    name="calibrated",
    inputs={
        "reading": aq.Selection.from_query(readings, measurement="reading"),
        "gain": aq.Selection.outputs_of(calibration, "gain", as_of=HOUR),
    },
    ...
)
```

Names must be nonempty. The function's input frame carries each row's input
name in its `input` column, so a function with several inputs can tell their
rows apart. Selections listed under one name must agree on `as_of` and `view`.

## Input selections

`Selection.from_query(query, measurement=..., group=None, parameters=None, view=None)`
turns a query into a selection. `measurement` is the alias of the query's
measurement node whose streams the input reads. Without a `group`, each matched
stream gets its own calculation. With an entity alias as `group`, all streams
for that entity feed one calculation.

The selection stores the query, rather than a fixed list of matches. Acquirium
resolves it again when the graph changes. Scope queries to the intended points
or sources; `exclude_app` can exclude an app's own outputs from a broad query.

`Selection(query, stream="stream", group="group")` accepts SPARQL
returning the named columns. Here `group=None` combines all selected streams into
one app-wide group. Returned columns are available as strings in each input
row's JSON `context`.

`target_unit` adds resolved conversion factors to that row context. The function
must apply the factors; declaring a target unit does not convert the frame.

#
