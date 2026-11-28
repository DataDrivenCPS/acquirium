---
title: App outputs
---

## Outputs of another app

`Selection.outputs_of(app, port, combine=False)` selects an upstream
output. By default it preserves the upstream groups. `combine=True` feeds every
upstream group into one calculation. The upstream app and port must exist before
deployment, and the upstream app cannot be removed while a deployed app selects it.

Output points also carry `acq:producedBy`, `acq:outputPort`, and `acq:outputGroup`
for selection through SPARQL.

### Group parameters

A selection's `parameters` maps group IDs to JSON values:

```python
aq.Selection.outputs_of(fill, "turbidity", parameters={
    "urn:bulk/loc-2": {"limit": 0.5},
    "urn:bulk/loc-3": {"limit": 0.3},
})
```

Only the listed groups are selected; skipped groups appear in deployment
warnings. The function reads the values from `context["parameters"]`. A changed
parameter recalculates that group's history. Two selections giving the same
group conflicting values for a parameter fail deployment.

Acquirium always includes the selected group key as
`context["parameters"]["group"]`, including when no settings are configured.
In Python this key is always a tuple. Grouping by tank gives `(tank_uri,)`;
grouping by stream gives `(stream_reference_uri,)`;
`Selection(..., group=None)` gives `(app_name,)`. These keys have one item
because a selection currently groups by one column. JSON deployment responses
represent the tuple as an array. The keys in the selection's `parameters`
mapping remain group ID strings. The name `group` is reserved and cannot be
configured to a different value.

## Outputs

`Output(unit=None, quantity_kind=None, label=None, on_group=False, value_kind="numeric")` describes a
new measurement. The port name is the key in the app's `outputs` mapping and in
the function's return value.

`on_group=True` attaches the output point to its group using `s223:hasProperty`.
The group must be a URI. Queries can then find the derived measurement through
the entity it describes, alongside its source measurements.

Output references depend on app name, group, and port. Changing the group's
membership keeps that identity. Derived source IDs are reserved as
`derived:<app-name>`. Membership applies to the whole stored history; see
[Derived stream updates](../../explanation/apps.md).

### Replacement outputs

`ReplacementOutput(of=None, unit=None, label=None)` continues the measurement
an input selects, instead of describing a new one. Reads and other apps'
inputs that select that measurement then find the replacement's values.

```python
aq.App(
    name="turbidity-clean",
    inputs={"raw": aq.Selection.from_query(turbidity, measurement="turbidity", group="location")},
    outputs={"turbidity": aq.ReplacementOutput()},
    ...
)
```

`of` names the input replaced; it may be omitted when the app has one input.
That input must select exactly one physical stream per group and must drive
execution, rather than use `as_of`. The replacement inherits the input's
quantity kind and value kind. It inherits the unit too, unless `unit` names
another unit of the same dimension; a cleaner can convert degrees Fahrenheit to
Celsius, but not to NTU. Reads and consumers then see the new unit, and a
consumer's `target_unit` converts from it. A replacement is not attached to its
group; it is found through the measurement it replaces.

One input can have one replacement. A second replacement app selects the first
one's output and replaces it in turn, without naming the original input. An
ordinary diagnostic output may branch from any stage.

```text
raw ──replaced by──> cleaned ──replaced by──> filled
                        └──> diagnostic
```

The function returns the complete series it calculated over its input frame,
including unchanged rows that should survive. Acquirium selects rows at the
call's `output_time`, or at the scheduled times in a batch call. The declared
window still determines which timestamps the app maintains. An empty selected
result retracts that timestamp; it does not fall back to raw data. Other output
ports retain their normal timestamp checks.

Read methods accept `view`:

- `current` follows the chain to its final stage. This is the default for normal
  measurement queries and direct stream reads.
- `raw` follows the chain to its original input.
- `stored` reads the exact physical stream requested.

```python
measurement.dataframe()                # Final replacement stage.
measurement.dataframe(view="raw")      # Original samples.
client.stream(clean_ref).dataframe(view="stored")  # This particular stage.
```

Queries restricted by `app` or an explicit `ref` use `stored` by default.
Normal discovery collapses replacement stages into one logical measurement per
query node and entity context. An explicit `view` overrides the query default.
App input selections take the same `view`, as `Selection(..., view=...)` or
`Selection.from_query(..., view=...)`. Unset, a query selection binds
`current`: each matched identity binds the chain's final stage, once per
group, however many of its stages the query matched. `raw` binds the original
input, and `stored` binds exactly the stream selected. `Selection.outputs_of`
binds `stored`, because it names one stage. An input that a replacement
replaces always binds the stream it selects, and rejects `view="current"` or
`"raw"`; that is how chain stages are ordered. An input that would follow the
chain to its own app's output is rejected.

Adding a stage at the end of a chain rebinds every `current` selection of that
identity and recalculates its history, as any other input change does.
Removing the last stage rebinds them to the stage before it.

The active plan chooses the final stage. If it fails, its last successful
values remain visible with the app's stale status. A new stage that has never
succeeded has no values. A successful empty result is authoritative too. Raw
and earlier stages remain explicitly readable. Removing a middle stage
requires rebinding the stage that replaces it, and any `stored` selection of
it; removing the last stage exposes its input.
Historical reads resolve the chain from the plan active at their epoch.
