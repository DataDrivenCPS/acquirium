"""Replacement claims live in the plan; reads and inputs follow its stream links."""

import json

from acquirium.Storage.values import normalize_value_kind

VIEWS = ("current", "raw", "stored")


def input_views(declarations):
    """The view each app input role binds through, keyed by ``(app, role)``.

    An unset view follows the chain to its final stage, except for
    ``outputs_of`` selections, which name one stage. A role an output
    replaces always binds the stream it selects: that is how stages are
    ordered, and following the chain would lead back to the app itself.
    """
    views = {}
    for name, declaration in declarations.items():
        claimed = {spec["replaces"] for spec in declaration["outputs"].values()
                   if spec.get("replaces") is not None}
        for item in declaration["inputs"]:
            role, view = item["role"], item.get("view")
            if role in claimed:
                if view not in (None, "stored"):
                    raise ValueError(
                        f"app {name!r} replacement role {role!r} binds the stream it selects; "
                        f"it cannot use view {view!r}"
                    )
                view = "stored"
            elif view is None:
                view = "stored" if item.get("upstream") else "current"
            if views.setdefault((name, role), view) != view:
                raise ValueError(f"app {name!r} input {role!r} is read through different views")
    return views


def compile(operators, declarations, *, streams, previous_plan=None):
    """Bind and validate claims after semantic selections have resolved."""
    outputs = {ref for op in operators for ref in op["outputs"].values()}
    removed = {
        op["outputs"][port] for op in (previous_plan or {}).get("operators", [])
        for port in op.get("parameters", {}).get("__acquirium", {}).get("replaces", {})
    } - outputs
    views = input_views(declarations)
    claims = {}
    for operator in operators:
        name = operator["id"].split(":", 1)[0]
        specs = declarations[name]["outputs"]
        for item in operator["inputs"]:
            if item["stream"] in removed and views[(name, item["role"])] == "stored":
                raise ValueError(f"app {name!r} still uses removed output {item['stream']!r}; rebind its input")
        replacements = {}
        for port, spec in specs.items():
            role = spec.get("replaces")
            if role is None:
                continue
            refs = {item["stream"] for item in operator["inputs"] if item["role"] == role}
            if len(refs) != 1:
                raise ValueError(f"app {name!r} replacement role {role!r} must select exactly one stream per group")
            if role in operator.get("as_of", {}):
                raise ValueError(f"app {name!r} replacement role {role!r} must drive execution, not use as_of")
            source = refs.pop()
            if source in claims:
                raise ValueError(f"stream {source!r} has competing replacements: {claims[source]} and {name}.{port}")
            claims[source] = f"{name}.{port}"
            if source not in streams:
                raise ValueError(f"replacement input {source!r} has no registered metadata")
            original = streams[source]
            for field in ("unit", "quantity_kind", "value_kind"):
                before, after = original.get(field), spec.get(field)
                if field == "value_kind":
                    before, after = normalize_value_kind(before), normalize_value_kind(after)
                else:
                    before = str(before) if before is not None else None
                    after = str(after) if after is not None else None
                if before != after:
                    raise ValueError(f"app {name!r} replacement {port!r} must preserve {field}: {before!r} != {after!r}")
            replacements[port] = source
        if replacements:
            operator["parameters"]["__acquirium"] = {"replaces": replacements}
    _follow(operators, views, removed)
    return operators


def _follow(operators, views, removed):
    """Bind ``current`` and ``raw`` roles through the candidate plan's links.

    A query can match several stages of one identity, such as a source
    property and the replacement attached beside it; each resolves to the
    same stream and binds once.
    """
    plan = {"operators": operators}
    refs = {item["stream"] for operator in operators for item in operator["inputs"]}
    targets = {view: resolve(plan, refs, view=view) for view in ("current", "raw")}
    for operator in operators:
        name = operator["id"].split(":", 1)[0]
        own = set(operator["outputs"].values())
        inputs, positions = [], {}
        for item in operator["inputs"]:
            view = views[(name, item["role"])]
            if view == "stored":
                inputs.append(item)
                continue
            if item["stream"] in removed:
                # The removed stage's identity continues through its own input.
                continue
            target = targets[view][item["stream"]]
            if target in own and target != item["stream"]:
                raise ValueError(
                    f"app {name!r} input {item['role']!r} follows its own output {target!r}; "
                    "select it with view='raw' or view='stored'"
                )
            bound = dict(item, stream=target)
            key = (item["role"], target)
            if key not in positions:
                positions[key] = len(inputs)
                inputs.append(bound)
            elif item["stream"] == target:
                # Prefer the row that selected this stage, so its context describes it.
                inputs[positions[key]] = bound
        operator["inputs"] = sorted(
            inputs, key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )


def resolve(plan, refs, *, view="current"):
    """Map physical references to the chain head, base, or exact stored stage."""
    if view not in {"current", "raw", "stored"}:
        raise ValueError("view must be 'current', 'raw', or 'stored'")
    links = {}
    if view != "stored":
        for operator in (plan or {}).get("operators", []):
            for port, source in operator.get("parameters", {}).get("__acquirium", {}).get("replaces", {}).items():
                target = operator["outputs"][port]
                key, value = (source, target) if view == "current" else (target, source)
                if key in links and links[key] != value:
                    raise ValueError(f"conflicting replacement links for {key!r}")
                links[key] = value
    result = {}
    for ref in refs:
        current, visited = ref, set()
        while current in links:
            if current in visited:
                raise ValueError(f"replacement cycle at {current!r}")
            visited.add(current)
            current = links[current]
        result[ref] = current
    return result
