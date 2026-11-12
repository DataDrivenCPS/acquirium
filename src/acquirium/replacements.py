"""Replacement claims live in the plan; reads and inputs follow its stream links."""

import json

from acquirium.Storage.values import normalize_value_kind

VIEWS = ("current", "raw", "stored")


def input_views(declarations):
    """The view each app input binds through, keyed by ``(app, input name)``.

    An unset view follows the chain to its final stage, except for
    ``outputs_of`` selections, which name one stage. An input an output
    replaces always binds the stream it selects: that is how stages are
    ordered, and following the chain would lead back to the app itself.
    """
    views = {}
    for name, declaration in declarations.items():
        claimed = {spec["replaces"] for spec in declaration["outputs"].values()
                   if spec.get("replaces") is not None}
        for item in declaration["inputs"]:
            role, view = item["input"], item.get("view")
            if role in claimed:
                if view not in (None, "stored"):
                    raise ValueError(
                        f"app {name!r} replacement input {role!r} binds the stream it selects; "
                        f"it cannot use view {view!r}"
                    )
                view = "stored"
            elif view is None:
                view = "stored" if item.get("upstream") else "current"
            if views.setdefault((name, role), view) != view:
                raise ValueError(f"app {name!r} input {role!r} is read through different views")
    return views


def compile(operators, declarations, *, streams, previous_plan=None, compatible=None, rebind=None):
    """Bind replacement claims, inherit their metadata, and bind following inputs.

    ``streams`` maps each claimed source that is not itself a replacement to
    its unit, quantity kind and value kind. ``compatible(before, after)``
    says whether a replacement may change its input's unit; ``rebind(operator,
    item, target, metadata)`` returns an input moved to another stage, given
    that stage's metadata. Returns each replacement output's inherited
    metadata.
    """
    outputs = {ref for op in operators for ref in op["outputs"].values()}
    removed = {
        op["outputs"][port] for op in (previous_plan or {}).get("operators", [])
        for port in op.get("parameters", {}).get("__acquirium", {}).get("replaces", {})
    } - outputs
    views = input_views(declarations)
    claims, sources = {}, {}
    for operator in operators:
        name = operator["id"].split(":", 1)[0]
        specs = declarations[name]["outputs"]
        for item in operator["inputs"]:
            if item["stream"] in removed and views[(name, item["role"])] == "stored":
                raise ValueError(f"app {name!r} still uses removed output {item['stream']!r}; rebind its input")
        for port, spec in specs.items():
            role = spec.get("replaces")
            if role is None:
                continue
            refs = {item["stream"] for item in operator["inputs"] if item["role"] == role}
            if len(refs) != 1:
                raise ValueError(f"app {name!r} replacement input {role!r} must select exactly one stream per group")
            if role in operator.get("as_of", {}):
                raise ValueError(f"app {name!r} replacement input {role!r} must drive execution, not use as_of")
            source = refs.pop()
            if source in claims:
                raise ValueError(f"stream {source!r} has competing replacements: {claims[source]} and {name}.{port}")
            claims[source] = f"{name}.{port}"
            sources[operator["outputs"][port]] = (source, spec.get("unit"), name, port)

    resolved = {}

    def inherit(ref, visiting=()):
        if ref in resolved:
            return resolved[ref]
        if ref not in sources:
            if ref not in streams:
                raise ValueError(f"replacement input {ref!r} has no registered metadata")
            return streams[ref]
        if ref in visiting:
            raise ValueError(f"replacement cycle at {ref!r}")
        source, unit, name, port = sources[ref]
        base = inherit(source, (*visiting, ref))
        before = str(base["unit"]) if base.get("unit") is not None else None
        after = str(unit) if unit is not None else before
        if before is not None and after != before:
            if compatible is None or not compatible(before, after):
                raise ValueError(
                    f"app {name!r} replacement {port!r} unit {after!r} is not compatible "
                    f"with its input's unit {before!r}"
                )
        resolved[ref] = {
            "unit": after,
            "quantity_kind": str(base["quantity_kind"]) if base.get("quantity_kind") is not None else None,
            "value_kind": normalize_value_kind(base.get("value_kind")),
        }
        return resolved[ref]

    for ref in sources:
        inherit(ref)
    for operator in operators:
        claimed = {port: sources[ref][0] for port, ref in operator["outputs"].items() if ref in sources}
        if claimed:
            # Reads report the unit of the stage they resolve to.
            units = {port: resolved[operator["outputs"][port]]["unit"] for port in claimed
                     if resolved[operator["outputs"][port]]["unit"] is not None}
            operator["parameters"]["__acquirium"] = {"replaces": claimed, **({"units": units} if units else {})}
    if rebind is not None:
        moved = rebind

        def rebind(operator, item, target):
            return moved(operator, item, target, resolved.get(target) or streams.get(target) or {})
    _follow(operators, views, removed, rebind)
    return resolved


def units(plan):
    """The unit each replacement stage in ``plan`` reports, by stream."""
    result = {}
    for operator in (plan or {}).get("operators", []):
        for port, unit in operator.get("parameters", {}).get("__acquirium", {}).get("units", {}).items():
            result[operator["outputs"][port]] = unit
    return result


def _follow(operators, views, removed, rebind=None):
    """Bind ``current`` and ``raw`` inputs through the candidate plan's links.

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
            if target == item["stream"]:
                bound = item
            elif rebind is not None:
                bound = rebind(operator, item, target)
            else:
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
