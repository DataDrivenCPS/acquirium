"""Resolve replacement chains for reads and app inputs.

Each replacement output claims one input stream. A current view follows
those claims forward to the final stage, a raw view follows them backward
to the base, and a stored view reads the physical reference itself. Compile
the claims before rebinding consumers so they can see the whole chain.
"""

from acquirium.Storage.values import normalize_value_kind

VIEWS = ("current", "raw", "stored")


def claimed_inputs(declaration):
    """Each replacement output of an app declaration, mapped to the input it replaces."""
    return {port: spec["replaces"] for port, spec in declaration["outputs"].items()
            if spec.get("replaces") is not None}


def annotation(instance, key):
    """One entry of the replacement metadata on a plan instance."""
    return (instance.get("annotations") or {}).get(key, {})


def _instances(plan):
    return (plan or {}).get("instances", ())


def input_views(declarations):
    """The view each app input binds through, keyed by ``(app, input name)``.

    An unset view follows the chain to its final stage, except for
    ``outputs_of`` selections, which name one stage. An input an output
    replaces always binds the stream it selects: that is how stages are
    ordered, and following the chain would lead back to the app itself.
    """
    views = {}
    for name, declaration in declarations.items():
        claimed = set(claimed_inputs(declaration).values())
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


def compile(plan, bindings, declarations, views, *, streams, rebind, previous_plan=None, compatible=None):
    """Bind replacement claims, inherit their metadata, and bind following inputs.

    ``streams`` maps each claimed source that is not itself a replacement to
    its unit, quantity kind and value kind. ``compatible(before, after)``
    says whether a replacement may change its input's unit. ``rebind`` moves
    a binding to a different stage and updates its context. Bindings are
    indexed by native instance identity. Returns inherited output metadata.
    """
    instances = plan["instances"]
    outputs = {ref for instance in instances for ref in instance["outputs"].values()}
    removed = {
        op["outputs"][port] for op in _instances(previous_plan)
        for port in annotation(op, "replaces")
    } - outputs
    as_of = {
        (name, item["input"])
        for name, declaration in declarations.items()
        for item in declaration["inputs"]
        if item.get("as_of") is not None
    }
    claims, sources = {}, {}
    for instance in instances:
        name = instance["template"]
        inputs = bindings[name, instance["key"]]
        for item in inputs:
            if item["stream"] in removed and views[(name, item["role"])] == "stored":
                raise ValueError(f"app {name!r} still uses removed output {item['stream']!r}; rebind its input")
        for port, role in claimed_inputs(declarations[name]).items():
            refs = {item["stream"] for item in inputs if item["role"] == role}
            if len(refs) != 1:
                raise ValueError(f"app {name!r} replacement input {role!r} must select exactly one stream per group")
            if (name, role) in as_of:
                raise ValueError(f"app {name!r} replacement input {role!r} must drive execution, not use as_of")
            source = refs.pop()
            if source in claims:
                raise ValueError(f"stream {source!r} has competing replacements: {claims[source]} and {name}.{port}")
            claims[source] = f"{name}.{port}"
            ref = instance["outputs"][port]
            sources[ref] = (source, declarations[name]["outputs"][port].get("unit"), name, port)

    resolved = {}

    def inherit(ref, visiting=()):
        """Resolve one stage's metadata from its immediate input, detecting cycles."""
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
    for instance in instances:
        outputs = instance["outputs"]
        claimed = {port: sources[ref][0] for port, ref in outputs.items() if ref in sources}
        if claimed:
            # Reads report the unit of the stage they resolve to.
            units = {port: resolved[outputs[port]]["unit"] for port in claimed
                     if resolved[outputs[port]]["unit"] is not None}
            instance["annotations"] = {"replaces": claimed, **({"units": units} if units else {})}
    _follow(plan, bindings, views, removed, rebind, {**streams, **resolved})
    return resolved


def units(plan):
    """The unit each replacement stage in ``plan`` reports, by stream."""
    result = {}
    for operator in _instances(plan):
        for port, unit in annotation(operator, "units").items():
            result[operator["outputs"][port]] = unit
    return result


def _follow(plan, bindings, views, removed, rebind, metadata):
    """Bind ``current`` and ``raw`` inputs through the candidate plan's links.

    A query can match several stages of one identity, such as a source
    property and the replacement attached beside it; each resolves to the
    same stream and binds once. The compiler orders the final bindings.
    """
    refs = {item["stream"] for items in bindings.values() for item in items}
    targets = {view: resolve(plan, refs, view=view) for view in ("current", "raw")}
    for instance in plan["instances"]:
        name, group = instance["template"], instance["key"]
        own = set(instance["outputs"].values())
        stored, followed = [], {}
        for item in bindings[name, group]:
            view = views[(name, item["role"])]
            if view == "stored":
                stored.append(item)
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
            bound = item if target == item["stream"] else rebind(item, target, metadata.get(target, {}))
            key = (item["role"], target)
            # Prefer the row that selected this stage, so its context describes it.
            if key not in followed or item["stream"] == target:
                followed[key] = bound
        bindings[name, group] = [*stored, *followed.values()]


def resolve(plan, refs, *, view="current"):
    """Map physical references to the chain head, base, or exact stored stage."""
    if view not in VIEWS:
        raise ValueError("view must be 'current', 'raw', or 'stored'")
    links = {}
    if view != "stored":
        for operator in _instances(plan):
            for port, source in annotation(operator, "replaces").items():
                target = operator["outputs"][port]
                # Each traversal has a single next stage. Reverse the links
                # for raw reads without changing how the chain is walked.
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
