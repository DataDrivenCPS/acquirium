"""Replacement claims live in the plan; reads follow its physical stream links."""

from acquirium.Storage.values import normalize_value_kind


def compile(operators, declarations, *, streams, previous_plan=None):
    """Bind and validate claims after semantic selections have resolved."""
    outputs = {ref for op in operators for ref in op["outputs"].values()}
    removed = {
        op["outputs"][port] for op in (previous_plan or {}).get("operators", [])
        for port in op.get("parameters", {}).get("__acquirium", {}).get("replaces", {})
    } - outputs
    claims = {}
    for operator in operators:
        name = operator["id"].split(":", 1)[0]
        specs = declarations[name]["outputs"]
        for item in operator["inputs"]:
            if item["stream"] in removed:
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
    return operators


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
