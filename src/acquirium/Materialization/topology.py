"""Resolve pure transformation queries into immutable binding topologies."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Any, Callable, Mapping

from acquirium.Client.explore.core import Query
from acquirium.Materialization.bindings import BindingSpec
from acquirium.Materialization.definitions import MaterializationDefinition, definition_spec
from acquirium.Materialization.worker import load_entrypoint


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


class _GraphQueryClient:
    """Small query-builder client backed by the immutable graph snapshot."""

    def __init__(self, graph: object, resolver: Callable[..., Any] | None = None) -> None:
        self._graph = graph
        self._resolver = resolver

    def sparql_query(self, query: str, include_dependencies: bool = True, *, wait_for_fresh: bool = False) -> dict:
        kwargs = {"include_dependencies": include_dependencies}
        if wait_for_fresh:
            kwargs["wait_for_fresh"] = True
        return self._graph.sparql_query(query, **kwargs)

    def resolve(self, value: str, kind: str, **kwargs: Any) -> str | None:
        if isinstance(value, str) and value.startswith(("http://", "https://", "urn:")):
            return value
        if self._resolver is None:
            raise ValueError(f"query builder value {value!r} requires the server text resolver")
        resolved = self._resolver(value, kind, **kwargs)
        if isinstance(resolved, str):
            return resolved
        if isinstance(resolved, list) and resolved:
            first = resolved[0]
            if isinstance(first, Mapping):
                uri = first.get("uri")
                return str(uri) if uri is not None else None
        return None

    def expand_uri(self, value: str) -> str:
        return value

    def graph_version(self) -> int:
        status = getattr(self._graph, "graph_status", lambda: {"source_version": 0})()
        return int(status.get("source_version", status.get("published_version", 0)))


@dataclass(frozen=True)
class _QueryFacade:
    client: _GraphQueryClient

    def query(self) -> Query:
        return Query(client=self.client)


def _output_spec(value: object) -> dict[str, Any]:
    if hasattr(value, "__dataclass_fields__"):
        value = {name: getattr(value, name) for name in value.__dataclass_fields__}
    if value is None:
        return {"value_kind": "numeric"}
    if not isinstance(value, Mapping):
        raise ValueError("each output spec must be a mapping or outputs.stream(...) result")
    result = {str(key): item for key, item in value.items()}
    result.setdefault("value_kind", "numeric")
    return result


def _output_specs(spec: Mapping[str, object]) -> dict[str, dict[str, Any]]:
    outputs = spec.get("outputs")
    if not isinstance(outputs, Mapping) or not outputs:
        raise ValueError("a transformation requires a non-empty outputs mapping")
    return {str(name): _output_spec(value) for name, value in outputs.items()}


def _query_rows(query: Query) -> tuple[dict[str, list[dict[str, str | None]]], ...]:
    if not isinstance(query, Query):
        raise TypeError("build_query() must return an Acquirium query")
    result = query.execute(include_dependencies=True)
    columns = list(result.get("columns", ()))
    indices = {column: index for index, column in enumerate(columns)}
    graph = query.query_graph
    rows: list[dict[str, list[dict[str, str | None]]]] = []
    seen: set[str] = set()
    for raw in result.get("rows", ()):
        def cell(name: str) -> Any:
            index = indices.get(name)
            return raw[index] if index is not None and index < len(raw) else None

        streams: dict[str, list[dict[str, str | None]]] = {}
        for node_id in graph.data_nodes:
            point = cell(f"v{node_id}")
            ref = cell(f"ext{node_id}")
            if ref is None:
                continue
            alias = graph.aliases_reverse.get(node_id, f"data_{node_id}")
            unit_column = f"unit{node_id}"
            extunit_column = f"extunit{node_id}"
            unit = cell(unit_column)
            if unit is None:
                unit = cell(extunit_column)
            streams.setdefault(alias, []).append({
                "ref_uri": str(ref),
                "point_uri": str(point) if point is not None else None,
                "unit": str(unit) if unit is not None else None,
            })
        if not streams:
            continue
        key = sha256(_canonical({
            alias: sorted(item["ref_uri"] for item in matches)
            for alias, matches in streams.items()
        }).encode()).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        rows.append(streams)
    return tuple(rows)


def _derived_output_uri(name: str, output_name: str, logical_key: str, output: Mapping[str, Any], invocation: str) -> str:
    explicit = output.get("ref_uri")
    if explicit:
        return str(explicit)
    prefix = str(output.get("prefix") or "urn:acquirium:derived")
    if invocation == "whole_query":
        return f"{prefix}:{name}:{output_name}"
    digest = sha256(logical_key.encode()).hexdigest()[:20]
    return f"{prefix}:{name}:{output_name}:{digest}"


def _query_bindings(
    spec: Mapping[str, object],
    *,
    definition_name: str,
    query: Query,
) -> tuple[BindingSpec, ...]:
    invocation = str(spec.get("invocation", "whole_query"))
    if invocation not in {"whole_query", "per_row"}:
        raise ValueError("invocation must be 'whole_query' or 'per_row'")
    outputs = _output_specs(spec)
    rows = _query_rows(query)
    if invocation == "whole_query":
        grouped: dict[str, list[dict[str, str | None]]] = {}
        for row in rows:
            for alias, matches in row.items():
                grouped.setdefault(alias, []).extend(matches)
        for alias, matches in grouped.items():
            deduped = {item["ref_uri"]: item for item in matches}
            grouped[alias] = [deduped[key] for key in sorted(deduped)]
        candidates = (("whole-query", grouped),) if grouped else ()
    else:
        candidates = tuple(
            (
                sha256(_canonical({
                    alias: sorted(item["ref_uri"] for item in matches)
                    for alias, matches in row.items()
                }).encode()).hexdigest(),
                row,
            )
            for row in rows
        )

    bindings: list[BindingSpec] = []
    for logical_key, streams in candidates:
        inputs = {
            alias: tuple(sorted(str(item["ref_uri"]) for item in matches))
            for alias, matches in streams.items()
        }
        metadata = {
            "input_streams": {
                alias: [dict(item) for item in matches]
                for alias, matches in sorted(streams.items())
            },
            "output_specs": outputs,
            "invocation": invocation,
        }
        planned_outputs = {
            output_name: (_derived_output_uri(definition_name, output_name, logical_key, output, invocation),)
            for output_name, output in outputs.items()
        }
        bindings.append(BindingSpec(logical_key, inputs, planned_outputs, metadata))
    return tuple(bindings)


def resolve_bindings(
    spec_or_definition: Mapping[str, object] | MaterializationDefinition,
    graph: object,
    *,
    entrypoint: str | None = None,
    source_digest: str | None = None,
    query_resolver: Callable[..., Any] | None = None,
) -> tuple[BindingSpec, ...]:
    """Run a transformation's builder and resolve its query once.

    ``build_query`` is called only here, during epoch construction.  The
    returned query is then executed against the same graph snapshot; workers
    receive only the resolved stream identities and never the graph client.
    """
    if isinstance(spec_or_definition, MaterializationDefinition):
        spec = definition_spec(spec_or_definition)
        entrypoint = spec_or_definition.entrypoint
        source_digest = spec_or_definition.source_digest
        definition_name = spec_or_definition.name
    else:
        spec = dict(spec_or_definition)
        definition_name = str(spec.get("name", "transformation"))
    if "invocation" not in spec:
        raise ValueError("definition does not contain the query-driven invocation field")
    if not entrypoint:
        raise ValueError("query-driven binding resolution requires an entrypoint")
    target = load_entrypoint(entrypoint, source_digest)
    if not isinstance(target, type) or not hasattr(target, "build_query"):
        raise TypeError("transformation entrypoint must implement build_query")
    builder = target()
    query = builder.build_query(_QueryFacade(_GraphQueryClient(graph, query_resolver)))
    return _query_bindings(spec, definition_name=definition_name, query=query)
