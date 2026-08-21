from __future__ import annotations

import hashlib
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import quote

from acquirium.Apps.base import App, Output
from acquirium.internals.models import AppContext, AppOutputSpec


SAME_AS_INPUT = "same_as_input"


@dataclass(frozen=True)
class OutputTemplate:
    """Metadata shared by every output of a one-input/one-output mapped app."""

    name: str
    value_kind: str = "numeric"
    unit: str | None = SAME_AS_INPUT
    quantity_kind: str | None = None
    data_source: str | None = None
    storage_backend: str | None = None

    def __post_init__(self) -> None:
        if not self.name or "/" in self.name:
            raise ValueError("OutputTemplate.name must be non-empty and cannot contain '/'")
        if self.value_kind not in {"numeric", "text"}:
            raise ValueError("OutputTemplate.value_kind must be 'numeric' or 'text'")


@dataclass(frozen=True)
class MappedStream:
    """One selected input and its deterministic derived-output identity."""

    input_alias: str
    input_point_uri: str
    input_ref_uri: str | None
    input_unit: str | None
    output_point_uri: str
    output_ref_name: str
    values: Any


@dataclass(frozen=True)
class StreamMapping:
    """Resolved identity of one input stream and its derived output stream."""

    input_point_uri: str
    input_ref_uri: str | None
    output_point_uri: str
    output_ref_name: str | None
    input_unit: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "input_point_uri": self.input_point_uri,
            "input_ref_uri": self.input_ref_uri,
            "output_point_uri": self.output_point_uri,
            "output_ref_name": self.output_ref_name,
        }


def mapped_output_identity(
    app_name: str,
    output_name: str,
    input_point_uri: str,
) -> tuple[str, str]:
    """Return stable ``(ref_name, point_uri)`` for a mapped output stream."""
    digest = hashlib.sha256(input_point_uri.encode("utf-8")).hexdigest()[:20]
    ref_name = f"{output_name}/{digest}"
    point_uri = (
        "urn:acquirium:derived:"
        f"{quote(app_name, safe='')}:{quote(output_name, safe='')}:{digest}"
    )
    return ref_name, point_uri


class MappedApp(App):
    """Base class for applying one transformation independently to many streams.

    Subclasses provide a semantic query whose ``input_alias`` may match any
    number of points, an :class:`OutputTemplate`, and ``transform``. Acquirium
    deterministically declares one derived stream per matched input and records
    direct ``isCalculatedFrom`` lineage for each pair.
    """

    input_alias: str = "sensor"
    output: OutputTemplate
    # Optional query fetch bound. ``None`` fetches the selected time range.
    fetch_limit: int | None = None
    cast_value: str | None = "float"

    def _template(self) -> OutputTemplate:
        template = getattr(self, "output", None)
        if not isinstance(template, OutputTemplate):
            raise TypeError("MappedApp subclasses must define output = OutputTemplate(...)")
        return template

    def validate_definition(self) -> None:
        self._template()
        if not isinstance(self.input_alias, str) or not self.input_alias:
            raise ValueError("MappedApp.input_alias must be a non-empty string")
        if self.fetch_limit is not None and self.fetch_limit <= 0:
            raise ValueError("MappedApp.fetch_limit must be positive or None")

    def _input_query(self, queries: dict[str, Any]) -> Any:
        query = queries.get("default") or (
            next(iter(queries.values())) if queries else None
        )
        if query is None:
            raise ValueError("MappedApp query bundle is empty")
        return query

    def _bindings(self, queries: dict[str, Any]) -> list[Any]:
        query = self._input_query(queries)
        data = query.data(cast_value=self.cast_value)
        bindings = [b for b in data.bindings if b.alias == self.input_alias]
        if not bindings:
            graph = getattr(query, "query_graph", None)
            node_id = getattr(graph, "aliases", {}).get(self.input_alias)
            if node_id in getattr(graph, "data_nodes", {}):
                return []
            available = sorted({b.alias for b in data.bindings})
            raise ValueError(
                f"MappedApp input alias {self.input_alias!r} matched no streams; "
                f"available data aliases: {available}"
            )
        # Multiple storage refs can represent one semantic point. Choose the
        # first deterministic binding because mapped identity is point-based.
        by_point: dict[str, Any] = {}
        for binding in sorted(bindings, key=lambda b: (b.point_uri, b.ref_uri)):
            if binding.point_uri.startswith(
                f"urn:acquirium:derived:{quote(self.name, safe='')}:"
            ):
                continue
            by_point.setdefault(binding.point_uri, binding)
        return list(by_point.values())

    def resolve_output_specs(self, queries: dict[str, Any]) -> list[AppOutputSpec]:
        """Resolve current input matches into deterministic output declarations."""
        template = self._template()
        specs: list[AppOutputSpec] = []
        for mapping in self.resolve_mappings(queries):
            unit = template.unit
            if unit == SAME_AS_INPUT:
                unit = mapping.input_unit
            specs.append(AppOutputSpec(
                kind="timeseries",
                ref_name=mapping.output_ref_name,
                point_uri=mapping.output_point_uri,
                value_kind=template.value_kind,
                quantity_kind=template.quantity_kind,
                unit=unit,
                data_source=template.data_source,
                storage_backend=template.storage_backend,
                depends_on=[mapping.input_point_uri],
            ))
        return specs

    def resolve_mappings(self, queries: dict[str, Any]) -> list[StreamMapping]:
        """Resolve selector matches into stable input/output stream pairs."""
        template = self._template()
        mappings: list[StreamMapping] = []
        for binding in self._bindings(queries):
            ref_name, point_uri = mapped_output_identity(
                self.name, template.name, binding.point_uri
            )
            mappings.append(StreamMapping(
                input_point_uri=binding.point_uri,
                input_ref_uri=binding.ref_uri,
                input_unit=binding.property_unit or binding.ref_unit,
                output_point_uri=point_uri,
                output_ref_name=ref_name,
            ))
        return mappings

    def input_data(self, ctx: AppContext) -> Any:
        """Fetch this run's selected inputs; override for custom window policy."""
        return ctx.query.data(
            start=ctx.start,
            end=ctx.end,
            limit=self.fetch_limit,
            order="desc" if self.fetch_limit is not None else "asc",
            cast_value=self.cast_value,
        )

    @abstractmethod
    def transform(self, stream: MappedStream, ctx: AppContext) -> Any:
        """Return rows or a ``[time, value]`` Polars DataFrame for one input."""
        raise NotImplementedError

    def streams(self, ctx: AppContext) -> list[MappedStream]:
        """Fetch and materialize the independently transformed input streams.

        This is public primarily for interactive debugging and tests. The
        production ``run`` path uses the same method, so a stream inspected in
        ``acquirium app debug`` has exactly the shape passed to ``transform``.
        """
        data = self.input_data(ctx)
        binding_by_point: dict[str, Any] = {}
        for binding in sorted(
            (b for b in data.bindings if b.alias == self.input_alias),
            key=lambda b: (b.point_uri, b.ref_uri),
        ):
            binding_by_point.setdefault(binding.point_uri, binding)
        streams: list[MappedStream] = []
        for input_point_uri, frame in data.iter(self.input_alias):
            if input_point_uri.startswith(
                f"urn:acquirium:derived:{quote(self.name, safe='')}:"
            ):
                continue
            binding = binding_by_point.get(input_point_uri)
            input_unit = None
            if binding is not None:
                input_unit = binding.property_unit or binding.ref_unit
            ref_name, point_uri = mapped_output_identity(
                self.name, self._template().name, input_point_uri
            )
            streams.append(MappedStream(
                input_alias=self.input_alias,
                input_point_uri=input_point_uri,
                input_ref_uri=binding.ref_uri if binding is not None else None,
                input_unit=input_unit,
                output_point_uri=point_uri,
                output_ref_name=ref_name,
                values=frame,
            ))
        return streams

    def run(self, ctx: AppContext) -> list[Output]:
        outputs: list[Output] = []
        for stream in self.streams(ctx):
            transformed = self.transform(stream, ctx)
            out = self.wrap_transform_result(stream, transformed)
            if out is not None:
                outputs.append(out)
        return outputs

    def wrap_transform_result(self, stream: MappedStream, transformed: Any) -> Output | None:
        """Normalize one ``transform()`` return value into an ``Output``.

        Shared by the preview ``run()`` path above and the continuous
        runtime's per-stream dispatch (``AppRunner._transform_mapped``), so
        both accept the same three return shapes: ``None`` (skip), an
        ``Output`` returned directly, or rows / a ``[time, value]`` Polars
        DataFrame wrapped into a timeseries output on this stream's
        deterministic destination.
        """
        if transformed is None:
            return None
        if isinstance(transformed, Output):
            return transformed
        if hasattr(transformed, "select") and hasattr(transformed, "iter_rows"):
            missing = {"time", "value"} - set(transformed.columns)
            if missing:
                raise ValueError(
                    f"MappedApp.transform DataFrame is missing columns {sorted(missing)}"
                )
            rows: Iterable[tuple[Any, Any]] = transformed.select("time", "value").iter_rows()
        else:
            rows = transformed
        return Output.timeseries(
            point_uri=stream.output_point_uri,
            ref_name=stream.output_ref_name,
            rows=rows,
        )

    def resolve_deletes(self, stream: MappedStream, deleted_timestamps: list[Any]) -> list[Output]:
        """Continuous-runtime hook: how an input delete propagates to this
        stream's output. Default: retract the same timestamps on the
        output (continuous_batch.md's "Mapped transformations propagate
        deletes unless explicitly resolved otherwise"). Override to
        suppress propagation (return ``[]``) or transform it differently.
        """
        return [Output.delete(
            point_uri=stream.output_point_uri,
            ref_name=stream.output_ref_name,
            timestamps=deleted_timestamps,
        )]
