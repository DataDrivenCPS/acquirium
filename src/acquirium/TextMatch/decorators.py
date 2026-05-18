from dataclasses import dataclass
import inspect
from typing import Any, Callable, Concatenate, ParamSpec, Sequence, TypeVar
import functools
from rdflib import URIRef

P = ParamSpec("P")
T = TypeVar("T")

@dataclass(frozen=True)
class FlexSpec:
    arg: str
    kind: str = "any"


def _looks_like_uri(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(("http://", "https://", "urn:"))


def _coerce(self: Any, v: Any, kind: str, context: list[str] | None = None) -> Any:
    if isinstance(v, URIRef):
        return v
    if _looks_like_uri(v):
        return URIRef(v)
    return self._resolve_rdf(v, kind, context)


def _as_context_uris(value: Any) -> list[str]:
    """Collect already-resolved URI(s) from a coerced argument value.

    ``_coerce`` yields a ``URIRef`` (passthrough) or a URI string (resolved by
    ``_resolve_rdf``); both are usable as sibling disambiguation context.
    """
    out: list[str] = []
    for v in value if isinstance(value, list) else [value]:
        if isinstance(v, URIRef) or _looks_like_uri(v):
            out.append(str(v))
    return out


def flex_query_rdf_inputs(
    *, specs: Sequence[FlexSpec],
) -> Callable[[Callable[Concatenate[Any, P], T]], Callable[Concatenate[Any, P], T]]:
    def deco(fn: Callable[Concatenate[Any, P], T]) -> Callable[Concatenate[Any, P], T]:
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def wrapper(self: Any, *args: P.args, **kwargs: P.kwargs) -> T:
            # Bind positional args to parameter names so a call like
            # `q.filter_by_unit("mg/l")` is treated the same as
            # `q.filter_by_unit(unit="mg/l")` for text-matching purposes.
            try:
                bound = sig.bind_partial(self, *args, **kwargs)
            except TypeError:
                return fn(self, *args, **kwargs)

            arguments = bound.arguments

            def _coerce_spec(spec: FlexSpec, ctx: list[str] | None) -> None:
                v = arguments[spec.arg]
                arguments[spec.arg] = (
                    [_coerce(self, x, spec.kind, ctx) for x in v]
                    if isinstance(v, list)
                    else _coerce(self, v, spec.kind, ctx)
                )

            present = [
                s for s in specs if s.arg in arguments and arguments[s.arg] is not None
            ]

            # Two passes so a unit can be disambiguated by its siblings, the
            # way register_stream does it: resolve non-unit specs first and
            # feed their resolved URIs as context when resolving units (e.g.
            # quantity kind "mass" steers "kg" to KiloGM, not KiloGAUSS). A
            # sibling that fails resolution raises as before (unchanged
            # contract); one that resolves simply joins the context.
            unit_specs = [s for s in present if s.kind == "unit"]
            context: list[str] = []
            for spec in present:
                if spec.kind == "unit":
                    continue
                _coerce_spec(spec, None)
                context.extend(_as_context_uris(arguments[spec.arg]))

            for spec in unit_specs:
                _coerce_spec(spec, context or None)

            return fn(*bound.args, **bound.kwargs)
        return wrapper  # type: ignore[return-value]
    return deco
