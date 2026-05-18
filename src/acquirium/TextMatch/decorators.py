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


def _coerce(self: Any, v: Any, kind: str) -> Any:
    if isinstance(v, URIRef):
        return v
    if _looks_like_uri(v):
        return URIRef(v)
    return self._resolve_rdf(v, kind)


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
            present = [
                s for s in specs if s.arg in arguments and arguments[s.arg] is not None
            ]

            # Scalar text specs are resolved jointly so related siblings
            # disambiguate each other (e.g. a quantity kind steers an
            # ambiguous unit). URI/URIRef values and list values bypass the
            # joint call and coerce element-wise.
            record = {
                s.arg: (arguments[s.arg], s.kind)
                for s in present
                if not isinstance(arguments[s.arg], (list, URIRef))
                and not _looks_like_uri(arguments[s.arg])
            }
            resolved = self._resolve_record(record) if record else {}

            for s in present:
                v = arguments[s.arg]
                if isinstance(v, list):
                    arguments[s.arg] = [_coerce(self, x, s.kind) for x in v]
                elif isinstance(v, URIRef):
                    arguments[s.arg] = v
                elif _looks_like_uri(v):
                    arguments[s.arg] = URIRef(v)
                else:
                    uri = resolved.get(s.arg)
                    if uri is None:
                        raise ValueError(f"Could not resolve {v!r} as {s.kind}")
                    arguments[s.arg] = uri

            return fn(*bound.args, **bound.kwargs)
        return wrapper  # type: ignore[return-value]
    return deco
