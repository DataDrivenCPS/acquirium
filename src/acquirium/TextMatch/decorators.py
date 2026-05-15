# acquirium/TextMatch/query_decorators.py

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
            for spec in specs:
                if spec.arg not in arguments:
                    continue
                v = arguments[spec.arg]
                if v is None:
                    continue
                arguments[spec.arg] = (
                    [_coerce(self, x, spec.kind) for x in v]
                    if isinstance(v, list)
                    else _coerce(self, v, spec.kind)
                )

            return fn(*bound.args, **bound.kwargs)
        return wrapper  # type: ignore[return-value]
    return deco
