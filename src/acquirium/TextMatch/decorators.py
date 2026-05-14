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
    return isinstance(value, str) and ("http" in value or "urn:" in value)


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

                if isinstance(v, list):
                    resolved = []
                    for x in v:
                        if isinstance(x, URIRef):
                            resolved.append(x)
                        elif _looks_like_uri(x):
                            resolved.append(URIRef(x))
                        else:
                            resolved.append(self._resolve_rdf(x, spec.kind))
                    arguments[spec.arg] = resolved
                elif isinstance(v, URIRef):
                    pass
                elif _looks_like_uri(v):
                    arguments[spec.arg] = URIRef(v)
                else:
                    arguments[spec.arg] = self._resolve_rdf(v, spec.kind)

            return fn(*bound.args, **bound.kwargs)
        return wrapper  # type: ignore[return-value]
    return deco
