# acquirium/TextMatch/query_decorators.py

from dataclasses import dataclass
from typing import Any, Callable, Concatenate, ParamSpec, Sequence, TypeVar
import functools
from rdflib import URIRef

P = ParamSpec("P")
T = TypeVar("T")

@dataclass(frozen=True)
class FlexSpec:
    arg: str
    kind: str = "any"

def flex_query_rdf_inputs(
    *, specs: Sequence[FlexSpec],
) -> Callable[[Callable[Concatenate[Any, P], T]], Callable[Concatenate[Any, P], T]]:
    def deco(fn: Callable[Concatenate[Any, P], T]) -> Callable[Concatenate[Any, P], T]:
        @functools.wraps(fn)
        def wrapper(self: Any, *args: P.args, **kwargs: P.kwargs) -> T:
            for spec in specs:
                if spec.arg not in kwargs:
                    continue
                v = kwargs.get(spec.arg)
                if v is None:
                    continue

                if isinstance(v, list):
                    kwargs[spec.arg] = []
                    for x in v:
                        if isinstance(x, URIRef):
                            kwargs[spec.arg].append(x)
                        elif "http" in x or "urn:" in x:
                            kwargs[spec.arg].append(URIRef(x))
                        else:
                            kwargs[spec.arg].append(self._resolve_rdf(x, spec.kind))
                elif isinstance(v, URIRef):
                    kwargs[spec.arg] = v
                elif "http" in v or "urn:" in v:
                    kwargs[spec.arg] = URIRef(v)
                else: 
                    kwargs[spec.arg] = self._resolve_rdf(v, spec.kind)

            return fn(self, *args, **kwargs)
        return wrapper  # type: ignore[return-value]
    return deco
