# acquirium/TextMatch/query_decorators.py

from dataclasses import dataclass
from typing import Any, Callable, Sequence
import functools
from rdflib import URIRef

@dataclass(frozen=True)
class FlexSpec:
    arg: str
    kind: str = "any"

def flex_query_rdf_inputs(*, specs: Sequence[FlexSpec]):
    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
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
        return wrapper
    return deco
