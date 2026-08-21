"""Small public declaration API. Declarations compile to immutable specs."""

from __future__ import annotations

from typing import Any, Callable, TypeVar, overload

from acquirium.Materialization.bindings import Selector
from acquirium.Materialization.definitions import MaterializationDefinition, definition_for
from acquirium.Materialization.impact import ImpactPolicy, pointwise

F = TypeVar("F", bound=Callable[..., Any])


class _Outputs:
    def per_input(self, **options: Any) -> dict[str, Any]:
        return {"mode": "per_input", **options}


outputs = _Outputs()


def select(**criteria: Any) -> Selector:
    return Selector(criteria)


def transform(*, name: str | None = None, inputs: object | None = None, bind: object | None = None,
              outputs: object | None = None, impact: ImpactPolicy | None = None,
              parameters_schema: dict[str, Any] | None = None) -> Callable[[F], F]:
    """Attach a serializable transformation definition to a callable.

    Scalar per-input functions default to pointwise impact; batch definitions
    must explicitly declare impact, preventing accidental under-invalidation.
    """
    if inputs is not None and bind is not None:
        raise ValueError("declare either inputs or bind, not both")
    if inputs is None and bind is None:
        raise ValueError("a transformation requires inputs or bind")
    if outputs is None:
        raise ValueError("a transformation requires outputs")
    def decorate(function: F) -> F:
        resolved_impact = impact
        if resolved_impact is None and inputs is not None and isinstance(outputs, dict) and outputs.get("mode") == "per_input":
            resolved_impact = pointwise()
        if resolved_impact is None:
            raise ValueError("batch and multi-input transformations must declare impact")
        definition = definition_for(function, name=name or function.__name__, inputs=inputs, bind=bind,
                                    outputs=outputs, impact=resolved_impact,
                                    parameters_schema=parameters_schema or {})
        setattr(function, "__acquirium_definition__", definition)
        return function
    return decorate


class StatefulTransformation:
    """Base marker for stateful transformations; durable state is external."""

    __acquirium_definition__: MaterializationDefinition
