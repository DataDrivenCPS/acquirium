"""Small public declaration API. Declarations compile to immutable specs."""

from __future__ import annotations

from abc import ABC, abstractmethod
import inspect
from typing import Any, ClassVar, Literal

from acquirium.Materialization.bindings import Selector
from acquirium.Materialization.definitions import MaterializationDefinition, definition_for
from acquirium.Materialization.impact import ImpactPolicy, pointwise


class _Outputs:
    def per_input(self, **options: Any) -> dict[str, Any]:
        return {"mode": "per_input", **options}


outputs = _Outputs()


def select(**criteria: Any) -> Selector:
    return Selector(criteria)


class _Application(ABC):
    """Common definition hook for class-based bounded and reactive work."""

    name: ClassVar[str | None] = None
    parameters_schema: ClassVar[dict[str, Any]] = {}
    kind: ClassVar[Literal["experiment", "service"]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if inspect.isabstract(cls):
            return
        definition = definition_for(
            cls,
            name=cls.name or cls.__name__,
            kind=cls.kind,
            parameters_schema=cls.parameters_schema,
        )
        setattr(cls, "__acquirium_definition__", definition)


class Experiment(_Application):
    """Class-based bounded application declaration."""

    kind: ClassVar[Literal["experiment"]] = "experiment"

    @abstractmethod
    def run(self, context: Any) -> Any:
        raise NotImplementedError


class Service(_Application):
    """Class-based persistent application declaration."""

    kind: ClassVar[Literal["service"]] = "service"

    @abstractmethod
    def on_change(self, change: Any, context: Any) -> Any:
        raise NotImplementedError


class Transformation(ABC):
    """Class-based stateless transformation declaration.

    Subclasses declare their topology as class attributes and implement
    :meth:`transform`. The immutable definition is attached when the class is
    created, so it deploys through ``aq.deploy_transformation``.
    """

    name: ClassVar[str | None] = None
    inputs: ClassVar[object | None] = None
    bind: ClassVar[object | None] = None
    outputs: ClassVar[object | None] = None
    impact: ClassVar[ImpactPolicy | None] = None
    execution: ClassVar[Literal["batch", "scalar"]] = "batch"
    parameters_schema: ClassVar[dict[str, Any]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if inspect.isabstract(cls):
            return
        if cls.inputs is not None and cls.bind is not None:
            raise ValueError("declare either inputs or bind, not both")
        if cls.inputs is None and cls.bind is None:
            raise ValueError("a transformation requires inputs or bind")
        if cls.outputs is None:
            raise ValueError("a transformation requires outputs")
        if cls.execution not in {"batch", "scalar"}:
            raise ValueError("execution must be 'batch' or 'scalar'")
        resolved_impact = cls.impact or (pointwise() if cls.execution == "scalar" else None)
        if resolved_impact is None:
            raise ValueError("batch and multi-input transformations must declare impact")
        definition = definition_for(
            cls,
            name=cls.name or cls.__name__,
            inputs=cls.inputs,
            bind=cls.bind,
            outputs=cls.outputs,
            impact=resolved_impact,
            execution=cls.execution,
            parameters_schema=cls.parameters_schema,
        )
        setattr(cls, "__acquirium_definition__", definition)

    @abstractmethod
    def transform(self, batch: Any, context: Any) -> Any:
        raise NotImplementedError


class MappedTransformation(Transformation):
    """A class-based transformation resolved into one binding per match."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not inspect.isabstract(cls) and cls.bind is None:
            raise ValueError("a mapped transformation requires bind")


class StatefulTransformation(ABC):
    """Base for artifact-backed class transformations.

    ``setup_worker`` and decoded state are disposable caches.  Only bytes from
    an immutable artifact revision are authoritative.
    """

    name: ClassVar[str | None] = None
    inputs: ClassVar[object | None] = None
    bind: ClassVar[object | None] = None
    outputs: ClassVar[object | None] = None
    impact: ClassVar[ImpactPolicy | None] = None
    parameters_schema: ClassVar[dict[str, Any]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if inspect.isabstract(cls):
            return
        if cls.inputs is not None and cls.bind is not None:
            raise ValueError("declare either inputs or bind, not both")
        if cls.inputs is None and cls.bind is None:
            raise ValueError("a stateful transformation requires inputs or bind")
        if cls.outputs is None:
            raise ValueError("a stateful transformation requires outputs")
        resolved_impact = cls.impact
        if resolved_impact is None and cls.inputs is not None and isinstance(cls.outputs, dict) and cls.outputs.get("mode") == "per_input":
            resolved_impact = pointwise()
        if resolved_impact is None:
            raise ValueError("batch and multi-input transformations must declare impact")
        definition = definition_for(
            cls,
            name=cls.name or cls.__name__,
            inputs=cls.inputs,
            bind=cls.bind,
            outputs=cls.outputs,
            impact=resolved_impact,
            execution="batch",
            parameters_schema=cls.parameters_schema,
        )
        setattr(cls, "__acquirium_definition__", definition)

    def setup_worker(self):
        return None

    def load_artifact(self, artifact: bytes, worker: Any):
        return artifact

    @abstractmethod
    def transform(self, batch: Any, state: Any, context: Any):
        raise NotImplementedError

    __acquirium_definition__: MaterializationDefinition
