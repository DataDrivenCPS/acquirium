"""Small public declaration API. Declarations compile to immutable specs."""

from __future__ import annotations

from abc import ABC, abstractmethod
import inspect
from typing import Any, ClassVar, Literal, Mapping

from dataclasses import dataclass

from acquirium.Materialization.definitions import MaterializationDefinition, definition_for
from acquirium.Materialization.impact import ImpactPolicy, pointwise


class _Outputs:
    def stream(self, **options: Any) -> "OutputSpec":
        """Describe one logical output exposed by a transformation.

        Output names belong to the transformation's ``outputs`` mapping.  The
        spec only carries stream metadata and, optionally, a fixed URI.  A
        worker turns the spec into an :class:`OutputStream` with
        ``context.outputs.declare(...)``.
        """
        return OutputSpec(**options)

@dataclass(frozen=True)
class OutputSpec:
    """Metadata used to plan and validate one logical output stream."""

    value_kind: str = "numeric"
    unit: str | None = None
    quantity_kind: str | None = None
    ref_uri: str | None = None
    prefix: str | None = None

    def __post_init__(self) -> None:
        if self.value_kind not in {"numeric", "text"}:
            raise ValueError("output value_kind must be 'numeric' or 'text'")
        for name in ("unit", "quantity_kind", "ref_uri", "prefix"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"output {name} must be a string or None")


outputs = _Outputs()


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

    Subclasses implement pure :meth:`build_query` and :meth:`transform`; the
    immutable definition is attached when the class is created, so it deploys
    through ``aq.deploy_transformation``.
    """

    name: ClassVar[str | None] = None
    invocation: ClassVar[Literal["whole_query", "per_row"]] = "whole_query"
    outputs: ClassVar[dict[str, OutputSpec | Mapping[str, Any]] | None] = None
    impact: ClassVar[ImpactPolicy | None] = None
    parameters_schema: ClassVar[dict[str, Any]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if inspect.isabstract(cls):
            return
        if not callable(getattr(cls, "build_query", None)):
            raise ValueError("a transformation must implement build_query(self, aq)")
        if cls.outputs is None:
            raise ValueError("a transformation requires outputs")
        if not isinstance(cls.outputs, dict) or not cls.outputs:
            raise ValueError("outputs must be a non-empty name-to-output-spec mapping")
        if cls.invocation not in {"whole_query", "per_row"}:
            raise ValueError("invocation must be 'whole_query' or 'per_row'")
        resolved_impact = cls.impact or pointwise()
        definition = definition_for(
            cls,
            name=cls.name or cls.__name__,
            outputs=cls.outputs,
            impact=resolved_impact,
            invocation=cls.invocation,
            parameters_schema=cls.parameters_schema,
        )
        setattr(cls, "__acquirium_definition__", definition)

    @abstractmethod
    def build_query(self, aq: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def transform(self, inputs: Any, context: Any) -> Any:
        raise NotImplementedError


class StatefulTransformation(ABC):
    """Base for artifact-backed class transformations.

    ``setup_worker`` and decoded state are disposable caches.  Only bytes from
    an immutable artifact revision are authoritative.
    """

    name: ClassVar[str | None] = None
    invocation: ClassVar[Literal["whole_query", "per_row"]] = "whole_query"
    outputs: ClassVar[dict[str, OutputSpec | Mapping[str, Any]] | None] = None
    impact: ClassVar[ImpactPolicy | None] = None
    parameters_schema: ClassVar[dict[str, Any]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if inspect.isabstract(cls):
            return
        if not callable(getattr(cls, "build_query", None)):
            raise ValueError("a stateful transformation must implement build_query(self, aq)")
        if cls.outputs is None:
            raise ValueError("a stateful transformation requires outputs")
        if not isinstance(cls.outputs, dict) or not cls.outputs:
            raise ValueError("outputs must be a non-empty name-to-output-spec mapping")
        if cls.invocation not in {"whole_query", "per_row"}:
            raise ValueError("invocation must be 'whole_query' or 'per_row'")
        resolved_impact = cls.impact or pointwise()
        definition = definition_for(
            cls,
            name=cls.name or cls.__name__,
            outputs=cls.outputs,
            impact=resolved_impact,
            invocation=cls.invocation,
            parameters_schema=cls.parameters_schema,
        )
        setattr(cls, "__acquirium_definition__", definition)

    def setup_worker(self):
        return None

    def load_artifact(self, artifact: bytes, worker: Any):
        return artifact

    @abstractmethod
    def build_query(self, aq: Any):
        raise NotImplementedError

    @abstractmethod
    def transform(self, batch: Any, state: Any, context: Any):
        raise NotImplementedError

    __acquirium_definition__: MaterializationDefinition
