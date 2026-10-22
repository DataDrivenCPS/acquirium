"""Explicit declarations and readers for models from any Python library."""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Protocol

from acquirium.ExperimentValues import check_kind, to_json

UNSET = object()
# An encoded JSON object keeps arbitrary keys/nulls/empty containers separate
# from the graph writer's attribute names and deletion semantics.
ADAPTER_FORMAT = "_adapter_format"


def metadata_fields(value: Mapping[str, Any]) -> dict[str, Any]:
    """Namespace arbitrary JSON; the shared metadata backend preserves it."""
    return {ADAPTER_FORMAT: 2, "metadata": deepcopy(value)}


@dataclass(frozen=True)
class Component:
    """One stable experiment declaration; readers live separately."""
    label: str
    kind: str
    unit: str | None = None
    keys: tuple[str, ...] = ()
    observed: Mapping[tuple, str] | str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    role: str = "output"
    quantity_kind: str | None = None
    source_path: str | None = None
    description: str | None = None
    source_unit: str | None = None


class ModelAdapter(Protocol):
    def components(self) -> Iterable[Component]: ...
    def read(self, component: Component) -> Any: ...
    def sample(self, component: Component, *, model_time: Any = UNSET) -> Any: ...


def validate_value(component: Component, value: Any) -> Any:
    """Normalize and validate a reader result without writing anything."""
    if component.kind == "table":
        from acquirium.Experiments import _parquet
        _parquet(value)
        return value
    if component.kind == "timeseries":
        from acquirium.Experiments import _stream_key
        if component.keys:
            if not isinstance(value, Mapping):
                raise TypeError("a keyed time-series reader must return a key-to-number mapping")
            return {_stream_key(key, component.keys): _number(item) for key, item in value.items()}
        return _number(value)
    value = to_json(value)
    check_kind(component.kind, value, label=repr(component.label))
    return value


def _number(value):
    value = to_json(value)
    check_kind("scalar", value)
    return value


def read_component(adapter: ModelAdapter, component: Component, *, sampling=False, model_time=UNSET):
    """Use one labeled error and validation path for preview and recording."""
    try:
        value = adapter.sample(component, model_time=model_time) if sampling else adapter.read(component)
        return validate_value(component, value)
    except Exception as error:
        details = f", source={component.source_path!r}" if component.source_path else ""
        if model_time is not UNSET:
            details += f", model_time={model_time!r}"
        raise ValueError(f"could not read {component.label!r}{details}: {error}") from error


@dataclass(frozen=True)
class _ReaderBinding:
    component: Component
    read: Callable[[], Any] | None
    read_at: Callable[[Any], Any] | None


class _Declarations:
    """Role-specific view sharing one owner's declarations."""
    def __init__(self, owner, role):
        self._owner, self._role = owner, role

    def scalar(self, label, source=UNSET, *, read=None, unit=UNSET, quantity_kind=None, metadata=None):
        return self._owner._declare("scalar", label, source, read=read, unit=unit,
            quantity_kind=quantity_kind, metadata=metadata, role=self._role)

    def text(self, label, source=UNSET, *, read=None, metadata=None):
        return self._owner._declare("text", label, source, read=read, metadata=metadata, role=self._role)

    def json(self, label, source=UNSET, *, read=None, metadata=None):
        return self._owner._declare("json", label, source, read=read, metadata=metadata, role=self._role)

    def table(self, label, source=UNSET, *, read=None, metadata=None):
        return self._owner._declare("table", label, source, read=read, metadata=metadata, role=self._role)

    def timeseries(self, label, source=UNSET, *, read=None, read_at=None, unit=UNSET,
                   quantity_kind=None, observed=None, metadata=None):
        return self._owner._declare("timeseries", label, source, read=read, read_at=read_at,
            unit=unit, quantity_kind=quantity_kind, observed=observed, metadata=metadata, role=self._role)


class AdapterDefinition(_Declarations):
    """Declare variables explicitly, with output as the default namespace."""
    def __init__(self):
        super().__init__(self, "output")
        self._bindings: dict[str, _ReaderBinding] = {}
        self.input = _Declarations(self, "input")
        self.output = _Declarations(self, "output")
        self._frozen = False

    def _declare(self, kind, label, source=UNSET, *, read=None, read_at=None, unit=UNSET,
                 quantity_kind=None, observed=None, metadata=None, role="output"):
        if source is not UNSET:
            raise TypeError("a general adapter takes read=; use adapters.pyomo(model) for Pyomo sources")
        component = Component(label, kind, unit=None if unit is UNSET else unit,
            observed=observed, metadata={} if metadata is None else metadata,
            role=role, quantity_kind=quantity_kind)
        return self.add(component, read=read, read_at=read_at)

    def add(self, component: Component, *, read=None, read_at=None) -> Component:
        """Low-level registration, also supporting explicitly keyed components."""
        from acquirium.Experiments import _stream_keys
        if self._frozen:
            raise RuntimeError("this declared adapter is frozen; build a new adapter to change declarations")
        if not isinstance(component.label, str) or not component.label.strip():
            raise ValueError("a non-empty label is required")
        if component.label in self._bindings:
            raise ValueError(f"duplicate component label {component.label!r}")
        if component.role not in {"input", "output"}:
            raise ValueError("adapter declarations must be inputs or outputs")
        if component.kind not in {"scalar", "text", "json", "table", "timeseries"}:
            raise ValueError(f"unsupported model component kind {component.kind!r}")
        _stream_keys(component.kind, {"keys": component.keys})
        if (read is None) == (read_at is None):
            raise ValueError("supply exactly one of read or read_at")
        if read_at is not None and component.kind != "timeseries":
            raise ValueError("read_at is only valid for time-series declarations")
        if not callable(read if read is not None else read_at):
            raise TypeError("readers must be callable")
        payload = to_json(component.metadata, path=f"{component.label}.metadata")
        if not isinstance(payload, dict):
            raise TypeError("metadata must be a string-keyed JSON object")
        from dataclasses import replace
        component = replace(component, metadata=payload, keys=tuple(component.keys))
        self._bindings[component.label] = _ReaderBinding(deepcopy(component), read, read_at)
        return deepcopy(component)

    def components(self):
        return [deepcopy(binding.component) for binding in self._bindings.values()]

    def read(self, component):
        binding = self._bindings[component.label]
        if component.kind == "timeseries":
            raise TypeError("use sample() to read time-series declarations")
        return binding.read()

    def sample(self, component, *, model_time=UNSET):
        binding = self._bindings[component.label]
        if component.kind != "timeseries":
            raise TypeError("sample() reads time-series declarations")
        if binding.read_at is not None:
            if model_time is UNSET:
                raise ValueError("this reader requires model_time")
            return binding.read_at(model_time)
        return binding.read()

    def freeze(self):
        """Freeze descriptors/readers while retaining their live model references."""
        frozen = AdapterDefinition()
        frozen._bindings = {label: _ReaderBinding(deepcopy(binding.component), binding.read, binding.read_at)
                            for label, binding in self._bindings.items()}
        frozen._frozen = True
        return frozen

    def frame(self):
        """Describe declarations without reading model values or contacting a server."""
        from acquirium.ExperimentMetadata import records_frame
        return records_frame([dict(label=c.label, role=c.role, kind=c.kind, unit=c.unit,
            quantity_kind=c.quantity_kind, source_path=c.source_path, description=c.description,
            reader="indexed" if self._bindings[c.label].read_at is not None else "current",
            metadata=deepcopy(c.metadata)) for c in self.components()])

    def preview(self, *, model_time=UNSET):
        """Read and validate all values locally, preserving declaration order."""
        return {c.label: read_component(self, c, sampling=c.kind == "timeseries", model_time=model_time)
                for c in self.components()}
