"""Explicit declarations and readers for models from any Python library."""
from __future__ import annotations

from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Protocol

from acquirium.ExperimentValues import parquet, json_value, sampled_values

from acquirium.ExperimentDeclarations import Declaration as Component, validate_declaration
UNSET = object()


class ModelAdapter(Protocol):
    def components(self) -> Iterable[Component]: ...
    def read(self, component: Component) -> Any: ...
    def sample(self, component: Component, *, model_time: Any = UNSET) -> Any: ...


def read_component(adapter: ModelAdapter, component: Component, *, sampling=False, model_time=UNSET, prepare=None):
    """Use one labeled error and validation path for preview and recording."""
    try:
        value = adapter.sample(component, model_time=model_time) if sampling else adapter.read(component)
        if prepare is not None:
            return prepare(value)
        if component.kind == "table":
            parquet(value)
            return value
        if component.kind == "object":
            from acquirium.ObjectCodecs import encode, load_codec
            encode(load_codec(component.codec), value, label=component.label)
            return value
        return sampled_values(component, value) if sampling else json_value(component.kind, value, label=repr(component.label))
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

    def object(self, label, *, read, codec, metadata=None):
        """A value `read()` returns, saved through `codec`; see acquirium.ObjectCodecs."""
        return self._owner._declare_object(label, read=read, codec=codec, metadata=metadata, role=self._role)

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

    def _declare_object(self, label, *, read, codec, metadata=None, role="output"):
        from acquirium.ObjectCodecs import codec_path
        component = Component(label, "object", codec=codec_path(codec), media_type=codec.media_type,
            metadata={} if metadata is None else metadata, role=role)
        return self.add(component, read=read)

    def add(self, component: Component, *, read=None, read_at=None) -> Component:
        """Low-level registration, also supporting explicitly keyed components."""
        if self._frozen:
            raise RuntimeError("this declared adapter is frozen; build a new adapter to change declarations")
        component = validate_declaration(component, model=True)
        if component.label in self._bindings:
            raise ValueError(f"duplicate component label {component.label!r}")
        if (read is None) == (read_at is None):
            raise ValueError("supply exactly one of read or read_at")
        if read_at is not None and component.kind != "timeseries":
            raise ValueError("read_at is only valid for time-series declarations")
        if not callable(read if read is not None else read_at):
            raise TypeError("readers must be callable")
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
