"""Explicit Pyomo declarations backed by the general adapter builder."""
from dataclasses import replace
from copy import deepcopy

from acquirium.ModelAdapters import AdapterDefinition, UNSET


class PyomoAdapter(AdapterDefinition):
    def __init__(self, model):
        try:
            import pyomo.environ as pyo
        except ImportError as error:
            raise ImportError("the Pyomo adapter requires acquirium[pyomo]") from error
        super().__init__()
        self.model, self.pyo = model, pyo

    def _declare(self, kind, label, source=UNSET, *, read=None, read_at=None, unit=UNSET,
                 quantity_kind=None, observed=None, metadata=None, role="output"):
        if any(reader is not None and not callable(reader) for reader in (read, read_at)):
            raise TypeError("readers must be callable")
        pyo = self.pyo
        source_unit, path, description = None, None, None
        if source is not UNSET:
            if not hasattr(source, "is_indexed"):
                raise TypeError("a Pyomo source must be a component; use read= for other values")
            if source.model() is not self.model.model():
                raise ValueError("the source belongs to a different Pyomo model")
            path, description = source.name, getattr(source, "doc", None)
            if read is None and read_at is None:
                if source.is_indexed():
                    raise ValueError(f"{label!r}: indexed Pyomo components require an explicit read or read_at reader")
                read = lambda: pyo.value(source)
            if kind in {"scalar", "timeseries"}:
                try:
                    if hasattr(source, "get_units"):
                        found = source.get_units()
                    else:
                        sample = next(iter(source.values()), None) if source.is_indexed() else source
                        found = pyo.units.get_units(sample) if sample is not None else None
                    if found is not None and str(found) != "dimensionless":
                        source_unit = str(found)
                except ImportError as error:
                    raise ImportError("reading Pyomo units requires acquirium[pyomo]") from error
        if unit is UNSET:
            unit = source_unit
        elif unit is None and source_unit is not None:
            raise ValueError(f"{label!r} has units {source_unit}; an override cannot discard known units")

        def evaluate(reader):
            if reader is None:
                return None
            if kind in {"scalar", "timeseries", "text"}:
                return lambda *args: pyo.value(reader(*args))
            return reader

        component = super()._declare(kind, label, read=evaluate(read), read_at=evaluate(read_at),
            unit=unit, quantity_kind=quantity_kind, observed=observed, metadata=metadata, role=role)
        component = replace(component, source_path=path, description=description, source_unit=source_unit)
        self._bindings[label] = replace(self._bindings[label], component=deepcopy(component))
        return component
