"""Explicit model adapters; optional model libraries are imported lazily."""
from acquirium.ModelAdapters import AdapterDefinition, Component, ModelAdapter


def define() -> AdapterDefinition:
    """Create an empty adapter with input/output namespaces and explicit readers."""
    return AdapterDefinition()


def pyomo(model, **legacy_options):
    """Create an explicit Pyomo declaration builder, initially empty.

    Declare scalar/text/JSON/table values and time series on this object or
    its input/output namespaces. A Pyomo source supplies units, description,
    and model path. Indexed sources require an explicit reader.
    """
    if legacy_options:
        raise TypeError("automatic Pyomo adapter options are no longer supported; declare components explicitly and use sample(at=..., model_time=...)")
    from ._pyomo import PyomoAdapter
    return PyomoAdapter(model)
