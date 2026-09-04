"""Public declaration API for authoring apps."""
from dataclasses import asdict
from typing import Any, Mapping
from acquirium.Materialization.incremental import App, OutputSpec, align, output

__all__ = ["App", "OutputSpec", "align", "output"]


def _coerce_output_spec(value: Any) -> OutputSpec:
    if isinstance(value, OutputSpec):
        return value
    if not isinstance(value, Mapping): raise TypeError("output declarations must be OutputSpec values")
    return OutputSpec(**value)


def _output_spec_dict(value: Any) -> dict[str, Any]:
    """Private JSON conversion used by the graph compiler."""
    return asdict(_coerce_output_spec(value))
