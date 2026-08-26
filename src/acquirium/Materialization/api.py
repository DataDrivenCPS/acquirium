"""Public declaration API for revision-frontier transformations."""
from dataclasses import asdict
from typing import Any, Mapping
from acquirium.Materialization.incremental import (
    AllAvailable, AroundChange, Changed, Current, Every, OnChange, OutputSpec,
    RowWiseTransformation, Transformation, outputs,
)

__all__ = ["AllAvailable", "AroundChange", "Changed", "Current", "Every", "OnChange", "OutputSpec", "RowWiseTransformation", "Transformation", "outputs"]


def _coerce_output_spec(value: Any) -> OutputSpec:
    if isinstance(value, OutputSpec):
        return value
    if value is None: return OutputSpec()
    if not isinstance(value, Mapping): raise TypeError("output declarations must be OutputSpec values")
    return OutputSpec(**value)


def _output_spec_dict(value: Any) -> dict[str, Any]:
    """Private JSON conversion used by the graph compiler."""
    return asdict(_coerce_output_spec(value))
