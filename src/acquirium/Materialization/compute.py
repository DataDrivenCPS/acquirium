"""First compute adapter: trusted Python code over Arrow tables."""
from __future__ import annotations
from typing import Any, Callable, Protocol
import pyarrow as pa
from acquirium.Materialization.context import ComputeRequest
from acquirium.Materialization.validation import validate_output

class ComputeAdapter(Protocol):
    def execute(self, target: Callable[..., Any], request: ComputeRequest) -> pa.Table: ...

class PythonArrowAdapter:
    """Execute either an explicit Arrow function or scalar pointwise callable."""
    def execute(self, target: Callable[..., Any], request: ComputeRequest) -> pa.Table:
        if request.scalar:
            result = self._scalar(target, request)
        else:
            result = target(request.inputs, request.context)
            if not isinstance(result, pa.Table):
                raise TypeError("batch transformations must return a pyarrow.Table")
        return validate_output(result, request)

    def _scalar(self, target: Callable[[Any], Any], request: ComputeRequest) -> pa.Table:
        if len(request.output_refs) != 1:
            raise ValueError("scalar adapter requires exactly one owned output stream")
        output_ref = next(iter(request.output_refs))
        numeric = request.inputs.column("numeric_value").to_pylist()
        text = request.inputs.column("text_value").to_pylist()
        values = [number if number is not None else string for number, string in zip(numeric, text)]
        produced = [target(value) for value in values]
        if any(value is not None and (isinstance(value, bool) or not isinstance(value, (float, int, str))) for value in produced):
            raise TypeError("scalar transformations must return float, int, str, or None")
        numeric_values = [float(value) if isinstance(value, (float, int)) and not isinstance(value, bool) else None for value in produced]
        text_values = [value if isinstance(value, str) else None for value in produced]
        return pa.table({"ref_uri": [output_ref] * len(produced), "ts": request.inputs.column("ts"),
                         "numeric_value": numeric_values, "text_value": text_values})
