"""Validation at the Arrow compute boundary."""
from __future__ import annotations
import pyarrow as pa
from acquirium.Materialization.context import ComputeRequest

OUTPUT_COLUMNS = ("ref_uri", "ts", "numeric_value", "text_value")

class OutputValidationError(ValueError):
    pass

def validate_output(table: pa.Table, request: ComputeRequest) -> pa.Table:
    missing = set(OUTPUT_COLUMNS) - set(table.column_names)
    if missing:
        raise OutputValidationError(f"output is missing columns: {sorted(missing)}")
    result = table.select(OUTPUT_COLUMNS)
    refs, timestamps = result.column("ref_uri").to_pylist(), result.column("ts").to_pylist()
    for ref, timestamp in zip(refs, timestamps):
        if ref not in request.output_refs:
            raise OutputValidationError(f"output {ref!r} is not owned by this binding")
        if timestamp is None or not (request.context.interval.start <= timestamp < request.context.interval.end):
            raise OutputValidationError("output timestamp lies outside the partition range")
    return result
