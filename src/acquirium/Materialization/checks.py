"""Shared dry-run result rendering and output-window enforcement."""
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.compute as pc

from acquirium.Materialization.models import Binding, InputBatch


def check_entry(binding: Binding) -> dict[str, Any]:
    return {
        "inputs": {alias: [{"ref_uri": d.ref_uri, "label": d.label, "unit": d.unit}
                           for d in streams] for alias, streams in binding.inputs.items()},
        "row": dict(binding.row) if binding.row else None,
        "outputs": {}, "error": None,
    }


def check_outputs(binding: Binding, context: InputBatch,
                  results: Mapping[str, pa.Table], limit: int | None) -> dict[str, Any]:
    outputs = {}
    window = context.output_window or context.changed_window
    for name, port in binding.outputs.items():
        table = results.get(name)
        rows, values = 0, []
        if table is not None:
            table = table.filter(pc.and_(pc.greater_equal(table['time'], pa.scalar(window.start)),
                                         pc.less_equal(table['time'], pa.scalar(window.end))))
            rows = table.num_rows
            shown = table if limit is None else table.slice(0, limit)
            values = [{"time": t.isoformat(), "value": v}
                      for t, v in zip(shown['time'].to_pylist(), shown['value'].to_pylist())]
        outputs[name] = {"stream": port.ref_uri, "ref_name": port.ref_name,
                         "value_kind": port.spec.value_kind, "rows": rows,
                         "assigned": name in results, "truncated": len(values) < rows,
                         "values": values}
    return outputs
