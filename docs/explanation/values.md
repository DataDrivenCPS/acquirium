---
title: Numeric observations
---

The initial Escapement-backed Acquirium runtime accepts finite non-null numeric
observations. Text, booleans, NaN, and infinity are rejected before admission.
There is no text fallback store or automatic null-to-delete conversion. Delete
observations explicitly by stream and UTC nanosecond timestamp.

Tabular drivers parse numeric cells and log skipped missing/nonfinite source
cells. Unsupported text cells fail parsing. Driver authors should explicitly
configure columns to ingest; the transport receives normalized numeric values.

Timeseries Arrow responses use Float64 values and `timestamp[ns, UTC]` timestamps.
The current DataObject presentation retains `value_numeric`/`value_text` columns;
server-sourced values occupy `value_numeric` and the text column is null.
Unit conversion uses frozen multiplier/offset factors. `value_mode` accepts
`default` or `numeric`; no text/coalescing projection is supported by the engine.
