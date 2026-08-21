"""InputBatch: the pinned, materialized input for one continuous-batch turn.

Wraps the Arrow rows a ``next_app_batch`` call returned
(:data:`~acquirium.Storage.continuous.types.MUTATION_SCHEMA`: operation,
ref_uri, ts, numeric_value, text_value) so ``AppRunner.process_pending`` and
``MappedApp`` transforms can read them without touching the wire format
directly. continuous_batch.md's transform contract: durable output is
derived only from these materialized rows, never from a live storage read.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import polars as pl
import pyarrow as pa


@dataclass(frozen=True)
class InputBatch:
    """The batch of input mutations pinned for one ``process_pending`` turn."""

    rows: pa.Table  # MUTATION_SCHEMA columns

    @classmethod
    def from_arrow(cls, rows: pa.Table) -> "InputBatch":
        return cls(rows=rows)

    def ref_uris(self) -> list[str]:
        """Distinct input ref_uris touched by this batch, in the order the
        framework should process them (sorted, for determinism)."""
        if self.rows.num_rows == 0:
            return []
        return sorted(set(self.rows.column("ref_uri").to_pylist()))

    def _frame(self, ref_uri: str) -> pl.DataFrame:
        df = pl.from_arrow(self.rows)
        return df.filter(pl.col("ref_uri") == pl.lit(ref_uri))

    def upserts_frame(self, ref_uri: str, *, cast_value: str | None = "float") -> pl.DataFrame:
        """This batch's live (upsert) rows for one input ref_uri, as a
        ``(time, value)`` frame -- the same shape ``MappedApp.transform``
        receives in preview via ``stream.values``.

        ``cast_value="float"`` (the default, matching ``MappedApp``'s own
        default) coalesces to the numeric column, falling back to parsing
        text; any other value coalesces to text, falling back to the
        numeric column's string form. Deletes are never included here --
        the framework propagates them separately (see
        :meth:`delete_timestamps`); a transform never has to reason about
        them.
        """
        df = self._frame(ref_uri).filter(pl.col("operation") == "upsert")
        if cast_value == "float":
            value = pl.coalesce(
                [pl.col("numeric_value"), pl.col("text_value").cast(pl.Float64, strict=False)]
            )
        else:
            value = pl.coalesce([pl.col("text_value"), pl.col("numeric_value").cast(pl.Utf8)])
        return df.select(pl.col("ts").alias("time"), value.alias("value")).sort("time")

    def delete_timestamps(self, ref_uri: str) -> list[datetime]:
        """Timestamps deleted on *ref_uri* in this batch."""
        return self._frame(ref_uri).filter(pl.col("operation") == "delete")["ts"].to_list()

    def is_empty(self) -> bool:
        return self.rows.num_rows == 0
