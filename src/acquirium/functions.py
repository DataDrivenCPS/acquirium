"""Helpers for app calculation files.

A Python function returns one frame per output port. ``emit`` and ``empty``
build those frames for a function called once per window, and ``emit_windows``
for a batch function. A sequential function reads its earlier results with
``previous_value``, ``previous_outputs`` and ``previous_rows``, and
``same_period`` says whether a period boundary lies between the previous window
and this one. ``SCHEMA`` gives the columns of every output frame.
"""

from escapement.functions import (
    SCHEMA,
    emit,
    emit_windows,
    empty,
    previous_outputs,
    previous_rows,
    previous_value,
    same_period,
)

__all__ = [
    "SCHEMA",
    "emit",
    "emit_windows",
    "empty",
    "previous_outputs",
    "previous_rows",
    "previous_value",
    "same_period",
]
