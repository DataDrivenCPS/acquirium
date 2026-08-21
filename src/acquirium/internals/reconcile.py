"""Reconciling a stream's declared semantics against a linked point's.

A reference and the point that links to it can each carry a unit, quantity
kind, medium and substance. When both do and they disagree, that is either a
convertible difference (Celsius against Fahrenheit) or a mistake. This module
decides which, and is deliberately free of any graph or HTTP dependency so
the decision table can be tested directly.

The point always wins at read time; nothing here writes to the point.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

#: Fields where a difference is a hard error with no automated remedy.
#: A wrong medium or substance is a wiring mistake — there is nothing to
#: convert — so ``allow_unit_mismatch`` deliberately does not cover them.
STRICT_FIELDS = ("medium", "substance")


@dataclass(frozen=True)
class Conflict:
    """One irreconcilable field on one stream."""

    source_id: str
    ref_name: str
    field: str
    ref_value: str
    point_value: str
    point_uri: str
    reason: str

    def message(self) -> str:
        stream = f"({self.source_id!r}, {self.ref_name!r})"
        base = (
            f"stream {stream} declares {self.field}={self.ref_value!r} but its "
            f"point <{self.point_uri}> has {self.point_value!r} ({self.reason})"
        )
        if self.field == "unit":
            return base + "; pass allow_unit_mismatch=True to register anyway"
        return base


@dataclass(frozen=True)
class Reconciliation:
    """The outcome for one stream: what conflicts, and what merely differs."""

    conflicts: tuple[Conflict, ...] = ()
    warnings: tuple[str, ...] = ()


def reconcile_stream(
    *,
    source_id: str,
    ref_name: str,
    point_uri: str | None,
    ref_values: dict[str, str | None],
    point_values: dict[str, str | None],
    verdict: Callable[[str, str], str],
    allow_unit_mismatch: bool = False,
) -> Reconciliation:
    """Compare a reference's semantics with its point's, field by field.

    - equal, or only one side has a value -> nothing to do. The present value
      applies to both; it is *not* copied onto the other node, because reads
      already fall back from point to reference.
    - unit differs and is convertible -> fine. Reads convert into the point's
      unit; nothing is persisted.
    - unit differs and is not (or cannot be shown to be) -> a conflict, unless
      ``allow_unit_mismatch``.
    - quantity_kind differs -> a warning only. It is largely redundant with
      unit, and QUDT ships near-synonyms (Temperature against
      ThermodynamicTemperature) that would make this raise constantly.
    - medium or substance differs -> always a conflict.

    ``verdict`` is :meth:`QUDTUnitConverter.compatibility_verdict`, injected so
    this stays testable without a QUDT graph.
    """
    if point_uri is None:
        return Reconciliation()

    conflicts: list[Conflict] = []
    warnings: list[str] = []

    def _conflict(field: str, ref_value: str, point_value: str, reason: str) -> Conflict:
        return Conflict(
            source_id=source_id,
            ref_name=ref_name,
            field=field,
            ref_value=ref_value,
            point_value=point_value,
            point_uri=point_uri,
            reason=reason,
        )

    for field in ("unit", "quantity_kind", *STRICT_FIELDS):
        ref_value = ref_values.get(field)
        point_value = point_values.get(field)
        # One side missing means the other applies to both.
        if not ref_value or not point_value or ref_value == point_value:
            continue

        if field == "unit":
            answer = verdict(ref_value, point_value)
            if answer in ("match", "convertible"):
                continue
            reason = (
                "not convertible" if answer == "incompatible"
                else "neither carries a dimension vector or quantity kind, so "
                     "convertibility cannot be established"
            )
            if allow_unit_mismatch:
                warnings.append(
                    f"stream ({source_id!r}, {ref_name!r}): unit {ref_value!r} and "
                    f"point unit {point_value!r} are irreconcilable ({reason}); "
                    "registered anyway, reads will return the point's unit unconverted"
                )
                continue
            conflicts.append(_conflict(field, ref_value, point_value, reason))
        elif field == "quantity_kind":
            warnings.append(
                f"stream ({source_id!r}, {ref_name!r}): quantity_kind "
                f"{ref_value!r} differs from point <{point_uri}>'s {point_value!r}; "
                "the point's value applies"
            )
        else:
            conflicts.append(
                _conflict(field, ref_value, point_value, "no conversion exists")
            )

    return Reconciliation(tuple(conflicts), tuple(warnings))


class StreamMetadataConflict(ValueError):
    """Raised when registering streams whose semantics contradict their points."""

    def __init__(self, conflicts: Iterable[Conflict], *, message: str | None = None):
        self.conflicts = tuple(conflicts)
        if message is None:
            detail = "\n  ".join(c.message() for c in self.conflicts)
            message = (
                f"{len(self.conflicts)} stream registration(s) conflict with an "
                f"existing point:\n  {detail}"
            )
        super().__init__(message)

    @classmethod
    def from_detail(cls, detail: str) -> "StreamMetadataConflict":
        """Rebuild from a server response, which carries the text but not the
        structured conflicts. ``conflicts`` is empty on such an instance."""
        return cls((), message=detail)
