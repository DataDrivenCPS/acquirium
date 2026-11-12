"""Vocabulary shared by every layer of the resampling API.

The DataObject methods ``resample()`` and ``align()``, the client, the HTTP
route and the two stores all speak in the same terms: a bucket width
(``every``), an aggregate name (``agg``), a grid anchor (``origin``) and a
bucket label convention (``label``). This module holds the parsing and
validation of those terms so each layer accepts exactly the same values.
"""

from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone

#: Aggregates a bucket's numeric values can be reduced with. The storage
#: backends compute each one in SQL; ``DataObject`` computes the same ones in
#: polars when the rows are already fetched.
AGGREGATES: tuple[str, ...] = ("mean", "min", "max", "sum", "count", "first", "last", "median")

_AGG_ALIASES = {"avg": "mean", "average": "mean"}

#: Where a bucket's time sits in its window: ``"left"`` labels the window
#: ``[t, t + every)`` by ``t``; ``"center"`` labels ``[t - every/2, t + every/2)``
#: by ``t``.
LABELS: tuple[str, ...] = ("left", "center")

#: The default grid anchor. A bucket starts at ``origin + k * every`` for
#: some integer ``k``, so two streams bucketed with the same ``every`` and
#: ``origin`` share their bucket times.
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_UNIT_SECONDS = {
    "us": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "min": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}
_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(us|ms|min|[smhdw])\s*$")

_MICROSECOND = timedelta(microseconds=1)


def parse_every(every: "str | timedelta | int | float") -> timedelta:
    """Return ``every`` as a positive ``timedelta``.

    Accepts a ``timedelta``, a number of seconds, or a string of one number
    and one unit: ``"500ms"``, ``"10s"``, ``"5m"`` (minutes, as in polars),
    ``"1.5h"``, ``"1d"``, ``"1w"``. Units that vary in length (months,
    years) are not accepted. Raises ``ValueError`` for anything else or for
    a duration that is not positive.
    """
    if isinstance(every, bool):
        raise TypeError(f"every: expected a duration, got {every!r}")
    if isinstance(every, timedelta):
        td = every
    elif isinstance(every, (int, float)):
        if not math.isfinite(every):
            raise ValueError(f"every: expected a finite number of seconds, got {every!r}")
        td = timedelta(seconds=every)
    elif isinstance(every, str):
        m = _DURATION_RE.match(every)
        if not m:
            raise ValueError(
                f"every: could not parse {every!r}; use a timedelta, seconds, or a string "
                "like '500ms', '10s', '5m', '1h', '1d', '1w'"
            )
        td = timedelta(seconds=float(m.group(1)) * _UNIT_SECONDS[m.group(2)])
    else:
        raise TypeError(f"every: expected a str, timedelta or number of seconds, got {type(every).__name__}")
    if td <= timedelta(0):
        raise ValueError(f"every: must be a positive duration, got {td!r}")
    return td


def format_every(every: timedelta) -> str:
    """The exact text form of ``every`` for a request parameter: whole
    microseconds, which :func:`parse_every` reads back without loss."""
    return f"{every // _MICROSECOND}us"


def normalize_agg(agg: str) -> str:
    """The canonical name of an aggregate (``"avg"`` reads as ``"mean"``);
    raises ``ValueError`` for a name outside :data:`AGGREGATES`."""
    if not isinstance(agg, str):
        raise TypeError(f"agg: expected one of {list(AGGREGATES)}, got {type(agg).__name__}")
    name = _AGG_ALIASES.get(agg.lower(), agg.lower())
    if name not in AGGREGATES:
        raise ValueError(f"agg: unknown aggregate {agg!r}; choose from {list(AGGREGATES)}")
    return name


def normalize_label(label: str) -> str:
    if label not in LABELS:
        raise ValueError(f"label: expected one of {list(LABELS)}, got {label!r}")
    return label


def as_utc(dt: datetime) -> datetime:
    """``dt`` in UTC; a naive datetime is taken to be UTC already."""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


#: Bucket widths ``nice_every`` chooses from, in seconds.
_NICE_SECONDS: tuple[float, ...] = (
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5,
    1, 2, 5, 10, 15, 30,
    60, 120, 300, 600, 900, 1800,
    3600, 7200, 10800, 21600, 43200,
    86400, 172800, 604800,
)


def nice_every(gap: timedelta) -> timedelta:
    """The conventional bucket width closest to ``gap``: the member of a fixed
    ladder (1, 2, 5, 10, 15, 30 seconds, minutes, hours, days, a week)
    nearest to it on a log scale. Used by ``align()`` to turn a stream's
    average sample spacing into a grid step.
    """
    seconds = gap.total_seconds()
    if seconds <= 0:
        raise ValueError(f"nice_every: expected a positive gap, got {gap!r}")
    if seconds <= _NICE_SECONDS[0]:
        return timedelta(seconds=_NICE_SECONDS[0])
    if seconds >= _NICE_SECONDS[-1]:
        return timedelta(seconds=_NICE_SECONDS[-1])
    target = math.log(seconds)
    best = min(_NICE_SECONDS, key=lambda s: abs(math.log(s) - target))
    return timedelta(seconds=best)


def describe_every(every: timedelta) -> str:
    """``every`` in the largest unit that divides it evenly: ``"10s"``,
    ``"5m"``, ``"1h"``, ``"1d"``, ``"1w"``, ``"500ms"`` or ``"250us"``."""
    us = every // _MICROSECOND
    for unit, size in (("w", 604_800_000_000), ("d", 86_400_000_000), ("h", 3_600_000_000),
                       ("m", 60_000_000), ("s", 1_000_000), ("ms", 1_000)):
        if us % size == 0:
            return f"{us // size}{unit}"
    return f"{us}us"
