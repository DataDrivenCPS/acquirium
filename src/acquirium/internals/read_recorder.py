"""Record which streams a run actually read — the observed side of provenance.

A query tells us which streams an app *could* read (declared provenance,
``acq:mayUse``). Which ones a run *actually* consumed is only knowable at
run time, and only for reads — which read fed which output lives inside
the Python body and cannot be recovered without taint tracking through
native code, so it is never guessed.

The recorder is a :class:`contextvars.ContextVar` holding a set of stream
(ref) URIs. :class:`DataObject` adds every binding's ref URI to it when it
materializes values; the app runner and the task host open a recording
scope around each run and collect the set afterwards. A contextvar — not a
global — because the task host runs many tasks' bodies in one process and
the app runner's emit threads share it too: each run's scope sees only its
own reads. Outside a scope, recording is a no-op with zero cost.
"""
from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import Iterable, Iterator

_reads: contextvars.ContextVar[set[str] | None] = contextvars.ContextVar(
    "acquirium_observed_reads", default=None
)


def record_reads(ref_uris: Iterable[str]) -> None:
    """Add stream URIs to the active recording scope, if any."""
    current = _reads.get()
    if current is None:
        return
    current.update(str(u) for u in ref_uris if u)


@contextmanager
def recording_reads() -> Iterator[set[str]]:
    """Open a recording scope; yields the (live) set of observed ref URIs.

    Nested scopes are independent: an inner scope does not leak into the
    outer one, and the outer resumes untouched when the inner exits.
    """
    reads: set[str] = set()
    token = _reads.set(reads)
    try:
        yield reads
    finally:
        _reads.reset(token)
