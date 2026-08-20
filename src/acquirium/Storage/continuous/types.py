"""Data types and the storage-agnostic contract for the continuous runtime.

``ContinuousStore`` is the seam described in ``continuous_batch_plan.md``'s
"Ground rules": every algorithm in the design doc (``continuous_batch.md``)
is implemented once against this interface by ``ContinuousDuckDB`` and
``ContinuousPostgres``, so callers (the Manager, the change router, and the
app actors) never depend on which backend is active.

Every table carrying rows across the wire (mutations, batch rows, bootstrap
rows) uses :data:`MUTATION_SCHEMA` or a close variant of it, so a caller can
build one Arrow table once and hand it to either backend unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

import pyarrow as pa

# Canonical mutation frame: one row per (ref_uri, ts) a writer wants to
# publish, or per key a batch read returns. ``operation`` is "upsert" or
# "delete"; for a delete, numeric_value/text_value are ignored (may be null).
MUTATION_SCHEMA = pa.schema(
    [
        pa.field("operation", pa.string()),
        pa.field("ref_uri", pa.string()),
        pa.field("ts", pa.timestamp("us", tz="UTC")),
        pa.field("numeric_value", pa.float64()),
        pa.field("text_value", pa.string()),
    ]
)

# Bootstrap staging rows carry no operation -- every staged row is a live
# canonical value at the time the snapshot was captured (see begin_bootstrap).
BOOTSTRAP_ROW_SCHEMA = pa.schema(
    [
        pa.field("ref_uri", pa.string()),
        pa.field("ts", pa.timestamp("us", tz="UTC")),
        pa.field("numeric_value", pa.float64()),
        pa.field("text_value", pa.string()),
    ]
)


class PublicationConflict(ValueError):
    """Raised when a publication id is reused with a different payload hash.

    Per the publication protocol (continuous_batch.md), a writer's stable
    ``publication_id`` makes retries idempotent only when the retried
    payload is byte-identical (after normalization) to the first attempt.
    A different payload under the same id is a programming error in the
    writer, not a race to recover from.
    """

    def __init__(self, publication_id: str):
        super().__init__(
            f"publication {publication_id!r} was already committed with a "
            f"different payload; retries must reuse the exact same mutations"
        )
        self.publication_id = publication_id


class BatchIdMismatch(ValueError):
    """Raised when a commit's derived batch id does not match the claimed id.

    The batch id is a pure function of ``(generation, sorted input ranges)``
    (continuous_batch_plan.md Decision 5); a mismatch means the actor is
    committing against ranges storage did not actually hand it, which must
    never be allowed to silently advance subscriptions incorrectly.
    """


class GenerationMismatch(ValueError):
    """Raised when a batch/commit request targets a stale app generation.

    A generation changes on reset, topology replacement, or code replace.
    Continuing to process a batch from a superseded generation would produce
    output under a lineage the app no longer owns.
    """


@dataclass(frozen=True)
class PublicationRequest:
    """One writer's atomic mutation set, addressed by a stable id.

    ``mutations`` follows :data:`MUTATION_SCHEMA`. ``publication_id`` is
    assigned by the writer (a driver's buffered-batch id, or
    ``app:{app_id}:{batch_id}`` for an app's output commit) and must be
    stable across retries -- see continuous_batch_plan.md Decision 5.
    """

    publication_id: str
    mutations: pa.Table


@dataclass(frozen=True)
class PublicationReceipt:
    """Durable idempotency record returned by :meth:`ContinuousStore.publish`.

    ``versions`` maps every ``ref_uri`` touched by the publication to the
    stream version it now carries. ``deduplicated`` is True when this call
    found an existing receipt with a matching payload hash and returned it
    without mutating storage again -- the writer-retry path.
    """

    publication_id: str
    payload_hash: str
    row_count: int
    versions: dict[str, int]
    deduplicated: bool = False


@dataclass(frozen=True)
class BatchInputRange:
    """One subscribed stream's ``(from_version, to_version]`` slice of a batch."""

    ref_uri: str
    from_version: int
    to_version: int


@dataclass(frozen=True)
class AppBatch:
    """One unit of pending work returned by :meth:`ContinuousStore.next_app_batch`.

    ``rows`` follows :data:`MUTATION_SCHEMA` and carries the latest-state
    upsert/delete frame for every key touched across ``inputs`` -- already
    reduced to one row per key per the "coalesced corrections" rule in
    continuous_batch.md. It may be empty while ``inputs`` still advances
    versions (a cursor-only batch, when every touched key was superseded by
    a later version already visible in the snapshot); such a batch must
    still be committed. ``has_more`` is True when more pending work exists
    beyond this batch's target-key budget, so the caller should call
    ``next_app_batch`` again after committing this one.

    For ``batch_kind == "bootstrap"``, ``inputs`` is empty (a bootstrap page
    isn't derived from stream version ranges) and ``bootstrap_id``/
    ``end_ordinal`` carry what :meth:`ContinuousStore.commit_bootstrap_page`
    needs; the caller commits via that method (with ``batch_id`` as its
    ``page_id``) rather than :meth:`ContinuousStore.commit_app_batch`, and
    calls :meth:`ContinuousStore.finalize_bootstrap` once ``has_more`` goes
    False after a successful commit.
    """

    batch_id: str
    batch_kind: str  # "tail" | "bootstrap"
    generation: int
    has_more: bool
    inputs: list[BatchInputRange]
    rows: pa.Table
    bootstrap_id: str | None = None
    end_ordinal: int | None = None


@dataclass(frozen=True)
class WebhookIntent:
    """A durable request to deliver one webhook, recorded inside a commit.

    Delivery itself happens out of band (a server-side worker polling
    ``app_webhook_intents``); recording the intent inside the same
    transaction as the rest of the commit is what makes it durable even if
    the process crashes before delivery is attempted.
    """

    url: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class CommitRequest:
    """An app actor's request to atomically commit one processed batch.

    ``batch_kind`` must match the :class:`AppBatch` this request answers
    (``"tail"`` or ``"bootstrap"``); a bootstrap-page commit does not publish
    ``outputs`` immediately -- it stages them for :meth:`ContinuousStore.finalize_bootstrap`
    to apply as one atomic replacement, so it should be committed via
    :meth:`ContinuousStore.commit_bootstrap_page` instead of this request
    type. ``outputs`` follows :data:`MUTATION_SCHEMA`.
    """

    app_id: str
    generation: int
    batch_id: str
    batch_kind: str
    inputs: list[BatchInputRange]
    outputs: pa.Table
    webhook_intents: list[WebhookIntent] = field(default_factory=list)


@dataclass(frozen=True)
class CommitResult:
    """Outcome of :meth:`ContinuousStore.commit_app_batch`.

    ``already_committed`` is True when this batch id was previously
    committed and the stored result is being replayed verbatim -- the
    lost-commit-response recovery path from continuous_batch.md's failure
    behavior section.
    """

    rows_inserted: int
    already_committed: bool
    output_versions: dict[str, int]


@dataclass(frozen=True)
class AppRuntimeRow:
    """Authoritative lifecycle state for one app, read from ``app_runtime``.

    ``status`` is one of ``registered``, ``bootstrapping``, ``active``,
    ``stopping``, ``stopped``, or ``failed``. ``generation`` increments on
    reset, topology replacement, or code replace; ``topology_version``
    increments when the app's selector resolves a different set of inputs
    (e.g. a mapped app matching a newly added stream).
    """

    app_id: str
    generation: int
    status: str
    topology_version: int
    updated_at: datetime


@dataclass(frozen=True)
class BootstrapState:
    """Result of :meth:`ContinuousStore.begin_bootstrap`.

    ``streams`` is the input stream-version vector captured in the snapshot
    transaction that staged historical rows; it becomes the app's initial
    subscription versions once the bootstrap finalizes.
    """

    bootstrap_id: str
    app_id: str
    generation: int
    streams: dict[str, int]


@dataclass(frozen=True)
class BootstrapPage:
    """One page of staged historical rows, ready to be transformed.

    ``rows`` follows :data:`BOOTSTRAP_ROW_SCHEMA`. ``page_id`` is derived
    from ``(bootstrap_id, start_ordinal, end_ordinal)`` so a retried commit
    of the same page is idempotent, mirroring tail batch ids.
    """

    bootstrap_id: str
    page_id: str
    start_ordinal: int
    end_ordinal: int
    has_more: bool
    rows: pa.Table


@dataclass(frozen=True)
class CompactReport:
    """Summary of one compaction pass, for logging and the metrics endpoint."""

    manifest_rows_deleted: int
    refs_advanced: int


class ContinuousStore(Protocol):
    """Storage-agnostic contract implemented by ``ContinuousDuckDB`` and
    ``ContinuousPostgres``.

    See continuous_batch_plan.md Phase 1b/1c for the canonical signatures
    and the transaction-by-transaction algorithm each method must follow;
    docstrings on the concrete implementations carry backend-specific notes.
    Every method here executes as one durable transaction (or, for
    ``next_app_batch``/``begin_bootstrap``/``bootstrap_page``, one repeatable
    snapshot read) -- callers never see partial effects.
    """

    def publish(self, req: PublicationRequest) -> PublicationReceipt:
        """Atomically apply one writer's mutation set (steps 1-10, continuous_batch.md)."""
        ...

    def next_app_batch(
        self, app_id: str, generation: int, target_keys: int = 50_000
    ) -> AppBatch | None:
        """Return the next pending batch for an app, or None if nothing is pending."""
        ...

    def commit_app_batch(self, req: CommitRequest) -> CommitResult:
        """Atomically commit one processed tail or bootstrap-page batch."""
        ...

    def register_app_runtime(self, app_id: str) -> None:
        """Create ``app_runtime`` state for a newly registered app (status=registered)."""
        ...

    def begin_bootstrap(
        self, app_id: str, input_ref_uris: list[str], output_ref_uris: list[str]
    ) -> BootstrapState:
        """Snapshot input history into staging and mark the app bootstrapping."""
        ...

    def bootstrap_page(self, bootstrap_id: str, page_size: int) -> BootstrapPage | None:
        """Return the next unprocessed page of staged bootstrap rows."""
        ...

    def commit_bootstrap_page(
        self, bootstrap_id: str, page_id: str, end_ordinal: int, outputs: pa.Table
    ) -> None:
        """Stage one bootstrap page's transformed outputs (not yet published).

        ``end_ordinal`` is the page's exclusive upper bound (from the
        :class:`BootstrapPage` this commit answers) -- it, together with the
        bootstrap's current ``next_ordinal``, is what lets the backend
        recompute and verify ``page_id`` and makes a retried commit of the
        same page idempotent.
        """
        ...

    def finalize_bootstrap(self, bootstrap_id: str) -> None:
        """Atomically replace the app's output streams from staged pages and go active."""
        ...

    def set_app_status(self, app_id: str, status: str) -> None:
        """Update ``app_runtime.status`` for lifecycle transitions outside a batch."""
        ...

    def app_runtime(self, app_id: str) -> AppRuntimeRow | None:
        """Return the current runtime row for an app, or None if never registered."""
        ...

    def reset_app(self, app_id: str) -> int:
        """Start a new generation for an app (reset/topology replace); returns it."""
        ...

    def has_subscriptions(self, app_id: str, generation: int) -> bool:
        """True if the app has ever bootstrapped (has subscription rows) at
        *generation*. Distinguishes a never-started app (bootstrap) from a
        stopped one (resume or reconcile, per :meth:`resumable`)."""
        ...

    def resumable(self, app_id: str, generation: int) -> bool:
        """True if every subscribed ref's version is still covered by the
        retained manifest floor (``stream_version >= retained_from_version``).

        A stopped app whose subscriptions fall below the floor cannot resume
        via ``next_app_batch`` -- the change-key rows it would need were
        already compacted away, per continuous_batch.md's "Compacted stopped
        cursors cause canonical reconciliation." An app with no subscriptions
        at all (never bootstrapped) is vacuously resumable; the caller
        distinguishes that case to bootstrap instead of resume.
        """
        ...

    def delete_app_runtime(self, app_id: str) -> None:
        """Remove all runtime, subscription, and bootstrap state owned by an app."""
        ...

    def subscription_index(self) -> dict[str, list[str]]:
        """Return ``{ref_uri: [app_id, ...]}`` for every active/bootstrapping subscription."""
        ...

    def lagging_apps(self) -> list[str]:
        """Return app ids whose subscription version trails a subscribed stream's head.

        Used by the change router's periodic safety scan to recover lost
        wake-ups without depending on router-held state.
        """
        ...

    def compact(self, chunk_rows: int = 100_000) -> CompactReport:
        """Delete change-key manifest rows no longer needed by any subscriber."""
        ...

    def metrics(self) -> dict[str, Any]:
        """Return head/floor/lag/manifest/bootstrap counters for observability."""
        ...
