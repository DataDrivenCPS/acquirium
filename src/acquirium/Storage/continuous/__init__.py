"""The continuous latest-state microbatch runtime's storage layer.

This subpackage implements the durable protocol described in
``continuous_batch.md`` (publication, snapshot batch reads, atomic app
commits, bootstrap, compaction) as one backend-agnostic contract,
:class:`~acquirium.Storage.continuous.types.ContinuousStore`, with two
concrete implementations:

- :class:`~acquirium.Storage.continuous.duckdb.ContinuousDuckDB` wraps a
  :class:`~acquirium.Storage.duckdb_store.DuckDBStore`.
- :class:`~acquirium.Storage.continuous.postgres.ContinuousPostgres` wraps a
  ``psycopg_pool.ConnectionPool`` against the same database a
  :class:`~acquirium.Storage.timescale_store.TimescaleStore` uses.

Everything above this layer (the server's ``Manager``, the change router, and
the app actors) is written against :class:`ContinuousStore` alone, so a
future native (e.g. Rust) port of the hot path stays possible without
touching callers -- see ``continuous_batch_plan.md``'s "Ground rules".
"""

from acquirium.Storage.continuous.types import (
    AppBatch,
    AppRuntimeRow,
    BatchIdMismatch,
    BatchInputRange,
    BootstrapPage,
    BootstrapState,
    CommitRequest,
    CommitResult,
    CompactReport,
    ContinuousStore,
    GenerationMismatch,
    MUTATION_SCHEMA,
    PublicationConflict,
    PublicationReceipt,
    PublicationRequest,
    WebhookIntent,
)

__all__ = [
    "AppBatch",
    "AppRuntimeRow",
    "BatchIdMismatch",
    "BatchInputRange",
    "BootstrapPage",
    "BootstrapState",
    "CommitRequest",
    "CommitResult",
    "CompactReport",
    "ContinuousStore",
    "GenerationMismatch",
    "MUTATION_SCHEMA",
    "PublicationConflict",
    "PublicationReceipt",
    "PublicationRequest",
    "WebhookIntent",
]
