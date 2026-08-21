"""Shared contract tests for ContinuousStore implementations.

Mirrors the pattern in test_timeseries_store_contract.py: one suite
parameterized over ["duckdb", "timescale"], skipping the Postgres param when
TimescaleDB is unavailable. This suite is the gate for
continuous_batch_plan.md Phase 1 -- nothing in later phases should start
until every test here passes on both backends.
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from uuid import uuid4

import psycopg
import pyarrow as pa
import pytest

from acquirium.Storage.continuous import ids
from acquirium.Storage.continuous.duckdb import ContinuousDuckDB
from acquirium.Storage.continuous.postgres import ContinuousPostgres
from acquirium.Storage.continuous.types import (
    BatchInputRange,
    BatchIdMismatch,
    CommitRequest,
    GenerationMismatch,
    MUTATION_SCHEMA,
    PublicationConflict,
    PublicationRequest,
    WebhookIntent,
)
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB
from acquirium.Storage.materialization.postgres import MaterializationPostgres
from acquirium.Storage.timescale_store import TimescaleStore


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def mutation_table(rows: list[tuple]) -> pa.Table:
    """Build a MUTATION_SCHEMA table from (operation, ref_uri, ts, numeric_value, text_value) rows."""
    return pa.table(
        {
            "operation": [r[0] for r in rows],
            "ref_uri": [r[1] for r in rows],
            "ts": [r[2] for r in rows],
            "numeric_value": [r[3] for r in rows],
            "text_value": [r[4] for r in rows],
        },
        schema=MUTATION_SCHEMA,
    )


@pytest.fixture(params=["duckdb", "timescale"])
def contract_store(request, tmp_path, pg_dsn):
    """Yield (ContinuousStore, raw TimeseriesStore) for reads/cleanup."""
    if request.param == "duckdb":
        store = DuckDBStore(tmp_path / "continuous_contract.duckdb", recreate=True)
        cs = ContinuousDuckDB(store)
        yield cs, store
        store.close()
    else:
        try:
            store = TimescaleStore(dsn=pg_dsn, connect_timeout=2, recreate=False)
        except psycopg.OperationalError as exc:
            pytest.skip(f"TimescaleDB is not available: {exc}")
        cs = ContinuousPostgres(pg_dsn)
        yield cs, store
        cs.close()
        store.close()


@pytest.fixture
def p() -> str:
    """A unique prefix so tests sharing one Postgres database never collide."""
    return f"contract:{uuid4()}"


def uri(p: str, name: str) -> str:
    return f"urn:test:{p}:{name}"


# ---------------------------------------------------------------------------
# publication protocol
# ---------------------------------------------------------------------------


def test_singleton_and_multirow_publication_one_version_increment(contract_store, p):
    cs, _ = contract_store
    s1, s2 = uri(p, "s1"), uri(p, "s2")

    r1 = cs.publish(PublicationRequest(f"{p}:pub1", mutation_table([
        ("upsert", s1, _utc(2026, 1, 1), 1.0, None),
    ])))
    assert r1.versions == {s1: 1}
    assert r1.row_count == 1

    r2 = cs.publish(PublicationRequest(f"{p}:pub2", mutation_table([
        ("upsert", s1, _utc(2026, 1, 2), 2.0, None),
        ("upsert", s2, _utc(2026, 1, 1), 3.0, None),
    ])))
    assert r2.versions == {s1: 2, s2: 1}
    assert r2.row_count == 2


def test_duplicate_key_normalization_within_one_publication(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    t0 = _utc(2026, 1, 1)
    receipt = cs.publish(PublicationRequest(f"{p}:pub", mutation_table([
        ("upsert", s1, t0, 1.0, None),
        ("upsert", s1, t0, 2.0, None),  # last wins
    ])))
    assert receipt.row_count == 1
    assert receipt.versions == {s1: 1}


def test_publication_retry_same_hash_returns_receipt(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    m = mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])
    r1 = cs.publish(PublicationRequest(f"{p}:pub", m))
    assert not r1.deduplicated
    r2 = cs.publish(PublicationRequest(f"{p}:pub", m))
    assert r2.deduplicated
    assert r2.versions == r1.versions
    assert r2.payload_hash == r1.payload_hash


def test_publication_retry_different_hash_conflicts(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    cs.publish(PublicationRequest(f"{p}:pub", mutation_table([
        ("upsert", s1, _utc(2026, 1, 1), 1.0, None),
    ])))
    with pytest.raises(PublicationConflict):
        cs.publish(PublicationRequest(f"{p}:pub", mutation_table([
            ("upsert", s1, _utc(2026, 1, 1), 99.0, None),
        ])))


def test_publish_atomicity_on_constraint_violation(contract_store, p):
    """A row violating the numeric/text mutual-exclusion CHECK constraint
    must fail the whole publication, leaving no partial head advance."""
    cs, store = contract_store
    s1 = uri(p, "s1")
    bad = mutation_table([
        ("upsert", s1, _utc(2026, 1, 1), 1.0, None),
    ])
    # Force both value columns non-null to violate the CHECK constraint.
    bad = bad.set_column(
        bad.schema.get_field_index("text_value"),
        "text_value",
        pa.array(["also-set"], type=pa.string()),
    )
    with pytest.raises(Exception):
        cs.publish(PublicationRequest(f"{p}:pub-bad", bad))

    # The stream must be untouched: a fresh publish starts at version 1.
    good = cs.publish(PublicationRequest(f"{p}:pub-good", mutation_table([
        ("upsert", s1, _utc(2026, 1, 1), 5.0, None),
    ])))
    assert good.versions == {s1: 1}


def test_overlapping_concurrent_writers_no_deadlock(contract_store, p):
    cs, _ = contract_store
    s1, s2 = uri(p, "s1"), uri(p, "s2")
    results: list[Exception | None] = [None, None]

    def write(idx: int, pub_id: str, refs: list[str]):
        try:
            cs.publish(PublicationRequest(pub_id, mutation_table([
                ("upsert", ref, _utc(2026, 1, 1), float(idx), None) for ref in refs
            ])))
        except Exception as exc:  # pragma: no cover - only on failure
            results[idx] = exc

    # Opposite lock orders (s1,s2 vs s2,s1) exercise the sorted-locking
    # deadlock prevention continuous_batch.md requires.
    t1 = threading.Thread(target=write, args=(0, f"{p}:pubA", [s1, s2]))
    t2 = threading.Thread(target=write, args=(1, f"{p}:pubB", [s2, s1]))
    t1.start(); t2.start()
    t1.join(timeout=30); t2.join(timeout=30)
    assert not t1.is_alive() and not t2.is_alive(), "writers deadlocked or hung"
    assert results == [None, None]


def test_range_manifests_are_queryable_with_the_same_contract(contract_store, p, pg_dsn):
    """Canonical publication emits scheduler-readable half-open ranges on both backends."""
    cs, store = contract_store
    source = uri(p, "range-source")
    timestamp = _utc(2026, 1, 1, 12, 0, 5)
    receipt = cs.publish(PublicationRequest(f"{p}:ranges", mutation_table([
        ("upsert", source, timestamp, 1.0, None),
        ("delete", source, timestamp.replace(second=35), None, None),
    ])))
    runtime = MaterializationDuckDB(store) if isinstance(store, DuckDBStore) else MaterializationPostgres(pg_dsn)
    try:
        ranges = runtime.change_ranges(source, after_version=0, through_version=receipt.versions[source])
    finally:
        if isinstance(runtime, MaterializationPostgres):
            runtime.close()
    assert len(ranges) == 1
    item = ranges[0]
    assert item.ref_uri == source
    assert item.stream_version == receipt.versions[source]
    assert item.change_kind == "mixed"
    assert item.row_count == 2
    assert item.interval.start == _utc(2026, 1, 1, 12)
    assert item.interval.end == _utc(2026, 1, 1, 12, 1)


# ---------------------------------------------------------------------------
# next_app_batch / commit_app_batch (tail path)
# ---------------------------------------------------------------------------


def _start_app(cs, app_id: str, input_uri: str) -> int:
    """Register, bootstrap (empty history), and finalize an app so it's
    active with a subscription to *input_uri* at version 0."""
    cs.register_app_runtime(app_id)
    state = cs.begin_bootstrap(app_id, input_ref_uris=[input_uri], output_ref_uris=[])
    # Drain any bootstrap pages (there should be none for a fresh stream).
    while True:
        page = cs.bootstrap_page(state.bootstrap_id, page_size=1000)
        if page is None:
            break
        cs.commit_bootstrap_page(state.bootstrap_id, page.page_id, page.end_ordinal, mutation_table([]))
    cs.finalize_bootstrap(state.bootstrap_id)
    return state.generation


def test_next_app_batch_cursor_only_batch_on_superseded_key(contract_store, p):
    """A key written twice before the app ever reads it must be delivered
    once, with its newest value (coalesced corrections)."""
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)

    t0 = _utc(2026, 1, 1)
    cs.publish(PublicationRequest(f"{p}:v1", mutation_table([("upsert", s1, t0, 10.0, None)])))
    cs.publish(PublicationRequest(f"{p}:v2", mutation_table([("upsert", s1, t0, 20.0, None)])))
    cs.publish(PublicationRequest(f"{p}:v3", mutation_table([("upsert", s1, t0, 30.0, None)])))

    batch = cs.next_app_batch(app_id, gen)
    assert batch is not None
    assert batch.rows.num_rows == 1
    row = batch.rows.to_pylist()[0]
    assert row["operation"] == "upsert"
    assert row["numeric_value"] == 30.0

    commit = cs.commit_app_batch(CommitRequest(
        app_id=app_id, generation=gen, batch_id=batch.batch_id, batch_kind="tail",
        inputs=batch.inputs, outputs=mutation_table([]),
    ))
    assert not commit.already_committed

    assert cs.next_app_batch(app_id, gen) is None


def test_next_app_batch_oversized_publication_taken_whole(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)

    # One publication with 5 rows; target_keys=1 must still take it whole.
    rows = [("upsert", s1, _utc(2026, 1, 1, hour=i), float(i), None) for i in range(5)]
    cs.publish(PublicationRequest(f"{p}:big", mutation_table(rows)))

    batch = cs.next_app_batch(app_id, gen, target_keys=1)
    assert batch is not None
    assert batch.rows.num_rows == 5
    assert batch.has_more is False


def test_next_app_batch_none_when_no_pending_work(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)
    assert cs.next_app_batch(app_id, gen) is None


def test_next_app_batch_delete_propagation(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)
    t0 = _utc(2026, 1, 1)

    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, t0, 1.0, None)])))
    batch1 = cs.next_app_batch(app_id, gen)
    cs.commit_app_batch(CommitRequest(
        app_id=app_id, generation=gen, batch_id=batch1.batch_id, batch_kind="tail",
        inputs=batch1.inputs, outputs=mutation_table([]),
    ))

    cs.publish(PublicationRequest(f"{p}:del", mutation_table([("delete", s1, t0, None, None)])))
    batch2 = cs.next_app_batch(app_id, gen)
    assert batch2.rows.num_rows == 1
    row = batch2.rows.to_pylist()[0]
    assert row["operation"] == "delete"


# ---------------------------------------------------------------------------
# commit_app_batch idempotency / validation
# ---------------------------------------------------------------------------


def test_commit_app_batch_idempotent_duplicate(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    out1 = uri(p, "out1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)
    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))

    batch = cs.next_app_batch(app_id, gen)
    outputs = mutation_table([("upsert", out1, _utc(2026, 1, 1), 2.0, None)])
    req = CommitRequest(
        app_id=app_id, generation=gen, batch_id=batch.batch_id, batch_kind="tail",
        inputs=batch.inputs, outputs=outputs,
    )
    r1 = cs.commit_app_batch(req)
    assert not r1.already_committed
    r2 = cs.commit_app_batch(req)
    assert r2.already_committed
    assert r2.rows_inserted == r1.rows_inserted
    assert r2.output_versions == r1.output_versions


def test_commit_app_batch_rejects_tampered_batch_id(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)
    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))
    batch = cs.next_app_batch(app_id, gen)

    with pytest.raises(BatchIdMismatch):
        cs.commit_app_batch(CommitRequest(
            app_id=app_id, generation=gen, batch_id="not-the-real-id", batch_kind="tail",
            inputs=batch.inputs, outputs=mutation_table([]),
        ))


def test_commit_app_batch_empty_output_advances_without_publication(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)
    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))

    batch = cs.next_app_batch(app_id, gen)
    result = cs.commit_app_batch(CommitRequest(
        app_id=app_id, generation=gen, batch_id=batch.batch_id, batch_kind="tail",
        inputs=batch.inputs, outputs=mutation_table([]),
    ))
    assert result.rows_inserted == 0
    assert result.output_versions == {}
    # Subscription still advanced -- no more pending work.
    assert cs.next_app_batch(app_id, gen) is None


def test_commit_app_batch_webhook_intents_recorded(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)
    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))
    batch = cs.next_app_batch(app_id, gen)

    cs.commit_app_batch(CommitRequest(
        app_id=app_id, generation=gen, batch_id=batch.batch_id, batch_kind="tail",
        inputs=batch.inputs, outputs=mutation_table([]),
        webhook_intents=[WebhookIntent(url="http://example.invalid/hook", payload={"x": 1})],
    ))
    # Recorded durably: metrics/queries aside, re-driving the same commit
    # must not duplicate the intent (idempotent path returns the same result
    # without re-executing insert statements).
    result2 = cs.commit_app_batch(CommitRequest(
        app_id=app_id, generation=gen, batch_id=batch.batch_id, batch_kind="tail",
        inputs=batch.inputs, outputs=mutation_table([]),
        webhook_intents=[WebhookIntent(url="http://example.invalid/hook", payload={"x": 1})],
    ))
    assert result2.already_committed


def test_commit_app_batch_wrong_generation_rejected(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)
    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))
    batch = cs.next_app_batch(app_id, gen)

    # A batch_id honestly derived from the stale generation still gets
    # rejected once app_runtime has moved on to a new generation (e.g. via
    # reset), isolating the generation check from batch_id verification.
    cs.reset_app(app_id)
    with pytest.raises(GenerationMismatch):
        cs.commit_app_batch(CommitRequest(
            app_id=app_id, generation=gen, batch_id=batch.batch_id, batch_kind="tail",
            inputs=batch.inputs, outputs=mutation_table([]),
        ))


# ---------------------------------------------------------------------------
# bootstrap
# ---------------------------------------------------------------------------


def test_bootstrap_snapshot_excludes_concurrent_writes(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    cs.register_app_runtime(app_id)

    cs.publish(PublicationRequest(f"{p}:before", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))
    state = cs.begin_bootstrap(app_id, input_ref_uris=[s1], output_ref_uris=[])
    # Written after the snapshot -- must NOT appear in the staged page, and
    # must remain pending for tail processing once the app goes active.
    cs.publish(PublicationRequest(f"{p}:after", mutation_table([("upsert", s1, _utc(2026, 1, 2), 2.0, None)])))

    page = cs.bootstrap_page(state.bootstrap_id, page_size=1000)
    assert page.rows.num_rows == 1
    assert page.rows.to_pylist()[0]["numeric_value"] == 1.0

    cs.commit_bootstrap_page(state.bootstrap_id, page.page_id, page.end_ordinal, mutation_table([]))
    assert cs.bootstrap_page(state.bootstrap_id, page_size=1000) is None
    cs.finalize_bootstrap(state.bootstrap_id)

    runtime = cs.app_runtime(app_id)
    assert runtime.status == "active"
    tail = cs.next_app_batch(app_id, runtime.generation)
    assert tail is not None
    assert tail.rows.num_rows == 1
    assert tail.rows.to_pylist()[0]["numeric_value"] == 2.0


def test_bootstrap_no_partial_output_before_finalize(contract_store, p):
    cs, store = contract_store
    s1 = uri(p, "s1")
    out1 = uri(p, "out1")
    app_id = f"{p}:app"
    cs.register_app_runtime(app_id)
    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))

    state = cs.begin_bootstrap(app_id, input_ref_uris=[s1], output_ref_uris=[out1])
    page = cs.bootstrap_page(state.bootstrap_id, page_size=1000)
    cs.commit_bootstrap_page(
        state.bootstrap_id, page.page_id, page.end_ordinal,
        mutation_table([("upsert", out1, _utc(2026, 1, 1), 2.0, None)]),
    )
    # Not finalized yet -- output must not be visible through the raw store.
    assert sum(b.num_rows for b in store.timeseries(out1)) == 0

    cs.finalize_bootstrap(state.bootstrap_id)
    assert sum(b.num_rows for b in store.timeseries(out1)) == 1


def test_bootstrap_finalize_tombstones_stale_output(contract_store, p):
    """A second bootstrap (e.g. after a selector shrink) that no longer
    produces a previously-owned output ref must retract that stream."""
    cs, store = contract_store
    s1 = uri(p, "s1")
    out1 = uri(p, "out1")
    app_id = f"{p}:app"
    cs.register_app_runtime(app_id)
    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))

    state1 = cs.begin_bootstrap(app_id, input_ref_uris=[s1], output_ref_uris=[out1])
    page1 = cs.bootstrap_page(state1.bootstrap_id, page_size=1000)
    cs.commit_bootstrap_page(
        state1.bootstrap_id, page1.page_id, page1.end_ordinal,
        mutation_table([("upsert", out1, _utc(2026, 1, 1), 2.0, None)]),
    )
    cs.finalize_bootstrap(state1.bootstrap_id)
    assert sum(b.num_rows for b in store.timeseries(out1)) == 1

    # Reconcile again, this time declaring out1 as a target but producing no
    # rows for it (selector no longer matches) -- out1 must be retracted.
    new_gen = cs.reset_app(app_id)
    state2 = cs.begin_bootstrap(app_id, input_ref_uris=[s1], output_ref_uris=[out1])
    assert state2.generation == new_gen
    page2 = cs.bootstrap_page(state2.bootstrap_id, page_size=1000)
    cs.commit_bootstrap_page(state2.bootstrap_id, page2.page_id, page2.end_ordinal, mutation_table([]))
    cs.finalize_bootstrap(state2.bootstrap_id)

    assert sum(b.num_rows for b in store.timeseries(out1)) == 0


# ---------------------------------------------------------------------------
# compaction / reset
# ---------------------------------------------------------------------------


def test_compaction_advances_floor_and_retains_receipts(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)

    pub_id = f"{p}:up"
    cs.publish(PublicationRequest(pub_id, mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))
    batch = cs.next_app_batch(app_id, gen)
    cs.commit_app_batch(CommitRequest(
        app_id=app_id, generation=gen, batch_id=batch.batch_id, batch_kind="tail",
        inputs=batch.inputs, outputs=mutation_table([]),
    ))

    report = cs.compact()
    assert report.manifest_rows_deleted >= 1

    # The publication receipt survives compaction: retrying with the same id
    # and payload still returns a deduplicated receipt.
    retry = cs.publish(PublicationRequest(pub_id, mutation_table([
        ("upsert", s1, _utc(2026, 1, 1), 1.0, None),
    ])))
    assert retry.deduplicated


def test_has_subscriptions_and_resumable(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    cs.register_app_runtime(app_id)
    runtime = cs.app_runtime(app_id)
    assert cs.has_subscriptions(app_id, runtime.generation) is False
    assert cs.resumable(app_id, runtime.generation) is True  # vacuously, no subs

    gen = _start_app(cs, app_id, s1)
    assert cs.has_subscriptions(app_id, gen) is True
    assert cs.resumable(app_id, gen) is True

    # Advance far enough that compaction can move the floor past this app's
    # subscription, then verify resumable() flips to False.
    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))
    batch = cs.next_app_batch(app_id, gen)
    cs.commit_app_batch(CommitRequest(
        app_id=app_id, generation=gen, batch_id=batch.batch_id, batch_kind="tail",
        inputs=batch.inputs, outputs=mutation_table([]),
    ))
    cs.set_app_status(app_id, "stopped")  # not active/bootstrapping -> not counted by compact()
    # Advance the head past what the stopped app consumed, so compaction has
    # somewhere to move the floor to.
    cs.publish(PublicationRequest(f"{p}:up2", mutation_table([("upsert", s1, _utc(2026, 1, 2), 2.0, None)])))
    cs.compact()
    assert cs.resumable(app_id, gen) is False


def test_stopped_app_below_retained_floor_requires_reset(contract_store, p):
    """continuous_batch.md: a cursor below a retained floor is invalid to
    resume from directly -- reset_app starts a fresh generation instead."""
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)

    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))
    batch = cs.next_app_batch(app_id, gen)
    cs.commit_app_batch(CommitRequest(
        app_id=app_id, generation=gen, batch_id=batch.batch_id, batch_kind="tail",
        inputs=batch.inputs, outputs=mutation_table([]),
    ))
    cs.compact()  # advances the retained floor past this app's own version, harmlessly

    new_gen = cs.reset_app(app_id)
    assert new_gen == gen + 1
    runtime = cs.app_runtime(app_id)
    assert runtime.status == "registered"
    assert runtime.generation == new_gen


# ---------------------------------------------------------------------------
# router/compactor support surfaces
# ---------------------------------------------------------------------------


def test_subscription_index_and_lagging_apps(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    gen = _start_app(cs, app_id, s1)

    index = cs.subscription_index()
    assert app_id in index.get(s1, [])
    # The store is shared across the test session on the Postgres backend
    # (unlike a fresh DuckDB file per test), so only assert about this
    # test's own app_id, not the full lagging-apps list.
    assert app_id not in cs.lagging_apps()

    cs.publish(PublicationRequest(f"{p}:up", mutation_table([("upsert", s1, _utc(2026, 1, 1), 1.0, None)])))
    assert app_id in cs.lagging_apps()


def test_delete_app_runtime_removes_all_state(contract_store, p):
    cs, _ = contract_store
    s1 = uri(p, "s1")
    app_id = f"{p}:app"
    _start_app(cs, app_id, s1)
    cs.delete_app_runtime(app_id)
    assert cs.app_runtime(app_id) is None
    assert app_id not in sum(cs.subscription_index().values(), [])


# ---------------------------------------------------------------------------
# id derivation (backend-independent; no fixture needed)
# ---------------------------------------------------------------------------


def test_payload_hash_is_order_independent_and_backend_independent():
    t = mutation_table([
        ("upsert", "a", _utc(2026, 1, 1), 1.0, None),
        ("delete", "b", _utc(2026, 1, 2), None, None),
    ])
    reordered = t.take([1, 0])
    assert ids.payload_hash(t) == ids.payload_hash(reordered)


def test_tail_batch_id_is_pure_function_of_ranges():
    ranges_a = [("x", 0, 5), ("a", 2, 3)]
    ranges_b = [("a", 2, 3), ("x", 0, 5)]  # different input order
    assert ids.tail_batch_id(1, ranges_a) == ids.tail_batch_id(1, ranges_b)
    assert ids.tail_batch_id(1, ranges_a) != ids.tail_batch_id(2, ranges_a)


def test_bootstrap_page_id_depends_on_ordinal_range():
    a = ids.bootstrap_page_id("boot1", 0, 100)
    b = ids.bootstrap_page_id("boot1", 100, 200)
    assert a != b
    assert a == ids.bootstrap_page_id("boot1", 0, 100)
