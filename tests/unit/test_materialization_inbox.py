"""Ingestion lands in the inbox; a step commits it as one revision.

Runs against DuckDB and, when ACQUIRIUM_TEST_PG_DSN is set, TimescaleDB.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from acquirium.Materialization.engine import Engine
from acquirium.Storage.publication.revision import RevisionPublisher
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def mutations(ref: str, values: list[float], start: datetime = T0) -> pa.Table:
    times = [start + timedelta(minutes=i) for i in range(len(values))]
    return pa.table({"operation": ["upsert"] * len(values), "ref_uri": [ref] * len(values),
                     "ts": pa.array(times, pa.timestamp("us", tz="UTC")),
                     "numeric_value": values, "text_value": [None] * len(values)}, schema=MUTATION_SCHEMA)


def rows(engine: Engine, ref: str) -> list[tuple]:
    with engine.store._own_conn() as conn:
        return [tuple(r) for r in engine._execute(
            conn, "SELECT t.ts, t.numeric_value, t.last_revision FROM timeseries t JOIN ref_ids r ON r.ref_id=t.ref_id "
                  "WHERE r.ref_uri=? AND NOT t.deleted ORDER BY t.ts", [ref]).fetchall()]


@pytest.fixture
def setup(materialization_store):
    engine = Engine(materialization_store)
    engine.deferred = True
    return engine, engine, RevisionPublisher(engine)


def test_a_publication_waits_in_the_inbox_until_a_step(setup):
    store, engine, publisher = setup
    receipt = publisher.publish(PublicationRequest("p1", mutations("urn:a", [1.0, 2.0])))

    assert receipt.row_count == 2 and receipt.versions == {"seq": 1}
    assert engine.pending() == 1 and rows(engine, "urn:a") == []

    report = engine.step()

    assert report.step_no == 1 and report.entries == 1 and report.rows == 2
    assert engine.pending() == 0 and engine.last_step() == 1
    assert [(v, rev) for _, v, rev in rows(engine, "urn:a")] == [(1.0, report.revision), (2.0, report.revision)]


def test_one_step_applies_many_entries_in_order_at_one_revision(setup):
    store, engine, publisher = setup
    publisher.publish(PublicationRequest("p1", mutations("urn:a", [1.0, 2.0])))
    publisher.publish(PublicationRequest("p2", mutations("urn:b", [5.0])))
    publisher.publish(PublicationRequest("p3", mutations("urn:a", [9.0])))  # overwrites the first value

    report = engine.step()

    assert (report.entries, report.rows, report.seq_from, report.seq_to) == (3, 4, 1, 3)
    assert [v for _, v, _ in rows(engine, "urn:a")] == [9.0, 2.0]
    assert {rev for _, _, rev in rows(engine, "urn:a") + rows(engine, "urn:b")} == {report.revision}


def test_a_replacement_after_readings_supersedes_them(setup):
    store, engine, publisher = setup
    publisher.publish(PublicationRequest("p1", mutations("urn:a", [1.0, 2.0, 3.0])))
    publisher.replace(PublicationRequest("p2", mutations("urn:a", [7.0], T0 + timedelta(hours=1))), "urn:a")

    report = engine.step()

    assert [v for _, v, _ in rows(engine, "urn:a")] == [7.0]
    with engine.store._own_conn() as conn:
        assert engine._execute(conn, "SELECT last_revision FROM stream_resets WHERE ref_uri='urn:a'").fetchone()[0] == report.revision


def test_a_limit_leaves_the_rest_for_the_next_step(setup):
    store, engine, publisher = setup
    for i in range(3):
        publisher.publish(PublicationRequest(f"p{i}", mutations(f"urn:{i}", [float(i)])))

    first, second, third = engine.step(limit=2), engine.step(limit=2), engine.step()

    assert (first.step_no, first.entries, first.seq_to) == (1, 2, 2)
    assert (second.step_no, second.entries, second.seq_from) == (2, 1, 3)
    assert third is None and engine.pending() == 0
    assert second.revision > first.revision


def test_a_failing_step_leaves_the_inbox_and_the_tables_untouched(setup, monkeypatch):
    store, engine, publisher = setup
    publisher.publish(PublicationRequest("p1", mutations("urn:a", [1.0])))

    def explode(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(engine.store, "_insert_frame", explode)
    with pytest.raises(RuntimeError, match="disk full"):
        engine.step()
    monkeypatch.undo()

    assert engine.pending() == 1 and engine.last_step() == 0 and rows(engine, "urn:a") == []
    assert engine.step().rows == 1


def test_an_undeferred_engine_steps_on_every_submission(materialization_store):
    engine = Engine(materialization_store)
    publisher = RevisionPublisher(engine)

    publisher.publish(PublicationRequest("p1", mutations("urn:a", [4.0])))

    assert [v for _, v, _ in rows(engine, "urn:a")] == [4.0]


def test_an_empty_publication_is_not_recorded(setup):
    store, engine, publisher = setup
    empty = mutations("urn:a", [])

    receipt = publisher.publish(PublicationRequest("p1", empty))

    assert receipt.row_count == 0 and receipt.versions == {} and engine.pending() == 0


def test_a_step_reports_where_its_time_went(setup):
    store, engine, publisher = setup
    publisher.publish(PublicationRequest("p1", mutations("urn:a", [1.0, 2.0])))

    report = engine.step()

    assert set(report.timings) == {"prepare", "apply", "evaluate", "commit"}
    assert all(t >= 0 for t in report.timings.values())
    assert sum(report.timings.values()) <= report.seconds


def test_the_ingest_event_carries_the_inbox_receipt_time(setup, tmp_path):
    import json
    import time
    from acquirium.Materialization.events import EventLog
    _, engine, publisher = setup
    engine.store.events = EventLog(tmp_path / "events.jsonl")
    try:
        publisher.publish(PublicationRequest("p1", mutations("urn:a", [1.0])))
        time.sleep(0.2)
        engine.step()
    finally:
        engine.store.events.close()
        engine.store.events = None
    ingest = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()
              if json.loads(line)["kind"] == "ingest"]
    assert len(ingest) == 1
    # Received when it entered the inbox, applied by the step that followed.
    assert ingest[0]["received"] <= ingest[0]["t"] and ingest[0]["t"] - ingest[0]["received"] >= 0.2
