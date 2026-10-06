"""The opt-in event log records ingestion, invocations, commits and plans."""
from datetime import datetime, timedelta, timezone
import json

import pyarrow as pa

from acquirium.Materialization import (
    App, Binding, InProcessExecutor, RevisionStore, Scheduler, StreamDescriptor, output,
)
from acquirium.Materialization.events import EventLog, emit
from acquirium.Materialization.planner import output_port
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.publication.revision import RevisionPublisher
from acquirium.Storage.publication.types import PublicationRequest

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class Double(App):
    grouping = "per_match"
    backfill = True
    outputs = {"out": output.stream(value_kind="numeric")}

    def transform(self, inputs, output, context):
        source = inputs["source"].collect()
        output["out"] = pa.table({"time": source["time"], "value": [2.0 * v for v in source["value"].to_pylist()]})


def _lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def _binding():
    inputs = {"source": (StreamDescriptor("urn:source"),)}
    return Binding("double", "digest", inputs, {"out": output_port("double", "out", inputs, Double.outputs["out"])})


def test_emit_is_a_no_op_without_a_log(tmp_path):
    store = DuckDBStore(tmp_path / "ts.duckdb")
    emit(store, "ingest", revision=1)  # no ``events`` attribute: nothing happens
    assert not list(tmp_path.glob("*.jsonl"))


def test_ingest_invocation_and_commit_lines_share_revisions(tmp_path):
    store = DuckDBStore(tmp_path / "ts.duckdb")
    store.events = EventLog(tmp_path / "events.jsonl")
    mutations = pa.table({
        "operation": ["upsert", "upsert"], "ref_uri": ["urn:source", "urn:source"],
        "ts": pa.array([NOW, NOW + timedelta(minutes=1)], pa.timestamp("us", tz="UTC")),
        "numeric_value": [1.0, 2.0], "text_value": pa.array([None, None], pa.string()),
    })
    RevisionPublisher(store).publish(PublicationRequest("pub-1", mutations))

    binding = _binding()
    scheduler = Scheduler(RevisionStore(store), InProcessExecutor())
    assert scheduler.run_once(binding, Double())
    store.events.close()

    lines = _lines(tmp_path / "events.jsonl")
    kinds = [line["kind"] for line in lines]
    assert kinds == ["ingest", "invocation", "commit"]
    ingest, invocation, commit = lines
    assert ingest["rows"] == 2 and ingest["streams"] == 1 and ingest["publication_id"] == "pub-1"
    assert invocation["binding"] == binding.signature and invocation["rows_read"] == {"source": 2}
    assert invocation["seconds"] >= 0
    # The commit consumed the ingest revision and published at a later one.
    assert commit["from_revision"] < ingest["revision"] <= commit["to_revision"]
    assert commit["revision"] > ingest["revision"]
    assert commit["rows_written"] == {"out": 2}
    assert all(line["t"] > 0 and line["mono"] > 0 for line in lines)


def test_replacement_and_failure_are_logged(tmp_path):
    store = DuckDBStore(tmp_path / "ts.duckdb")
    store.events = EventLog(tmp_path / "events.jsonl")
    store.replace_rows("urn:source", [(NOW, 1.0)], value_kind="numeric")

    class Broken(Double):
        def transform(self, inputs, output, context):
            raise RuntimeError("boom")

    binding = _binding()
    scheduler = Scheduler(RevisionStore(store), InProcessExecutor())
    scheduler.run_layer([binding], {binding.signature: Broken()})
    store.events.close()

    lines = _lines(tmp_path / "events.jsonl")
    assert [line["kind"] for line in lines] == ["ingest", "invocation", "failure"]
    assert lines[0]["replace"] is True and lines[0]["ref_uri"] == "urn:source"
    assert "boom" in lines[2]["error"]
