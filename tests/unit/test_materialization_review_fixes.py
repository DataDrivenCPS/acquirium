from datetime import datetime, timedelta, timezone

import pytest

from acquirium.Materialization.impact import TimeRange
from acquirium.Materialization.state import ArtifactCandidate, ArtifactRequest
from acquirium.Storage.artifacts import ArtifactRecord
from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB


def test_expired_artifact_lease_cannot_complete(tmp_path):
    store = DuckDBStore(tmp_path / "artifact-lease.duckdb", recreate=True)
    materialization = MaterializationDuckDB(store)
    try:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        request = ArtifactRequest(
            "request-1", "transform", "deployment", "binding", {"urn:input": 1},
            TimeRange(start, start + timedelta(seconds=1)),
        )
        materialization.create_artifact_request(request)
        lease = materialization.lease_artifact_request("worker", duration=timedelta(microseconds=1))
        assert lease is not None

        candidate = ArtifactCandidate(b"candidate")
        artifact = ArtifactRecord(candidate.digest, "urn:artifact", len(candidate.data), "application/octet-stream", {})
        with pytest.raises(ValueError, match="stale"):
            materialization.complete_artifact_request(lease, artifact, candidate)
    finally:
        store.close()
