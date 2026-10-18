"""Real native workload: immutable model reuse, corrections, and cold recovery."""

import json

import pytest

from examples.model_workload.run import run_case


@pytest.mark.integration
@pytest.mark.parametrize("restart", [False, True])
def test_model_load_is_verified_across_recycling_and_restart(tmp_path, restart):
    directory = tmp_path / "load"
    result = run_case(
        directory,
        pool_size=1,
        units=2,
        copies=4,
        duration=0.2,
        rates=(5, 100, 5),
        max_jobs=3,
        max_pending=2,
        epoch_requests=2,
        restart=restart,
    )
    assert result["verified"]
    assert len(result["sessions"]) == (2 if restart else 1)
    assert result["admitted"] == result["publication_observed_ms"]["count"]
    assert result["checks"][-1]["rows"] < result["checks"][-2]["rows"]
    assert result["capacity_retries"] > 0
    assert all(
        session["processor"]["engine"]["pending_requests"] == 0
        for session in result["sessions"]
    )
    assert (
        sum(session["python_workers"]["spawned"] for session in result["sessions"]) > 1
    )
    samples = [
        json.loads(line)
        for line in (directory / "samples.jsonl").read_text().splitlines()
    ]
    assert max(sample["python_workers"]["resident"] for sample in samples) == 1
    assert max(sample["engine"]["pending_requests"] for sample in samples) <= 2
    for session in result["sessions"]:
        stats = session["python_workers"]
        telemetry = stats["telemetry"]
        assert stats["failures"] == stats["queue_timeouts"] == 0
        assert stats["queued"] == 0
        assert telemetry["returned_jobs"] == telemetry["lease_requests"]
        assert telemetry["cold_service"]["count"] == stats["spawned"]
        assert telemetry["warm_service"]["count"] == stats["reused"]
        assert len(telemetry["workers"]) <= 1
