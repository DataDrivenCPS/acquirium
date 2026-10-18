"""The diagnostic launcher must preserve real native worker results and accounting."""

import pytest

from examples.profiling.run import STAGES, run_case, summarize


@pytest.mark.integration
def test_actual_worker_profiles_cover_every_executed_operator(tmp_path):
    results = []
    for instrumented in (False, True):
        directory = tmp_path / ("profile" if instrumented else "control")
        directory.mkdir()
        results.append(
            run_case(
                {
                    "directory": str(directory),
                    "scenario": "batch8",
                    "workers": 2,
                    "instrumented": instrumented,
                    "samples": 1,
                }
            )
        )
    control, profile = results
    assert control["verified"] and profile["verified"]
    for result in results:
        stats = result["python_worker_stats"]
        assert 1 <= stats["spawned"] <= 2
        assert stats["reused"] >= 4
        assert stats["resident"] == stats["idle"] <= 2
    assert control["epochs"][0]["requests"] == profile["epochs"][0]["requests"] == 8
    assert [
        execution["groups"] for execution in profile["epochs"][0]["executions"]
    ] == [8] * 4
    assert len(profile["workers"]) == 4
    for worker in profile["workers"]:
        assert worker["groups"] == 8
        assert worker["input_rows"] > 0
        assert not worker["cold_worker"]
        assert worker["polars_import_ns"] == worker["pyarrow_import_ns"] == 0
        measured_ns = sum(worker[f"{stage}_ns"] for stage in STAGES)
        assert all(worker[f"{stage}_ns"] >= 0 for stage in STAGES)
        assert worker["external_and_unattributed_ms"] >= 0
        assert measured_ns / 1e6 <= worker["native_operator_ms"]
    summary = summarize(results, {})
    assert summary["verified"]
    assert len(summary["cases"]) == 2
    assert summary["cases"][1]["worker_mean"]["groups"] == 8
