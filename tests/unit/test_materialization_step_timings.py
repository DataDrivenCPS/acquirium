"""A step says where its time went, so the step cost can be measured by phase."""
from tests.unit.test_materialization_effective_time import T, _runtime


def test_an_evaluated_step_reports_reads_transforms_and_publication(tmp_path):
    store, graph, runtime = _runtime(tmp_path, ["urn:a", "urn:b"])
    try:
        timings = runtime.timings
        assert set(timings) == {"read", "transform", "publish"} and all(t >= 0 for t in timings.values())
        store.upsert_rows("urn:a", [(t, 2.0) for t in T], value_kind="numeric")
        report = runtime.engine.step()
        assert report is not None and {"prepare", "apply", "evaluate", "commit", "read", "transform", "publish"} <= set(report.timings)
        assert report.timings["read"] + report.timings["transform"] + report.timings["publish"] <= report.timings["evaluate"] + 1e-6
    finally:
        runtime.close(); store.close()
