"""End-to-end test: graph_version endpoint + worker rebuild on graph change.

Assumes a running acquirium server on localhost:8000 (e.g. ``make up``).
Run with: ``uv run python scripts/e2e/test_graph_version_refresh.py``.

Steps
-----
1. Probe ``/graph_version`` baseline.
2. Insert a tiny graph and assert the version bumps by exactly 1.
3. Register the ``VersionRefreshApp`` and start it in keep-alive mode with a
   short interval.
4. Wait for the worker to log its initial build (E2E_BUILD_QUERY count=1).
5. Insert another graph and assert the version bumps again.
6. Wait one or two intervals and assert the worker logs a *second* build
   (E2E_BUILD_QUERY count=2) plus a "Graph version changed" line.
7. Stop the keep-alive container.

The script exits non-zero on any failed assertion.
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts" / "e2e"))

from acquirium import Acquirium  # noqa: E402

from version_refresh_app import VersionRefreshApp  # noqa: E402

INTERVAL = 3.0  # seconds between worker runs
BASE_GRAPH = """@prefix ex: <http://example.org/> .
ex:s1 ex:p ex:o1 .
"""
SECOND_GRAPH = """@prefix ex: <http://example.org/> .
ex:s2 ex:p ex:o2 .
"""


def fail(msg: str) -> None:
    print(f"\033[31mFAIL\033[0m {msg}", flush=True)
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"\033[32mOK\033[0m   {msg}", flush=True)


def step(msg: str) -> None:
    print(f"\033[36m==>\033[0m {msg}", flush=True)


def container_logs(container_id: str) -> str:
    """Return the full stdout+stderr of a docker container."""
    result = subprocess.run(
        ["docker", "logs", container_id],
        capture_output=True,
        text=True,
        check=False,
    )
    return (result.stdout or "") + (result.stderr or "")


def wait_for_log(container_id: str, pattern: str, timeout: float) -> str | None:
    """Poll docker logs until ``pattern`` matches or ``timeout`` elapses."""
    deadline = time.time() + timeout
    rx = re.compile(pattern)
    last_logs = ""
    while time.time() < deadline:
        last_logs = container_logs(container_id)
        if rx.search(last_logs):
            return last_logs
        time.sleep(0.5)
    print("---- last container logs ----", flush=True)
    print(last_logs, flush=True)
    print("-----------------------------", flush=True)
    return None


def main() -> int:
    aq = Acquirium(server_url="localhost", server_port=8000)

    step("1. Probe /graph_version baseline")
    v0 = aq.client.graph_version()
    print(f"     baseline version = {v0}")

    step("2. Insert base graph")
    aq.insert_graph(BASE_GRAPH, replace=True)
    v1 = aq.client.graph_version()
    if v1 != v0 + 1:
        fail(f"expected version to bump by 1, got {v0} -> {v1}")
    ok(f"version bumped {v0} -> {v1}")

    step("3. Register VersionRefreshApp")
    app = VersionRefreshApp()
    # The trivial query has no nodes, so skip dependency resolution.
    aq.register_app(app, depends_on=[], resolve_dependencies=False)
    v_after_register = aq.client.graph_version()
    if v_after_register <= v1:
        fail(
            f"register_app should bump version (writes RDF), got {v1} -> {v_after_register}"
        )
    ok(f"register_app bumped version {v1} -> {v_after_register}")

    step(f"4. Start keep-alive worker (interval={INTERVAL}s)")
    run_resp = aq.run_app(app.name, keep_alive=True, interval=INTERVAL)
    run_id = run_resp.get("run_id")
    if not run_id:
        fail(f"run_app returned no run_id: {run_resp}")
    print(f"     run_id={run_id[:12]}")

    try:
        step("5. Wait for initial build (E2E_BUILD_QUERY count=1)")
        logs = wait_for_log(run_id, r"E2E_BUILD_QUERY count=1", timeout=30)
        if logs is None:
            fail("worker never logged the initial build")
        ok("worker built query once at startup")

        # Make sure the worker has fetched its initial graph_version too.
        logs = wait_for_log(run_id, r"Initial graph version: \d+", timeout=15)
        if logs is None:
            fail("worker never logged 'Initial graph version'")
        match = re.search(r"Initial graph version: (\d+)", logs)
        worker_v_initial = int(match.group(1)) if match else -1
        ok(f"worker captured initial graph version = {worker_v_initial}")

        # Confirm at least one run has happened (so we know the loop is alive).
        if not wait_for_log(run_id, r"E2E_RUN", timeout=INTERVAL * 2 + 5):
            fail("worker never executed run()")
        ok("worker executed at least one run()")

        step("6. Mutate the graph and verify rebuild")
        v_before_mut = aq.client.graph_version()
        aq.insert_graph(SECOND_GRAPH, replace=False)
        v_after_mut = aq.client.graph_version()
        if v_after_mut <= v_before_mut:
            fail(f"graph mutation didn't bump version: {v_before_mut} -> {v_after_mut}")
        ok(f"graph mutation bumped version {v_before_mut} -> {v_after_mut}")

        # The worker should poll between runs (after _run_once finishes), so
        # within ~2 intervals we should see the rebuild log line.
        timeout = INTERVAL * 4 + 5
        step(f"7. Wait up to {timeout:.0f}s for worker rebuild log")
        logs = wait_for_log(
            run_id,
            r"Graph version changed \(\d+ -> \d+\); rebuilding query",
            timeout=timeout,
        )
        if logs is None:
            fail("worker never logged the rebuild")
        ok("worker logged graph_version change")

        # And it should have called build_query a second time.
        logs = wait_for_log(run_id, r"E2E_BUILD_QUERY count=2", timeout=timeout)
        if logs is None:
            fail("worker never re-ran build_query")
        ok("worker re-ran build_query (count=2)")

        # Pin down that the rebuild happened *after* the first run, not at startup.
        rebuild_match = re.search(
            r"Graph version changed \((\d+) -> (\d+)\); rebuilding query", logs
        )
        if rebuild_match:
            from_v = int(rebuild_match.group(1))
            to_v = int(rebuild_match.group(2))
            print(f"     rebuild observed: {from_v} -> {to_v}")
            if to_v < v_after_mut:
                fail(
                    f"worker observed stale version {to_v}, expected >= {v_after_mut}"
                )
            ok("worker observed at least the post-mutation version")

        step("8. All assertions passed")
    finally:
        try:
            stop_resp = aq.stop_app(run_id=run_id)
            print(f"     stop_app: {stop_resp}")
        except Exception as exc:
            print(f"     stop_app failed: {exc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
