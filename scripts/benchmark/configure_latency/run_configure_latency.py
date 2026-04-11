"""
Configure-latency benchmark driver.

Spawns N real worker containers via the existing run_app() path, each running
the same ExternalReferenceWarning app the scalability benchmark uses, and
collects per-container "build_query() + Query.execute()" timings reported by
the instrumented worker (src/acquirium/Apps/worker.py).

Two events are captured per container:

  * ``initial`` — emitted once when the container's worker first builds its
    query at startup.
  * ``refresh`` — emitted whenever the worker's keep-alive loop detects that
    /graph_version has bumped and rebuilds its cached query. The driver
    explicitly bumps the version by re-inserting the same model after every
    container has reported its initial event.

The driver runs the receiver in-process on a background thread so it can
inspect the live event list directly (by event type) instead of polling a CSV
file. The receiver also writes a CSV alongside for offline analysis.

Sweep axis: only the concurrent app count N. The graph model is fixed for one
run; vary N across runs by re-invoking this script.

Usage:
    uv run scripts/benchmark/configure_latency/run_configure_latency.py \
        --model deployments/DPR/dpr-combined-model.ttl \
        --n 10 \
        --out scripts/benchmark/configure_latency/results/results_10.csv
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import threading
import time
from http.server import HTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
# Reuse ExternalReferenceWarning + the TTL helper from scalability.py — same
# app as the existing scalability benchmark, so configure-latency numbers
# correspond to a real production app.
sys.path.insert(0, str(REPO_ROOT / "scripts" / "benchmark"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scalability import ExternalReferenceWarning, _data_node_uris_from_ttl  # noqa: E402

from acquirium import Acquirium  # noqa: E402

from receiver import ConfigureLatencyHandler  # noqa: E402


# Same default as scalability.py — must match the host alias the worker
# containers can reach. Linux is covered by the manager's
# extra_hosts={"host.docker.internal": "host-gateway"} bridge.
DEFAULT_RECEIVER_HOST = os.environ.get("CONFIG_LATENCY_HOST", "host.docker.internal")
DEFAULT_RECEIVER_PORT = int(os.environ.get("CONFIG_LATENCY_PORT", "10001"))


def _start_receiver_thread(csv_path: Path, port: int) -> HTTPServer:
    ConfigureLatencyHandler.reset(str(csv_path))
    server = HTTPServer(("0.0.0.0", port), ConfigureLatencyHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"[driver] receiver thread up on http://0.0.0.0:{port}/configure_latency")
    return server


def _ensure_port_free(port: int) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("0.0.0.0", port))
    except OSError as exc:
        raise SystemExit(f"port {port} is already bound: {exc}") from exc
    finally:
        s.close()


def _wait_for(
    *,
    event: str,
    expected: int,
    timeout: float,
    label: str,
) -> int:
    """Block until N reports of ``event`` are in, or until the timeout fires.

    Returns the number actually received. Logs progress every couple seconds
    so a stuck cell is obvious.
    """
    deadline = time.time() + timeout
    last_logged = -1
    while True:
        got = ConfigureLatencyHandler.count(event=event)
        if got != last_logged:
            # print(f"[driver] {label}: {got}/{expected} {event!r} reports")
            last_logged = got
        if got >= expected:
            return got
        if time.time() >= deadline:
            print(
                f"[driver] WARNING: {label}: timed out waiting for {event!r} "
                f"reports ({got}/{expected})"
            )
            return got
        time.sleep(0.5)


def run_one(
    *,
    model_path: Path,
    n: int,
    refreshes: int,
    out_csv: Path,
    interval: float,
    startup_delay: float,
    server_url: str,
    server_port: int,
    receiver_host: str,
    receiver_port: int,
    initial_timeout: float,
    refresh_timeout: float,
) -> None:
    print(f"[driver] model={model_path.name} n={n} refreshes={refreshes} out={out_csv}")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    _ensure_port_free(receiver_port)
    server = _start_receiver_thread(out_csv, receiver_port)

    aq = Acquirium(server_url=server_url, server_port=server_port)

    # Receiver URL the worker containers will POST to. The container reaches
    # the host via host.docker.internal (Mac/Win) or the bridge alias the
    # manager adds for Linux.
    receiver_url = f"http://{receiver_host}:{receiver_port}/configure_latency"

    apps: list[ExternalReferenceWarning] = []
    started = False

    try:
        print(f"[driver] inserting graph: {model_path}")
        aq.insert_graph(str(model_path), replace=True)

        uris = _data_node_uris_from_ttl(str(model_path))
        if not uris:
            raise SystemExit(
                f"No hasExternalReference data nodes found in {model_path}"
            )
        print(f"[driver] {len(uris)} candidate URIs in model")

        # Build N app instances. Cycle URIs when N exceeds the model's URI
        # count — the SPARQL shape is identical regardless of which point URI
        # is bound, so cycling doesn't bias the configure-latency timing.
        for i in range(n):
            uri = uris[i % len(uris)]
            app = ExternalReferenceWarning(uri)
            app.name = f"configure_latency_{i}"
            apps.append(app)

        # Register + start every container. Each gets the receiver URL via
        # params (worker.py picks it up via _resolve_configure_latency_url).
        print(f"[driver] starting {n} containers (startup_delay={startup_delay}s)")
        for i, app in enumerate(apps):
            aq.register_app(app)
            aq.run_app(
                app.name,
                keep_alive=True,
                interval=interval,
                params={
                    "point_uri": app.point_uri,
                    "__config_latency_url": receiver_url,
                },
            )
            time.sleep(startup_delay)
        started = True
        print(f"[driver] all {n} containers started")

        # Wait for every container to emit its initial build report.
        _wait_for(
            event="initial",
            expected=n,
            timeout=initial_timeout,
            label="initial-build",
        )

        # Loop: on each round, bump the graph version once and wait for every
        # container to report a fresh rebuild before bumping again. This
        # ordering matters. The worker's refresh check only compares "current
        # version != last version", so two back-to-back bumps would collapse
        # into a single rebuild. Waiting for all N reports before bumping
        # again guarantees exactly ``refreshes * n`` refresh events.
        round_wallclocks: list[float] = []
        for round_i in range(refreshes):
            expected_total = (round_i + 1) * n
            # print(
            #     f"[driver] refresh round {round_i + 1}/{refreshes}: "
            #     f"re-inserting graph to bump version"
            # )
            bump_ts = time.perf_counter()
            aq.insert_graph(str(model_path), replace=True)

            _wait_for(
                event="refresh",
                expected=expected_total,
                timeout=refresh_timeout,
                label=f"refresh-round-{round_i + 1}",
            )
            elapsed_ms = (time.perf_counter() - bump_ts) * 1000.0
            round_wallclocks.append(elapsed_ms)
            # print(
            #     f"[driver] round {round_i + 1}/{refreshes}: +{n} reports "
            #     f"in {elapsed_ms:.0f} ms"
            #     + (f" (MISSING {missing})" if missing > 0 else "")
            # )

        # total_refresh = ConfigureLatencyHandler.count(event="refresh")
        # avg_wall = sum(round_wallclocks) / len(round_wallclocks) if round_wallclocks else 0.0
        # print(
        #     f"[driver] summary: initial={got_initial}/{n} "
        #     f"refresh={total_refresh}/{refreshes * n} "
        #     f"avg_round_wallclock={avg_wall:.0f} ms"
        # )

    finally:
        if started:
            print("[driver] stopping containers")
            for app in apps:
                try:
                    aq.stop_app(app_id=app.name)
                except Exception as exc:
                    print(f"[driver] failed to stop {app.name}: {exc}")
        server.shutdown()
        server.server_close()
        print(f"[driver] receiver collected {ConfigureLatencyHandler.count()} reports")
        print(f"[driver] CSV written to {out_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, help="TTL file with hasExternalReference data nodes.")
    parser.add_argument("--n", type=int, required=True, help="Number of concurrent app containers.")
    parser.add_argument(
        "--refreshes",
        type=int,
        default=1,
        help="Number of graph-change refresh rounds to run. Each round bumps "
             "the graph version once and waits for all N containers to rebuild.",
    )
    parser.add_argument("--out", required=True, help="Output CSV path.")
    parser.add_argument("--interval", type=float, default=2.0, help="Worker keep-alive poll interval (s).")
    parser.add_argument(
        "--startup-delay",
        type=float,
        default=0.5,
        help="Sleep between consecutive container starts (s). Higher = gentler on docker daemon at high N.",
    )
    parser.add_argument("--server-url", default="localhost")
    parser.add_argument("--server-port", type=int, default=8000)
    parser.add_argument("--receiver-host", default=DEFAULT_RECEIVER_HOST)
    parser.add_argument("--receiver-port", type=int, default=DEFAULT_RECEIVER_PORT)
    parser.add_argument(
        "--initial-timeout",
        type=float,
        default=None,
        help="Seconds to wait for the initial-build phase. Default scales with n + startup_delay.",
    )
    parser.add_argument(
        "--refresh-timeout",
        type=float,
        default=None,
        help="Seconds to wait for the refresh phase. Default scales with n + interval.",
    )
    args = parser.parse_args()

    model_path = Path(args.model)
    if not model_path.is_file():
        raise SystemExit(f"Model TTL not found: {model_path}")

    initial_timeout = args.initial_timeout
    if initial_timeout is None:
        initial_timeout = max(60.0, args.n * args.startup_delay + 60.0)

    refresh_timeout = args.refresh_timeout
    if refresh_timeout is None:
        # Budget has to cover every round: worker polling delay + SPARQL cost
        # per container, scaled by the total number of refresh events. Add a
        # generous floor so low-N cells don't fail on cold-start jitter.
        refresh_timeout = max(
            60.0,
            args.refreshes * (args.n * args.interval + 30.0),
        )

    run_one(
        model_path=model_path,
        n=args.n,
        refreshes=args.refreshes,
        out_csv=Path(args.out),
        interval=args.interval,
        startup_delay=args.startup_delay,
        server_url=args.server_url,
        server_port=args.server_port,
        receiver_host=args.receiver_host,
        receiver_port=args.receiver_port,
        initial_timeout=initial_timeout,
        refresh_timeout=refresh_timeout,
    )


if __name__ == "__main__":
    main()
