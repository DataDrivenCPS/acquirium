"""
Configure-latency receiver.

Collects per-container build/SPARQL timing reports emitted by the instrumented
worker (src/acquirium/Apps/worker.py). Each container POSTs once on initial
build and once on every refresh after a graph version bump. The worker only
emits when ACQUIRIUM_CONFIG_LATENCY_URL is set, so passing this receiver's URL
into the container env is what turns benchmarking on.

CSV columns:
    msg_id, app_id, event, build_ms, sparql_ms, total_ms, graph_version, ts,
    endpoint_receipt

Usage:
    python scripts/benchmark/configure_latency/receiver.py <csv_output_file> [port]
"""

from __future__ import annotations

import csv
import json
import sys
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

CSV_FIELDS = [
    "msg_id",
    "app_id",
    "event",
    "build_ms",
    "sparql_ms",
    "total_ms",
    "graph_version",
    "ts",
    "endpoint_receipt",
]


class ConfigureLatencyHandler(BaseHTTPRequestHandler):
    """HTTP handler for configure-latency reports.

    Class-level state lets a driver process import this module and inspect
    the live event list to know when all expected reports have arrived,
    while still optionally appending to a CSV for offline analysis.
    """

    csv_file: str | None = None
    _lock = threading.Lock()
    reports: list[dict] = []

    @classmethod
    def reset(cls, csv_file: str | None) -> None:
        with cls._lock:
            cls.csv_file = csv_file
            cls.reports = []
        if csv_file:
            with open(csv_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writeheader()

    @classmethod
    def count(cls, *, event: str | None = None) -> int:
        with cls._lock:
            if event is None:
                return len(cls.reports)
            return sum(1 for r in cls.reports if r.get("event") == event)

    def do_POST(self) -> None:
        endpoint_receipt = datetime.utcnow().isoformat()

        if self.path != "/configure_latency":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b""
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            return

        with ConfigureLatencyHandler._lock:
            msg_id = len(ConfigureLatencyHandler.reports) + 1
            row = {
                "msg_id": msg_id,
                "app_id": payload.get("app_id", ""),
                "event": payload.get("event", ""),
                "build_ms": payload.get("build_ms", ""),
                "sparql_ms": payload.get("sparql_ms", ""),
                "total_ms": payload.get("total_ms", ""),
                "graph_version": payload.get("graph_version", ""),
                "ts": payload.get("ts", ""),
                "endpoint_receipt": endpoint_receipt,
            }
            ConfigureLatencyHandler.reports.append(row)
            csv_file = ConfigureLatencyHandler.csv_file

        if csv_file:
            with open(csv_file, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writerow(row)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python receiver.py <csv_output_file> [port]")
        sys.exit(1)

    csv_file = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 10001

    ConfigureLatencyHandler.reset(csv_file)

    server = HTTPServer(("0.0.0.0", port), ConfigureLatencyHandler)
    print(f"Configure-latency receiver listening on http://0.0.0.0:{port}/configure_latency")
    print(f"Writing results to: {csv_file}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print(f"\nReceived {ConfigureLatencyHandler.count()} messages total")


if __name__ == "__main__":
    main()
