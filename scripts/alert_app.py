"""
Dummy alert receiver for Acquirium trigger outputs.

Runs a small HTTP server that listens on localhost:10000 and prints
incoming POST /alerts payloads to stdout.
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import datetime
bench_file = "scripts/results_20_1.txt"

class AlertHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        if self.path != "/alerts":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b""
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            payload = {"_raw": raw.decode("utf-8", errors="replace")}
        
        print("Received alert payload:", payload)
        data_ts = payload.get("message",{}).get("data", {}).get("timestamp", "no_timestamp")
        msg_ts = payload.get("ts", "no_timestamp")
        
        with open(bench_file, "a") as f:
            if "no_timestamp" not in (data_ts, msg_ts):
                data_ts = datetime.datetime.fromisoformat(data_ts)
                msg_ts = datetime.datetime.fromisoformat(msg_ts)
                latency = (msg_ts - data_ts).total_seconds() * 1000  # latency in milliseconds
                f.write(f"{latency}\n")

        # print("Received alert payload:", json.dumps(payload, ensure_ascii=True))

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Dummy Acquirium alert receiver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=10000)
    args = parser.parse_args()

    server = HTTPServer((args.host, args.port), AlertHandler)
    print(f"Alert receiver listening on http://{args.host}:{args.port}/alerts")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
