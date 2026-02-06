"""
Latency tracking receiver for Acquirium scalability benchmarks.

Records processing latency at each stage:
- measurement_time: when sensor data was recorded (data.timestamp)
- time_received: when the app received/started processing the data
- time_completed: when the app finished processing
- endpoint_receipt: when the POST arrived at this endpoint

Usage:
    python scripts/benchmark/latency_receiver.py <csv_output_file>
"""

from __future__ import annotations

import csv
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import json


class LatencyHandler(BaseHTTPRequestHandler):
    csv_file: str = ""
    message_count: int = 0

    def do_POST(self) -> None:
        endpoint_receipt = datetime.utcnow()

        if self.path != "/alerts":
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

        # Extract timestamps from payload
        message = payload.get("message", {})
        data = message.get("data", {})

        measurement_time_str = data.get("timestamp")
        time_received_str = message.get("time_received")
        time_completed_str = message.get("time_completed")
        app_id = message.get("app_id", "unknown")

        # Parse timestamps (strip timezone info to avoid naive/aware mismatch)
        def parse_ts(s: str | None) -> datetime | None:
            if not s:
                return None
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt

        measurement_time = parse_ts(measurement_time_str)
        time_received = parse_ts(time_received_str)
        time_completed = parse_ts(time_completed_str)

        # Calculate latencies (in milliseconds)
        def delta_ms(t1: datetime | None, t2: datetime | None) -> float | None:
            if t1 and t2:
                return (t2 - t1).total_seconds() * 1000
            return None

        latency_measurement_to_received = delta_ms(measurement_time, time_received)
        latency_received_to_completed = delta_ms(time_received, time_completed)
        latency_completed_to_endpoint = delta_ms(time_completed, endpoint_receipt)
        latency_total = delta_ms(measurement_time, endpoint_receipt)

        LatencyHandler.message_count += 1
        msg_id = LatencyHandler.message_count

        # Write to CSV
        with open(LatencyHandler.csv_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                msg_id,
                app_id,
                measurement_time_str or "",
                time_received_str or "",
                time_completed_str or "",
                endpoint_receipt.isoformat(),
                latency_measurement_to_received or "",
                latency_received_to_completed or "",
                latency_completed_to_endpoint or "",
                latency_total or "",
            ])

        def fmt(v: float | None) -> str:
            return f"{v:.2f}" if v is not None else "N/A"

        # print(
        #     f"[{msg_id}] {app_id}: "
        #     f"meas→recv={fmt(latency_measurement_to_received)}ms | "
        #     f"recv→done={fmt(latency_received_to_completed)}ms | "
        #     f"done→endpoint={fmt(latency_completed_to_endpoint)}ms | "
        #     f"total={fmt(latency_total)}ms"
        # )

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python latency_receiver.py <csv_output_file> [port]")
        sys.exit(1)

    csv_file = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 10000

    LatencyHandler.csv_file = csv_file

    # Write CSV header
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "msg_id",
            "app_id",
            "measurement_time",
            "time_received",
            "time_completed",
            "endpoint_receipt",
            "latency_measurement_to_received_ms",
            "latency_received_to_completed_ms",
            "latency_completed_to_endpoint_ms",
            "latency_total_ms",
        ])

    server = HTTPServer(("0.0.0.0", port), LatencyHandler)
    print(f"Latency receiver listening on http://0.0.0.0:{port}/alerts")
    print(f"Writing results to: {csv_file}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print(f"\nReceived {LatencyHandler.message_count} messages total")


if __name__ == "__main__":
    main()
