"""
Chain latency receiver for Acquirium chain benchmark.

Receives POST /chain messages from each level in the chain and computes
per-level and end-to-end latency statistics.

Records:
- Per-level processing time (time_completed - time_received)
- Cumulative latency from measurement to each level
- End-to-end latency for complete chains

Usage:
    python scripts/benchmark/chain_receiver.py <csv_output_file> [port]

Example:
    python scripts/benchmark/chain_receiver.py chain_results.csv 10000
"""

from __future__ import annotations

import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any


class ChainStats:
    """Tracks statistics for chain latency measurements.

    NOTE: Only processing_time (time_completed - time_received) is reliable
    since both timestamps come from the same Docker container. Cross-machine
    latencies are subject to clock skew and are not tracked.
    """

    def __init__(self):
        # Per-level processing times (time_completed - time_received) - RELIABLE
        self.level_processing_times: dict[int, list[float]] = defaultdict(list)
        # Count of messages per level
        self.message_counts: dict[int, int] = defaultdict(int)
        # Complete chain counts
        self.complete_chains: dict[int, int] = defaultdict(int)

    def record(self, msg: dict[str, Any], endpoint_receipt: datetime) -> dict[str, float | None]:
        """Record a chain message and return computed latencies.

        NOTE: Only processing_time (time_completed - time_received) is reliable
        since both timestamps come from the same machine (the Docker container).
        Cross-machine latencies (to_endpoint, total) are subject to clock skew
        between Docker containers and the host.
        """
        level = msg.get("level", 0)
        chain_depth = msg.get("chain_depth", 1)
        is_final = msg.get("is_final", False)

        time_received_str = msg.get("time_received")
        time_completed_str = msg.get("time_completed")

        # Parse timestamps
        time_received = self._parse_ts(time_received_str)
        time_completed = self._parse_ts(time_completed_str)

        # Compute processing time (RELIABLE - same clock)
        processing_time = self._delta_ms(time_received, time_completed)

        # Record statistics - only processing_time is reliable
        self.message_counts[level] += 1

        if processing_time is not None:
            self.level_processing_times[level].append(processing_time)

        if is_final:
            self.complete_chains[chain_depth] += 1

        return {
            "processing_time_ms": processing_time,
        }

    def _parse_ts(self, s: str | None) -> datetime | None:
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except (ValueError, TypeError):
            return None

    def _delta_ms(self, t1: datetime | None, t2: datetime | None) -> float | None:
        if t1 and t2:
            return (t2 - t1).total_seconds() * 1000
        return None

    def print_summary(self):
        """Print summary statistics."""
        print("\n" + "=" * 60)
        print("CHAIN LATENCY STATISTICS")
        print("=" * 60)

        if not self.message_counts:
            print("No messages received.")
            return

        print("\n--- Per-Level Processing Time (time_completed - time_received) ---")
        print("    (Reliable: both timestamps from same Docker container)")
        total_processing_times = []
        for level in sorted(self.level_processing_times.keys()):
            times = self.level_processing_times[level]
            if times:
                total_processing_times.extend(times)
                print(f"  Level {level}: count={len(times)}, "
                      f"mean={statistics.mean(times):.2f}ms, "
                      f"median={statistics.median(times):.2f}ms, "
                      f"min={min(times):.2f}ms, max={max(times):.2f}ms")
                if len(times) > 1:
                    print(f"           stdev={statistics.stdev(times):.2f}ms")

        if total_processing_times:
            print(f"\n  TOTAL (all levels): count={len(total_processing_times)}, "
                  f"mean={statistics.mean(total_processing_times):.2f}ms, "
                  f"median={statistics.median(total_processing_times):.2f}ms")

        print("\n--- Message Counts ---")
        for level in sorted(self.message_counts.keys()):
            count = self.message_counts[level]
            print(f"  Level {level}: {count} messages")

        print(f"\n--- Complete Chains ---")
        for depth in sorted(self.complete_chains.keys()):
            count = self.complete_chains[depth]
            print(f"  Depth {depth}: {count} complete chains")

        # Estimate total chain processing time by summing per-level means
        if len(self.level_processing_times) > 1:
            print(f"\n--- Estimated Chain Processing Time ---")
            print("    (Sum of per-level mean processing times)")
            for depth in sorted(self.complete_chains.keys()):
                levels_in_chain = [l for l in self.level_processing_times.keys() if l <= depth]
                if levels_in_chain:
                    total_mean = sum(statistics.mean(self.level_processing_times[l])
                                    for l in levels_in_chain if self.level_processing_times[l])
                    print(f"  Depth {depth}: ~{total_mean:.2f}ms (sum of {len(levels_in_chain)} levels)")

        print("=" * 60 + "\n")


class ChainHandler(BaseHTTPRequestHandler):
    csv_file: str = ""
    csv_writer: Any = None
    csv_fp: Any = None
    stats: ChainStats = ChainStats()
    message_count: int = 0

    def do_POST(self) -> None:
        endpoint_receipt = datetime.utcnow()

        if self.path != "/chain":
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

        # Extract message (may be nested under "message" key from trigger)
        msg = payload.get("message", payload)

        # Record stats
        latencies = ChainHandler.stats.record(msg, endpoint_receipt)

        ChainHandler.message_count += 1
        msg_id = ChainHandler.message_count

        level = msg.get("level", 0)
        chain_depth = msg.get("chain_depth", 1)
        is_final = msg.get("is_final", False)
        value = msg.get("value", "")
        app_id = msg.get("app_id", "unknown")

        # Write to CSV
        if ChainHandler.csv_writer:
            ChainHandler.csv_writer.writerow([
                msg_id,
                level,
                chain_depth,
                is_final,
                app_id,
                value,
                msg.get("time_received", ""),
                msg.get("time_completed", ""),
                endpoint_receipt.isoformat(),
                latencies["processing_time_ms"] or "",
            ])
            ChainHandler.csv_fp.flush()

        # Log
        def fmt(v: float | None) -> str:
            return f"{v:.2f}" if v is not None else "N/A"

        final_marker = " [FINAL]" if is_final else ""
        print(
            f"[{msg_id}] L{level}/{chain_depth}{final_marker} {app_id}: "
            f"processing={fmt(latencies['processing_time_ms'])}ms"
        )

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python chain_receiver.py <csv_output_file> [port]")
        sys.exit(1)

    csv_file = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 10000

    ChainHandler.csv_file = csv_file
    ChainHandler.stats = ChainStats()

    # Open CSV and write header
    ChainHandler.csv_fp = open(csv_file, "w", newline="")
    ChainHandler.csv_writer = csv.writer(ChainHandler.csv_fp)
    ChainHandler.csv_writer.writerow([
        "msg_id",
        "level",
        "chain_depth",
        "is_final",
        "app_id",
        "value",
        "time_received",
        "time_completed",
        "endpoint_receipt",
        "processing_time_ms",
    ])

    server = HTTPServer(("0.0.0.0", port), ChainHandler)
    print(f"Chain receiver listening on http://0.0.0.0:{port}/chain")
    print(f"Writing results to: {csv_file}")
    print("Press Ctrl-C to stop and see statistics.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        ChainHandler.csv_fp.close()
        print(f"\nReceived {ChainHandler.message_count} messages total")
        ChainHandler.stats.print_summary()


if __name__ == "__main__":
    main()
