"""
Chain latency benchmark for Acquirium soft sensors.

Creates a chain of soft sensors where each sensor consumes the output of the
previous one and increments the value by 1. Measures end-to-end latency as
chain depth increases.

Chain structure:
  Benicia sensor data -> Sensor_0 -> Sensor_1 -> ... -> Sensor_N -> Trigger

Usage:
    python scripts/benchmark/chain_latency.py <chain_depth> [timeout_seconds]

Example:
    python scripts/benchmark/chain_latency.py 5 60
"""

import atexit
import os
import signal
import sys
import time
from datetime import datetime, timezone

from acquirium import Acquirium, App, AppContext, Output

# Alert endpoint configuration
ALERT_HOST = os.environ.get("ALERT_HOST", "host.docker.internal")
ALERT_PORT = os.environ.get("ALERT_PORT", "10000")

def make_chain_point_uri(level: int) -> str:
    """Generate point_uri for a chain level."""
    return f"urn:derived:chain_level_{level}"


def parse_chain_config_from_app_id(app_id: str) -> tuple[int, int, bool]:
    """
    Parse level and chain_depth from app_id.
    Expected formats:
      - chain_level_0_of_5 -> level=0, chain_depth=5, is_final=False
      - chain_final_5_of_5 -> level=5, chain_depth=5, is_final=True
    Returns (level, chain_depth, is_final)
    """
    import re
    # Try chain_level_N_of_M or chain_final_N_of_M
    match = re.match(r"chain_(level|final)_(\d+)_of_(\d+)", app_id)
    if match:
        sensor_type = match.group(1)
        level = int(match.group(2))
        chain_depth = int(match.group(3))
        is_final = sensor_type == "final"
        return level, chain_depth, is_final
    # Fallback defaults
    return 0, 1, False


def build_chain_message(
    *,
    level: int,
    chain_depth: int,
    value: float,
    measurement_time: str,
    time_received: str,
    time_completed: str,
    app_id: str,
    is_final: bool = False,
) -> dict:
    """Build a consistent payload for the chain receiver."""
    msg = {
        "level": level,
        "chain_depth": chain_depth,
        "value": value,
        "measurement_time": measurement_time,
        "time_received": time_received,
        "time_completed": time_completed,
        "app_id": app_id,
        # Keep a data envelope for parity with other benchmark receivers.
        "data": {
            "timestamp": measurement_time,
            "value": value,
            "level": level,
            "chain_depth": chain_depth,
        },
    }
    if is_final:
        msg["is_final"] = True
    return msg


class ChainBaseSensor(App):
    """
    Base sensor in the chain. Reads from Benicia and outputs to chain_level_0.
    Also sends a trigger to the receiver for latency tracking.
    """
    name = "chain_base"
    version = "0.1"
    app_type = "soft_sensor"
    command = "python -m acquirium.Apps.worker"

    def __init__(self, chain_depth: int = 1):
        super().__init__()
        # Parse config from ACQUIRIUM_APP_ID env var (set by worker)
        app_id = os.environ.get("ACQUIRIUM_APP_ID", "")
        if app_id:
            self.level, self.chain_depth, _ = parse_chain_config_from_app_id(app_id)
        else:
            self.level = 0
            self.chain_depth = chain_depth
        self.output_point_uri = make_chain_point_uri(0)
        self.outputs = [
            {
                "kind": "timeseries",
                "point_uri": self.output_point_uri,
            },
            {
                "kind": "trigger",
                "point_uri": f"urn:derived:chain_log_0",
            }
        ]

    def build_query(self, aq: Acquirium):
        return aq.find_entity(_class="Chlorination Basin").find_related_data(unit=["MilliGM-PER-L"])

    def run(self, ctx: AppContext) -> list[Output]:
        now = datetime.now(timezone.utc)
        df = ctx.query.latest_data(cast_value='float')
        time_received = datetime.now(timezone.utc).isoformat()

        if df.is_empty() or df.shape[0] == 0:
            value = 0.0
            measurement_time = now.isoformat()
        else:
            value = float(df[0, 1])
            measurement_time = df[0, 0].isoformat() if hasattr(df[0, 0], 'isoformat') else str(df[0, 0])

        time_completed = datetime.now(timezone.utc).isoformat()

        log_message = build_chain_message(
            level=self.level,
            chain_depth=self.chain_depth,
            value=value,
            measurement_time=measurement_time,
            time_received=time_received,
            time_completed=time_completed,
            app_id=ctx.app_id,
        )

        return [
            Output.timeseries(
                point_uri=self.output_point_uri,
                rows=[(now, value)],
            ),
            Output.trigger(
                url=f"{ALERT_HOST}:{ALERT_PORT}/chain",
                message=log_message,
            ),
        ]


class ChainIntermediateSensor(App):
    """
    Intermediate sensor in the chain. Reads from previous level and outputs to next level.
    Increments the value by 1. Also sends a trigger for latency tracking.
    """
    name = "chain_intermediate"
    version = "0.1"
    app_type = "soft_sensor"
    command = "python -m acquirium.Apps.worker"

    def __init__(self, level: int = 1, chain_depth: int = 1):
        super().__init__()
        # Parse config from ACQUIRIUM_APP_ID env var (set by worker)
        app_id = os.environ.get("ACQUIRIUM_APP_ID", "")
        if app_id:
            self.level, self.chain_depth, _ = parse_chain_config_from_app_id(app_id)
        else:
            self.level = level
            self.chain_depth = chain_depth
        self.input_point_uri = make_chain_point_uri(self.level - 1)
        self.output_point_uri = make_chain_point_uri(self.level)
        self.name = f"chain_level_{self.level}_of_{self.chain_depth}"
        self.outputs = [
            {
                "kind": "timeseries",
                "point_uri": self.output_point_uri,
            },
            {
                "kind": "trigger",
                "point_uri": f"urn:derived:chain_log_{self.level}",
            }
        ]

    def build_query(self, aq: Acquirium):
        return aq.find_all_data(uri=self.input_point_uri)

    def run(self, ctx: AppContext) -> list[Output]:
        time_received = datetime.now(timezone.utc).isoformat()

        # Query the previous sensor's output via the query interface
        df = ctx.query.latest_data(cast_value='float')
        now = datetime.now(timezone.utc)

        if df.is_empty() or df.shape[0] == 0:
            # No data from previous sensor yet
            value = float(self.level)
            measurement_time = now.isoformat()
        else:
            # Get the latest value and increment by 1
            prev_value = float(df[0, "value"]) if "value" in df.columns else 0.0
            timestamp = df[0, "ts"] if "ts" in df.columns else now
            measurement_time = timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
            value = prev_value + 1

        time_completed = datetime.now(timezone.utc).isoformat()

        log_message = build_chain_message(
            level=self.level,
            chain_depth=self.chain_depth,
            value=value,
            measurement_time=measurement_time,
            time_received=time_received,
            time_completed=time_completed,
            app_id=ctx.app_id,
        )

        return [
            Output.timeseries(
                point_uri=self.output_point_uri,
                rows=[(now, value)],
            ),
            Output.trigger(
                url=f"{ALERT_HOST}:{ALERT_PORT}/chain",
                message=log_message,
            ),
        ]


class ChainFinalSensor(App):
    """
    Final sensor in the chain. Reads from previous level and sends a trigger.
    Marks is_final=True so the receiver knows the chain is complete.
    """
    name = "chain_final"
    version = "0.1"
    app_type = "soft_sensor"
    command = "python -m acquirium.Apps.worker"

    def __init__(self, level: int = 1, chain_depth: int = 1):
        super().__init__()
        # Parse config from ACQUIRIUM_APP_ID env var (set by worker)
        app_id = os.environ.get("ACQUIRIUM_APP_ID", "")
        if app_id:
            self.level, self.chain_depth, _ = parse_chain_config_from_app_id(app_id)
        else:
            self.level = level
            self.chain_depth = chain_depth
        self.input_point_uri = make_chain_point_uri(self.level - 1)
        self.name = f"chain_final_{self.level}_of_{self.chain_depth}"
        self.outputs = [
            {
                "kind": "trigger",
                "point_uri": f"urn:derived:chain_log_{self.level}",
            }
        ]

    def build_query(self, aq: Acquirium):
        return aq.find_all_data(uri=self.input_point_uri)

    def run(self, ctx: AppContext) -> list[Output]:
        time_received = datetime.now(timezone.utc).isoformat()

        # Query the previous sensor's output via the query interface
        df = ctx.query.latest_data(cast_value='float')
        now = datetime.now(timezone.utc)

        if df.is_empty() or df.shape[0] == 0:
            value = float(self.level)
            measurement_time = now.isoformat()
        else:
            prev_value = float(df[0, "value"]) if "value" in df.columns else 0.0
            timestamp = df[0, "ts"] if "ts" in df.columns else now
            measurement_time = timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
            value = prev_value + 1

        time_completed = datetime.now(timezone.utc).isoformat()

        log_message = build_chain_message(
            level=self.level,
            chain_depth=self.chain_depth,
            value=value,
            measurement_time=measurement_time,
            time_received=time_received,
            time_completed=time_completed,
            app_id=ctx.app_id,
            is_final=True,
        )

        return [Output.trigger(
            url=f"{ALERT_HOST}:{ALERT_PORT}/chain",
            message=log_message,
        )]


def main():
    if len(sys.argv) < 2:
        print("Usage: python chain_latency.py <chain_depth> [timeout_seconds]")
        print("  chain_depth: number of sensors in the chain (minimum 1)")
        sys.exit(1)

    chain_depth = int(sys.argv[1])
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else None

    if chain_depth < 1:
        print("Chain depth must be at least 1")
        sys.exit(1)

    state = {
        "stop_requested": False,
        "force_exit": False,
        "cleanup_in_progress": False,
    }

    # Build the chain of apps
    all_apps = []

    # Base sensor (level 0)
    base = ChainBaseSensor(chain_depth=chain_depth)
    base.name = f"chain_level_0_of_{chain_depth}"
    all_apps.append(base)

    # Intermediate sensors (levels 1 to chain_depth-1)
    for level in range(1, chain_depth):
        sensor = ChainIntermediateSensor(level=level, chain_depth=chain_depth)
        sensor.name = f"chain_level_{level}_of_{chain_depth}"
        all_apps.append(sensor)

    # Final sensor (sends trigger)
    final = ChainFinalSensor(level=chain_depth, chain_depth=chain_depth)
    final.name = f"chain_final_{chain_depth}_of_{chain_depth}"
    all_apps.append(final)

    print(f"Chain configuration:")
    print(f"  Depth: {chain_depth}")
    print(f"  Total sensors: {len(all_apps)}")
    print(f"  Chain endpoint: {ALERT_HOST}:{ALERT_PORT}/chain")
    print()

    acq = Acquirium(
        server_url="localhost",
        server_port=8000,
        lexicon_path="ontologies/lexicon.json"
    )

    def cleanup():
        if state["cleanup_in_progress"]:
            return
        state["cleanup_in_progress"] = True
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        for app in all_apps:
            try:
                print(f"Stopping {app.name}...")
                acq.stop_app(app_id=app.name)
            except Exception as exc:
                print(f"Failed to stop {app.name}: {exc}")

    def handle_sigint(_signum, _frame):
        if state["cleanup_in_progress"]:
            print("\nCleanup in progress, please wait...")
            return
        if not state["stop_requested"]:
            state["stop_requested"] = True
            print("\nCaught Ctrl-C. Stopping apps...")
        else:
            state["force_exit"] = True
            print("\nSecond Ctrl-C received. Exiting after cleanup.")

    signal.signal(signal.SIGINT, handle_sigint)
    atexit.register(cleanup)

    try:
        acq.insert_graph("deployments/BENICIA/benicia-model-with-refs-thresholds.ttl")

        # Start apps in order (base first, then intermediate, then final)
        for app in all_apps:
            print(f"Starting {app.name}...")
            acq.register_app(app)
            acq.run_app(app.name, keep_alive=True, interval=1)

        print(f"\nStarted chain of {len(all_apps)} sensors.")

        if timeout:
            print(f"Running for {timeout} seconds...")
            last_len = 0
            for remaining in range(timeout, 0, -1):
                if state["stop_requested"]:
                    break
                line = f"{remaining} seconds remaining"
                padding = " " * max(0, last_len - len(line))
                print(f"{line}{padding}", end="\r", flush=True)
                last_len = len(line)
                time.sleep(1)
            if state["stop_requested"]:
                print("Stopping early...".ljust(last_len))
            else:
                print("0 seconds remaining".ljust(last_len))
        else:
            print("Press Enter to stop...")
            try:
                input()
            except KeyboardInterrupt:
                state["stop_requested"] = True
    finally:
        cleanup()
        if state["force_exit"]:
            sys.exit(130)


if __name__ == "__main__":
    main()
