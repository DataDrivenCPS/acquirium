import os
from datetime import datetime
from acquirium import App, Output
from acquirium import Acquirium, AppContext

# Alert endpoint configuration
# - Mac/Windows Docker Desktop: host.docker.internal (default)
# - Linux: set ALERT_HOST=172.17.0.1 or use --add-host=host.docker.internal:host-gateway
ALERT_HOST = os.environ.get("ALERT_HOST", "host.docker.internal")
ALERT_PORT = os.environ.get("ALERT_PORT", "10000")


class ChlorineLevelWarning(App):
    name = "chlorine_level_warning"
    version = "0.1"
    app_type = "soft_sensor"
    command = "python -m acquirium.Apps.worker"
    outputs = [
        {
            "kind": "trigger",
            "point_uri": "urn:derived:chlorine_level",
        }
    ]

    def build_query(self, aq: Acquirium):
        return aq.find_entity(_class="Chlorination Basin").find_related_data(unit=["MilliGM-PER-L"])

    def run(self, ctx: AppContext) -> list[Output]:
        time_received = datetime.utcnow().isoformat()
        df = ctx.query.latest_data(cast_value='float')
        if df.is_empty() or df.shape[0] == 0:
            message = {"text" : "No data available for chlorine level.",
                       "severity": "LOW",
                       "data": {},
                       "time_received": time_received,
                       "time_completed": datetime.utcnow().isoformat(),
                       "app_id": ctx.app_id}
        elif df[0,1] >75:
            message = {"text" : "Chlorine level exceeds safe threshold.",
                       "severity": "HIGH",
                       "data": {"chlorine_level": df[0,1], "timestamp": df[0,0].isoformat()},
                       "time_received": time_received,
                       "time_completed": datetime.utcnow().isoformat(),
                       "app_id": ctx.app_id}
        else:
            message = {"text" : "Chlorine level is within safe limits.",
                       "severity": "NORMAL",
                       "data": {"chlorine_level": df[0,1], "timestamp": df[0,0].isoformat()},
                       "time_received": time_received,
                       "time_completed": datetime.utcnow().isoformat(),
                       "app_id": ctx.app_id}
        return [Output.trigger(
            url= f"{ALERT_HOST}:{ALERT_PORT}/alerts",
            message=message
        )]

if __name__ == "__main__":
    import atexit
    import signal
    import sys
    import time

    state = {
        "stop_requested": False,
        "force_exit": False,
        "cleanup_in_progress": False,
    }

    number_of_instances = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    timeout = int(sys.argv[2]) if len(sys.argv) > 2 else None
    all_apps = []
    for i in range(number_of_instances):
        all_apps.append(ChlorineLevelWarning())
        all_apps[i].name = f"chlorine_level_warning_{i}"

    acq = Acquirium(server_url="localhost", server_port=8000, lexicon_path="ontologies/lexicon.json")

    def cleanup():
        if state["cleanup_in_progress"]:
            return
        state["cleanup_in_progress"] = True
        # Avoid interrupting cleanup; we always want to stop apps on exit.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        for app in all_apps:
            try:
                print(f"Stopping app {app.name}...")
                acq.stop_app(app_id=app.name)
            except Exception as exc:
                print(f"Failed to stop app {app.name}: {exc}")

    def handle_sigint(_signum, _frame):
        if state["cleanup_in_progress"]:
            print("\nCleanup in progress, please wait...")
            return
        if not state["stop_requested"]:
            state["stop_requested"] = True
            print("\nCaught Ctrl-C. Stopping apps... Press Ctrl-C again to exit after cleanup.")
        else:
            state["force_exit"] = True
            print("\nSecond Ctrl-C received. Exiting after cleanup.")

    signal.signal(signal.SIGINT, handle_sigint)
    atexit.register(cleanup)

    try:
        acq.insert_graph("deployments/BENICIA/benicia-model-with-refs-thresholds.ttl")
        i = 0
        for app in all_apps:
            print(f"Starting app instance {app.name}...")
            acq.register_app(app)
            acq.run_app(app.name, keep_alive=True, interval=1)
            i += 1
        print(f"Started {i} instances of ChlorineLevelWarning app.")
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
                input()  # wait for user input to stop
            except KeyboardInterrupt:
                state["stop_requested"] = True
    finally:
        cleanup()
        if state["force_exit"]:
            sys.exit(130)
