import os
import hashlib
from datetime import datetime, timezone
from rdflib import Graph as RDFGraph, URIRef
from acquirium import App, Output
from acquirium import Acquirium, AppContext
from acquirium.internals.internals_namespaces import HAS_EXTERNAL_REFERENCE

# Alert endpoint configuration
# - Mac/Windows Docker Desktop: host.docker.internal (default)
# - Linux: set ALERT_HOST=172.17.0.1 or use --add-host=host.docker.internal:host-gateway
ALERT_HOST = os.environ.get("ALERT_HOST", "host.docker.internal")
ALERT_PORT = os.environ.get("ALERT_PORT", "10000")


class ExternalReferenceWarning(App):
    name = "external_reference_warning"
    version = "0.1"
    app_type = "soft_sensor"
    command = "python -m acquirium.Apps.worker"
    outputs = []

    def __init__(self, point_uri: str, threshold: float = 75.0):
        self.point_uri = point_uri
        self.threshold = threshold
        self.outputs = [
            {
                "kind": "trigger",
                "point_uri": _warning_output_uri(point_uri),
            }
        ]

    def build_query(self, aq: Acquirium):
        return aq.find_all_data(uri=self.point_uri)

    def run(self, ctx: AppContext) -> list[Output]:
        time_received = datetime.now(timezone.utc).isoformat()
        df = ctx.query.latest_data(cast_value='float')
        if df.is_empty() or df.shape[0] == 0 or df.shape[1] < 2 or df[0, 1] is None:
            message = {"text" : "No data available for point.",
                       "severity": "LOW",
                       "data": {"point_uri": self.point_uri},
                       "time_received": time_received,
                       "time_completed": datetime.now(timezone.utc).isoformat(),
                       "app_id": ctx.app_id}
        else:
            value = df[0, 1]
            try:
                value_f = float(value)
            except (TypeError, ValueError):
                message = {"text" : "Non-numeric value received.",
                           "severity": "LOW",
                           "data": {"value": value, "timestamp": _format_ts(df[0, 0]), "point_uri": self.point_uri},
                           "time_received": time_received,
                           "time_completed": datetime.now(timezone.utc).isoformat(),
                           "app_id": ctx.app_id}
                return [Output.trigger(
                    url= f"{ALERT_HOST}:{ALERT_PORT}/alerts",
                    message=message,
                    point_uri=self.point_uri,
                )]

        if value_f > self.threshold:
            message = {"text" : "Value exceeds safe threshold.",
                       "severity": "HIGH",
                       "data": {"value": value_f, "timestamp": _format_ts(df[0, 0]), "threshold": self.threshold, "point_uri": self.point_uri},
                       "time_received": time_received,
                       "time_completed": datetime.now(timezone.utc).isoformat(),
                       "app_id": ctx.app_id}
        else:
            message = {"text" : "Value is within safe limits.",
                       "severity": "NORMAL",
                       "data": {"value": value_f, "timestamp": _format_ts(df[0, 0]), "threshold": self.threshold, "point_uri": self.point_uri},
                       "time_received": time_received,
                       "time_completed": datetime.now(timezone.utc).isoformat(),
                       "app_id": ctx.app_id}
        return [Output.trigger(
            url= f"{ALERT_HOST}:{ALERT_PORT}/alerts",
            message=message,
            point_uri=self.point_uri,
        )]


def _warning_output_uri(point_uri: str) -> str:
    digest = hashlib.sha1(point_uri.encode("utf-8")).hexdigest()[:12]
    return f"urn:derived:warning:{digest}"


def _format_ts(value) -> str:
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def _data_node_uris_from_ttl(ttl_path: str) -> list[str]:
    graph = RDFGraph()
    graph.parse(ttl_path, format="turtle")
    uris = set()
    for subj, _pred, _obj in graph.triples((None, HAS_EXTERNAL_REFERENCE, None)):
        if isinstance(subj, URIRef):
            uris.add(str(subj))
    return sorted(uris)

if __name__ == "__main__":
    import atexit
    import argparse
    import signal
    import sys
    import time

    state = {
        "stop_requested": False,
        "force_exit": False,
        "cleanup_in_progress": False,
    }

    parser = argparse.ArgumentParser(description="Run warning apps for all data nodes with external references.")
    parser.add_argument("ttl_path", help="Path to a TTL file to load.")
    parser.add_argument("--timeout", type=int, default=None, help="Stop after N seconds.")
    parser.add_argument("--interval", type=float, default=1.0, help="Keep-alive polling interval in seconds.")
    parser.add_argument("--threshold", type=float, default=75.0, help="Warning threshold for all apps.")
    parser.add_argument("--server-url", default="localhost")
    parser.add_argument("--server-port", type=int, default=8000)
    parser.add_argument("--lexicon-path", default="ontologies/lexicon.json")
    parser.add_argument("--multiplier", type=int, default=1, help="Number of times to duplicate the app instances.")
    args = parser.parse_args()

    timeout = args.timeout
    data_node_uris = _data_node_uris_from_ttl(args.ttl_path)
    if not data_node_uris:
        print("No data nodes with external references found.")
        sys.exit(1)

    all_apps = []
    for i, uri in enumerate(data_node_uris):
        for j in range(args.multiplier):
            app = ExternalReferenceWarning(uri, threshold=args.threshold)
            app.name = f"external_reference_warning_{i}_{j}"
            all_apps.append(app)
            print(f"Prepared app {app.name} for data node {uri}.")
    # exit()

    acq = Acquirium(server_url=args.server_url, server_port=args.server_port, lexicon_path=args.lexicon_path)

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
        import time
        acq.insert_graph(args.ttl_path)
        i = 0
        for app in all_apps:
            print(f"Starting app instance {app.name}...")
            acq.register_app(app)
            acq.run_app(app.name, keep_alive=True, interval=args.interval,params={'point_uri':app.point_uri})
            i += 1
            time.sleep(5)  # slight delay to avoid overwhelming the server
        print(f"Started {i} instances of ExternalReferenceWarning app.")
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
