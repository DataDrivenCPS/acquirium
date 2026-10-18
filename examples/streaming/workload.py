"""Continuously drive and consume the tank-temperature DAG over real HTTP.

uv run --locked python -m examples.streaming.workload --ticks 60 --burst 4
"""

import argparse
import json
import math
import threading
import time
from collections import defaultdict
from pathlib import Path
from statistics import mean
from urllib.parse import urlsplit

import polars as pl
import requests
from filelock import FileLock

import acquirium as aq
from acquirium.apps import output_reference
from acquirium.internals.models import compute_ref_uri
from examples.soft_sensors.declarations import (
    ALARM,
    BUCKET_NS,
    HOTTEST,
    MEAN,
    PLANT,
    SOURCE,
    TANK_A,
    TANK_B,
)

from .consumer import ChangeConsumer
from .producer import WorkloadDriver
from .state import save


def connect(address):
    url = urlsplit(address)
    return aq.Acquirium(
        server_url=url.hostname,
        server_port=url.port,
        use_ssl=url.scheme == "https",
        health_timeout=None,
    )


class ConsumerLoop:
    def __init__(self, address, path, page_size):
        self.client = connect(address)
        self.consumer = ChangeConsumer(self.client, path, page_size=page_size)
        self.stop = threading.Event()
        self.error = None
        self.reconnects = 0
        self.thread = threading.Thread(
            target=self.run, name="change-consumer", daemon=True
        )
        self.thread.start()

    def run(self):
        try:
            while not self.stop.is_set():
                try:
                    page = self.consumer.poll()
                except requests.RequestException as error:
                    if (
                        isinstance(error, requests.HTTPError)
                        and error.response.status_code < 500
                    ):
                        raise
                    self.reconnects += 1
                    self.stop.wait(0.1)
                    continue
                if not page["has_more"]:
                    self.stop.wait(0.05)
        except Exception as error:  # noqa: BLE001 -- propagate worker failures to the owner.
            self.error = error
        finally:
            self.client.client._http.close()

    def close(self):
        self.stop.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise TimeoutError("consumer did not stop")
        if self.error:
            raise self.error


def expected_values(records):
    """Recompute all source and derived values independently of native functions."""
    raw = {probe: {} for probe in ("a1", "a2", "b1")}
    for record in records:
        if not record.get("receipt"):
            raise RuntimeError("source journal still has unacknowledged work")
        for probe, values in record["streams"].items():
            if record["kind"] == "delete":
                for timestamp in values:
                    raw[probe].pop(timestamp, None)
            else:
                raw[probe].update(values)
    result = {
        (str(compute_ref_uri(SOURCE, probe)), timestamp, 0): value
        for probe, readings in raw.items()
        for timestamp, value in readings.items()
    }
    buckets = defaultdict(list)
    for probe, readings in raw.items():
        group = TANK_B if probe == "b1" else TANK_A
        for timestamp, value in readings.items():
            buckets[group, timestamp // BUCKET_NS * BUCKET_NS].append(value)
    hottest = {}
    for (group, timestamp), values in buckets.items():
        value = mean(values)
        result[output_reference(MEAN, group, "mean"), timestamp, 0] = value
        hottest[timestamp] = max(value, hottest.get(timestamp, -math.inf))
    for timestamp, value in hottest.items():
        result[output_reference(HOTTEST, PLANT, "hottest"), timestamp, 0] = value
        if value > 27:
            result[output_reference(ALARM, PLANT, "alarm"), timestamp, 0] = value
    return result


def verify(client, driver, consumer):
    expected = expected_values(driver.records())
    actual = consumer.values()
    if actual.keys() != expected.keys():
        raise AssertionError(
            f"sink keys differ: missing={expected.keys() - actual.keys()}, "
            f"extra={actual.keys() - expected.keys()}"
        )
    for slot, value in expected.items():
        if not math.isclose(actual[slot], value, rel_tol=1e-10, abs_tol=1e-10):
            raise AssertionError((slot, actual[slot], value))
    epoch = client.engine_snapshot()["epoch"]
    references = {str(compute_ref_uri(SOURCE, probe)) for probe in ("a1", "a2", "b1")}
    for name in (MEAN, HOTTEST, ALARM):
        for operator in client.inspect_app(name)["app"]["operators"]:
            references.update(operator["outputs"].values())
    for reference in references:
        frame = client.client.timeseries_df(reference, epoch=epoch)
        rows = dict(zip(frame["ts"].cast(pl.Int64), frame["value"]))
        wanted = {
            timestamp: value
            for (stream, timestamp, _), value in expected.items()
            if stream == reference
        }
        if rows.keys() != wanted.keys() or any(
            not math.isclose(rows[timestamp], value, rel_tol=1e-10, abs_tol=1e-10)
            for timestamp, value in wanted.items()
        ):
            raise AssertionError(f"snapshot differs at {reference}")
    return len(expected)


def latency(records, field):
    values = sorted(
        (record[field] - record["created_ns"]) / 1e6
        for record in records
        if field in record
    )
    if not values:
        return None
    return {
        "count": len(values),
        "p50_ms": values[(len(values) - 1) // 2],
        "p95_ms": values[math.ceil(0.95 * len(values)) - 1],
        "max_ms": values[-1],
    }


def run(
    directory,
    *,
    ticks=0,
    interval=0.25,
    burst=1,
    page_size=128,
    restart_every=0,
    timeout=60,
):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    with FileLock(directory / "workload.lock", timeout=0):
        client = aq.init(data_dir=directory / "server", exact_only=True, timeout=180)
        loop = None
        started = time.monotonic()
        admitted, retries, restarts, peak_backlog, peak_rss = 0, 0, 0, 0, 0
        last_progress, next_report = time.monotonic(), time.monotonic()
        try:
            driver = WorkloadDriver(client, directory / "source")
            if not client.list_apps()["apps"]:
                driver.setup()
            loop = ConsumerLoop(client.address, directory / "consumer.json", page_size)

            def observe():
                nonlocal peak_backlog, peak_rss
                if loop.error:
                    raise loop.error
                status = client.engine_status()
                if status["error"]:
                    raise RuntimeError(f"native processor paused: {status['error']}")
                peak_backlog = max(peak_backlog, status["engine"]["pending_requests"])
                peak_rss = max(peak_rss, status["process"]["peak_rss_bytes"] or 0)
                driver.observe(status, loop.consumer.state["complete_through"])
                return status

            try:
                while ticks == 0 or admitted < ticks:
                    for _ in range(min(burst, ticks - admitted) if ticks else burst):
                        try:
                            driver.tick()
                        except requests.RequestException as error:
                            if (
                                isinstance(error, requests.HTTPError)
                                and error.response.status_code < 500
                            ):
                                raise
                            retries += 1
                            if time.monotonic() - last_progress > timeout:
                                raise TimeoutError(
                                    "source made no delivery progress"
                                ) from error
                            time.sleep(max(interval, 0.05))
                            break
                        admitted += 1
                        last_progress = time.monotonic()
                    status = observe()
                    if time.monotonic() >= next_report:
                        report = status["last_report"]
                        last_epoch = None
                        if report is not None:
                            last_epoch = {
                                field: report[field]
                                for field in ("epoch", "requests", "peak_workers")
                            }
                            last_epoch.update(
                                {
                                    field: round(value, 2)
                                    for field, value in report["timings"].items()
                                }
                            )
                        print(
                            json.dumps(
                                {
                                    "admitted": admitted,
                                    "pending": status["engine"]["pending_requests"],
                                    "published_epoch": status["engine"]["epoch"],
                                    "consumed_through": loop.consumer.state[
                                        "complete_through"
                                    ],
                                    "last_epoch": last_epoch,
                                    "python_workers": status["python_workers"],
                                }
                            ),
                            flush=True,
                        )
                        next_report = time.monotonic() + 1
                    if (
                        restart_every
                        and admitted
                        and admitted // restart_every > restarts
                    ):
                        loop.close()
                        aq.shutdown()
                        client = aq.init(
                            data_dir=directory / "server", exact_only=True, timeout=180
                        )
                        driver = WorkloadDriver(client, directory / "source")
                        loop = ConsumerLoop(
                            client.address, directory / "consumer.json", page_size
                        )
                        restarts += 1
                    time.sleep(interval)
            except KeyboardInterrupt:
                print("Stopping generation; draining accepted work.", flush=True)

            deadline = time.monotonic() + timeout
            while True:
                # A lost response can leave the last durable source request
                # unacknowledged. Redeliver it before declaring the run drained.
                pending = [
                    record for record in driver.records() if not record.get("receipt")
                ]
                for record in pending:
                    driver.deliver(record)
                status = observe()
                if (
                    not status["engine"]["pending_requests"]
                    and loop.consumer.state["complete_through"]
                    >= status["engine"]["epoch"]
                ):
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError("engine/consumer did not drain")
                time.sleep(0.05)
            loop.close()
            status = observe()
            rows = verify(client, driver, loop.consumer)
            records = driver.records()
            report = {
                "verified": True,
                "requests": len(records),
                "new_requests": admitted,
                "rows": rows,
                "elapsed_seconds": time.monotonic() - started,
                "delivery_retries": retries,
                "server_restarts": restarts,
                "peak_pending_requests": peak_backlog,
                "server_peak_rss_bytes": peak_rss,
                "python_workers": status["python_workers"],
                "publication_latency_upper_bound": latency(records, "published_ns"),
                "consumption_latency_upper_bound": latency(records, "consumed_ns"),
                "snapshot_epoch": status["engine"]["epoch"],
                "consumer_complete_through": loop.consumer.state["complete_through"],
            }
            save(directory / "metrics.json", report)
            print(json.dumps(report, indent=2), flush=True)
            return report
        finally:
            try:
                if loop is not None:
                    loop.close()
            finally:
                aq.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path(".streaming-workload"))
    parser.add_argument(
        "--ticks", type=int, default=0, help="additional requests; 0 runs until Ctrl-C"
    )
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--burst", type=int, default=1)
    parser.add_argument("--page-size", type=int, default=128)
    parser.add_argument("--restart-every", type=int, default=0)
    parser.add_argument(
        "--timeout", type=float, default=60, help="outage/drain deadline in seconds"
    )
    args = parser.parse_args()
    if (
        args.ticks < 0
        or args.restart_every < 0
        or args.burst < 1
        or not 1 <= args.page_size <= 10_000
        or not math.isfinite(args.interval)
        or args.interval < 0
        or not math.isfinite(args.timeout)
        or args.timeout <= 0
    ):
        parser.error("invalid workload bounds")
    run(
        args.data_dir,
        ticks=args.ticks,
        interval=args.interval,
        burst=args.burst,
        page_size=args.page_size,
        restart_every=args.restart_every,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
