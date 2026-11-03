"""Publication latency under sustained ingest, through the native engine.

Offers readings at each rate in turn, paced on a clock by one or more
senders, and records how long each request takes from submission until the
epoch that applies it is published. A rate is sustained when the backlog left
at its end drains within a second; the ramp stops at the first rate that is
not.
"""

import argparse
import heapq
import importlib.metadata
import json
import os
import platform
import random
import threading
import time
from collections import deque
from pathlib import Path

from escapement import Mutation, build_info

from .run import START, SECOND, derived, engine, plan, rss_mib, source, submit

RATES = (10_000, 25_000, 50_000, 100_000, 150_000, 200_000, 250_000, 300_000)
# Backlog that drains within this long after the last request counts as kept up.
DRAINED_SECONDS = 1.0
# Fraction of the offered rate the senders must reach for a rate to count.
SENT_FRACTION = 0.95
BUCKET = 15 * 60 * SECOND


def verdict(step):
    """Sustained, falling behind, or why the offered rate was not reached:
    admission when most of the senders' time went to ingest, which waits while
    an epoch holds the store, and the senders themselves otherwise."""
    if step["sender_limited"]:
        return ("admission-limited" if step["sender_ingest_fraction"] > 0.5
                else "sender-limited")
    return "sustained" if step["sustained"] else "falling behind"


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def ms(ns):
    return None if ns is None else ns / 1e6


class Readings:
    """Time-major readings of some streams, each at one instant before the
    next, cut into requests of a fixed size. Values random-walk at three
    decimals."""

    def __init__(self, indices, interval_ns, seed):
        self.rng = random.Random(seed)
        self.names = [source(index) for index in indices]
        self.values = [self.rng.uniform(10, 100) for _ in indices]
        self.interval_ns = interval_ns
        self.position = 0  # readings generated so far
        self.unsent = []  # generated ahead of a request that was not sent

    def sent(self):
        return self.position - len(self.unsent)

    def batch(self, size):
        if len(self.unsent) == size:
            mutations, self.unsent = self.unsent, []
            return mutations
        if self.unsent:
            raise ValueError("requests change size only between runs")
        mutations = []
        streams = len(self.names)
        for position in range(self.position, self.position + size):
            instant, index = divmod(position, streams)
            self.values[index] += self.rng.uniform(-0.05, 0.05)
            mutations.append(Mutation.put(self.names[index], START + instant * self.interval_ns,
                                          round(self.values[index], 3)))
        self.position += size
        return mutations


class Publications:
    """Matches submitted requests to the epochs that publish them, using the
    processor's clock for both. Senders record requests from their threads;
    only the main thread collects epochs."""

    def __init__(self, processor):
        self.processor = processor
        self.lock = threading.Lock()
        self.waiting = []  # heap of (sequence, submitted_ns, measured)
        self.recent = deque()  # (submitted_ns, readings, ingest_ns), for progress
        self.last = 0  # highest sequence submitted
        self.published = []  # (published_ns, latency_ns, measured)
        self.epochs = []  # (completed_ns, report)
        self.applied = 0

    def sent(self, sequence, submitted_ns, readings, ingest_ns, measured):
        with self.lock:
            heapq.heappush(self.waiting, (sequence, submitted_ns, measured))
            self.recent.append((submitted_ns, readings, ingest_ns))
            self.last = max(self.last, sequence)

    def collect(self):
        reports = self.processor.take_reports()
        with self.lock:
            for item in reports:
                completed, report = item["completed_ns"], item["report"]
                self.epochs.append((completed, report))
                self.applied = max(self.applied, report["applied"])
                while self.waiting and self.waiting[0][0] <= report["applied"]:
                    _, submitted, measured = heapq.heappop(self.waiting)
                    self.published.append((completed, completed - submitted, measured))

    def progress(self, label, started_ns):
        """Prints the last second: rates, pending requests and latencies."""
        self.collect()
        now = self.processor.clock_ns()
        since = now - SECOND
        with self.lock:
            while self.recent and self.recent[0][0] < since:
                self.recent.popleft()
            sent = sum(readings for _, readings, _ in self.recent)
            ingest = percentile([ingest for _, _, ingest in self.recent], 0.99)
            latencies = [latency for published, latency, _ in reversed(self.published)
                         if published > since]
            epochs = [report for completed, report in reversed(self.epochs) if completed > since]
            pending = self.last - self.applied
        published = sum(report["source_mutations"] for report in epochs)
        latency = (f"p50 {ms(percentile(latencies, 0.5)):6.1f} ms  "
                   f"p99 {ms(percentile(latencies, 0.99)):7.1f} ms" if latencies
                   else "p50      - ms  p99       - ms")
        ingest = f"{ms(ingest):6.1f} ms" if ingest is not None else "     - ms"
        print(f"  {label:5} {(now - started_ns) / 1e9:5.1f} s  sent {sent:9,}/s  "
              f"published {published:9,}/s  epochs {len(epochs):4}/s  pending {pending:6,}  "
              f"latency {latency}  ingest p99 {ingest}", flush=True)
        return pending


class Sender:
    """One driver: its own streams, paced at its share of the offered rate."""

    def __init__(self, number, indices, args):
        self.number = number
        self.readings = Readings(indices, args.interval * SECOND, args.seed + number)

    def offer(self, target, publications, size, interval_ns, first_ns, ends_ns, measured_ns,
              label):
        processor = publications.processor
        clock = processor.clock_ns
        self.sent = self.sent_measured = self.generate_ns = self.lag_ns = 0
        self.blocked = 0.0
        self.ingests = []  # nanoseconds each measured request spent in ingest
        batch = self.readings.batch(size)
        count = 0
        try:
            while (due := first_ns + count * interval_ns) < ends_ns:
                now = clock()
                if now < due:
                    time.sleep((due - now) / 1e9)
                else:
                    self.lag_ns = max(self.lag_ns, now - due)
                submitted = clock()
                receipt, waited = submit(target, f"{label}-{self.number}-{count}", batch)
                accepted = clock()
                processor.wake()
                self.blocked += waited
                measured = due >= measured_ns
                publications.sent(receipt.sequence, submitted, size, accepted - submitted,
                                  measured)
                self.sent += size
                if measured:
                    self.sent_measured += size
                    self.ingests.append(accepted - submitted)
                count += 1
                batch = self.readings.batch(size)
                self.generate_ns += clock() - accepted
        finally:
            self.readings.unsent = batch
            self.requests = count


def offer(target, publications, senders, args, rate, number):
    """Offers `rate` readings a second for args.duration seconds, then waits
    for the backlog to drain. Returns the rate's summary."""
    size = args.request_readings
    processor = publications.processor
    clock = processor.clock_ns
    # Each sender sends every interval_ns, staggered evenly among them.
    interval_ns = SECOND * size * len(senders) // rate
    duration_ns, warmup_ns = int(args.duration * SECOND), int(args.warmup * SECOND)
    print(f"{rate:,} readings/s for {args.duration:g} s, requests of {size:,} from "
          f"{len(senders)} sender{'s' if len(senders) > 1 else ''}", flush=True)
    started_ns = clock()
    ends_ns, measured_ns = started_ns + duration_ns, started_ns + warmup_ns
    failures = []

    def run(sender):
        try:
            sender.offer(target, publications, size, interval_ns,
                         started_ns + sender.number * interval_ns // len(senders),
                         ends_ns, measured_ns, number)
        except BaseException as error:
            failures.append(error)

    threads = [threading.Thread(target=run, args=(sender,), name=f"sender-{sender.number}")
               for sender in senders]
    for thread in threads:
        thread.start()
    pending_peak = 0
    next_print = started_ns + SECOND
    while any(thread.is_alive() for thread in threads):
        time.sleep(max(0.0, min(0.1, (next_print - clock()) / 1e9)))
        if clock() >= next_print:
            next_print += SECOND
            pending_peak = max(pending_peak, publications.progress("", started_ns))
    for thread in threads:
        thread.join()
    if failures:
        raise failures[0]
    sending_ns = clock() - started_ns
    stopped_ns = max(clock(), ends_ns)
    # Wait for the backlog, printing while it drains.
    deadline = time.perf_counter() + max(60.0, 10 * args.duration)
    while True:
        publications.collect()
        if publications.applied >= publications.last:
            break
        if time.perf_counter() > deadline:
            raise SystemExit(f"{rate:,} readings/s: the backlog did not drain")
        time.sleep(0.01)
        if clock() >= next_print:
            next_print += SECOND
            pending_peak = max(pending_peak, publications.progress("drain", started_ns))
    drained_ns = max(completed for completed, _ in publications.epochs)
    drain_seconds = max(0.0, (drained_ns - stopped_ns) / 1e9)

    latencies = [latency for published, latency, measured in publications.published
                 if measured and published > started_ns]
    epochs = [report for completed, report in publications.epochs
              if measured_ns < completed <= stopped_ns]
    ingests = [ingest for sender in senders for ingest in sender.ingests]
    seconds = (stopped_ns - measured_ns) / 1e9
    sent_rate = sum(sender.sent_measured for sender in senders) / seconds
    published_rate = sum(report["source_mutations"] for report in epochs) / seconds
    sender_limited = sent_rate < SENT_FRACTION * rate
    result = {
        "offered_per_second": rate,
        "senders": len(senders),
        "sent_per_second": sent_rate,
        "published_per_second": published_rate,
        "requests": sum(sender.requests for sender in senders),
        "readings": sum(sender.sent for sender in senders),
        "latency_ms": {name: ms(percentile(latencies, fraction)) for name, fraction in
                       (("p50", 0.5), ("p95", 0.95), ("p99", 0.99), ("max", 1.0))},
        # Time inside ingest, which waits while an epoch holds the store.
        "ingest_ms": {name: ms(percentile(ingests, fraction)) for name, fraction in
                      (("p50", 0.5), ("p99", 0.99), ("max", 1.0))},
        # Shares of the senders' time spent in ingest and generating requests.
        "sender_ingest_fraction": sum(ingests) / max(1, len(senders) * (sending_ns - warmup_ns)),
        "sender_generate_fraction": (sum(sender.generate_ns for sender in senders)
                                     / max(1, len(senders) * sending_ns)),
        "epochs_per_second": len(epochs) / seconds,
        "requests_per_epoch": (sum(report["requests"] for report in epochs) / len(epochs)
                               if epochs else 0),
        "epoch_ms": {phase: (sum(report["timings"][phase] for report in epochs) / len(epochs)
                             if epochs else 0)
                     for phase in ("prepare_ms", "compute_ms", "publish_ms", "total_ms")},
        "pending_peak": pending_peak,
        "drain_seconds": drain_seconds,
        "sender_lag_seconds": max(sender.lag_ns for sender in senders) / 1e9,
        "blocked_seconds": sum(sender.blocked for sender in senders),
        "rss_mib": rss_mib(),
        "sender_limited": sender_limited,
        "sustained": not sender_limited and drain_seconds <= DRAINED_SECONDS,
    }
    latency = result["latency_ms"]
    print(f"  => {verdict(result)}: published {published_rate:,.0f}/s, latency p50 "
          f"{latency['p50']:.1f} p99 {latency['p99']:.1f} max {latency['max']:.1f} ms, drained "
          f"in {drain_seconds:.2f} s; senders spent {result['sender_ingest_fraction']:.0%} of "
          f"their time in ingest and {result['sender_generate_fraction']:.0%} generating\n",
          flush=True)
    return result


def preload(target, publications, senders, args):
    """Loads args.history readings as fast as the engine takes them."""
    print(f"Loading {args.history:,} readings of history", flush=True)
    started = time.perf_counter()
    loaded = number = 0
    receipt = None
    while loaded < args.history:
        sender = senders[number % len(senders)]
        size = min(args.request_readings, args.history - loaded)
        receipt, _ = submit(target, f"history-{number}", sender.readings.batch(size))
        publications.processor.wake()
        loaded += size
        number += 1
    publications.processor.read_after(receipt, timeout=3600)
    publications.collect()
    seconds = time.perf_counter() - started
    print(f"  loaded in {seconds:.1f} s\n", flush=True)
    return {"readings": args.history, "seconds": seconds}


def verify(target, senders, args):
    """Every sent reading is stored, and stream 0's last full mean is right."""
    snapshot = target.snapshot()
    counts = [(snapshot.stats(source(index)) or {"count": 0})["count"]
              for index in range(args.streams)]
    if sum(counts) != sum(sender.readings.sent() for sender in senders):
        return False
    if not args.derived or not counts[0]:
        return True
    last = snapshot.stats(source(0))["last_ns"]
    bucket = last - last % BUCKET - BUCKET
    if bucket < START:
        return True
    rows = snapshot.query(source(0), start=bucket, end=bucket + BUCKET, limit=10**6)
    mean = sum(row.value for row in rows) / len(rows)
    return abs(snapshot.value(derived(0), bucket) - mean) < 1e-9


def summary(arguments, steps):
    senders = arguments["senders"]
    lines = [
        f"{arguments['streams']} streams in requests of {arguments['request_readings']:,} "
        f"readings from {senders} sender{'s' if senders > 1 else ''}, "
        f"{arguments['duration']:g} s per rate after {arguments['history']:,} readings of history"
        + (", a native 15-minute mean of each stream" if arguments["derived"] else "")
        + ". Latency is from submission to publication.",
        "",
        "| Offered/s | Sent/s | Published/s | p50 ms | p95 ms | p99 ms | Max ms | Epochs/s "
        "| Requests/epoch | Epoch ms | Ingest p50 ms | Ingest p99 ms | Peak pending | Drain s "
        "| Result |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: "
        "| ---: | ---: | --- |",
    ]
    for step in steps:
        latency = step["latency_ms"]
        lines.append(
            f"| {step['offered_per_second']:,} | {step['sent_per_second']:,.0f} "
            f"| {step['published_per_second']:,.0f} | {latency['p50']:.1f} "
            f"| {latency['p95']:.1f} | {latency['p99']:.1f} | {latency['max']:.1f} "
            f"| {step['epochs_per_second']:.1f} | {step['requests_per_epoch']:.1f} "
            f"| {step['epoch_ms']['total_ms']:.1f} | {step['ingest_ms']['p50']:.1f} "
            f"| {step['ingest_ms']['p99']:.1f} | {step['pending_peak']:,} "
            f"| {step['drain_seconds']:.2f} | {verdict(step)} |")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="A new directory for results")
    parser.add_argument("--rates", type=int, nargs="+", default=RATES,
                        help="Offered readings per second, in order")
    parser.add_argument("--duration", type=float, default=20.0, help="Seconds per rate")
    parser.add_argument("--warmup", type=float, default=2.0,
                        help="Seconds at the start of each rate left out of its summary")
    parser.add_argument("--streams", type=int, default=100)
    parser.add_argument("--interval", type=int, default=1, help="Seconds between readings")
    parser.add_argument("--request-readings", type=int, default=1_000,
                        help="Readings per request")
    parser.add_argument("--senders", type=int, default=1,
                        help="Concurrent senders, each with its share of the streams")
    parser.add_argument("--history", type=int, default=0,
                        help="Readings loaded before the first rate")
    parser.add_argument("--derived", action="store_true",
                        help="Also maintain a native 15-minute mean of every stream")
    parser.add_argument("--keep-going", action="store_true",
                        help="Offer every rate, even after one is not sustained")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    if args.warmup >= args.duration:
        parser.error("--warmup must be shorter than --duration")
    if not 1 <= args.senders <= args.streams:
        parser.error("--senders must be between 1 and --streams")

    args.output.mkdir(parents=True, exist_ok=False)
    database = args.output / "sustained.duckdb"
    senders = [Sender(number, range(number, args.streams, args.senders), args)
               for number in range(args.senders)]
    steps = []
    with engine(database) as target:
        if args.derived:
            target.activate("plan", plan(args.streams))
        processor = target.start(report_capacity=100_000)
        publications = Publications(processor)
        history = preload(target, publications, senders, args) if args.history else None
        for number, rate in enumerate(args.rates):
            step = offer(target, publications, senders, args, rate, number)
            steps.append(step)
            if not step["sustained"] and not args.keep_going:
                break
        processor.stop()
        if processor.status()["stats"]["reports_dropped"]:
            raise SystemExit("epoch reports were dropped; latencies are incomplete")
        verified = verify(target, senders, args)
    for path in database.parent.glob(database.name + "*"):
        path.unlink()
    if not verified:
        raise SystemExit("the stored readings do not match those sent")

    arguments = {key: str(value) if isinstance(value, Path) else value
                 for key, value in vars(args).items() if key != "output"}
    environment = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "acquirium": importlib.metadata.version("acquirium"),
        "escapement": build_info(),
    }
    (args.output / "results.json").write_text(json.dumps(
        {"arguments": arguments, "environment": environment, "history": history,
         "verified": verified, "steps": steps}, indent=2))
    text = summary(arguments, steps)
    (args.output / "summary.md").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
