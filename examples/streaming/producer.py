"""A driver with immutable source requests recorded before HTTP delivery."""

from pathlib import Path
from time import time_ns
from uuid import uuid4

from filelock import FileLock

from acquirium import Driver
from examples.soft_sensors.declarations import BUCKET_NS, SOURCE, deploy, register

from .state import load, save

SECOND = 1_000_000_000


class WorkloadDriver(Driver):
    def __init__(self, client, directory):
        self.directory = Path(directory)
        super().__init__(
            client,
            {
                "server": {"data_dir": str(self.directory)},
                "driver": {"driver_id": "streaming-workload"},
            },
        )
        self.source_id = SOURCE
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = FileLock(self.directory / "producer.lock")
        with self.lock:
            self.session = load(self.directory / "session.json", None)
            if self.session is None:
                self.session = {
                    "id": uuid4().hex,
                    "start_ns": time_ns() // BUCKET_NS * BUCKET_NS,
                }
                save(self.directory / "session.json", self.session)

    def setup(self):
        # Registration is idempotent. On server restart do not reset metadata
        # or redeploy a plan: the engine/controller recover their own state.
        register(self.aq)
        deploy(self.aq)

    def records(self):
        return [
            load(path, None)
            for path in sorted((self.directory / "requests").glob("*.json"))
        ]

    def _prepare(self):
        records = self.records()
        for record in records:
            if record.get("receipt") is None:
                return record
        index = len(records)
        timestamp = self.session["start_ns"] + index * SECOND
        phase = index % 8
        if phase == 5:
            kind, streams = (
                "upsert",
                {"a1": [[timestamp - 4 * SECOND + SECOND // 4, 29.0]]},
            )
        elif phase == 6:
            kind, streams = "upsert", {"a2": [[timestamp - 6 * SECOND, 18.0]]}
        elif phase == 7:
            kind, streams = "delete", {"b1": [timestamp - 7 * SECOND]}
        else:
            kind, streams = (
                "upsert",
                {
                    "a1": [[timestamp, float(20 + index % 5)]],
                    "a2": [[timestamp, float(32 + index % 7)]],
                    "b1": [[timestamp, float(24 + index % 4)]],
                },
            )
        record = {
            "index": index,
            "request_id": f"{self.session['id']}:{index}",
            "kind": kind,
            "streams": streams,
            "created_ns": time_ns(),
            "attempts": 0,
            "receipt": None,
        }
        save(self.record_path(record), record)
        return record

    def record_path(self, record):
        return self.directory / "requests" / f"{record['index']:012d}.json"

    def prepare(self):
        with self.lock:
            return self._prepare()

    def deliver(self, record):
        """Retry the same durable payload after rejection or an unknown outcome."""
        record = dict(record, attempts=record["attempts"] + 1)
        save(self.record_path(record), record)
        if record["kind"] == "upsert":
            result = self.aq.insert_timeseries_batch(
                SOURCE,
                record["streams"],
                request_id=record["request_id"],
                wait=0,
                timeout=5,
            )
            receipt = result["receipts"][0]
        else:
            ref_name, timestamps = next(iter(record["streams"].items()))
            receipt = self.aq.delete_timeseries(
                SOURCE,
                ref_name,
                timestamps,
                request_id=record["request_id"],
                wait=0,
                timeout=5,
            )
        if not receipt["accepted"]:
            raise RuntimeError(receipt)
        record.update(receipt=receipt, accepted_ns=time_ns())
        save(self.record_path(record), record)
        return record

    def tick(self):
        with self.lock:
            return self.deliver(self._prepare())

    def observe(self, status, consumed_through):
        """Persist upper bounds on publication and complete-epoch consumption latency."""
        with self.lock:
            for record in self.records():
                receipt = record.get("receipt")
                if not receipt:
                    continue
                changed = False
                if (
                    "published_ns" not in record
                    and status["engine"]["applied"] >= receipt["sequence"]
                ):
                    record.update(
                        published_ns=time_ns(),
                        published_epoch=status["engine"]["epoch"],
                    )
                    changed = True
                if (
                    "published_epoch" in record
                    and "consumed_ns" not in record
                    and consumed_through >= record["published_epoch"]
                ):
                    record["consumed_ns"] = time_ns()
                    changed = True
                if changed:
                    save(self.record_path(record), record)
