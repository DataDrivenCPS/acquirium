"""What each committed step changed in the derived streams, and who hears about it.

A step's evaluation records, per derived stream it wrote, the output window
it replaced and the rows it put there. Nothing is published while the step's
transaction is open: a step that rolls back has no delta. After the commit
the delta goes to an in-memory feed that clients read by step number, and to
the URL of every output declared with ``aq.output.trigger(url)``.

The feed is bounded and process-local. A reader that falls behind its oldest
step, or a server restart, is told so (``gap``) and should read the
committed streams instead. Triggers are posted once, in the background, after
the commit; a failed post is recorded and not retried, since the committed
rows remain the record.
"""
from __future__ import annotations

import json
import threading
import time
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa


@dataclass(frozen=True)
class StreamChange:
    """One derived stream's change in one step: its window was replaced by ``rows``."""
    ref_uri: str
    application_name: str
    port: str
    window: tuple[str, str]
    rows: pa.Table
    trigger: str | None = None

    def to_json(self) -> dict[str, Any]:
        times = [t.isoformat() for t in self.rows.column("time").to_pylist()]
        values = self.rows.column("value").to_pylist()
        return {"ref_uri": self.ref_uri, "app": self.application_name, "port": self.port,
                "window": list(self.window), "rows": [{"time": t, "value": v} for t, v in zip(times, values)]}


@dataclass(frozen=True)
class StepChanges:
    step_no: int
    revision: int
    changes: tuple[StreamChange, ...]

    def to_json(self) -> dict[str, Any]:
        return {"step_no": self.step_no, "revision": self.revision,
                "changes": [change.to_json() for change in self.changes]}


class ChangeFeed:
    """A bounded, ordered record of committed steps' derived-stream changes."""

    def __init__(self, capacity: int = 1000) -> None:
        self._steps: deque[StepChanges] = deque(maxlen=capacity)
        self._condition = threading.Condition()
        self._triggers = ThreadPoolExecutor(max_workers=2, thread_name_prefix="acquirium-trigger")
        self.trigger_errors: dict[str, str] = {}

    def publish(self, step: StepChanges) -> None:
        with self._condition:
            self._steps.append(step)
            self._condition.notify_all()
        for change in step.changes:
            if change.trigger:
                self._triggers.submit(self._post, change, step)

    def since(self, after: int, *, wait: float = 0.0) -> dict[str, Any]:
        """Steps after ``after``, waiting up to ``wait`` seconds for one to commit.

        ``gap`` is true when steps after ``after`` have already left the feed
        (or were committed before this process started), so the caller has
        missed changes and should reread the committed streams.
        """
        deadline = time.monotonic() + wait
        with self._condition:
            while True:
                newer = [s for s in self._steps if s.step_no > after]
                remaining = deadline - time.monotonic()
                if newer or remaining <= 0:
                    break
                self._condition.wait(remaining)
            oldest = self._steps[0].step_no if self._steps else None
            last = self._steps[-1].step_no if self._steps else after
        gap = oldest is not None and after < oldest - 1
        return {"steps": [s.to_json() for s in newer], "last_step": last, "gap": gap}

    def _post(self, change: StreamChange, step: StepChanges) -> None:
        body = json.dumps({"step_no": step.step_no, "revision": step.revision, **change.to_json()}).encode()
        request = urllib.request.Request(change.trigger, data=body, method="POST",
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                response.read()
            self.trigger_errors.pop(change.ref_uri, None)
        except Exception as error:   # a trigger is best-effort; the committed rows are the record
            self.trigger_errors[change.ref_uri] = f"{type(error).__name__}: {error}"

    def close(self) -> None:
        self._triggers.shutdown(wait=True)
