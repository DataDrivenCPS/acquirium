"""Read stored readings through the epoch engine's Arrow API in tests."""

from contextlib import closing, contextmanager, nullcontext
from hashlib import sha256
from io import BytesIO

import pyarrow as pa
from escapement import NotFound


class _TestTransport:
    """Give the Acquirium client its requests-style read interface over TestClient."""

    def __init__(self, http):
        self.http = http

    def __getattr__(self, name):
        return getattr(self.http, name)

    def get(self, *args, stream=False, **kwargs):
        if (params := kwargs.get("params")) is not None:
            kwargs["params"] = {key: value for key, value in params.items() if value is not None}
        response = self.http.get(*args, **kwargs)
        if stream:
            response.raw = BytesIO(response.content)
            return nullcontext(response)
        return response


@contextmanager
def epoch_server(directory, *, read_batch_size=101, **options):
    """Run HTTP requests against an in-process manager, restoring app state on exit."""
    import pytest
    from fastapi.testclient import TestClient

    from acquirium import Acquirium
    from acquirium.Server.app import app
    from acquirium.Server.manager import Manager

    with closing(Manager(data_dir=directory, exact_only=True, **options)) as manager:
        manager.recover_apps()
        # The manager above owns startup and shutdown for this test.
        with pytest.MonkeyPatch.context() as patch, closing(TestClient(app)) as http:
            patch.setattr(app.state, "manager", manager, raising=False)
            patch.setattr(app.state, "read_batch_size", read_batch_size, raising=False)
            client = Acquirium(health_timeout=None)
            client.client._http.close()
            client.client._http = _TestTransport(http)
            yield manager, http, client


def readings(snapshot, stream, *, start=None, end=None):
    """Read numeric and text values from one stream at the supplied epoch."""
    try:
        rows = snapshot.read([stream], start=start, end=end)
    except NotFound:
        return []
    with pa.RecordBatchReader.from_stream(rows) as reader:
        return [
            number.as_py() if number.is_valid else text.as_py()
            for batch in reader
            for number, text in zip(batch["number"], batch["text"])
        ]


def value(snapshot, stream, time):
    """Return the reading at one nanosecond timestamp, or None if it is absent."""
    results = readings(snapshot, stream, start=time, end=time + 1)
    return results[0] if results else None


def plan(module, *, name="app", source="urn:sensor", output="urn:derived", width=None):
    """Build one engine operator from a calculation file, source, and output."""
    return {
        "templates": [{
            "id": name,
            "definition": {
                "function": {
                    "module": module.stem, "name": "calculate",
                    "digest": sha256(module.read_bytes()).hexdigest(), "mode": "per_window",
                },
                "window": ({"anchors": {"instant": {}}, "body": "point"} if width is None
                           else {"anchors": {"grid": {"origin": 0, "step": width}}, "body": "bucket"}),
                "roles": {
                    "x": {"reach": {"span": {"before": 0, "after": 0}}, "reads": "bound", "driving": True},
                },
                "ports": ["out"],
                "freshness": "synchronous",
            },
            "annotations": None,
        }],
        "instances": [{
            "template": name, "key": "one", "outputs": {"out": output},
            "parameters": {}, "annotations": None,
        }],
        "bindings": [{
            "template": name, "instance": "one", "stream": source, "role": "x",
            "valid_from": None, "valid_to": None, "context": "",
        }],
    }


@contextmanager
def pause_publication(core, directory, stream):
    """Hold publication of writes to a stream until the test exits this block."""
    flag = directory / "blocked"
    module = directory / "blocking_calculation.py"
    module.write_text(
        "import time\n"
        "from pathlib import Path\n"
        "def calculate(frame, context):\n"
        f"    while Path({str(flag)!r}).exists(): time.sleep(0.01)\n"
        "    return {'out': frame.select('time', 'value', 'text')}\n"
    )
    core.wait(core.activate("pause-publication", plan(module, source=stream)))
    flag.touch()
    try:
        yield
    finally:
        flag.unlink()


def all_readings(core):
    """Every sample in the ledger's Escapement, as (stream, time, value)."""
    snapshot = core.wait_for(core.engine.status()["accepted"])
    streams = [row["stream"] for row in snapshot.describe()]
    with pa.RecordBatchReader.from_stream(snapshot.read(streams)) as reader:
        return [
            (stream.as_py(), time.as_py(), number.as_py())
            for batch in reader
            for stream, time, number in zip(batch["stream"], batch["time"].cast(pa.int64()), batch["number"])
        ]
