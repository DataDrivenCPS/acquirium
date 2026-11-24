"""Read stored readings through the epoch engine's Arrow API in tests."""

from contextlib import contextmanager
from hashlib import sha256

import pyarrow as pa
from escapement_next import NotFound


def readings(snapshot, stream, *, start=None, end=None):
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
    results = readings(snapshot, stream, start=time, end=time + 1)
    return results[0] if results else None


def plan(module, *, name="app", source="urn:sensor", output="urn:derived", width=None):
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
