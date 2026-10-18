"""Consumer/source-owned checkpoint files, separate from engine persistence."""

import json
import os
from pathlib import Path


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as handle:
        json.dump(value, handle, sort_keys=True, allow_nan=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def load(path, default):
    path = Path(path)
    return json.loads(path.read_text()) if path.exists() else default


def key(row):
    slot = row["key"]
    return slot["stream"], slot["time"], slot["sample"]
