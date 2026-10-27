"""Codecs that turn Python objects into stored bytes and back.

An `object` variable records whatever its codec's `dumps()` returns and gives
the object back through `observation.load()`. The bytes are opaque to
Acquirium: the server stores and serves them and never decodes them, so a
codec runs only in the client that records or loads.

A declaration saves its codec as an import path, so a codec is a module-level
class that `load()` can import and build with no arguments. Configure a codec
by subclassing it rather than by passing constructor arguments.
"""
from __future__ import annotations

import importlib
import json
import pickle
from typing import Any, Protocol, runtime_checkable

from acquirium.ExperimentValues import to_json


@runtime_checkable
class Codec(Protocol):
    """What an `object` variable needs to save and restore its values."""

    media_type: str

    def dumps(self, value: Any) -> bytes: ...

    def loads(self, data: bytes) -> Any: ...


class BytesCodec:
    """Store bytes exactly as given, and load them back as bytes."""

    media_type = "application/octet-stream"
    suffix = ""

    def dumps(self, value: Any) -> bytes:
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError(f"BytesCodec records bytes, not {type(value).__name__}")
        return bytes(value)

    def loads(self, data: bytes) -> bytes:
        return data


class JsonCodec:
    """Store a JSON document as an opaque file rather than a queryable value.

    Use it for large documents nobody filters on, such as a full model spec;
    a `json` variable keeps its value in the observation instead.
    """

    media_type = "application/json"
    suffix = ".json"

    def dumps(self, value: Any) -> bytes:
        return json.dumps(to_json(value), separators=(",", ":"), sort_keys=True).encode()

    def loads(self, data: bytes) -> Any:
        return json.loads(data)


class PickleCodec:
    """Python's pickle.

    Loading a pickle runs code named in its bytes, so load only pickles from
    people you trust. A pickle also loads only where the classes it names can
    still be imported, usually with the same library versions that wrote it.
    """

    media_type = "application/x-python-pickle"
    suffix = ".pkl"

    def dumps(self, value: Any) -> bytes:
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    def loads(self, data: bytes) -> Any:
        return pickle.loads(data)


class CloudpickleCodec(PickleCodec):
    """cloudpickle, which also saves lambdas and classes defined in a notebook.

    Its output is a pickle, so it carries the same trust and version caveats.
    Recording requires the cloudpickle package; so does loading anything that
    pickle alone could not have saved.
    """

    def dumps(self, value: Any) -> bytes:
        try:
            import cloudpickle
        except ImportError as error:
            raise ImportError("CloudpickleCodec requires the cloudpickle package") from error
        return cloudpickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)


def codec_path(codec: type) -> str:
    """Return the import path a declaration saves for `codec`."""
    if not isinstance(codec, type):
        raise TypeError(f"pass the codec class, not an instance; load() rebuilds it from its import path "
                        f"(got {type(codec).__name__})")
    if "<locals>" in codec.__qualname__:
        raise ValueError(f"codec {codec.__qualname__} must be defined at module level so load() can import it")
    check_codec(codec(), name=codec.__qualname__)
    return f"{codec.__module__}:{codec.__qualname__}"


def check_codec(codec: Any, *, name: str) -> Any:
    if not isinstance(getattr(codec, "media_type", None), str) or not codec.media_type:
        raise TypeError(f"codec {name} needs a media_type string")
    if not callable(getattr(codec, "dumps", None)) or not callable(getattr(codec, "loads", None)):
        raise TypeError(f"codec {name} needs dumps(value) and loads(data) methods")
    return codec


def load_codec(path: str) -> Any:
    """Import and build the codec a declaration names."""
    module_name, _, qualname = path.partition(":")
    try:
        target = importlib.import_module(module_name)
        for part in qualname.split("."):
            target = getattr(target, part)
    except (ImportError, AttributeError) as error:
        raise ImportError(f"cannot import codec {path!r}: {error}; install it, or pass load(codec=...)") from error
    return check_codec(target(), name=path)


def encode(codec: Any, value: Any, *, label: str) -> bytes:
    """Serialize one value, naming the variable when the codec misbehaves."""
    data = codec.dumps(value)
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError(f"{label!r}: {type(codec).__qualname__}.dumps() returned {type(data).__name__}, not bytes")
    return bytes(data)
