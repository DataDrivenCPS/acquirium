"""Immutable, deterministic transformation definition bundles."""

from __future__ import annotations

from hashlib import sha256
import inspect
from pathlib import Path


def source_digest(target: object) -> str:
    """Digest the importable executable module and qualified entrypoint."""
    module_name = getattr(target, "__module__", "")
    qualname = getattr(target, "__qualname__", target.__class__.__qualname__)
    module = inspect.getmodule(target)
    module_file = getattr(module, "__file__", None)
    if module_file:
        try:
            content = Path(module_file).read_bytes()
        except OSError:
            content = b""
        if content:
            return sha256(
                module_name.encode() + b":" + qualname.encode() + b"\0" + content
            ).hexdigest()
    try:
        source = inspect.getsource(target)
    except (OSError, TypeError):
        source = qualname
    return sha256(f"{module_name}:{qualname}\n{source}".encode()).hexdigest()
