"""Deterministic source digests for application entrypoints."""

from __future__ import annotations

from hashlib import sha256
import inspect
from pathlib import Path


def source_digest(target: object) -> str:
    """Identify an entrypoint by its module bytes and qualified name.

    Hash the whole module because a transform can depend on helpers or constants
    outside its class body. This deliberately treats comment-only edits as new
    executable identities too; Binding.progress_key separately preserves progress.
    Imported modules, dependencies, and environment are not included, so this is
    a code-version check rather than a reproducible environment or security proof.
    Source inspection is a fallback when module bytes are unavailable.
    """
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
