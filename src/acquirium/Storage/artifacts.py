"""Content-addressed filesystem storage for immutable runtime artifacts."""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import os
from pathlib import Path
import tempfile
import time
from typing import Mapping


@dataclass(frozen=True)
class ArtifactRecord:
    digest: str
    uri: str
    size_bytes: int
    media_type: str
    metadata: Mapping[str, object]


class FilesystemArtifactStore:
    """Atomic local store; reads always re-verify the content digest."""
    def __init__(self, root: str | Path, *, fsync: bool = True) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._fsync = fsync

    def _path(self, digest: str) -> Path:
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("artifact digest must be lowercase SHA-256 hex")
        path = (self.root / digest[:2] / digest).resolve()
        if self.root not in path.parents:
            raise ValueError("artifact path escapes configured root")
        return path

    def put(self, data: bytes, *, media_type: str = "application/octet-stream",
            metadata: Mapping[str, object] | None = None) -> ArtifactRecord:
        digest = sha256(data).hexdigest(); path = self._path(digest)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not path.exists():
            descriptor, temporary = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(data); handle.flush()
                    if self._fsync:
                        os.fsync(handle.fileno())
                os.replace(temporary, path)
            except BaseException:
                try: os.unlink(temporary)
                except FileNotFoundError: pass
                raise
        self.get(digest)
        return ArtifactRecord(digest, path.as_uri(), len(data), media_type, dict(metadata or {}))

    def get(self, digest: str) -> bytes:
        try: data = self._path(digest).read_bytes()
        except FileNotFoundError as error: raise KeyError(digest) from error
        if sha256(data).hexdigest() != digest:
            raise ValueError(f"artifact {digest} failed digest verification")
        return data

    def delete(self, digest: str) -> bool:
        try: self._path(digest).unlink()
        except FileNotFoundError: return False
        return True

    def sweep_temporary_files(self, *, older_than_seconds: float = 3600) -> int:
        """Remove only abandoned atomic-write temporaries, never final blobs."""
        cutoff = time.time() - older_than_seconds
        removed = 0
        for path in self.root.rglob(".tmp-*"):
            if path.is_file() and path.stat().st_mtime <= cutoff:
                path.unlink(missing_ok=True)
                removed += 1
        return removed

    def sweep_orphans(self, referenced_digests: set[str], *, older_than_seconds: float = 86400) -> int:
        """Collect aged blobs that have no durable revision reference.

        A producer writes the immutable file before its database transaction
        records a candidate revision.  The age guard leaves that short window
        recoverable while allowing abandoned producer output to be reclaimed.
        """
        cutoff = time.time() - older_than_seconds
        removed = 0
        for directory in self.root.iterdir():
            if not directory.is_dir() or len(directory.name) != 2:
                continue
            for path in directory.iterdir():
                digest = path.name
                if (not path.is_file() or len(digest) != 64 or
                        any(char not in "0123456789abcdef" for char in digest) or
                        digest in referenced_digests or path.stat().st_mtime > cutoff):
                    continue
                path.unlink()
                removed += 1
        return removed
