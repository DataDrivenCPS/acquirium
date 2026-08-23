"""Executable digests are enforced identities, not descriptive metadata."""

import importlib
import sys

import pytest

from acquirium.Materialization.definitions import source_digest
from acquirium.Materialization.worker import load_entrypoint


def test_worker_rejects_an_entrypoint_that_does_not_match_its_digest():
    digest = source_digest(abs)
    assert load_entrypoint("builtins:abs", digest) is abs
    with pytest.raises(ValueError, match="digest mismatch"):
        load_entrypoint("builtins:abs", "0" * 64)


def test_digest_covers_sibling_code_in_the_executable_module(tmp_path, monkeypatch):
    module_path = tmp_path / "digest_fixture.py"
    module_path.write_text("def helper(value):\n    return value + 1\n\ndef target(value):\n    return helper(value)\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module("digest_fixture")
    before = source_digest(module.target)

    module_path.write_text("def helper(value):\n    return value + 2\n\ndef target(value):\n    return helper(value)\n")
    try:
        assert source_digest(module.target) != before
    finally:
        sys.modules.pop("digest_fixture", None)
