"""The docs capture script, without an instance: parsing, running, rewriting."""

import importlib.util
import sys
from pathlib import Path

import pytest

_PATH = Path(__file__).parents[2] / "scripts" / "docs_outputs.py"
_spec = importlib.util.spec_from_file_location("docs_outputs", _PATH)
docs_outputs = importlib.util.module_from_spec(_spec)
sys.modules["docs_outputs"] = docs_outputs
_spec.loader.exec_module(docs_outputs)

capture = docs_outputs.capture


def page(*parts: str) -> str:
    return "\n".join(parts)


def test_output_block_is_replaced_by_the_last_expression():
    text = page("```python", "x = 2", "x * 21", "```", "```text", "stale", "```", "after")
    assert capture(text, {}) == page("```python", "x = 2", "x * 21", "```", "```text", "42", "```", "after")


def test_printed_text_comes_before_the_value():
    text = page("```python", "print('a')", "'b'", "```", "```text", "```")
    assert capture(text, {}) == page("```python", "print('a')", "'b'", "```", "```text", "a", "'b'", "```")


def test_blocks_share_one_namespace():
    text = page("```python", "x = 5", "```", "", "prose", "", "```python", "x + 1", "```", "```text", "```")
    assert capture(text, {}).splitlines()[-2] == "6"


def test_a_text_block_after_prose_is_not_an_output():
    text = page("```python", "1", "```", "", "The error looks like this:", "", "```text", "kept", "```")
    assert capture(text, {}) == text


def test_other_languages_are_left_alone():
    text = page("```bash", "acquirium server", "```", "```text", "kept", "```")
    assert capture(text, {}) == text


def test_skip_does_not_run_the_block():
    text = page("<!-- capture: skip -->", "```python", "raise SystemExit", "```", "```text", "kept", "```")
    assert capture(text, {}) == text


def test_raises_writes_the_error():
    text = page("<!-- capture: raises -->", "```python", "int('x')", "```", "```text", "```")
    assert "ValueError: invalid literal for int()" in capture(text, {})


def test_an_unexpected_error_names_the_line():
    text = page("intro", "", "```python", "1 / 0", "```")
    with pytest.raises(RuntimeError, match="line 3: ZeroDivisionError"):
        capture(text, {})


def test_raises_block_that_does_not_raise_fails():
    text = page("<!-- capture: raises -->", "```python", "1", "```", "```text", "```")
    with pytest.raises(RuntimeError, match="did not raise"):
        capture(text, {})


def test_ports_are_normalised():
    text = page("```python", "'http://127.0.0.1:55173/x'", "```", "```text", "```")
    assert "'http://127.0.0.1:8000/x'" in capture(text, {})


def test_empty_output_empties_the_block():
    text = page("```python", "x = 1", "```", "```text", "stale", "```")
    assert capture(text, {}) == page("```python", "x = 1", "```", "```text", "```")


def test_pages_opt_in_with_the_instance_comment():
    assert docs_outputs.INSTANCE.search("<!-- capture: instance=seawater-ro -->").group(1) == "seawater-ro"
    assert docs_outputs.INSTANCE.search("# a page without it") is None
