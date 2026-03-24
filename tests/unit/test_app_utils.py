"""Tests for acquirium.internals.app_utils — URI construction helpers."""

import pytest

from acquirium.internals.app_utils import _safe_fragment, app_uri_for, make_stream_ref_uri


class TestSafeFragment:
    def test_alphanumeric_passthrough(self):
        assert _safe_fragment("hello") == "hello"

    def test_spaces_encoded(self):
        result = _safe_fragment("hello world")
        assert " " not in result
        assert "hello%20world" == result

    def test_special_chars_encoded(self):
        result = _safe_fragment("a/b#c")
        assert "/" not in result
        assert "#" not in result


class TestAppUriFor:
    def test_simple_name(self):
        uri = app_uri_for("my_app")
        assert uri.startswith("urn:acquirium#app/")
        assert "my_app" in uri

    def test_name_with_spaces(self):
        uri = app_uri_for("my app")
        assert "urn:acquirium#app/" in uri
        assert " " not in uri


class TestMakeStreamRefUri:
    def test_deterministic(self):
        uri1 = make_stream_ref_uri("urn:test:point1")
        uri2 = make_stream_ref_uri("urn:test:point1")
        assert uri1 == uri2

    def test_different_inputs_differ(self):
        uri1 = make_stream_ref_uri("urn:test:point1")
        uri2 = make_stream_ref_uri("urn:test:point2")
        assert uri1 != uri2

    def test_format(self):
        uri = make_stream_ref_uri("urn:test:point1")
        assert uri.startswith("urn:acquirium#stream/")
        # 12 hex chars after prefix
        suffix = uri.split("stream/")[1]
        assert len(suffix) == 12
        int(suffix, 16)  # should not raise
