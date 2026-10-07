"""Shared fixtures for unit tests."""

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def naive_dt():
    return datetime(2025, 6, 15, 10, 30, 0)


@pytest.fixture
def utc_dt():
    return datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def mock_http_response():
    """Factory fixture: returns a mock requests.Response with given status and JSON."""

    def _make(status_code=200, json_data=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = json_data or {}
        resp.raise_for_status.side_effect = (
            None if status_code < 400 else Exception(f"HTTP {status_code}")
        )
        return resp

    return _make


@pytest.fixture(autouse=True)
def _clear_explore_caches():
    """The explore layer caches per server key, and the fake clients derive
    that key from an object id, which a later test's object can reuse. Start
    every test with empty caches so results never leak between tests."""
    from acquirium.Client.explore.attributes import clear_registry_cache
    from acquirium.Client.explore.facets import clear_facet_cache
    from acquirium.Client.explore.traverse import clear_segment_cache

    clear_registry_cache()
    clear_facet_cache()
    clear_segment_cache()
    yield
