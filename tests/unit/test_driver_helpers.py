from __future__ import annotations

import pytest

from acquirium.Driver import Driver
from acquirium.internals.models import compute_handle


class DummyDriver(Driver):
    def setup(self) -> None:
        self._source_id = "demo-source"

    def loop(self) -> None:
        return None


def test_reference_uri_uses_driver_source_id():
    driver = DummyDriver(aq=object(), config={})
    driver.setup()
    assert driver.source_id() == "demo-source"
    assert driver.reference_uri("cpu_percent") == compute_handle("demo-source", "cpu_percent")


def test_source_id_requires_driver_to_set_one():
    driver = DummyDriver(aq=object(), config={})
    with pytest.raises(AttributeError):
        driver.source_id()

