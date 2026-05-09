from __future__ import annotations

import pytest

from acquirium.Storage.pg_reference import PGReferenceInfo, PGReferenceRegistry


def test_pg_reference_rejects_split_value_modes_before_connecting():
    registry = PGReferenceRegistry()
    registry.register(
        "urn:test:pg-reference",
        PGReferenceInfo(dsn="postgresql://unused", table="external_values"),
    )

    with pytest.raises(ValueError, match="does not support value_mode 'numeric'"):
        list(registry.timeseries("urn:test:pg-reference", value_mode="numeric"))


def test_pg_reference_rejects_unknown_value_mode_before_connecting():
    registry = PGReferenceRegistry()
    registry.register(
        "urn:test:pg-reference",
        PGReferenceInfo(dsn="postgresql://unused", table="external_values"),
    )

    with pytest.raises(ValueError, match="value_mode must be"):
        list(registry.timeseries("urn:test:pg-reference", value_mode="registered"))
