"""Effective units, their provenance, and the auto-conversion gate.

A stream's unit can come from its point or from its external reference. The
point wins; the reference fills in. Where the two differ and are convertible,
values are converted into the point's unit — and where convertibility cannot
be established, they are deliberately left alone.
"""
from __future__ import annotations

import logging

import polars as pl
import pytest

from acquirium.Client.data_object import BindingInfo, DataObject

DEG_C = "http://qudt.org/vocab/unit/DEG_C"
DEG_F = "http://qudt.org/vocab/unit/DEG_F"
MGL = "http://qudt.org/vocab/unit/MilliGM-PER-L"
BLR = "urn:acquirium:unit#BLR"  # no dimension vector, no quantity kind


def binding(alias="m", *, point_unit=None, ref_unit=None, ref="urn:t#r1"):
    return BindingInfo(
        nid=1, point_uri="urn:t#p1", ref_uri=ref, alias=alias,
        entity_contexts=[{}], row_count=1,
        property_unit=point_unit, ref_unit=ref_unit,
    )


def data_object(*bindings, client=None) -> DataObject:
    obj = DataObject.__new__(DataObject)
    obj._bindings = list(bindings)
    obj._client = client
    return obj


# ------------------------------------------------------- effective unit

def test_point_unit_wins_over_the_reference():
    obj = data_object(binding(point_unit=DEG_C, ref_unit=DEG_F))
    assert obj.units() == {"m": DEG_C}


def test_reference_unit_is_adopted_when_the_point_has_none():
    obj = data_object(binding(ref_unit=DEG_F))
    assert obj.units() == {"m": DEG_F}


def test_no_unit_anywhere_is_none():
    assert data_object(binding()).units() == {"m": None}


def test_each_alias_resolves_independently():
    obj = data_object(
        binding("a", point_unit=DEG_C),
        binding("b", ref_unit=MGL),
        binding("c"),
    )
    assert obj.units() == {"a": DEG_C, "b": MGL, "c": None}


def test_first_binding_per_alias_decides():
    """Several streams can share an alias; the alias needs one answer."""
    obj = data_object(
        binding("m", point_unit=DEG_C, ref="urn:t#r1"),
        binding("m", point_unit=MGL, ref="urn:t#r2"),
    )
    assert obj.units() == {"m": DEG_C}


# ------------------------------------------------------------ provenance

@pytest.mark.parametrize("point_unit,ref_unit,expected", [
    (DEG_C, DEG_F, "point"),
    (DEG_C, None, "point"),
    (None, DEG_F, "reference"),
    (None, None, "none"),
])
def test_unit_sources_reports_where_the_unit_came_from(point_unit, ref_unit, expected):
    obj = data_object(binding(point_unit=point_unit, ref_unit=ref_unit))
    assert obj.unit_sources() == {"m": expected}


def test_units_and_unit_sources_agree_on_aliases():
    obj = data_object(binding("a", point_unit=DEG_C), binding("b", ref_unit=MGL))
    assert set(obj.units()) == set(obj.unit_sources())


# ------------------------------------------------- auto-conversion gate

class _Client:
    """Returns a fixed verdict, and records whether conversion was attempted."""

    def __init__(self, verdict: str, *, multiplier: float = 2.0):
        self.verdict = verdict
        self.multiplier = multiplier
        self.factor_calls: list[tuple[str, str]] = []

    def get_conversion_factors(self, from_unit, to_unit):
        self.factor_calls.append((from_unit, to_unit))
        return {
            "from_uri": from_unit, "to_uri": to_unit,
            "from_multiplier": self.multiplier, "from_offset": 0.0,
            "to_multiplier": 1.0, "to_offset": 0.0,
            # Deliberately True even for `unknown`, mirroring the server:
            # a caller branching on this rather than the verdict is the bug.
            "compatible": True,
            "verdict": self.verdict,
        }

    def timeseries_df(self, ref_uri, **kwargs):
        return pl.DataFrame({
            "ts": pl.Series("ts", [0], dtype=pl.Datetime(time_zone="UTC")),
            "value": [10.0],
            "uri": [ref_uri],
        })

    def timeseries_info_batch(self, uris):
        return {}


def materialized(verdict: str) -> tuple[DataObject, _Client]:
    client = _Client(verdict)
    obj = data_object(binding(point_unit=DEG_C, ref_unit=DEG_F), client=client)
    obj._query_params = {"cast_value": "float", "order": "asc"}
    obj._entity_columns = []
    obj._tall = None
    obj._materialized = False
    obj._pending_conversions = []
    obj._materialize()
    return obj, client


@pytest.mark.parametrize("verdict", ["convertible", "match"])
def test_convertible_pair_is_converted(verdict):
    obj, client = materialized(verdict)
    assert client.factor_calls == [(DEG_F, DEG_C)]
    assert obj._tall["value_numeric"].to_list() == [20.0]


def test_incompatible_pair_is_left_alone():
    obj, _ = materialized("incompatible")
    assert obj._tall["value_numeric"].to_list() == [10.0]


def test_unknown_pair_is_left_alone_despite_compatible_being_true():
    """The fail-open hazard: `compatible` is True here and applying the
    factors would silently rescale the series."""
    obj, _ = materialized("unknown")
    assert obj._tall["value_numeric"].to_list() == [10.0]


def test_unknown_pair_warns_about_the_missing_evidence(caplog):
    with caplog.at_level(logging.WARNING, logger="acquirium.data_object"):
        materialized("unknown")
    assert any("cannot tell whether" in r.message for r in caplog.records)


def test_incompatible_pair_warns(caplog):
    with caplog.at_level(logging.WARNING, logger="acquirium.data_object"):
        materialized("incompatible")
    assert any("incompatible units" in r.message for r in caplog.records)


def test_neither_case_raises():
    """Query time is warning-only; a bad annotation must not break a read."""
    for verdict in ("incompatible", "unknown"):
        materialized(verdict)
