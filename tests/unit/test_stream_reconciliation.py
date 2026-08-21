"""The reconciliation decision table.

Pure — no graph, no server. ``verdict`` is injected, so each case states the
convertibility answer it is exercising rather than depending on QUDT data.
"""
from __future__ import annotations

import pytest

from acquirium.internals.reconcile import (
    Conflict,
    Reconciliation,
    StreamMetadataConflict,
    reconcile_stream,
)

CELSIUS = "http://qudt.org/vocab/unit/DEG_C"
FAHRENHEIT = "http://qudt.org/vocab/unit/DEG_F"
MGL = "http://qudt.org/vocab/unit/MilliGM-PER-L"
WATER = "urn:nawi-water-ontology#Water"
AIR = "http://data.ashrae.org/standard223#Fluid-Air"


def _verdict(answer: str):
    """A stub compatibility_verdict that always returns *answer*."""
    return lambda a, b: answer


def _reconcile(*, ref=None, point=None, verdict="incompatible", allow=False,
               point_uri="urn:test:point"):
    return reconcile_stream(
        source_id="svcw-scada",
        ref_name="AB1_DO",
        point_uri=point_uri,
        ref_values=ref or {},
        point_values=point or {},
        verdict=_verdict(verdict),
        allow_unit_mismatch=allow,
    )


# ---------------------------------------------------------------- no point

def test_no_point_is_never_a_conflict():
    """A ref-only stream has nothing to reconcile against."""
    result = _reconcile(ref={"unit": CELSIUS, "medium": WATER}, point_uri=None)
    assert result == Reconciliation()


# ------------------------------------------------------- case 1: agreement

@pytest.mark.parametrize("field,value", [
    ("unit", CELSIUS), ("quantity_kind", "urn:qk:Temperature"),
    ("medium", WATER), ("substance", "urn:sub:DO"),
])
def test_equal_values_are_silent(field, value):
    result = _reconcile(ref={field: value}, point={field: value})
    assert result.conflicts == ()
    assert result.warnings == ()


# ------------------------------------------------ case 2: one side missing

@pytest.mark.parametrize("field,value", [
    ("unit", CELSIUS), ("quantity_kind", "urn:qk:Temperature"),
    ("medium", WATER), ("substance", "urn:sub:DO"),
])
def test_only_the_reference_has_it(field, value):
    assert _reconcile(ref={field: value}, point={}) == Reconciliation()


@pytest.mark.parametrize("field,value", [
    ("unit", CELSIUS), ("quantity_kind", "urn:qk:Temperature"),
    ("medium", WATER), ("substance", "urn:sub:DO"),
])
def test_only_the_point_has_it(field, value):
    assert _reconcile(ref={}, point={field: value}) == Reconciliation()


def test_empty_string_counts_as_missing():
    assert _reconcile(ref={"unit": ""}, point={"unit": CELSIUS}) == Reconciliation()


# ------------------------------------------------ case 3: unit differences

@pytest.mark.parametrize("verdict", ["match", "convertible"])
def test_convertible_unit_difference_is_accepted(verdict):
    """Celsius against Fahrenheit: reads convert into the point's unit."""
    result = _reconcile(ref={"unit": FAHRENHEIT}, point={"unit": CELSIUS},
                        verdict=verdict)
    assert result == Reconciliation()


def test_incompatible_unit_raises():
    result = _reconcile(ref={"unit": MGL}, point={"unit": CELSIUS},
                        verdict="incompatible")
    assert result.warnings == ()
    assert len(result.conflicts) == 1
    conflict = result.conflicts[0]
    assert conflict.field == "unit"
    assert conflict.ref_value == MGL
    assert conflict.point_value == CELSIUS
    assert "not convertible" in conflict.reason


def test_unknown_unit_compatibility_is_a_conflict_not_a_pass():
    """The fail-open hazard: `compatible` would be True here. Registration
    must refuse rather than let a bogus rescaling through at read time."""
    result = _reconcile(ref={"unit": "urn:acquirium:unit#BLR"},
                        point={"unit": MGL}, verdict="unknown")
    assert len(result.conflicts) == 1
    assert "cannot be established" in result.conflicts[0].reason


@pytest.mark.parametrize("verdict", ["incompatible", "unknown"])
def test_allow_unit_mismatch_downgrades_to_a_warning(verdict):
    result = _reconcile(ref={"unit": MGL}, point={"unit": CELSIUS},
                        verdict=verdict, allow=True)
    assert result.conflicts == ()
    assert len(result.warnings) == 1
    assert "registered anyway" in result.warnings[0]
    assert "point's unit unconverted" in result.warnings[0]


def test_unit_conflict_message_names_the_escape_hatch():
    conflict = _reconcile(ref={"unit": MGL}, point={"unit": CELSIUS}).conflicts[0]
    assert "allow_unit_mismatch=True" in conflict.message()


# -------------------------------------------- case 3c: quantity_kind warns

def test_quantity_kind_difference_only_warns():
    """Redundant with unit, and QUDT has near-synonyms; raising would be noise."""
    result = _reconcile(
        ref={"quantity_kind": "http://qudt.org/vocab/quantitykind/Temperature"},
        point={"quantity_kind": "http://qudt.org/vocab/quantitykind/ThermodynamicTemperature"},
    )
    assert result.conflicts == ()
    assert len(result.warnings) == 1
    assert "quantity_kind" in result.warnings[0]


def test_quantity_kind_ignores_allow_unit_mismatch():
    """It never raised, so the flag has nothing to downgrade."""
    result = _reconcile(
        ref={"quantity_kind": "urn:qk:A"}, point={"quantity_kind": "urn:qk:B"},
        allow=True,
    )
    assert result.conflicts == ()
    assert len(result.warnings) == 1


# ------------------------------------- case 3d: medium/substance always raise

@pytest.mark.parametrize("field,ref_value,point_value", [
    ("medium", WATER, AIR),
    ("substance", "urn:sub:Chlorine", "urn:sub:Ammonia"),
])
def test_strict_field_difference_raises(field, ref_value, point_value):
    result = _reconcile(ref={field: ref_value}, point={field: point_value})
    assert result.warnings == ()
    assert len(result.conflicts) == 1
    assert result.conflicts[0].field == field
    assert "no conversion exists" in result.conflicts[0].reason


@pytest.mark.parametrize("field", ["medium", "substance"])
def test_allow_unit_mismatch_does_not_cover_strict_fields(field):
    """The flag says `unit`, and it means it — a wrong medium has no remedy
    but fixing the mapping or the model."""
    result = _reconcile(ref={field: "urn:a"}, point={field: "urn:b"}, allow=True)
    assert len(result.conflicts) == 1
    assert result.conflicts[0].field == field


def test_strict_conflict_message_offers_no_escape_hatch():
    conflict = _reconcile(ref={"medium": WATER}, point={"medium": AIR}).conflicts[0]
    assert "allow_unit_mismatch" not in conflict.message()


# ------------------------------------------------------------- accumulation

def test_every_failing_field_is_reported_not_just_the_first():
    result = _reconcile(
        ref={"unit": MGL, "medium": WATER, "substance": "urn:sub:A"},
        point={"unit": CELSIUS, "medium": AIR, "substance": "urn:sub:B"},
    )
    assert {c.field for c in result.conflicts} == {"unit", "medium", "substance"}


def test_conflicts_and_warnings_coexist():
    result = _reconcile(
        ref={"medium": WATER, "quantity_kind": "urn:qk:A"},
        point={"medium": AIR, "quantity_kind": "urn:qk:B"},
    )
    assert [c.field for c in result.conflicts] == ["medium"]
    assert len(result.warnings) == 1


# --------------------------------------------------------------- exception

def test_exception_lists_every_conflict():
    conflicts = [
        Conflict("s", "a", "unit", "u1", "u2", "urn:p1", "not convertible"),
        Conflict("s", "b", "medium", "m1", "m2", "urn:p2", "no conversion exists"),
    ]
    error = StreamMetadataConflict(conflicts)
    assert error.conflicts == tuple(conflicts)
    text = str(error)
    assert "2 stream registration(s)" in text
    assert "'a'" in text and "'b'" in text
    assert "urn:p1" in text and "urn:p2" in text
