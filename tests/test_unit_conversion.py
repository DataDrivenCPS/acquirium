"""Tests for QUDT unit conversion and DataObject.convert_to()."""

from importlib.resources import files as _res_files
from pathlib import Path

import pytest

from acquirium.internals.qudt_units import (
    QUDTUnitConverter,
    UnitNotFound,
    IncompatibleUnits,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

QUDT_UNIT_PATH = Path(str(_res_files("acquirium._ontologies") / "qudt_unit.ttl"))


@pytest.fixture(scope="module")
def converter() -> QUDTUnitConverter:
    """Load the QUDT unit graph once for the entire module."""
    return QUDTUnitConverter(str(QUDT_UNIT_PATH))


# ---------------------------------------------------------------------------
# QUDTUnitConverter.resolve_unit
# ---------------------------------------------------------------------------

class TestResolveUnit:
    def test_resolve_by_local_name(self, converter):
        u = converter.resolve_unit("MilliL-PER-MIN")
        assert "MilliL-PER-MIN" in str(u.uri)
        assert u.multiplier != 0

    def test_resolve_by_symbol(self, converter):
        u = converter.resolve_unit("mL/min")
        assert "MilliL-PER-MIN" in str(u.uri)

    def test_resolve_by_uri(self, converter):
        u = converter.resolve_unit("http://qudt.org/vocab/unit/L-PER-MIN")
        assert "L-PER-MIN" in str(u.uri)

    def test_resolve_unknown_raises(self, converter):
        with pytest.raises(UnitNotFound):
            converter.resolve_unit("NoSuchUnit12345")

    def test_resolve_gallon(self, converter):
        u = converter.resolve_unit("GAL_US")
        assert u.label is not None
        assert u.multiplier > 0


# ---------------------------------------------------------------------------
# QUDTUnitConverter.convert — compatibility and correctness
# ---------------------------------------------------------------------------

class TestConvert:
    def test_mL_min_to_L_min(self, converter):
        """1000 mL/min should be 1 L/min."""
        result = converter.convert(1000.0, "MilliL-PER-MIN", "L-PER-MIN")
        assert abs(result - 1.0) < 1e-6

    def test_L_to_mL(self, converter):
        """1 L = 1000 mL."""
        result = converter.convert(1.0, "L", "MilliL")
        assert abs(result - 1000.0) < 1e-3

    def test_gallon_to_litre(self, converter):
        """1 US gallon ≈ 3.78541 litres."""
        result = converter.convert(1.0, "GAL_US", "L")
        assert abs(result - 3.78541) < 0.01

    def test_identity_conversion(self, converter):
        """Converting a unit to itself should return the same value."""
        result = converter.convert(42.0, "L", "L")
        assert abs(result - 42.0) < 1e-9

    def test_incompatible_raises(self, converter):
        """Trying to convert between incompatible units should raise."""
        with pytest.raises(IncompatibleUnits):
            converter.convert(1.0, "L", "DEG_C")

    def test_small_value_precision(self, converter):
        """Conversion should maintain precision for small values."""
        result = converter.convert(0.001, "L-PER-MIN", "MilliL-PER-MIN")
        assert abs(result - 1.0) < 1e-6


# ---------------------------------------------------------------------------
# Integration: server endpoints (requires running server)
# ---------------------------------------------------------------------------

@pytest.fixture
def acquirium_client():
    """Fixture to create an Acquirium client for unit conversion integration tests."""
    from acquirium import Acquirium
    from conftest import ACQUIRIUM_TEST_SERVER_HOST, ACQUIRIUM_TEST_SERVER_PORT

    acq = Acquirium(
        server_url=ACQUIRIUM_TEST_SERVER_HOST,
        server_port=ACQUIRIUM_TEST_SERVER_PORT,
        use_ssl=False,
    )
    acq.insert_graph("tests/test_model_units.ttl", source_id="plant")
    return acq


class TestServerEndpoints:
    def test_resolve_unit_endpoint(self, acquirium_client):
        info = acquirium_client.client.resolve_unit("L-PER-MIN")
        assert "uri" in info
        assert "L-PER-MIN" in info["uri"]
        assert info["multiplier"] > 0

    def test_conversion_factors_endpoint(self, acquirium_client):
        factors = acquirium_client.client.get_conversion_factors(
            "MilliL-PER-MIN", "L-PER-MIN"
        )
        assert factors["compatible"] is True
        assert "from_multiplier" in factors
        assert "to_multiplier" in factors

    def test_conversion_factors_incompatible(self, acquirium_client):
        factors = acquirium_client.client.get_conversion_factors("L", "DEG_C")
        assert factors["compatible"] is False


class TestDataObjectUnits:
    def test_units_returns_mapping(self, acquirium_client):
        """units() should return {alias: unit_uri_or_None}."""
        acq = acquirium_client
        query = acq.find_all_data()
        data = query.data()
        units = data.units()
        assert isinstance(units, dict)
        for alias in data.aliases:
            assert alias in units

    def test_convert_to_case2(self, acquirium_client):
        """Case 2: property has unit, user provides only to_unit."""
        acq = acquirium_client
        query = acq.find_all_data()
        data = query.data()

        units = data.units()
        # Find an alias that has a unit
        alias_with_unit = None
        for alias, unit_uri in units.items():
            if unit_uri and "MilliL-PER-MIN" in unit_uri:
                alias_with_unit = alias
                break

        if alias_with_unit is None:
            pytest.skip("No alias with MilliL-PER-MIN unit in test model")

        original = data[alias_with_unit]
        converted = data.convert_to("L-PER-MIN", alias=alias_with_unit)
        converted_df = converted[alias_with_unit]

        # Values should be scaled by 0.001 (mL -> L)
        if not original.is_empty() and not converted_df.is_empty():
            orig_val = original["value"][0]
            conv_val = converted_df["value"][0]
            assert abs(conv_val - orig_val * 0.001) < 1e-6

    def test_convert_to_case1(self, acquirium_client):
        """Case 1: no unit on property, user provides from_unit and to_unit."""
        acq = acquirium_client
        query = acq.find_all_data()
        data = query.data()

        units = data.units()
        # Find an alias without a unit
        alias_no_unit = None
        for alias, unit_uri in units.items():
            if unit_uri is None:
                alias_no_unit = alias
                break

        if alias_no_unit is None:
            pytest.skip("All aliases have units in test model")

        # Should raise without from_unit
        with pytest.raises(ValueError, match="No unit annotation"):
            data.convert_to("L", alias=alias_no_unit)

        # Should work with from_unit
        converted = data.convert_to("L", from_unit="MilliL", alias=alias_no_unit)
        assert isinstance(converted, type(data))

    def test_convert_to_returns_new_object(self, acquirium_client):
        """convert_to() should return a new DataObject, not mutate the original."""
        acq = acquirium_client
        query = acq.find_all_data()
        data = query.data()
        units = data.units()

        alias_with_unit = None
        for alias, unit_uri in units.items():
            if unit_uri is not None:
                alias_with_unit = alias
                break

        if alias_with_unit is None:
            pytest.skip("No alias with unit in test model")

        original_df = data[alias_with_unit].clone()
        converted = data.convert_to("L-PER-MIN", alias=alias_with_unit)
        after_df = data[alias_with_unit]

        # Original should be unchanged
        assert original_df.equals(after_df)
        assert converted is not data

    def test_convert_to_unresolvable_to_unit_raises(self, acquirium_client):
        """A garbage to_unit fails as 'no convertible pair': the lenient
        resolver may return fuzzy candidates, but none is compatible with
        the source unit, and the error names both sides."""
        acq = acquirium_client
        data = acq.find_all_data().data()

        with pytest.raises(ValueError, match=r"no convertible unit pair for .*NoSuchUnit12345"):
            data.convert_to("NoSuchUnit12345")

    def test_convert_to_unresolvable_from_unit_raises(self, acquirium_client):
        """A garbage from_unit is reported before any conversion runs."""
        acq = acquirium_client
        data = acq.find_all_data().data()

        with pytest.raises(ValueError, match=r"no convertible unit pair for 'NoSuchUnit12345'"):
            data.convert_to("L-PER-MIN", from_unit="NoSuchUnit12345")

    def test_convert_to_accepts_full_unit_uri(self, acquirium_client):
        """to_unit may be a full QUDT URI, not only a label/UCUM code."""
        acq = acquirium_client
        data = acq.find_all_data().data()

        units = data.units()
        alias_with_unit = next(
            (a for a, u in units.items() if u and "MilliL-PER-MIN" in u), None
        )
        if alias_with_unit is None:
            pytest.skip("No alias with MilliL-PER-MIN unit in test model")

        converted = data.convert_to(
            "http://qudt.org/vocab/unit/L-PER-MIN", alias=alias_with_unit
        )
        assert isinstance(converted, type(data))
