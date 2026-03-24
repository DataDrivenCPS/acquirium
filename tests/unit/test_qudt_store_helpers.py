"""Tests for pure helpers in acquirium.TextMatch.qudt_store."""

import pytest
from pathlib import Path

from acquirium.TextMatch.qudt_store import _split_local_name, _build_surfaces, QUDTStore


# ── _split_local_name ──────────────────────────────────────


class TestSplitLocalName:
    def test_camel_case(self):
        result = _split_local_name("http://qudt.org/vocab/unit/MilliLiter")
        assert "milli" in result
        assert "liter" in result

    def test_underscore_and_hyphen(self):
        result = _split_local_name("http://qudt.org/vocab/unit/cubic_meter-per-second")
        assert "cubic" in result
        assert "meter" in result
        assert "per" in result
        assert "second" in result

    def test_plain_word(self):
        result = _split_local_name("kelvin")
        assert result == ["kelvin"]


# ── _build_surfaces ────────────────────────────────────────


class TestBuildSurfaces:
    def test_labels_only(self):
        result = _build_surfaces("http://qudt.org/vocab/unit/Kelvin", ["Kelvin", "kelvin"], None, None)
        assert "kelvin" in result

    def test_with_symbol_and_ucum(self):
        result = _build_surfaces("http://qudt.org/vocab/unit/Kelvin", ["Kelvin"], "K", "K")
        assert "kelvin" in result
        assert "K" in result

    def test_deduplication(self):
        result = _build_surfaces("http://qudt.org/vocab/unit/Meter", ["meter", "meter"], "m", None)
        assert result.count("meter") == 1

    def test_empty_labels_uses_local_name(self):
        result = _build_surfaces("http://qudt.org/vocab/unit/MilliLiter", [], None, None)
        assert len(result) >= 1
        assert any("milli" in s for s in result)

    def test_all_empty(self):
        # URI with no meaningful local name
        result = _build_surfaces("", [], None, None)
        assert result == []

    def test_symbol_preserved_case(self):
        result = _build_surfaces("http://qudt.org/vocab/unit/DegreeCelsius", [], "degC", None)
        assert "degC" in result


# ── QUDTStore gz cache ─────────────────────────────────────


class TestGzCache:
    def test_save_and_load_roundtrip(self, tmp_path):
        data = [
            {"uri": "urn:unit1", "kind": "unit", "label": "Meter", "surfaces": ["meter", "m"]},
            {"uri": "urn:unit2", "kind": "unit", "label": "Kelvin", "surfaces": ["kelvin", "K"]},
        ]
        path = tmp_path / "test.json.gz"
        QUDTStore._save_gz(path, data)
        loaded = QUDTStore._load_gz(path)
        assert loaded == data

    def test_load_missing_file(self, tmp_path):
        path = tmp_path / "nonexistent.json.gz"
        result = QUDTStore._load_gz(path)
        assert result == []

    def test_load_cached_uris(self, tmp_path):
        store = QUDTStore(data_dir=tmp_path)
        units = [{"uri": "urn:u1"}, {"uri": "urn:u2"}]
        qks = [{"uri": "urn:qk1"}]
        QUDTStore._save_gz(store._units_path, units)
        QUDTStore._save_gz(store._qk_path, qks)
        uris = store._load_cached_uris()
        assert uris == {"urn:u1", "urn:u2", "urn:qk1"}
