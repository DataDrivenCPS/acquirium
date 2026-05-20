"""Tests for pure helpers in acquirium.TextMatch.qudt_store."""

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


# ── QUDTStore.extract_concepts (graph-fed) ─────────────────


class TestExtractConcepts:
    def _graph(self):
        from rdflib import Graph

        g = Graph()
        g.parse(
            data="""
            @prefix qudt: <http://qudt.org/schema/qudt/> .
            @prefix unit: <http://qudt.org/vocab/unit/> .
            @prefix qk:   <http://qudt.org/vocab/quantitykind/> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            unit:KiloGM a qudt:Unit ; rdfs:label "Kilogram" ;
                qudt:symbol "kg" ; qudt:hasQuantityKind qk:Mass .
            qk:Mass a qudt:QuantityKind ; rdfs:label "Mass" ;
                qudt:applicableUnit unit:KiloGM .
            """,
            format="turtle",
        )
        return g

    def test_unit_extraction(self):
        c = QUDTStore.extract_concepts(
            self._graph(), "http://qudt.org/schema/qudt/Unit"
        )
        assert len(c) == 1
        u = c[0]
        assert u["uri"] == "http://qudt.org/vocab/unit/KiloGM"
        assert u["kind"] == "unit"
        assert "kg" in u["surfaces"] and "kilogram" in u["surfaces"]
        assert u["related"] == ["http://qudt.org/vocab/quantitykind/Mass"]

    def test_quantity_kind_extraction(self):
        c = QUDTStore.extract_concepts(
            self._graph(), "http://qudt.org/schema/qudt/QuantityKind"
        )
        assert [x["uri"] for x in c] == [
            "http://qudt.org/vocab/quantitykind/Mass"
        ]
        assert c[0]["kind"] == "quantity_kind"
        assert c[0]["related"] == ["http://qudt.org/vocab/unit/KiloGM"]
