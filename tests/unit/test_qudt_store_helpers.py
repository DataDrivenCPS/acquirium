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
    KELVIN = "http://qudt.org/vocab/unit/Kelvin"

    def test_labels_only(self):
        surfaces, exact = _build_surfaces(self.KELVIN, ["Kelvin", "kelvin"], None, None, True)
        assert "kelvin" in surfaces
        assert exact == []

    def test_unit_symbol_and_ucum_are_exact_only(self):
        surfaces, exact = _build_surfaces(self.KELVIN, ["Kelvin"], "K", "K", True)
        assert surfaces == ["kelvin"]
        assert exact == ["K"]

    def test_quantity_kind_symbol_stays_embedded(self):
        surfaces, exact = _build_surfaces(
            "http://qudt.org/vocab/quantitykind/Temperature", ["Temperature"], "T", None, False
        )
        assert "T" in surfaces
        assert exact == []

    def test_deduplication(self):
        surfaces, _ = _build_surfaces("http://qudt.org/vocab/unit/Meter", ["meter", "meter"], "m", None, True)
        assert surfaces.count("meter") == 1

    def test_empty_labels_uses_local_name(self):
        surfaces, _ = _build_surfaces("http://qudt.org/vocab/unit/MilliLiter", [], None, None, True)
        assert len(surfaces) >= 1
        assert any("milli" in s for s in surfaces)

    def test_all_empty(self):
        # URI with no meaningful local name
        assert _build_surfaces("", [], None, None, True) == ([], [])

    def test_symbol_is_embedded_when_it_is_the_only_surface(self):
        # nothing else to embed: the concept must keep a vector
        surfaces, exact = _build_surfaces("", [], "degC", None, True)
        assert surfaces == ["degC"]
        assert exact == []

    def test_symbol_preserved_case(self):
        _, exact = _build_surfaces("http://qudt.org/vocab/unit/DegreeCelsius", [], "degC", None, True)
        assert "degC" in exact


# ── QUDTStore.extract_concepts (query-fed) ─────────────────


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

    def _extract(self, rdf_type):
        rows = self._graph().query(QUDTStore.concept_query(rdf_type))
        values = [tuple(None if cell is None else str(cell) for cell in row) for row in rows]
        return QUDTStore.extract_concepts(values, rdf_type)

    def test_unit_extraction(self):
        c = self._extract("http://qudt.org/schema/qudt/Unit")
        assert len(c) == 1
        u = c[0]
        assert u["uri"] == "http://qudt.org/vocab/unit/KiloGM"
        assert u["kind"] == "unit"
        assert "kilogram" in u["surfaces"]
        assert u["exact_surfaces"] == ["kg"]
        assert u["related"] == ["http://qudt.org/vocab/quantitykind/Mass"]

    def test_quantity_kind_extraction(self):
        c = self._extract("http://qudt.org/schema/qudt/QuantityKind")
        assert [x["uri"] for x in c] == [
            "http://qudt.org/vocab/quantitykind/Mass"
        ]
        assert c[0]["kind"] == "quantity_kind"
        assert c[0]["related"] == ["http://qudt.org/vocab/unit/KiloGM"]

    def test_label_rules_and_bare_subjects(self):
        unit = "http://qudt.org/schema/qudt/Unit"
        label = "http://www.w3.org/2000/01/rdf-schema#label"
        alt = "http://www.w3.org/2004/02/skos/core#altLabel"
        rows = [
            ("http://ex.org/u/B", label, "Zeta", ""),
            ("http://ex.org/u/B", label, "Alpha", "en-US"),
            ("http://ex.org/u/B", label, "Kilogramm", "de"),
            ("http://ex.org/u/B", alt, "Alpha", None),
            ("http://ex.org/u/A-Bare", None, None, None),
        ]
        a, b = QUDTStore.extract_concepts(rows, unit)
        assert a["uri"] == "http://ex.org/u/A-Bare" and a["label"] == "a bare"
        assert b["label"] == "Alpha"
        assert b["surfaces"][:2] == ["alpha", "zeta"]
        assert "kilogramm" not in b["surfaces"]
