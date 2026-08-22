"""The unit vocabulary: bundled QUDT plus `[ontologies] unit_extensions`.

Covers the extension mechanism itself (config parsing, merge semantics) and
the two properties it exists to provide: aliasing an existing QUDT unit so a
plant-floor abbreviation resolves, and defining a unit QUDT's bundled subset
lacks so it still converts and compatibility-checks.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from rdflib import Graph, Namespace, URIRef

from acquirium.internals.qudt_units import QUDTUnitConverter, UnitNotFound
from acquirium.Server.config import load_ontology_config
from acquirium.Storage.graph_store import load_unit_extensions, merge_unit_vocabulary
from acquirium.TextMatch.qudt_store import QUDTStore

QUDT = Namespace("http://qudt.org/schema/qudt/")
UNIT = Namespace("http://qudt.org/vocab/unit/")
AQUNIT = Namespace("urn:acquirium:unit#")

#: A fixture extension, not a copy of any deployment's file — the mechanism
#: should be testable without one. Loaded from disk so the tests exercise the
#: same path ``[ontologies] unit_extensions`` takes.
EXTENSION_TTL = Path(__file__).resolve().parents[1] / "test_unit_extension.ttl"


@pytest.fixture(scope="module")
def bundled() -> Graph:
    from acquirium._ontologies import bundled_dir

    g = Graph()
    g.parse(bundled_dir() / "qudt_unit.ttl", format="turtle")
    return g


@pytest.fixture(scope="module")
def extended(bundled: Graph) -> Graph:
    """The bundled graph merged with an extension, as unit_vocabulary does."""
    merged = Graph()
    for triple in bundled:
        merged.add(triple)
    ext = Graph()
    ext.parse(EXTENSION_TTL, format="turtle")
    for triple in ext:
        merged.add(triple)
    return merged


# --------------------------------------------------------------- config

def _write_config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "acquirium.toml"
    path.write_text(textwrap.dedent(body))
    return path


def test_unit_extensions_parsed_and_resolved_against_config_dir(tmp_path, monkeypatch):
    _write_config(tmp_path, """
        [ontologies]
        unit_extensions = ["./units.ttl"]
    """)
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(tmp_path / "acquirium.toml"))
    cfg = load_ontology_config()
    assert cfg.unit_extensions == (str(tmp_path / "units.ttl"),)
    assert cfg.sources == ()


def test_unit_extensions_accepts_a_bare_string(tmp_path, monkeypatch):
    _write_config(tmp_path, """
        [ontologies]
        unit_extensions = "./units.ttl"
    """)
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(tmp_path / "acquirium.toml"))
    assert load_ontology_config().unit_extensions == (str(tmp_path / "units.ttl"),)


def test_unit_extensions_absent_is_empty(tmp_path, monkeypatch):
    _write_config(tmp_path, """
        [ontologies]
        sources = ["./other.ttl"]
    """)
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(tmp_path / "acquirium.toml"))
    cfg = load_ontology_config()
    assert cfg.unit_extensions == ()
    assert len(cfg.sources) == 1


def test_unit_extensions_skips_non_string_entries(tmp_path, monkeypatch):
    _write_config(tmp_path, """
        [ontologies]
        unit_extensions = ["./units.ttl", 42]
    """)
    monkeypatch.setenv("ACQUIRIUM_CONFIG", str(tmp_path / "acquirium.toml"))
    assert load_ontology_config().unit_extensions == (str(tmp_path / "units.ttl"),)


# ------------------------------------------------- the gap being closed

@pytest.mark.parametrize("text", ["SCFM", "GPM", "Amps", "MGD"])
def test_abbreviations_do_not_resolve_without_the_extension(bundled, text):
    """Guards the premise: these are exactly what the extension has to fix."""
    converter = QUDTUnitConverter(bundled)
    with pytest.raises(UnitNotFound):
        converter.resolve_unit(text)
    with pytest.raises(UnitNotFound):
        converter.infer_unit(text)


# --------------------------------------------------------- 1. aliasing

@pytest.mark.parametrize("text,expected", [
    ("SCFM", UNIT["SCF-PER-MIN"]),
    ("GPM", UNIT["GAL_US-PER-MIN"]),
    ("Amps", UNIT["A"]),
])
def test_symbol_alias_resolves_to_the_existing_qudt_unit(extended, text, expected):
    assert QUDTUnitConverter(extended).resolve_unit(text).uri == expected


def test_aliasing_keeps_the_original_symbol_working(extended):
    """qudt:symbol is multi-valued; adding "Amps" must not displace "A"."""
    assert QUDTUnitConverter(extended).resolve_unit("A").uri == UNIT["A"]


def test_alias_must_use_symbol_not_altlabel(bundled):
    """skos:altLabel is invisible to the deterministic converter, which is
    why an extension must alias with qudt:symbol. The embedding matcher *does*
    read altLabel, so using it splits the two resolution paths: the same text
    resolves through one and not the other. Pinned so an extension cannot
    silently regress to it."""
    from rdflib import Literal
    from rdflib.namespace import SKOS

    g = Graph()
    for triple in bundled:
        g.add(triple)
    g.add((UNIT["A"], SKOS.altLabel, Literal("Amperage")))
    with pytest.raises(UnitNotFound):
        QUDTUnitConverter(g).resolve_unit("Amperage")


# ------------------------------------------------------ 2. custom units

def test_custom_unit_resolves(extended):
    assert QUDTUnitConverter(extended).resolve_unit("MGD").uri == AQUNIT.MGD


def test_custom_unit_uses_the_shared_acquirium_namespace(extended):
    """Not a per-deployment namespace, so two sites defining MGD agree."""
    assert str(QUDTUnitConverter(extended).resolve_unit("MGD").uri).startswith(
        "urn:acquirium:unit#"
    )


def test_custom_unit_converts_exactly(extended):
    """1 MGD is a million US gallons per day, by construction."""
    converted = QUDTUnitConverter(extended).convert(
        10, AQUNIT.MGD, UNIT["GAL_US-PER-DAY"]
    )
    assert converted == pytest.approx(10_000_000.0, rel=1e-12)


def test_custom_unit_round_trips(extended):
    converter = QUDTUnitConverter(extended)
    there = converter.convert(3.5, AQUNIT.MGD, UNIT["GAL_US-PER-MIN"])
    back = converter.convert(there, UNIT["GAL_US-PER-MIN"], AQUNIT.MGD)
    assert back == pytest.approx(3.5, rel=1e-12)


def test_custom_unit_is_compatible_with_other_volumetric_flows(extended):
    assert QUDTUnitConverter(extended).are_compatible(
        AQUNIT.MGD, UNIT["GAL_US-PER-MIN"]
    )


def test_custom_unit_is_incompatible_with_a_concentration(extended):
    assert not QUDTUnitConverter(extended).are_compatible(
        AQUNIT.MGD, UNIT["MilliGM-PER-L"]
    )


def test_custom_unit_carries_a_quantity_kind(extended):
    definition = QUDTUnitConverter(extended).resolve_unit("MGD")
    assert URIRef("http://qudt.org/vocab/quantitykind/VolumeFlowRate") in (
        definition.quantity_kinds
    )


# ------------------------------------------------------ the MG hazard

#: Units whose label/symbol/ucumCode equals "MG" case-insensitively. There is
#: no unit:MG, so resolve_unit("MG") falls all the way through to the
#: case-insensitive literal scan and returns whichever of these rdflib happens
#: to yield first — megagram and milligram differ by 10**9.
_MG_CANDIDATES = {UNIT["MegaGM"], UNIT["MilliG"], UNIT["MilliGM"]}


def test_mg_is_ambiguous_and_never_a_volume(extended):
    """Plant exports commonly abbreviate million-gallons as "MG", and QUDT
    already claims that string for mass. An extension should therefore leave
    it alone rather than alias it, and a mapping should leave such a tag's
    unit unset rather than guess.

    Asserted as set membership rather than one URI on purpose: the resolution
    is genuinely order-dependent (see _MG_CANDIDATES), so pinning a single
    answer would be pinning rdflib's iteration order. What matters, and what
    is stable, is that every answer is a mass unit and none is a volume."""
    resolved = QUDTUnitConverter(extended).resolve_unit("MG").uri
    assert resolved in _MG_CANDIDATES
    assert not QUDTUnitConverter(extended).are_compatible(resolved, AQUNIT.MegaGAL_US)


def test_mg_has_no_exact_index_surface(extended):
    """The other resolution path: the matcher has no exact "MG" surface, so it
    case-folds to milligram. Two paths, two different wrong answers."""
    surfaces: dict[str, list[str]] = {}
    for concept in QUDTStore.extract_concepts(extended, str(QUDT.Unit)):
        for surface in concept["surfaces"]:
            surfaces.setdefault(surface, []).append(concept["uri"])
    assert "MG" not in surfaces
    assert surfaces["mg"] == [str(UNIT["MilliGM"])]


# ------------------------------------------------------- index surfaces

def test_extension_units_are_indexed_for_text_matching(extended):
    concepts = QUDTStore.extract_concepts(extended, str(QUDT.Unit))
    surfaces = {c["uri"]: set(c["surfaces"]) for c in concepts}
    assert "MGD" in surfaces[str(AQUNIT.MGD)]
    assert "SCFM" in surfaces[str(UNIT["SCF-PER-MIN"])]


def test_extension_does_not_drop_bundled_units(bundled, extended):
    before = {c["uri"] for c in QUDTStore.extract_concepts(bundled, str(QUDT.Unit))}
    after = {c["uri"] for c in QUDTStore.extract_concepts(extended, str(QUDT.Unit))}
    assert before < after


# ---------------------------------------------------- loading and merging

def test_extension_file_is_loaded_from_disk():
    """The path `[ontologies] unit_extensions` takes."""
    graphs = load_unit_extensions([str(EXTENSION_TTL)])
    assert len(graphs) == 1 and len(graphs[0]) > 0


def test_an_unreadable_extension_is_skipped_not_raised(tmp_path, caplog):
    """A broken extension should cost its own units, not stop the server
    resolving units at all."""
    broken = tmp_path / "broken.ttl"
    broken.write_text("this is not turtle {{{")
    with caplog.at_level("ERROR", logger="acquirium.graph_store"):
        graphs = load_unit_extensions([str(broken), str(EXTENSION_TTL)])
    assert len(graphs) == 1                       # the good one still loaded
    assert any("failed to load extension" in r.message for r in caplog.records)


def test_no_extensions_loads_nothing():
    assert load_unit_extensions(None) == []
    assert load_unit_extensions([]) == []


def test_merge_adds_extension_units_without_dropping_bundled(bundled):
    merged = merge_unit_vocabulary(bundled, load_unit_extensions([str(EXTENSION_TTL)]))
    converter = QUDTUnitConverter(merged)
    assert converter.resolve_unit("MGD").uri == AQUNIT.MGD             # added
    assert converter.resolve_unit("SCFM").uri == UNIT["SCF-PER-MIN"]   # aliased
    assert converter.resolve_unit("mg/L").uri == UNIT["MilliGM-PER-L"] # bundled
    assert len(merged) > len(bundled)


# --------------------------------------- verdicts on under-described units

def test_an_undescribed_unit_reads_as_unknown_not_convertible(extended):
    """are_compatible answers True for this pair — it fails open whenever the
    two units never both carry the same tier of evidence. Converting on that
    basis rescales the series by the described side's multiplier."""
    converter = QUDTUnitConverter(extended)
    assert converter.are_compatible(AQUNIT.Undescribed, UNIT["GAL_US"]) is True
    assert converter.compatibility_verdict(AQUNIT.Undescribed, UNIT["GAL_US"]) == "unknown"


def test_two_described_units_of_different_dimension_are_incompatible(extended):
    converter = QUDTUnitConverter(extended)
    assert converter.compatibility_verdict(AQUNIT.MGD, AQUNIT.MegaGAL_US) == "incompatible"


def test_a_unit_matches_itself(extended):
    assert QUDTUnitConverter(extended).compatibility_verdict(
        AQUNIT.MGD, AQUNIT.MGD) == "match"
