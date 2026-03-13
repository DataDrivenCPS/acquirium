"""
Tests for the server-side embedding text matcher (/resolve_text endpoint).

To add a new test pair, append a tuple to the relevant list:

  CLASS_PAIRS:     (input_text, expected_uri)        — classes / types
  PREDICATE_PAIRS: (input_text, expected_uri)        — predicates / properties
  NO_MATCH_PAIRS:  (input_text,)                     — should return nothing

The test graph is the WaterTAP simple-pipe model which contains classes like
Pump, InletConnectionPoint, OutletConnectionPoint, QuantifiableObservableProperty
and predicates like hasConnectionPoint, hasUnit, hasMedium, etc.
"""

import csv
import time
from datetime import datetime
from pathlib import Path

import pytest
from acquirium import Acquirium

# Output paths
_OUTPUT_DIR = Path(__file__).parent / "text_match_results"
_FAILURES_FILE = _OUTPUT_DIR / "failures.txt"
_STATS_FILE = _OUTPUT_DIR / "stats.csv"


# ──────────────────────────────────────────────────────────────
# Test pairs — edit these lists to add new cases
# ──────────────────────────────────────────────────────────────

# (natural language text, expected top-1 URI)
CLASS_PAIRS = [
    ("pump",                        "urn:nawi-water-ontology#Pump"),
    ("inlet connection point",      "http://data.ashrae.org/standard223#InletConnectionPoint"),
    ("outlet connection point",     "http://data.ashrae.org/standard223#OutletConnectionPoint"),
    ("observable property",         "http://data.ashrae.org/standard223#QuantifiableObservableProperty"),
    ('aeration basin',              "urn:nawi-water-ontology#AerationBasin"),

    # --- add new class pairs below this line ---

    # water.ttl — equipment
    ("boiler",                      "urn:nawi-water-ontology#Boiler"),
    ("compressor",                  "urn:nawi-water-ontology#Compressor"),
    ("condenser",                   "urn:nawi-water-ontology#Condenser"),
    ("valve",                       "urn:nawi-water-ontology#Valve"),
    ("filter",                      "urn:nawi-water-ontology#Filter"),
    ("evaporator",                  "urn:nawi-water-ontology#Evaporator"),
    ("screen",                      "urn:nawi-water-ontology#Screen"),
    ("reservoir",                   "urn:nawi-water-ontology#Reservoir"),
    ("crystallizer",                "urn:nawi-water-ontology#Crystallizer"),
    ("digester",                    "urn:nawi-water-ontology#Digester"),
    ("check valve",                 "urn:nawi-water-ontology#CheckValve"),
    ("dewatering unit",             "urn:nawi-water-ontology#DewateringUnit"),
    ("sedimentation tank",          "urn:nawi-water-ontology#SedimentationTank"),

    # water.ttl — sensors
    ("temperature sensor",          "urn:nawi-water-ontology#TemperatureSensor"),
    ("pressure sensor",             "urn:nawi-water-ontology#PressureSensor"),
    ("flow sensor",                 "urn:nawi-water-ontology#FlowSensor"),
    ("conductivity sensor",         "urn:nawi-water-ontology#ConductivitySensor"),
    ("pH sensor",                   "urn:nawi-water-ontology#pHSensor"),
    ("turbidity meter",             "urn:nawi-water-ontology#TurbidityMeter"),
    ("level sensor",                "urn:nawi-water-ontology#LevelSensor"),
    ("concentration sensor",        "urn:nawi-water-ontology#ConcentrationSensor"),

    # water.ttl — membrane / filtration
    ("reverse osmosis membrane",    "urn:nawi-water-ontology#ReverseOsmosisMembrane"),
    ("RO membrane",    "urn:nawi-water-ontology#ReverseOsmosisMembrane"),
    ("RO",    "urn:nawi-water-ontology#ReverseOsmosisMembrane"),
    ("nanofiltration unit",         "urn:nawi-water-ontology#NanofiltrationUnit"),
    ("ultrafiltration unit",        "urn:nawi-water-ontology#UltrafiltrationUnit"),
    ("UF unit",        "urn:nawi-water-ontology#UltrafiltrationUnit"),
    ("UF",        "urn:nawi-water-ontology#UltrafiltrationUnit"),
    ("microfiltration unit",        "urn:nawi-water-ontology#MicrofiltrationUnit"),
    ("membrane bioreactor",         "urn:nawi-water-ontology#MembraneBioreactor"),

    # water.ttl — basins & reactors
    ("chlorination basin",          "urn:nawi-water-ontology#ChlorinationBasin"),
    ("flocculation basin",          "urn:nawi-water-ontology#FlocculationBasin"),
    ("coagulation basin",           "urn:nawi-water-ontology#CoagulationBasin"),
    ("aerobic digester",            "urn:nawi-water-ontology#AerobicDigester"),
    ("anaerobic digester",          "urn:nawi-water-ontology#AnaerobicDigester"),
    ("oxidation ditch",             "urn:nawi-water-ontology#OxidationDitch"),
]

PREDICATE_PAIRS = [
    ("has unit",              "http://qudt.org/schema/qudt/hasUnit"),
    ("has quantity kind",     "http://qudt.org/schema/qudt/hasQuantityKind"),
    ("has medium",            "http://data.ashrae.org/standard223#hasMedium"),
    ("has connection point",  "http://data.ashrae.org/standard223#hasConnectionPoint"),
    ("has property",          "http://data.ashrae.org/standard223#hasProperty"),

    # --- add new predicate pairs below this line ---

    # water.ttl — sensor/data quality predicates
    ("has accuracy",              "urn:nawi-water-ontology#hasAccuracy"),
    ("has precision",             "urn:nawi-water-ontology#hasPrecision"),
    ("has bias",                  "urn:nawi-water-ontology#hasBias"),
    ("has response time",         "urn:nawi-water-ontology#hasResponseTime"),
    ("has drop rate",             "urn:nawi-water-ontology#hasDropRate"),
    ("has measurement range",     "urn:nawi-water-ontology#hasMeasurementRange"),
    ("has numeric resolution",    "urn:nawi-water-ontology#hasNumericResolution"),
    ("has temporal resolution",   "urn:nawi-water-ontology#hasTemporalResolution"),
    ("has process",               "urn:nawi-water-ontology#hasProcess"),
    ("has calibration curve",     "urn:nawi-water-ontology#hasCalibrationCurve"),
    ("has processed data",        "urn:nawi-water-ontology#hasProcessedData"),
]

# (natural language text, expected top-1 URI) — QUDT units
UNIT_PAIRS = [
    ("kilogram",                    "http://qudt.org/vocab/unit/KiloGM"),
    ("kg",                          "http://qudt.org/vocab/unit/KiloGM"),
    ("meter per second",            "http://qudt.org/vocab/unit/M-PER-SEC"),
    ("m/s",                         "http://qudt.org/vocab/unit/M-PER-SEC"),
    ("liter",                       "http://qudt.org/vocab/unit/L"),
    ("pascal",                      "http://qudt.org/vocab/unit/PA"),
    ("milligram per liter",         "http://qudt.org/vocab/unit/MilliGM-PER-L"),
    ("mg/L",                        "http://qudt.org/vocab/unit/MilliGM-PER-L"),
    ("degree celsius",              "http://qudt.org/vocab/unit/DEG_C"),
    ("watt",                        "http://qudt.org/vocab/unit/W"),
    ("bar",                         "http://qudt.org/vocab/unit/BAR"),
    ("cubic meter per second",      "http://qudt.org/vocab/unit/M3-PER-SEC"),
    ("gallon per minute",           "http://qudt.org/vocab/unit/GAL_US-PER-MIN"),
    ("ampere",                      "http://qudt.org/vocab/unit/A"),
    # --- add new unit pairs below this line ---
]

# (natural language text, expected top-1 URI) — QUDT quantity kinds
QUANTITY_KIND_PAIRS = [
    ("temperature",                 "http://qudt.org/vocab/quantitykind/Temperature"),
    ("pressure",                    "http://qudt.org/vocab/quantitykind/Pressure"),
    ("volume flow rate",            "http://qudt.org/vocab/quantitykind/VolumeFlowRate"),
    ("mass",                        "http://qudt.org/vocab/quantitykind/Mass"),
    ("electric current",            "http://qudt.org/vocab/quantitykind/ElectricCurrent"),
    ("density",                     "http://qudt.org/vocab/quantitykind/Density"),
    ("viscosity",                   "http://qudt.org/vocab/quantitykind/DynamicViscosity"),
    ("concentration",               "http://qudt.org/vocab/quantitykind/MassConcentration"),
    ("power",                       "http://qudt.org/vocab/quantitykind/Power"),
    # --- add new quantity kind pairs below this line ---
]

# These should return zero matches at min_score=0.6
NO_MATCH_PAIRS = [
    "xyznonexistent12345",
    "flurbomatic greeble",
    # --- add new no-match strings below this line ---
]


# ──────────────────────────────────────────────────────────────
# Result collector (populated during tests, written at teardown)
# ──────────────────────────────────────────────────────────────

_results = {
    "class_misses": [],
    "predicate_misses": [],
    "unit_misses": [],
    "qk_misses": [],
    "class_pct": 0.0,
    "predicate_pct": 0.0,
    "unit_pct": 0.0,
    "qk_pct": 0.0,
    "start_time": None,
}


def _write_outputs():
    """Write the failures file (overwritten) and append to stats CSV."""
    _OUTPUT_DIR.mkdir(exist_ok=True)
    elapsed = time.time() - _results["start_time"]

    # --- failures.txt (rewritten every run) ---
    with open(_FAILURES_FILE, "w") as f:
        f.write(f"Text-matcher failures — {datetime.now().isoformat()}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"CLASS misses ({len(_results['class_misses'])}):\n")
        for line in _results["class_misses"]:
            f.write(f"  {line}\n")
        f.write(f"\nPREDICATE misses ({len(_results['predicate_misses'])}):\n")
        for line in _results["predicate_misses"]:
            f.write(f"  {line}\n")
        f.write(f"\nUNIT misses ({len(_results['unit_misses'])}):\n")
        for line in _results["unit_misses"]:
            f.write(f"  {line}\n")
        f.write(f"\nQUANTITY_KIND misses ({len(_results['qk_misses'])}):\n")
        for line in _results["qk_misses"]:
            f.write(f"  {line}\n")

    # --- stats.csv (append; create with header if missing) ---
    write_header = not _STATS_FILE.exists()
    with open(_STATS_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["timestamp", "duration_s", "class_match_pct", "predicate_match_pct", "unit_match_pct", "qk_match_pct", "notes"])
        writer.writerow([
            datetime.now().isoformat(timespec="seconds"),
            f"{elapsed:.2f}",
            f"{_results['class_pct']:.1f}",
            f"{_results['predicate_pct']:.1f}",
            f"{_results['unit_pct']:.1f}",
            f"{_results['qk_pct']:.1f}",
            "",
        ])


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _wait_for_qudt_ready(client: Acquirium, timeout: int = 30) -> None:
    """Poll resolve_text until the QUDT matcher returns results for a known unit."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        matches = client.client.resolve_text("kilogram", kind="unit", top_k=1, min_score=0.4)
        if matches:
            return
        time.sleep(1)
    pytest.skip("QUDT embedding index not ready within timeout")


# ──────────────────────────────────────────────────────────────
# Fixture
# ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def acq():
    """Shared Acquirium client with the WaterTAP graph loaded once."""
    _results["start_time"] = time.time()

    client = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
    )
    client.insert_graph(
        "deployments/BENICIA/benicia-model-with-refs-1.ttl",
        replace = False
    )
    client.insert_graph(
        "ontologies/water.ttl",
        replace = False
    )
    # QUDT unit/qk ontologies are now loaded automatically by QUDTStore
    # on server startup — no need to insert them as graphs.
    # Poll until the QUDT matcher is ready (background indexing).
    _wait_for_qudt_ready(client, timeout=30)

    yield client

    _write_outputs()


# ──────────────────────────────────────────────────────────────
# Parametrized tests
# ──────────────────────────────────────────────────────────────

MIN_MATCH_PERCENT = 70  # require at least 70% of pairs to match correctly
MIN_SCORE = 0.6

def test_resolve_class(acq):
    """At least MIN_MATCH_PERCENT% of CLASS_PAIRS should resolve to the expected URI."""
    hits = 0
    misses = []
    for text, expected_uri in CLASS_PAIRS:
        matches = acq.client.resolve_text(text, kind="class", top_k=1, min_score=MIN_SCORE)
        if matches and matches[0]["uri"] == expected_uri:
            hits += 1
        else:
            got = matches[0]["uri"] if matches else "<no matches>"
            misses.append(f"'{text}': expected '{expected_uri}', got '{got}'")
    pct = (hits / len(CLASS_PAIRS)) * 100
    _results["class_pct"] = pct
    _results["class_misses"] = misses
    assert pct >= MIN_MATCH_PERCENT, (
        f"Class match rate {pct:.0f}% ({hits}/{len(CLASS_PAIRS)}) "
        f"is below {MIN_MATCH_PERCENT}%.\nMisses:\n" + "\n".join(misses)
    )


def test_resolve_predicate(acq):
    """At least MIN_MATCH_PERCENT% of PREDICATE_PAIRS should resolve to the expected URI."""
    hits = 0
    misses = []
    for text, expected_uri in PREDICATE_PAIRS:
        matches = acq.client.resolve_text(text, kind="predicate", top_k=1, min_score=MIN_SCORE)
        if matches and matches[0]["uri"] == expected_uri:
            hits += 1
        else:
            got = matches[0]["uri"] if matches else "<no matches>"
            misses.append(f"'{text}': expected '{expected_uri}', got '{got}'")
    pct = (hits / len(PREDICATE_PAIRS)) * 100
    _results["predicate_pct"] = pct
    _results["predicate_misses"] = misses
    assert pct >= MIN_MATCH_PERCENT, (
        f"Predicate match rate {pct:.0f}% ({hits}/{len(PREDICATE_PAIRS)}) "
        f"is below {MIN_MATCH_PERCENT}%.\nMisses:\n" + "\n".join(misses)
    )


@pytest.mark.parametrize("text", NO_MATCH_PAIRS)
def test_resolve_no_match(acq, text):
    matches = acq.client.resolve_text(text, min_score=0.9)
    assert len(matches) == 0, f"Expected no matches for '{text}' but got {len(matches)}"


# ──────────────────────────────────────────────────────────────
# Structural tests
# ──────────────────────────────────────────────────────────────

def test_response_fields(acq):
    """Every match dict has the required keys with correct types."""
    matches = acq.client.resolve_text("pump", top_k=3)
    assert len(matches) >= 1
    for m in matches:
        assert isinstance(m["uri"], str)
        assert isinstance(m["kind"], str)
        assert isinstance(m["label"], str)
        assert isinstance(m["score"], float)
        assert isinstance(m["matched_surface"], str)


def test_kind_filtering(acq):
    """kind=class never returns predicates and vice versa."""
    for m in acq.client.resolve_text("has", kind="class", top_k=5, min_score=0.3):
        assert m["kind"] == "class"
    for m in acq.client.resolve_text("pump", kind="predicate", top_k=5, min_score=0.3):
        assert m["kind"] == "predicate"


def test_top_k_respected(acq):
    """Returned list never exceeds top_k."""
    for k in (1, 2, 3):
        matches = acq.client.resolve_text("connection", top_k=k, min_score=0.3)
        assert len(matches) <= k


def test_scores_descending(acq):
    """Matches are sorted by score, highest first."""
    matches = acq.client.resolve_text("connection point", top_k=5, min_score=0.3)
    scores = [m["score"] for m in matches]
    assert scores == sorted(scores, reverse=True)


# ──────────────────────────────────────────────────────────────
# QUDT unit / quantity kind tests
# ──────────────────────────────────────────────────────────────

def test_resolve_unit(acq):
    """At least MIN_MATCH_PERCENT% of UNIT_PAIRS should resolve to the expected URI."""
    hits = 0
    misses = []
    for text, expected_uri in UNIT_PAIRS:
        matches = acq.client.resolve_text(text, kind="unit", top_k=1, min_score=MIN_SCORE)
        if matches and matches[0]["uri"] == expected_uri:
            hits += 1
        else:
            got = matches[0]["uri"] if matches else "<no matches>"
            misses.append(f"'{text}': expected '{expected_uri}', got '{got}'")
    pct = (hits / len(UNIT_PAIRS)) * 100
    _results["unit_pct"] = pct
    _results["unit_misses"] = misses
    assert pct >= MIN_MATCH_PERCENT, (
        f"Unit match rate {pct:.0f}% ({hits}/{len(UNIT_PAIRS)}) "
        f"is below {MIN_MATCH_PERCENT}%.\nMisses:\n" + "\n".join(misses)
    )


def test_resolve_quantity_kind(acq):
    """At least MIN_MATCH_PERCENT% of QUANTITY_KIND_PAIRS should resolve to the expected URI."""
    hits = 0
    misses = []
    for text, expected_uri in QUANTITY_KIND_PAIRS:
        matches = acq.client.resolve_text(text, kind="quantity_kind", top_k=1, min_score=MIN_SCORE)
        if matches and matches[0]["uri"] == expected_uri:
            hits += 1
        else:
            got = matches[0]["uri"] if matches else "<no matches>"
            misses.append(f"'{text}': expected '{expected_uri}', got '{got}'")
    pct = (hits / len(QUANTITY_KIND_PAIRS)) * 100
    _results["qk_pct"] = pct
    _results["qk_misses"] = misses
    assert pct >= MIN_MATCH_PERCENT, (
        f"QuantityKind match rate {pct:.0f}% ({hits}/{len(QUANTITY_KIND_PAIRS)}) "
        f"is below {MIN_MATCH_PERCENT}%.\nMisses:\n" + "\n".join(misses)
    )


def test_kind_filtering_unit_qk(acq):
    """kind=unit never returns classes/predicates/QKs and vice versa."""
    for m in acq.client.resolve_text("kilogram", kind="unit", top_k=5, min_score=0.3):
        assert m["kind"] == "unit", f"Expected kind='unit', got '{m['kind']}' for uri={m['uri']}"
    for m in acq.client.resolve_text("temperature", kind="quantity_kind", top_k=5, min_score=0.3):
        assert m["kind"] == "quantity_kind", f"Expected kind='quantity_kind', got '{m['kind']}' for uri={m['uri']}"
    # unit filter should not return classes
    for m in acq.client.resolve_text("pump", kind="unit", top_k=5, min_score=0.3):
        assert m["kind"] == "unit", f"Expected kind='unit', got '{m['kind']}' for uri={m['uri']}"
    # quantity_kind filter should not return predicates
    for m in acq.client.resolve_text("has unit", kind="quantity_kind", top_k=5, min_score=0.3):
        assert m["kind"] == "quantity_kind", f"Expected kind='quantity_kind', got '{m['kind']}' for uri={m['uri']}"


def test_no_cross_contamination(acq):
    """Class/predicate resolution is unchanged after QUDT corpus is loaded."""
    # A known class should still resolve correctly via kind=class
    matches = acq.client.resolve_text("pump", kind="class", top_k=1, min_score=MIN_SCORE)
    assert matches, "Expected 'pump' to resolve to a class"
    assert matches[0]["uri"] == "urn:nawi-water-ontology#Pump"
    assert matches[0]["kind"] == "class"

    # A known predicate should still resolve correctly
    matches = acq.client.resolve_text("has unit", kind="predicate", top_k=1, min_score=MIN_SCORE)
    assert matches, "Expected 'has unit' to resolve to a predicate"
    assert matches[0]["kind"] == "predicate"
