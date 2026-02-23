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

import pytest
from acquirium import Acquirium


# ──────────────────────────────────────────────────────────────
# Test pairs — edit these lists to add new cases
# ──────────────────────────────────────────────────────────────

# (natural language text, expected top-1 URI)
CLASS_PAIRS = [
    ("pump",                        "urn:nawi-water-ontology#Pump"),
    ("inlet connection point",      "http://data.ashrae.org/standard223#InletConnectionPoint"),
    ("outlet connection point",     "http://data.ashrae.org/standard223#OutletConnectionPoint"),
    ("observable property",         "http://data.ashrae.org/standard223#QuantifiableObservableProperty"),
    # --- add new class pairs below this line ---
]

PREDICATE_PAIRS = [
    ("has unit",              "http://qudt.org/schema/qudt/hasUnit"),
    ("has quantity kind",     "http://qudt.org/schema/qudt/hasQuantityKind"),
    ("has medium",            "http://data.ashrae.org/standard223#hasMedium"),
    ("has connection point",  "http://data.ashrae.org/standard223#hasConnectionPoint"),
    ("has property",          "http://data.ashrae.org/standard223#hasProperty"),
    # --- add new predicate pairs below this line ---
]

# These should return zero matches at min_score=0.9
NO_MATCH_PAIRS = [
    "xyznonexistent12345",
    "flurbomatic greeble",
    # --- add new no-match strings below this line ---
]


# ──────────────────────────────────────────────────────────────
# Fixture
# ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def acq():
    """Shared Acquirium client with the WaterTAP graph loaded once."""
    client = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
    )
    client.insert_graph(
        "deployments/WATERTAP/models/watertap-simple-pipe-model-with-ext-refs.ttl"
    )
    return client


# ──────────────────────────────────────────────────────────────
# Parametrized tests
# ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("text, expected_uri", CLASS_PAIRS, ids=[t for t, _ in CLASS_PAIRS])
def test_resolve_class(acq, text, expected_uri):
    matches = acq.client.resolve_text(text, kind="class", top_k=1, min_score=0.4)
    assert len(matches) >= 1, f"No matches for '{text}'"
    assert matches[0]["uri"] == expected_uri, (
        f"Expected '{expected_uri}' but got '{matches[0]['uri']}' "
        f"(score={matches[0]['score']:.3f}, surface='{matches[0]['matched_surface']}')"
    )


@pytest.mark.parametrize("text, expected_uri", PREDICATE_PAIRS, ids=[t for t, _ in PREDICATE_PAIRS])
def test_resolve_predicate(acq, text, expected_uri):
    matches = acq.client.resolve_text(text, kind="predicate", top_k=1, min_score=0.4)
    assert len(matches) >= 1, f"No matches for '{text}'"
    assert matches[0]["uri"] == expected_uri, (
        f"Expected '{expected_uri}' but got '{matches[0]['uri']}' "
        f"(score={matches[0]['score']:.3f}, surface='{matches[0]['matched_surface']}')"
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
