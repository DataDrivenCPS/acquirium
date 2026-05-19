"""No-Docker tests for Acquirium.resolve_point_metadata.

Stubs the HTTP client; asserts the fixed semantic-field -> kind mapping
and the passthrough/None contract without a server.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from acquirium.Client.acquirium import Acquirium


def _aq(resolve_record_uris):
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.client.resolve_record_uris.side_effect = resolve_record_uris
    return aq


def test_field_name_is_the_kind():
    captured = {}

    def fake(fields, min_score=0.5):
        captured["fields"] = fields
        captured["min_score"] = min_score
        return {k: f"urn:{k}" for k in fields}

    aq = _aq(fake)
    out = aq.resolve_point_metadata({
        "unit": "kg",
        "quantity_kind": "mass",
        "medium": "water",
        "substance": "nitrate",
        "custom": "whatever",          # unknown field -> kind None (any)
    })
    assert captured["fields"] == {
        "unit": ("kg", "unit"),
        "quantity_kind": ("mass", "quantity_kind"),
        "medium": ("water", "class"),
        "substance": ("nitrate", "class"),
        "custom": ("whatever", None),
    }
    assert captured["min_score"] == 0.6  # point-metadata default
    assert out["unit"] == "urn:unit"


def test_passthrough_and_none_delegated():
    # resolve_record_uris owns URI/URIRef/None passthrough; just verify
    # resolve_point_metadata returns whatever it produced, keyed by field.
    aq = _aq(lambda fields, min_score=0.5: {
        "unit": "http://qudt.org/vocab/unit/W",   # passthrough
        "quantity_kind": None,                    # unresolved
    })
    out = aq.resolve_point_metadata(
        {"unit": "http://qudt.org/vocab/unit/W", "quantity_kind": "??"}
    )
    assert out == {"unit": "http://qudt.org/vocab/unit/W",
                   "quantity_kind": None}


def test_client_failure_degrades_to_none():
    def boom(fields, min_score=0.5):
        raise RuntimeError("server down")

    aq = _aq(boom)
    out = aq.resolve_point_metadata({"unit": "kg", "medium": "water"})
    assert out == {"unit": None, "medium": None}
