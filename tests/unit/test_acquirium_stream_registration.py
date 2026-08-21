"""What ``Acquirium.register_streams`` sends to the server.

Triple building moved server-side (see ``test_stream_graph.py``); the client's
job is now to normalise the batch and post it, so that is what is asserted
here.
"""
from __future__ import annotations

import warnings
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from rdflib import Literal, URIRef

from acquirium.Client.acquirium import Acquirium

_TEST_PROP = URIRef("urn:test:prop:fileLocation")


def _aq() -> Acquirium:
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.client.register_streams.return_value = {
        "ok": True, "registered": 0, "warnings": [],
    }
    return aq


def _posted(aq: Acquirium) -> list[dict]:
    aq.client.register_streams.assert_called_once()
    return aq.client.register_streams.call_args.args[0]


def test_register_streams_posts_one_batch():
    aq = _aq()
    aq.register_streams([
        {"source_id": "demo-source", "ref_name": "temp", "value_kind": "numeric"},
        {"source_id": "demo-source", "ref_name": "rh", "value_kind": "numeric"},
    ])
    assert [s["ref_name"] for s in _posted(aq)] == ["temp", "rh"]


def test_register_streams_batches_across_sources():
    """One request, not one per source — reconciliation is batch-scoped."""
    aq = _aq()
    aq.register_streams([
        {"source_id": "a", "ref_name": "x"},
        {"source_id": "b", "ref_name": "y"},
    ])
    assert {s["source_id"] for s in _posted(aq)} == {"a", "b"}


def test_free_text_semantics_are_sent_unresolved():
    """The whole point of the endpoint: the client does not resolve."""
    aq = _aq()
    aq.register_streams([{
        "source_id": "demo-source", "ref_name": "flow",
        "unit": "gal/min", "quantity_kind": "volume flow rate",
        "medium": "water", "substance": "chlorine",
    }])
    spec = _posted(aq)[0]
    assert spec["unit"] == "gal/min"
    assert spec["quantity_kind"] == "volume flow rate"
    assert spec["medium"] == "water"
    assert spec["substance"] == "chlorine"
    aq.client.resolve.assert_not_called()


def test_uriref_values_are_stringified_for_json():
    aq = _aq()
    aq.register_streams([{
        "source_id": "demo-source", "ref_name": "temp",
        "unit": URIRef("http://qudt.org/vocab/unit/DEG_C"),
        "point_uri": URIRef("urn:test:point:temp"),
    }])
    spec = _posted(aq)[0]
    assert spec["unit"] == "http://qudt.org/vocab/unit/DEG_C"
    assert spec["point_uri"] == "urn:test:point:temp"
    assert isinstance(spec["unit"], str)


def test_property_keys_and_values_are_json_safe():
    aq = _aq()
    aq.register_streams([{
        "source_id": "demo-source", "ref_name": "temp",
        "properties": {_TEST_PROP: Literal("demo.csv")},
    }])
    assert _posted(aq)[0]["properties"] == {str(_TEST_PROP): "demo.csv"}


def test_allow_unit_mismatch_is_forwarded():
    aq = _aq()
    aq.register_streams([{
        "source_id": "demo-source", "ref_name": "temp",
        "unit": "mg/L", "point_uri": "urn:test:point:temp",
        "allow_unit_mismatch": True,
    }])
    assert _posted(aq)[0]["allow_unit_mismatch"] is True


def test_caller_dicts_are_not_mutated():
    aq = _aq()
    original = {
        "source_id": "demo-source", "ref_name": "temp",
        "unit": URIRef("http://qudt.org/vocab/unit/DEG_C"),
    }
    aq.register_streams([original])
    assert original["unit"] == URIRef("http://qudt.org/vocab/unit/DEG_C")


def test_empty_batch_makes_no_request():
    aq = _aq()
    assert aq.register_streams([]) == {"ok": True, "registered": 0, "warnings": []}
    aq.client.register_streams.assert_not_called()


def test_missing_source_id_raises_before_any_request():
    aq = _aq()
    with pytest.raises(ValueError, match="non-empty source_id"):
        aq.register_streams([{"ref_name": "temp"}])
    aq.client.register_streams.assert_not_called()


def test_server_warnings_are_surfaced_to_the_caller():
    """A reconciliation that was allowed rather than refused must not be silent."""
    aq = _aq()
    aq.client.register_streams.return_value = {
        "ok": True, "registered": 1,
        "warnings": ["stream ('s', 'x'): unit ... registered anyway"],
    }
    with pytest.warns(UserWarning, match="registered anyway"):
        aq.register_streams([{"source_id": "s", "ref_name": "x"}])


def test_no_warning_when_the_server_reports_none():
    aq = _aq()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        aq.register_streams([{"source_id": "s", "ref_name": "x"}])


def test_insert_timeseries_batch_chunks_at_acquirium_facade():
    aq = Acquirium.__new__(Acquirium)
    aq.client = MagicMock()
    aq.insert_batch_rows = 3
    aq.client.insert_timeseries_batch.side_effect = lambda source_id, streams: {
        "ok": True,
        "rows_inserted": sum(len(rows) for rows in streams.values()),
    }

    ts = datetime(2026, 4, 28, tzinfo=timezone.utc)
    result = aq.insert_timeseries_batch(
        "source-a",
        {
            "temp": [(ts, 1.0), (ts, 2.0)],
            "rh": [(ts, 3.0), (ts, 4.0)],
        },
    )

    assert result == {"ok": True, "rows_inserted": 4, "batches": 2}
    assert aq.client.insert_timeseries_batch.call_count == 2
    first_source, first_chunk = aq.client.insert_timeseries_batch.call_args_list[0].args
    second_source, second_chunk = aq.client.insert_timeseries_batch.call_args_list[1].args
    assert first_source == second_source == "source-a"
    assert sum(len(rows) for rows in first_chunk.values()) == 3
    assert sum(len(rows) for rows in second_chunk.values()) == 1
    assert set(first_chunk) == {"temp", "rh"}
    assert set(second_chunk) == {"rh"}
