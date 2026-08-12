"""Tests for the vectorized value-column split in acquirium.Storage.values."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import polars.testing
import pytest

from acquirium.Storage.values import (
    _prepare_value_columns_rowwise,
    prepare_value_columns,
    typed_value_series,
)


def _ts(n: int) -> list[datetime]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return [base + timedelta(seconds=i) for i in range(n)]


def _frame(values: pl.Series, kinds: list[str | None]) -> pl.DataFrame:
    n = len(values)
    return pl.DataFrame(
        {
            "ref_uri": pl.Series("ref_uri", [f"urn:v:{i % 3}" for i in range(n)], dtype=pl.Utf8),
            "ts": pl.Series("ts", _ts(n), dtype=pl.Datetime("us", "UTC")),
            "value": values,
            "value_kind": pl.Series("value_kind", kinds, dtype=pl.Utf8),
        }
    )


def _assert_matches_rowwise(values: list, kinds: list[str | None]) -> None:
    """The vectorized split must agree with split_value row for row."""
    vectorized = prepare_value_columns(_frame(typed_value_series(values), kinds))
    reference = _prepare_value_columns_rowwise(
        _frame(pl.Series("value", values, dtype=pl.Object), kinds)
    )
    pl.testing.assert_frame_equal(vectorized, reference)


@pytest.mark.unit
def test_string_values_match_rowwise():
    values = ["1.5", "1e3", " 2.5 ", "", "   ", "abc", "inf", "-Inf", "NaN", "state", None, "-0.25"]
    _assert_matches_rowwise(values, ["numeric"] * len(values))
    _assert_matches_rowwise(values, ["text"] * len(values))
    _assert_matches_rowwise(values, ["numeric", "text"] * (len(values) // 2))


@pytest.mark.unit
def test_float_values_match_rowwise():
    values = [1.5, float("inf"), float("nan"), -2.0, None, 0.0]
    _assert_matches_rowwise(values, ["numeric"] * len(values))
    _assert_matches_rowwise(values, ["text"] * len(values))


@pytest.mark.unit
def test_int_values_match_rowwise():
    _assert_matches_rowwise([1, -7, None, 0], ["numeric", "numeric", "text", "text"])


@pytest.mark.unit
def test_bool_values_match_rowwise():
    _assert_matches_rowwise([True, False, None], ["numeric", "text", "numeric"])


@pytest.mark.unit
def test_all_none_values_match_rowwise():
    _assert_matches_rowwise([None, None], ["numeric", "text"])


@pytest.mark.unit
def test_mixed_scalar_batch_matches_rowwise():
    # One manager batch spanning numeric and text streams.
    values = [1.5, "abc", True, None, "2.5", 7]
    kinds = ["numeric", "text", "numeric", "text", "numeric", "numeric"]
    _assert_matches_rowwise(values, kinds)


@pytest.mark.unit
def test_randomized_batches_match_rowwise():
    """Seeded fuzz over value/kind combinations, vectorized vs row-wise.

    The pool omits the two documented divergences, which have their own tests:
    underscore numerals ("1_000") and floats whose repr uses exponent notation.
    """
    import random

    pool = [
        1.5, -2.0, 0.0, float("inf"), float("-inf"), float("nan"), 12345678.9,
        1, 0, -7, 10**20, True, False, None,
        "1.5", "1e3", "  2.5  ", "", "   ", "abc", "inf", "-Inf", "nan", "NaN",
        "0x10", "1,5", "+3", "-0", "state5", "1.5.2", "None", "true",
    ]
    kinds_pool = ["numeric", "text", None, "NUMERIC", " text ", "int", "status"]
    rng = random.Random(20260812)

    for _ in range(400):
        n = rng.randint(1, 12)
        if rng.random() < 0.5:  # homogeneous batch, the realistic shape
            t = rng.choice([float, int, str, bool, type(None)])
            candidates = [v for v in pool if type(v) is t] or [None]
            values = [rng.choice(candidates) for _ in range(n)]
        else:
            values = [rng.choice(pool) for _ in range(n)]
        _assert_matches_rowwise(values, [rng.choice(kinds_pool) for _ in range(n)])


@pytest.mark.unit
def test_typed_value_series_dtypes():
    assert typed_value_series([1.5, None, -2.0]).dtype == pl.Float64
    assert typed_value_series([1, None, 2]).dtype == pl.Int64
    assert typed_value_series(["a", None]).dtype == pl.String
    assert typed_value_series([True, False]).dtype == pl.Boolean
    assert typed_value_series([]).dtype == pl.String
    assert typed_value_series([None, None]).dtype == pl.String
    # Exact type scan: a bool among ints must NOT collapse to Int64 (True -> 1).
    s = typed_value_series([1, 2, True])
    assert s.dtype == pl.String
    assert s.to_list() == ["1", "2", "True"]
    # Mixed int/float stringifies so ints keep str() spelling ("2", not "2.0").
    s = typed_value_series([1.5, None, 2])
    assert s.dtype == pl.String
    assert s.to_list() == ["1.5", None, "2"]
    # int beyond float range: Object, so the row-wise path keeps text semantics.
    assert typed_value_series([10**400]).dtype == pl.Object
    # Exotic types stay Object.
    assert typed_value_series([datetime(2024, 1, 1)]).dtype == pl.Object


@pytest.mark.unit
def test_huge_int_in_homogeneous_batch_stored_as_text():
    out = prepare_value_columns(
        _frame(typed_value_series([10**400]), ["numeric"])
    )
    assert out["numeric_value"].to_list() == [None]
    assert out["text_value"].to_list() == [str(10**400)]


@pytest.mark.unit
def test_underscore_numeral_diverges_to_text():
    # Documented divergence: Python float() accepts "1_000"; the cast does not.
    out = prepare_value_columns(_frame(typed_value_series(["1_000"]), ["numeric"]))
    assert out["numeric_value"].to_list() == [None]
    assert out["text_value"].to_list() == ["1_000"]


@pytest.mark.unit
def test_fallback_warning_logged(caplog):
    import logging

    values = ["ok", "broken", "1.5"]
    with caplog.at_level(logging.WARNING, logger="acquirium.Storage.values"):
        prepare_value_columns(_frame(typed_value_series(values), ["numeric"] * 3))
    assert any("unparseable numeric value" in r.message for r in caplog.records)


@pytest.mark.unit
def test_no_value_kind_column_defaults_to_text():
    df = pl.DataFrame(
        {
            "ref_uri": pl.Series(["urn:v:0"], dtype=pl.Utf8),
            "ts": pl.Series(_ts(1), dtype=pl.Datetime("us", "UTC")),
            "value": typed_value_series([1.5]),
        }
    )
    out = prepare_value_columns(df)
    assert out["numeric_value"].to_list() == [None]
    assert out["text_value"].to_list() == ["1.5"]


@pytest.mark.unit
def test_unknown_value_kind_raises():
    with pytest.raises(ValueError):
        prepare_value_columns(_frame(typed_value_series(["x"]), ["bogus"]))


@pytest.mark.unit
def test_missing_value_column_raises():
    df = pl.DataFrame({"ref_uri": ["a"], "ts": _ts(1)})
    with pytest.raises(ValueError):
        prepare_value_columns(df)


@pytest.mark.unit
def test_presplit_frame_passes_through():
    df = pl.DataFrame(
        {
            "ref_uri": ["a"],
            "ts": _ts(1),
            "numeric_value": [1.0],
            "text_value": [None],
            "extra": ["dropped"],
        }
    )
    out = prepare_value_columns(df)
    assert out.columns == ["ref_uri", "ts", "numeric_value", "text_value"]
