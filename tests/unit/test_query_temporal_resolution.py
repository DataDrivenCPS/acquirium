"""A semantic query over a time range resolves once per version of the model inside that range.

Versions v1 (before 2025), v2 [2025, 2026), v3 [2026, ...). Streams A, B, C hold data over 2022-2027.
A query returns B, C on v1; A on v2; A, B on v3.
"""
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from acquirium.Client.explore.core import Query

CLS = "urn:t#Tank"
Y = lambda year: datetime(year, 1, 1, tzinfo=timezone.utc)
POINTS = [Y(2024), Y(2025), Y(2026)]
MEMBERS = {None: ["B", "C"], Y(2024): ["B", "C"], Y(2025): ["A"], Y(2026): ["A", "B"]}


class VersionedClient:
    def __init__(self, members=MEMBERS, points=POINTS):
        self.members, self.points, self.as_of_seen, self.fetches = members, points, [], []

    def graph_change_points(self, start=None, end=None):
        return [p for p in self.points if (start is None or p > start) and (end is None or p < end)]

    def _version(self, as_of):
        return max((p for p in self.points if as_of is not None and p <= as_of), default=None)

    def sparql_query(self, sparql, include_dependencies=True, *, as_of=None, **kwargs):
        self.as_of_seen.append(as_of)
        key = self._version(as_of) if as_of is not None else self.points[-1]
        return {"columns": ["v0", "v1", "ext1"], "rows": [[f"urn:t#tank{m}", f"urn:t#{m}", f"urn:ref#{m}"] for m in self.members[key]]}

    def timeseries_info_batch(self, uris):
        return {}

    def timeseries_df(self, ref_uri, start=None, end=None, **kwargs):
        self.fetches.append((ref_uri.rsplit("#", 1)[1], start, end))
        as_dt = lambda v, default: default if v is None else (v if isinstance(v, datetime) else datetime.fromisoformat(v))
        lo, hi = as_dt(start, Y(2022)), as_dt(end, Y(2027))
        times = [Y(y) for y in range(2022, 2028) if lo <= Y(y) <= hi]   # the server's end is inclusive
        return pl.DataFrame({"ts": pl.Series(times, dtype=pl.Datetime("us", "UTC")), "value": [1.0] * len(times), "uri": [ref_uri] * len(times)})


def _years(data):
    data._materialize()
    return sorted((r["ref_uri"].rsplit("#", 1)[1], r["time"].year) for r in data._tall.iter_rows(named=True))


def _query(client):
    return Query(client=client).entity(CLS, alias="tank").measurement(alias="m")


def test_q1_2025_to_2027_returns_a_throughout_and_b_only_in_the_last_version():
    client = VersionedClient()
    df = _query(client).data(start=Y(2025), end=Y(2027))
    assert _years(df) == [("A", 2025), ("A", 2026), ("A", 2027), ("B", 2026), ("B", 2027)]
    assert client.as_of_seen == [Y(2025), None]           # v2 at the window's start, then the live model


def test_q2_2022_to_2026_uses_the_first_version_before_the_log_and_never_the_live_one():
    client = VersionedClient()
    df = _query(client).data(start=Y(2022), end=Y(2026))
    # v1 covers 2022-2025 (A is not a member), v2 covers 2025-2026 (the window's inclusive end is still v2's).
    assert _years(df) == sorted([("B", 2022), ("B", 2023), ("B", 2024), ("C", 2022), ("C", 2023), ("C", 2024), ("A", 2025), ("A", 2026)])
    assert client.as_of_seen == [Y(2022), Y(2024), Y(2025)]   # never the live model: the window ends before the last change


def test_a_window_without_a_change_point_is_one_live_resolution():
    client = VersionedClient()
    df = _query(client).data(start=Y(2026), end=Y(2027))
    assert client.as_of_seen == [None]
    assert {r for r, _ in _years(df)} == {"A", "B"}
    assert client.fetches == [("A", Y(2026), Y(2027)), ("B", Y(2026), Y(2027))]   # the window's own bounds, as given


def test_no_window_resolves_from_before_the_first_change_to_now():
    client = VersionedClient()
    df = _query(client).data()
    assert client.as_of_seen[0] < Y(2024) and client.as_of_seen[1:] == [Y(2024), Y(2025), None]
    assert _years(df) == sorted([("B", 2022), ("B", 2023), ("B", 2024), ("C", 2022), ("C", 2023), ("C", 2024),
                                 ("A", 2025), ("A", 2026), ("A", 2027), ("B", 2026), ("B", 2027)])


def test_a_client_without_change_points_behaves_as_before():
    client = VersionedClient()
    del VersionedClient.graph_change_points
    try:
        df = _query(client).data(start=Y(2022))
        assert client.as_of_seen == [None] and {r for r, _ in _years(df)} == {"A", "B"}
    finally:
        VersionedClient.graph_change_points = lambda self, start=None, end=None: [p for p in self.points if (start is None or p > start) and (end is None or p < end)]
