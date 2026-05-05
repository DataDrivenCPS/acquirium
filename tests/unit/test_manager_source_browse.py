from __future__ import annotations

from datetime import datetime, timezone

from acquirium.Server.manager import Manager
from acquirium.internals.models import TimeseriesInfo


class _Graph:
    def sparql_query(self, query: str, use_union: bool = True) -> dict:
        return {
            "rows": [
                (
                    "urn:point:temp",
                    "urn:ref:temp",
                    "temp",
                    "Temperature",
                    "urn:acquirium#TimescaleDB",
                )
            ]
        }


def test_list_source_streams_uses_ref_uri_for_row_counts():
    mgr = Manager.__new__(Manager)
    mgr.graph_store = _Graph()
    calls: list[list[str]] = []

    def timeseries_info_batch(uris: list[str]) -> dict[str, TimeseriesInfo]:
        calls.append(uris)
        return {
            "urn:ref:temp": TimeseriesInfo(
                table="timeseries",
                row_count=42,
                earliest=datetime(2026, 1, 1, tzinfo=timezone.utc),
                latest=datetime(2026, 1, 2, tzinfo=timezone.utc),
            )
        }

    mgr.timeseries_info_batch = timeseries_info_batch

    streams = mgr.list_source_streams("demo")

    assert calls == [["urn:ref:temp"]]
    assert streams[0]["point_uri"] == "urn:point:temp"
    assert streams[0]["ref_uri"] == "urn:ref:temp"
    assert streams[0]["row_count"] == 42
