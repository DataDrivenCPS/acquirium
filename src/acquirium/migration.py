"""Copy registered raw readings from a legacy server into Escapement."""

from collections.abc import Callable, Iterator
from contextlib import closing

import polars as pl
from requests import HTTPError

from acquirium.Client.client import AcquiriumClient
from acquirium.Storage.graph_registry import DERIVED_SOURCE_PREFIX
from acquirium.internals.models import compute_stream_uri


_STREAMS = """
PREFIX acq: <urn:acquirium#>
PREFIX ref: <https://brickschema.org/schema/Brick/ref#>
PREFIX qudt: <http://qudt.org/schema/qudt/>
SELECT DISTINCT ?uri ?source_id ?stream_name ?property_uri ?value_kind ?unit ?quantity_kind
WHERE {
    ?uri acq:sourceId ?source_id ; acq:refName ?stream_name .
    OPTIONAL { ?uri acq:valueKind ?value_kind . }
    OPTIONAL { ?uri qudt:hasUnit ?ref_unit . }
    OPTIONAL {
        ?property_uri ref:hasExternalReference ?uri .
        OPTIONAL { ?property_uri qudt:hasUnit ?point_unit . }
        OPTIONAL { ?property_uri qudt:hasQuantityKind ?quantity_kind . }
    }
    BIND(COALESCE(?ref_unit, ?point_unit) AS ?unit)
}
ORDER BY ?uri
"""
_VALUE_MODES = ("numeric", "text")


def _catalog(client: AcquiriumClient, *, legacy: bool = False) -> dict[str, dict[str, str]]:
    result = client.sparql_query(_STREAMS, include_dependencies=False, wait_for_fresh=True)
    streams: dict[str, dict[str, str]] = {}
    for row in result["rows"]:
        registration = {key: value for key, value in zip(result["columns"], row) if value is not None}
        uri = registration.pop("uri")
        source, name = registration["source_id"], registration["stream_name"]
        if legacy and source.startswith((DERIVED_SOURCE_PREFIX, "experiment/")):
            continue
        if uri != str(compute_stream_uri(source, name)):
            raise ValueError(f"Stream {source}/{name} has a noncanonical URI: {uri}")
        if registration.get("property_uri") == f"{uri}__point":
            del registration["property_uri"]
        if uri in streams and streams[uri] != registration:
            raise ValueError(f"Stream {source}/{name} has conflicting registration metadata")
        streams[uri] = registration
    return streams


def _batches(client: AcquiriumClient, uri: str, mode: str) -> Iterator[pl.DataFrame]:
    with closing(client.timeseries_batches(uri, value_mode=mode, view="stored")) as batches:
        for batch in batches:
            # Legacy Arrow timestamps are UTC microseconds. Compare and write
            # both sides at the destination's nanosecond precision.
            yield batch.select(pl.col("ts").cast(pl.Datetime("ns", "UTC")), "value")


def _rows(client: AcquiriumClient, uri: str, mode: str) -> Iterator[tuple]:
    with closing(_batches(client, uri, mode)) as batches:
        for batch in batches:
            yield from batch.select(pl.col("ts").cast(pl.Int64), "value").iter_rows()


def _matching_prefix(source: AcquiriumClient, target: AcquiriumClient, uri: str, mode: str) -> int:
    """Count already copied rows, rejecting a different value or an extra timestamp."""
    with closing(_rows(source, uri, mode)) as expected, closing(_rows(target, uri, mode)) as observed:
        count = 0
        for actual in observed:
            if actual != next(expected, None):
                raise ValueError(f"Destination readings differ from the legacy {mode} readings for {uri}")
            count += 1
        return count


def migrate_readings(
    source: AcquiriumClient,
    target: AcquiriumClient,
    *,
    dry_run: bool,
    report: Callable[[str], None],
) -> None:
    """Plan or run the migration using the server APIs. The source is read only."""
    if source.address == target.address:
        raise ValueError("Source and destination must be different servers")

    source.health()
    try:
        source.engine_status()
    except HTTPError as error:
        if error.response is None or error.response.status_code != 404:
            raise
    else:
        raise ValueError("The source already uses Escapement. Use an Escapement backup to move it")

    try:
        status = target.engine_status()
    except HTTPError as error:
        if error.response is None or error.response.status_code != 404:
            raise
        raise ValueError("The destination must be a running Escapement server") from error
    if status["accepted"] != status["applied"] or status["fault"]:
        raise ValueError("The destination has pending or failed writes. Wait for it to drain before migrating")
    if target.list_apps()["apps"]:
        raise ValueError("Start with a destination that has no deployed apps")

    legacy_streams = _catalog(source, legacy=True)
    target_streams = _catalog(target)
    source_info = source.timeseries_info_batch(list(legacy_streams)) if legacy_streams else {}
    if legacy_streams.keys() - source_info.keys():
        raise ValueError("The legacy server did not report counts for all registered streams")
    populated = target.timeseries_exists_batch(list(target_streams), view="stored") if target_streams else set()
    if populated - legacy_streams.keys():
        raise ValueError("The destination contains readings outside the legacy stream inventory")

    copied: dict[tuple[str, str], int] = {}
    for uri, registration in legacy_streams.items():
        prior = target_streams.get(uri, {})
        for key in ("property_uri", "value_kind", "unit", "quantity_kind"):
            if key in prior and prior[key] != registration.get(key):
                raise ValueError(f"Destination {key} differs for stream {uri}")
        if uri in populated:
            for mode in _VALUE_MODES:
                copied[uri, mode] = _matching_prefix(source, target, uri, mode)

    expected = sum(source_info[uri].row_count for uri in legacy_streams)
    report(f"Found {len(legacy_streams)} raw streams and {expected} readings.")
    if copied:
        report(f"Verified {sum(copied.values())} already copied readings.")
    if dry_run:
        return

    # Register even empty streams. Plant topology and source-owned graphs can
    # be imported separately through the existing graph API.
    registrations = [stream for uri, stream in legacy_streams.items() if target_streams.get(uri) != stream]
    if registrations:
        target.register_streams(registrations)
    for uri, registration in legacy_streams.items():
        for mode in _VALUE_MODES:
            remaining = copied.get((uri, mode), 0)
            with closing(_batches(source, uri, mode)) as batches:
                for batch in batches:
                    skip = min(len(batch), remaining)
                    remaining -= skip
                    batch = batch.slice(skip)
                    if not batch.is_empty():
                        table = batch.with_columns(
                            pl.lit(registration["stream_name"]).alias("stream_name")
                        ).to_arrow()
                        result = target.insert_timeseries_arrow(registration["source_id"], table)
                        last = result["receipts"][-1]
                        if not last["published"]:
                            target.wait(last)

        actual = sum(_matching_prefix(source, target, uri, mode) for mode in _VALUE_MODES)
        if actual != source_info[uri].row_count:
            raise RuntimeError(
                f"Verification failed for {uri}: expected {source_info[uri].row_count} readings, found {actual}"
            )

    report(f"Verified {expected} readings in {len(legacy_streams)} destination streams.")
