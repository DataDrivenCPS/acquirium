"""Copy registered raw readings from an older Acquirium server into Escapement."""

from collections.abc import Callable, Collection, Iterator
from contextlib import closing
from heapq import merge
from operator import itemgetter

import polars as pl
from requests import HTTPError

from acquirium.Client.client import AcquiriumClient
from acquirium.Storage.graph_registry import DERIVED_SOURCE_PREFIX
from acquirium.internals.models import MAX_REQUEST_ROWS, compute_stream_uri


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


def _catalog(
    client: AcquiriumClient, *, legacy: bool = False, only: Collection[str] | None = None,
) -> dict[str, dict[str, str]]:
    result = client.sparql_query(_STREAMS, include_dependencies=False, wait_for_fresh=True)
    streams: dict[str, dict[str, str]] = {}
    for row in result["rows"]:
        registration = {key: value for key, value in zip(result["columns"], row) if value is not None}
        uri = registration.pop("uri")
        if only is not None and uri not in only:
            continue
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
            yield batch.select("ts", "value")


def _typed_rows(client: AcquiriumClient, uri: str, mode: str) -> Iterator[tuple[int, float | str]]:
    with closing(_batches(client, uri, mode)) as batches:
        for batch in batches:
            # Compare legacy microseconds and destination nanoseconds exactly.
            yield from batch.select(pl.col("ts").dt.epoch("ns"), "value").iter_rows()


def _readings(client: AcquiriumClient, uri: str) -> Iterator[tuple[int, float | str]]:
    # Merge the typed reads so a number and a string at the same timestamp
    # compare as a conflict, even if their printed values look the same.
    with closing(_typed_rows(client, uri, "numeric")) as numbers, closing(_typed_rows(client, uri, "text")) as text:
        yield from merge(numbers, text, key=itemgetter(0))


def _compare(source: AcquiriumClient, target: AcquiriumClient, uri: str) -> Iterator[tuple[int, float | str, bool]]:
    """Yield each legacy reading and whether the destination already has it."""
    with closing(_readings(source, uri)) as expected, closing(_readings(target, uri)) as observed:
        current = next(observed, None)
        for timestamp, value in expected:
            while current is not None and current[0] < timestamp:
                current = next(observed, None)
            matched = current is not None and current[0] == timestamp
            if matched and current[1] != value:
                raise ValueError(f"Destination readings differ for {uri} at {timestamp} UTC nanoseconds")
            yield timestamp, value, matched
            if matched:
                current = next(observed, None)


def _count_matches(source: AcquiriumClient, target: AcquiriumClient, uri: str) -> tuple[int, int]:
    total = matched = 0
    for _, _, present in _compare(source, target, uri):
        total += 1
        matched += present
    return total, matched


def _write_batch(target: AcquiriumClient, registration: dict[str, str], batch: pl.DataFrame) -> None:
    table = batch.with_columns(
        pl.col("ts").cast(pl.Datetime("ns", "UTC")),
        pl.lit(registration["stream_name"]).alias("stream_name"),
    ).to_arrow()
    result = target.insert_timeseries_arrow(registration["source_id"], table)
    last = result["receipts"][-1]
    if not last["published"]:
        target.wait(last)


def _copy_missing(
    source: AcquiriumClient, target: AcquiriumClient, uri: str, registration: dict[str, str],
) -> int:
    # Arrow writes need a single value type per batch.
    pending: dict[str, list[tuple[int, float | str]]] = {"numeric": [], "text": []}
    total = 0
    with closing(_compare(source, target, uri)) as readings:
        for timestamp, value, matched in readings:
            total += 1
            if matched:
                continue
            batch = pending["text" if isinstance(value, str) else "numeric"]
            batch.append((timestamp, value))
            if len(batch) == MAX_REQUEST_ROWS:
                _write_batch(target, registration, pl.DataFrame(batch, schema=["ts", "value"], orient="row"))
                batch.clear()
    for batch in pending.values():
        if batch:
            _write_batch(target, registration, pl.DataFrame(batch, schema=["ts", "value"], orient="row"))
    return total


def _check_valueless_rows(source: AcquiriumClient, uri: str, legacy_count: int, copyable: int) -> int:
    """Confirm that any rows missing from the typed reads really have no value."""
    if legacy_count == copyable:
        return 0

    rows = valueless = 0
    # Unlike numeric and text reads, the legacy coalesced read returns null
    # rows. Only scan it when the typed reads do not explain the legacy count.
    with closing(source.timeseries_batches(uri, value_mode="coalesce", view="stored")) as batches:
        for batch in batches:
            rows += len(batch)
            valueless += batch["value"].null_count()

    if rows != legacy_count or copyable + valueless != legacy_count:
        raise RuntimeError(
            f"Legacy counts disagree for {uri}: reported {legacy_count} rows, "
            f"found {copyable} readings and {valueless} rows with no value"
        )
    return valueless


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
        source_status = source.engine_status()
    except HTTPError as error:
        if error.response is None or error.response.status_code != 404:
            raise
    else:
        # An older Escapement server can export its published readings over HTTP.
        if source_status["accepted"] != source_status["applied"] or source_status["fault"]:
            raise ValueError("The source has pending or failed writes. Wait for it to drain before migrating")

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
    target_streams = _catalog(target, only=legacy_streams.keys())
    source_info = source.timeseries_info_batch(list(legacy_streams)) if legacy_streams else {}
    if legacy_streams.keys() - source_info.keys():
        raise ValueError("The legacy server did not report counts for all registered streams")
    populated = target.timeseries_exists_batch(list(target_streams), view="stored") if target_streams else set()
    inspected: dict[str, tuple[int, int]] = {}
    for uri, registration in legacy_streams.items():
        prior = target_streams.get(uri, {})
        for key in ("property_uri", "value_kind", "unit", "quantity_kind"):
            if key in prior and prior[key] != registration.get(key):
                raise ValueError(f"Destination {key} differs for stream {uri}")
        if uri in populated:
            inspected[uri] = _count_matches(source, target, uri)

    legacy_rows = sum(source_info[uri].row_count for uri in legacy_streams)
    report(f"Found {len(legacy_streams)} raw streams and {legacy_rows} legacy rows.")
    if inspected:
        report(f"Verified {sum(matched for _, matched in inspected.values())} already copied readings.")
    if dry_run:
        return

    # Register even empty streams. Plant topology and source-owned graphs can
    # be imported separately through the existing graph API.
    registrations = [stream for uri, stream in legacy_streams.items() if target_streams.get(uri) != stream]
    if registrations:
        target.register_streams(registrations)
    verified = skipped = 0
    for uri, registration in legacy_streams.items():
        checked = inspected.get(uri)
        if checked is not None and checked[0] == checked[1]:
            # The preflight already verified this entire stream.
            copyable, actual = checked
        else:
            if uri in populated:
                copyable = _copy_missing(source, target, uri, registration)
            else:
                # An empty destination can take the original Arrow batches.
                copyable = 0
                for mode in _VALUE_MODES:
                    with closing(_batches(source, uri, mode)) as batches:
                        for batch in batches:
                            copyable += len(batch)
                            if not batch.is_empty():
                                _write_batch(target, registration, batch)
            total, actual = _count_matches(source, target, uri)
            if total != copyable:
                raise RuntimeError(f"Legacy reading count changed during migration for {uri}")
            if actual != copyable:
                raise RuntimeError(
                    f"Verification failed for {uri}: expected {copyable} readings, matched {actual}"
                )
        valueless = _check_valueless_rows(source, uri, source_info[uri].row_count, copyable)
        verified += actual
        skipped += valueless
        if valueless:
            report(
                f"Skipped {valueless} legacy rows with no value for "
                f"{registration['source_id']}/{registration['stream_name']}."
            )

    report(f"Verified {verified} readings in {len(legacy_streams)} destination streams.")
    if skipped:
        report(f"Skipped {skipped} legacy rows with no value in total.")
