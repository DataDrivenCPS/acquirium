"""Copy registered raw readings from an older Acquirium server into Escapement.

Migration checks all registrations and populated streams before submitting
writes. The later copy and verification passes can use the preflight's gap
ranges to avoid rereading history that already matches.
"""

from collections.abc import Callable, Collection, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import monotonic

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
_MAX_WINDOWS = 8
_UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _quiet(_message: str) -> None:
    pass


@contextmanager
def _progress(log: Callable[[str], None], label: str) -> Iterator[Callable[[int], None]]:
    """Report the start, periodic reading counts, and successful finish of a phase."""
    log(f"{label}...")
    started = last_report = monotonic()
    readings = 0

    def update(count: int) -> None:
        nonlocal readings, last_report
        readings = count
        now = monotonic()
        if now - last_report >= 5:
            log(f"  {label} ({readings:,} processed, {now - started:.1f}s elapsed)")
            last_report = now

    yield update
    log(f"{label} done ({readings:,} processed in {monotonic() - started:.1f}s)")


@dataclass(frozen=True)
class _Window:
    """A closed UTC nanosecond range and its expected number of source readings."""

    start: int
    end: int
    rows: int

    def bounds(self) -> dict[str, str | None]:
        # Older servers parse microseconds and include the end. Escapement
        # excludes it. Request a superset and filter to the closed window.
        start = self.start // 1_000
        end = self.end // 1_000 + 1
        return {
            "start": (_UTC_EPOCH + timedelta(microseconds=start)).isoformat()
            if start * 1_000 >= -(2**63) else None,
            "end": (_UTC_EPOCH + timedelta(microseconds=end)).isoformat()
            if end * 1_000 < 2**63 else None,
        }


@dataclass(frozen=True)
class _Inspection:
    """Preflight counts and the ranges to copy. A ``None`` plan revisits the whole stream."""

    total: int
    matched: int
    windows: list[_Window] | None


def _catalog(
    client: AcquiriumClient, *, legacy: bool = False, only: Collection[str] | None = None,
) -> dict[str, dict[str, str]]:
    """Read canonical registrations keyed by stream URI for a metadata comparison."""
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
            # Stream registration recreates this default point on the destination.
            del registration["property_uri"]
        if uri in streams and streams[uri] != registration:
            raise ValueError(f"Stream {source}/{name} has conflicting registration metadata")
        streams[uri] = registration
    return streams


def _batches(
    client: AcquiriumClient, uri: str, mode: str, window: _Window | None = None,
) -> Iterator[pl.DataFrame]:
    bounds = window.bounds() if window is not None else {}
    with closing(client.timeseries_batches(uri, value_mode=mode, view="stored", **bounds)) as batches:
        for batch in batches:
            if batch.is_empty():
                continue
            # Legacy microseconds and Escapement nanoseconds share this UTC key.
            batch = batch.select(pl.col("ts").dt.epoch("ns"), "value")
            if window is not None:
                batch = batch.filter(pl.col("ts").is_between(window.start, window.end))
            if not batch.is_empty():
                yield batch


def _typed_batches(
    client: AcquiriumClient, uri: str, mode: str, window: _Window | None,
) -> Iterator[pl.DataFrame]:
    with closing(_batches(client, uri, mode, window)) as batches:
        for batch in batches:
            yield batch.select(
                "ts",
                (pl.col("value") if mode == "numeric" else pl.lit(None)).cast(pl.Float64).alias("number"),
                (pl.col("value") if mode == "text" else pl.lit(None)).cast(pl.String).alias("text"),
            )


def _compare(
    source: AcquiriumClient, target: AcquiriumClient, uri: str, window: _Window | None = None,
) -> Iterator[tuple[pl.Series, pl.DataFrame]]:
    """Yield source timestamps and missing readings in timestamp-aligned batches.

    A left join gives each source reading either a matching timestamp to
    compare or a gap to fill. Readings found only on the destination stay
    outside the result. Keeping separate numeric and text columns preserves
    the distinction between values such as ``1.0`` and ``"1.0"``.
    """
    with ExitStack() as stack:
        readers = [
            stack.enter_context(closing(_typed_batches(client, uri, mode, window)))
            for client in (source, target) for mode in _VALUE_MODES
        ]
        heads = [next(reader, None) for reader in readers]
        while heads[0] is not None or heads[1] is not None:
            # The earliest batch end is the last timestamp all readers can
            # compare now. Keep any later rows for the next iteration.
            through = min(batch["ts"][-1] for batch in heads if batch is not None)
            source_parts, target_parts = [], []
            for index, batch in enumerate(heads):
                if batch is None or batch["ts"][0] > through:
                    continue
                count = batch["ts"].search_sorted(through, side="right")
                (source_parts if index < 2 else target_parts).append(batch.slice(0, count))
                heads[index] = batch.slice(count) if count < len(batch) else next(readers[index], None)

            if not source_parts:
                continue
            expected = pl.concat(source_parts)
            if not target_parts:
                yield expected["ts"], expected
                continue
            compared = expected.join(pl.concat(target_parts), on="ts", how="left", suffix="_target")
            present = pl.col("number_target").is_not_null() | pl.col("text_target").is_not_null()
            differs = pl.col("number").ne_missing(pl.col("number_target")) | pl.col("text").ne_missing(
                pl.col("text_target")
            )
            conflict = compared.filter(present & differs)["ts"].min()
            if conflict is not None:
                raise ValueError(f"Destination readings differ for {uri} at {conflict} UTC nanoseconds")
            yield expected["ts"], compared.filter(~present).select("ts", "number", "text")


def _inspect(
    source: AcquiriumClient, target: AcquiriumClient, uri: str, update: Callable[[int], None],
) -> _Inspection:
    """Preflight one populated stream and plan the ranges to revisit."""
    total = matched = 0
    windows: list[_Window] | None = []
    with closing(_compare(source, target, uri)) as batches:
        for timestamps, missing in batches:
            total += len(timestamps)
            matched += len(timestamps) - len(missing)
            update(total)
            if windows is None or missing.is_empty():
                continue
            # Track one range per slice with gaps. Count matching rows inside
            # it too, so copy and verification can check what the range returned.
            start, end = missing["ts"].min(), missing["ts"].max()
            rows = timestamps.is_between(start, end).sum()
            if windows and start == windows[-1].end + 1:
                previous = windows.pop()
                windows.append(_Window(previous.start, end, previous.rows + rows))
            elif len(windows) < _MAX_WINDOWS:
                windows.append(_Window(start, end, rows))
            else:
                windows = None
    # Widely scattered gaps can make bounded reads cover most of the stream.
    # In that case a single range avoids repeated HTTP setup on each pass.
    if windows is not None and sum(window.rows for window in windows) * 2 > total:
        windows = None
    return _Inspection(total, matched, windows)


def _compare_ranges(
    source: AcquiriumClient, target: AcquiriumClient, uri: str, windows: list[_Window] | None,
) -> Iterator[tuple[pl.Series, pl.DataFrame]]:
    """Compare the planned ranges and check their preflight row counts."""
    for window in (None,) if windows is None else windows:
        total = 0
        with closing(_compare(source, target, uri, window)) as batches:
            for timestamps, missing in batches:
                total += len(timestamps)
                yield timestamps, missing
        if window is not None and total != window.rows:
            raise RuntimeError(f"Legacy reading count changed during migration for {uri}")


def _count_matches(
    source: AcquiriumClient, target: AcquiriumClient, uri: str, windows: list[_Window] | None,
    update: Callable[[int], None],
) -> tuple[int, int]:
    total = matched = 0
    with closing(_compare_ranges(source, target, uri, windows)) as batches:
        for timestamps, missing in batches:
            total += len(timestamps)
            matched += len(timestamps) - len(missing)
            update(total)
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
    windows: list[_Window] | None, update: Callable[[int], None],
) -> int:
    """Submit only missing rows and return the total source readings examined."""
    # Arrow writes need a single value type per batch.
    pending: dict[str, list[pl.DataFrame]] = {"number": [], "text": []}
    buffered = dict.fromkeys(pending, 0)
    total = 0
    with closing(_compare_ranges(source, target, uri, windows)) as readings:
        for timestamps, missing in readings:
            total += len(timestamps)
            update(total)
            if missing.is_empty():
                continue
            for column, parts in pending.items():
                batch = missing.select("ts", pl.col(column).alias("value")).drop_nulls("value")
                while not batch.is_empty():
                    count = min(MAX_REQUEST_ROWS - buffered[column], len(batch))
                    parts.append(batch.slice(0, count))
                    buffered[column] += count
                    batch = batch.slice(count)
                    if buffered[column] == MAX_REQUEST_ROWS:
                        _write_batch(target, registration, pl.concat(parts))
                        parts.clear()
                        buffered[column] = 0
    for parts in pending.values():
        if parts:
            _write_batch(target, registration, pl.concat(parts))
    return total


def _check_valueless_rows(
    source: AcquiriumClient, uri: str, legacy_count: int, copyable: int,
    update: Callable[[int], None],
) -> int:
    """Confirm that any rows missing from the typed reads really have no value."""
    rows = valueless = 0
    # Unlike numeric and text reads, the legacy coalesced read returns null
    # rows. Only scan it when the typed reads do not explain the legacy count.
    with closing(source.timeseries_batches(uri, value_mode="coalesce", view="stored")) as batches:
        for batch in batches:
            rows += len(batch)
            valueless += batch["value"].null_count()
            update(rows)

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
    progress: Callable[[str], None] = _quiet,
) -> None:
    """Plan or copy missing raw readings using the server APIs.

    Dry runs stop after preflight. A copy preserves destination-only
    readings and verifies imported readings against the source. Keep
    ingestion paused on both servers so their readings stay stable across
    passes. The callbacks receive summary lines and optional live progress.
    """
    if source.address == target.address:
        raise ValueError("Source and destination must be different servers")

    started = monotonic()
    progress("Checking source and destination servers...")
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

    progress("Reading stream registrations and row counts...")
    legacy_streams = _catalog(source, legacy=True)
    target_streams = _catalog(target, only=legacy_streams.keys())
    source_info = source.timeseries_info_batch(list(legacy_streams)) if legacy_streams else {}
    if legacy_streams.keys() - source_info.keys():
        raise ValueError("The legacy server did not report counts for all registered streams")
    populated = target.timeseries_exists_batch(list(target_streams), view="stored") if target_streams else set()
    inspected: dict[str, _Inspection] = {}
    for uri, registration in legacy_streams.items():
        prior = target_streams.get(uri, {})
        for key in ("property_uri", "value_kind", "unit", "quantity_kind"):
            if key in prior and prior[key] != registration.get(key):
                raise ValueError(f"Destination {key} differs for stream {uri}")
        if uri in populated:
            stream = f"{registration['source_id']}/{registration['stream_name']}"
            label = f"Preflight ({len(inspected) + 1}/{len(populated)}) {stream}"
            with _progress(progress, label) as update:
                inspected[uri] = _inspect(source, target, uri, update)

    legacy_rows = sum(source_info[uri].row_count for uri in legacy_streams)
    report(f"Found {len(legacy_streams)} raw streams and {legacy_rows} legacy rows.")
    if inspected:
        report(f"Verified {sum(check.matched for check in inspected.values())} already copied readings.")
    if dry_run:
        progress(f"Dry run finished ({monotonic() - started:.1f}s).")
        return

    # Register even empty streams. Plant topology and source-owned graphs can
    # be imported separately through the existing graph API.
    registrations = [stream for uri, stream in legacy_streams.items() if target_streams.get(uri) != stream]
    if registrations:
        progress(f"Registering destination streams ({len(registrations):,})...")
        target.register_streams(registrations)
    verified = skipped = 0
    for index, (uri, registration) in enumerate(legacy_streams.items(), start=1):
        stream = f"{registration['source_id']}/{registration['stream_name']}"
        label = f"({index}/{len(legacy_streams)}) {stream}"
        checked = inspected.get(uri)
        if checked is not None and checked.total == checked.matched:
            # The preflight already verified this entire stream.
            copyable = actual = checked.total
        else:
            windows = None if checked is None else checked.windows
            verified_in_preflight = 0
            if checked is not None:
                if windows is not None:
                    verified_in_preflight = checked.total - sum(window.rows for window in windows)
                scope = "whole-stream scan"
                if windows is not None:
                    unit = "window" if len(windows) == 1 else "windows"
                    scope = f"{len(windows)} gap {unit}"
                with _progress(progress, f"Copying {label} ({scope})") as update:
                    copyable = _copy_missing(source, target, uri, registration, windows, update)
                    if checked.total != copyable + verified_in_preflight:
                        raise RuntimeError(f"Legacy reading count changed during migration for {uri}")
            else:
                # An empty destination can take the original Arrow batches.
                copyable = 0
                with _progress(progress, f"Copying {label} (empty destination)") as update:
                    for mode in _VALUE_MODES:
                        with closing(_batches(source, uri, mode)) as batches:
                            for batch in batches:
                                _write_batch(target, registration, batch)
                                copyable += len(batch)
                                update(copyable)
            with _progress(progress, f"Verifying {label}") as update:
                total, actual = _count_matches(source, target, uri, windows, update)
                if total != copyable:
                    raise RuntimeError(f"Legacy reading count changed during migration for {uri}")
                if actual != copyable:
                    raise RuntimeError(
                        f"Verification failed for {uri}: expected {copyable} readings, matched {actual}"
                    )
            copyable += verified_in_preflight
            actual += verified_in_preflight
        legacy_count = source_info[uri].row_count
        valueless = 0
        if legacy_count != copyable:
            with _progress(progress, f"Checking legacy rows with no value for {label}") as update:
                valueless = _check_valueless_rows(source, uri, legacy_count, copyable, update)
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
    progress(f"Migration finished ({monotonic() - started:.1f}s).")
