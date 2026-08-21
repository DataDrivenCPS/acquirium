"""Characterize canonical Arrow publication throughput on DuckDB.

Run with ``uv run python benchmarks/publication_duckdb.py --rows 100000``.
This is deliberately a reporting harness, not a CI threshold: results depend
on host storage and DuckDB version.
"""

from __future__ import annotations

import argparse
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa

from acquirium.Storage.duckdb_store import DuckDBStore
from acquirium.Storage.materialization.duckdb import MaterializationDuckDB
from acquirium.Storage.publication.duckdb import PublicationDuckDB
from acquirium.Storage.publication.types import MUTATION_SCHEMA, PublicationRequest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--streams", type=int, default=100)
    args = parser.parse_args()
    if args.rows <= 0 or args.streams <= 0:
        raise SystemExit("--rows and --streams must be positive")

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    timestamps = [start + timedelta(seconds=index) for index in range(args.rows)]
    refs = [f"urn:benchmark:{index % args.streams}" for index in range(args.rows)]
    mutations = pa.table(
        {"operation": ["upsert"] * args.rows, "ref_uri": refs,
         "ts": pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
         "numeric_value": [float(index) for index in range(args.rows)],
         "text_value": [None] * args.rows},
        schema=MUTATION_SCHEMA,
    )
    with tempfile.TemporaryDirectory(prefix="acquirium-publication-") as directory:
        store = DuckDBStore(Path(directory) / "benchmark.duckdb", recreate=True)
        try:
            MaterializationDuckDB(store)
            publisher = PublicationDuckDB(store)
            before = time.perf_counter()
            receipt = publisher.publish(PublicationRequest("benchmark", mutations))
            elapsed = time.perf_counter() - before
        finally:
            store.close()
    print(f"rows={args.rows} streams={args.streams} seconds={elapsed:.3f} rows_per_second={args.rows / elapsed:.0f} publications={len(receipt.versions)}")


if __name__ == "__main__":
    main()
