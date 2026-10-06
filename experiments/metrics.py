"""Latency, cost and planning figures from a server's event log."""
from __future__ import annotations

import polars as pl


def _total(counts) -> float:
    """Sum a per-alias or per-port count; Polars widens these dicts to structs with nulls."""
    if not counts:
        return 0.0
    return float(sum(v for v in counts.values() if v is not None))


def _commits(events: pl.DataFrame) -> pl.DataFrame:
    frame = events.filter(pl.col("kind") == "commit")
    return frame.select("app", "binding", "from_revision", "to_revision", "revision", "t",
                        "rows_written").sort("t")


def visible_at(commits: pl.DataFrame, revision: int) -> tuple[float, int | None] | None:
    """When the first commit consuming ``revision`` landed, and what revision it wrote."""
    hit = commits.filter((pl.col("from_revision") < revision) & (pl.col("to_revision") >= revision))
    if hit.is_empty():
        return None
    row = hit.row(0, named=True)
    return float(row["t"]), row["revision"]


def latency(events: pl.DataFrame, chain: list[str]) -> pl.DataFrame:
    """Arrival-to-visibility latency per ingest revision along a chain of views.

    Depth 1 is the first view in ``chain``; depth ``n`` follows the output
    revision of depth ``n-1`` into the next view's commits. An ingest whose
    change produced no output at some depth stops there.
    """
    ingests = events.filter(pl.col("kind") == "ingest").select("revision", "t").sort("revision")
    commits = {app: _commits(events).filter(pl.col("app") == app) for app in chain}
    rows = []
    for ingest in ingests.iter_rows(named=True):
        revision, t0 = int(ingest["revision"]), float(ingest["t"])
        current = revision
        for depth, app in enumerate(chain, start=1):
            hit = visible_at(commits[app], current)
            if hit is None:
                break
            t_visible, produced = hit
            rows.append({"ingest_revision": revision, "app": app, "depth": depth, "latency_s": t_visible - t0})
            if produced is None:
                break
            current = int(produced)
    return pl.DataFrame(rows) if rows else pl.DataFrame(
        {"ingest_revision": [], "app": [], "depth": [], "latency_s": []})


def cost(events: pl.DataFrame) -> pl.DataFrame:
    """Per view: invocations, transform seconds, rows read and rows written."""
    invocations = events.filter(pl.col("kind") == "invocation")
    if invocations.is_empty():
        return pl.DataFrame()
    read = invocations.with_columns(
        pl.col("rows_read").map_elements(_total, return_dtype=pl.Float64).alias("rows_read_total"))
    per_app = read.group_by("app").agg(
        pl.len().alias("invocations"), pl.col("seconds").sum().alias("transform_seconds"),
        pl.col("rows_read_total").sum().alias("rows_read"))
    commits = events.filter(pl.col("kind") == "commit")
    if not commits.is_empty():
        written = commits.with_columns(
            pl.col("rows_written").map_elements(_total, return_dtype=pl.Float64).alias("rows_written_total")
        ).group_by("app").agg(pl.len().alias("commits"), pl.col("rows_written_total").sum().alias("rows_written"))
        per_app = per_app.join(written, on="app", how="left")
    return per_app.sort("app")


def plans(events: pl.DataFrame) -> pl.DataFrame:
    """Every replan with its compile and lineage-publication time."""
    frame = events.filter(pl.col("kind") == "plan")
    if frame.is_empty():
        return pl.DataFrame()
    return frame.select("t", "graph_revision", "bindings", "repairs", "errors",
                        "compile_seconds", "lineage_seconds", "seconds").sort("t")


def totals(events: pl.DataFrame) -> dict[str, float]:
    per_app = cost(events)
    out = {"transform_seconds": 0.0, "rows_read": 0.0, "rows_written": 0.0, "invocations": 0.0}
    if per_app.is_empty():
        return out
    for key in out:
        if key in per_app.columns:
            out[key] = float(per_app[key].fill_null(0).sum())
    p = plans(events)
    out["plan_seconds"] = float(p["seconds"].sum()) if not p.is_empty() else 0.0
    out["lineage_seconds"] = float(p["lineage_seconds"].sum()) if not p.is_empty() else 0.0
    return out
