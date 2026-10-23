"""The recipes that ship with acquirium.

Each is a plain function taking the client first, the same shape as a
recipe in a user's own file. ``acq.recipes.source(name)`` prints one.
"""

ACQUIRIUM = ">=0.4.0a8"

from datetime import timedelta

import polars as pl


def streams(acq, **where):
    """Every registered stream as a Query, filtered by metadata attributes.

    Keyword arguments are the filters ``where()`` accepts: quantity_kind,
    unit, substance, medium, label, data_source, app, enumeration_kind.
    Values are URIs or free text the server resolves, or a list of either.
    The result is a Query, so chain ``metadata()``, ``dataframe()``,
    ``context()`` or ``facets()`` onto it::

        acq.recipes.streams().metadata()
        acq.recipes.streams(quantity_kind="pressure", unit="psi").dataframe(limit=100)
    """
    return acq.query().measurement(**where)


def stream_stats(acq, query=None):
    """Row count, earliest and latest sample of every stream a query matches.

    ``query`` defaults to every stream on the server. No values are
    fetched: the numbers come from ``data().bindings``, which the server
    keeps per stream. Streams with no data show zero rows and empty bounds.
    """
    query = acq.query().measurement() if query is None else query
    rows = [
        (b.alias, acq.client.compact_uri(b.point_uri), b.point_label, b.row_count, b.earliest, b.latest)
        for b in query.data().bindings
    ]
    return pl.DataFrame(
        rows,
        schema={"alias": pl.Utf8, "stream": pl.Utf8, "label": pl.Utf8, "rows": pl.Int64,
                "earliest": pl.Datetime("us", "UTC"), "latest": pl.Datetime("us", "UTC")},
        orient="row",
    ).sort(["rows", "stream"], descending=[True, False])


def catalog(acq, query=None):
    """One row per stream: what it measures, what it sits on and which treatment step it belongs to.

    ``query`` defaults to every stream on the server; a query whose current
    node is a measurement node narrows the set. Three questions go to the
    graph: the stream's quantity kind, unit and substance; its host, the
    entity it hangs off; and the feeder, the entity upstream of that host.
    The step is the host's process or, for a sensor on a pipe, the feeder's
    process. Streams the plant model does not describe keep their row with
    empty host and step columns, so the frame also works on a SCADA-only
    server. A host with several types gives one row per type.
    """
    streams = (acq.query().measurement() if query is None else query).alias("stream")
    measures = streams.include("quantity_kind", "unit", "substance").metadata()
    hosts = streams.context(alias="host").include("type", "process").metadata()
    feeders = streams.context(via="upstream", alias="feeder").include("process").metadata()

    measures = _ensure(measures, "stream", "stream.label", "stream.quantity_kind", "stream.unit", "stream.substance")
    hosts = _ensure(hosts, "stream", "host", "host.type", "host.process")
    feeders = _ensure(feeders, "stream", "feeder", "feeder.process")
    return (
        measures.join(hosts.select("stream", "host", "host.type", "host.process"), on="stream", how="left")
        .join(feeders.select("stream", "feeder", "feeder.process"), on="stream", how="left")
        .select(
            stream="stream",
            label="stream.label",
            quantity_kind="stream.quantity_kind",
            unit="stream.unit",
            substance="stream.substance",
            host="host",
            host_type="host.type",
            feeder="feeder",
            process=pl.coalesce("host.process", "feeder.process"),
        )
    )


def processes(acq, *, neighbors=False, max_depth=12):
    """Treatment steps in the plant and how many units perform each.

    A step is the process attribute of a piece of equipment. With
    ``neighbors=True`` two list columns are added, ``upstream`` and
    ``downstream``: the steps directly before and after each one. They
    come from asking every unit for the nearest downstream unit that also
    performs a step, within ``max_depth`` flow hops, so pipes and valves in
    between are skipped by the query itself. A plant model without
    processes gives an empty frame.
    """
    units = acq.query().entity("equipment", alias="unit").include("process", required=True).metadata()
    steps = (
        units.group_by(pl.col("unit.process").alias("process"))
        .agg(units=pl.len())
        .sort(["units", "process"], descending=[True, False])
    )
    if not neighbors:
        return steps

    flow = (
        acq.query()
        .entity("equipment", alias="from_unit").include("process", required=True)
        .related("equipment", alias="to_unit", direction="downstream", nearest=True, max_depth=max_depth)
        .include("process", required=True)
        .metadata()
    )
    edges = (
        flow.select(frm="from_unit.process", to="to_unit.process")
        .filter(pl.col("frm") != pl.col("to"))
        .unique()
    )
    upstream = edges.group_by(pl.col("to").alias("process")).agg(upstream=pl.col("frm").unique().sort())
    downstream = edges.group_by(pl.col("frm").alias("process")).agg(downstream=pl.col("to").unique().sort())
    return steps.join(upstream, on="process", how="left").join(downstream, on="process", how="left")


def recent(acq, query, *, days=None, hours=None, shape="wide", cast_value="float"):
    """The last ``days`` or ``hours`` of data a query matches, anchored on its newest sample.

    The newest sample across the matched streams ends the window and the
    span counts back from it. Anchoring there rather than on the clock or
    on the oldest sample means a plant that stopped reporting last month
    still gives a full frame, and an export with corrupt early timestamps
    never sets the window. Nothing is fetched to find the anchor; it comes
    from ``data().bindings``. A query with no data gives an empty frame.
    """
    if (days is None) == (hours is None):
        raise ValueError("recent: give exactly one of days or hours")
    span = timedelta(days=days) if days is not None else timedelta(hours=hours)
    latest = [b.latest for b in query.data().bindings if b.latest is not None]
    if not latest:
        return query.dataframe(shape, cast_value=cast_value)
    end = max(latest)
    return query.dataframe(shape, start=end - span, end=end, cast_value=cast_value)


def _ensure(df, *columns):
    """Add any of ``columns`` the frame lacks as empty text columns."""
    missing = [c for c in columns if c not in df.columns]
    return df.with_columns([pl.lit(None, dtype=pl.Utf8).alias(c) for c in missing]) if missing else df
