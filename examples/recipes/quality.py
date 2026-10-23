"""Recipes that check the data and its description."""

ACQUIRIUM = ">=0.4.0a8"

import polars as pl

from acquirium.recipes.builtins import catalog

_TIME = pl.Datetime("us", "UTC")


def missing_metadata(acq, *attrs):
    """Streams the catalog shows without the given attributes, by default unit, quantity kind and host.

    ``attrs`` are catalog column names. A ``missing`` column lists which of
    them each stream lacks.
    """
    attrs = attrs or ("unit", "quantity_kind", "host")
    frame = catalog(acq)
    flags = [pl.when(pl.col(a).is_null()).then(pl.lit(a)) for a in attrs]
    return (
        frame.filter(pl.any_horizontal([pl.col(a).is_null() for a in attrs]))
        .with_columns(missing=pl.concat_list(flags).list.drop_nulls())
    )


def gaps(acq, query, *, longer_than, start=None, end=None):
    """Stretches with no reading longer than ``longer_than``, per stream.

    One row per gap with the stream, when it started, when it ended and
    its duration. ``longer_than`` is a ``timedelta``; ``start`` and ``end``
    bound the fetch.
    """
    frame = query.dataframe("narrow", start=start, end=end)
    if "point_id" not in frame.columns:
        return pl.DataFrame(schema={"stream": pl.Utf8, "from": _TIME, "to": _TIME, "duration": pl.Duration("us")})
    return (
        frame.sort("time")
        .with_columns(prev=pl.col("time").shift(1).over("point_id"))
        .with_columns(duration=pl.col("time") - pl.col("prev"))
        .filter(pl.col("duration") > longer_than)
        .select(pl.col("point_id").alias("stream"), pl.col("prev").alias("from"),
                pl.col("time").alias("to"), "duration")
        .sort(["stream", "from"])
    )


def exceedances(acq, query, limit, *, longer_than, start=None, end=None):
    """Runs of readings above ``limit`` that last longer than ``longer_than``, per stream.

    One row per run with the stream, its start, end, duration and peak
    value. A run is consecutive readings above the limit; it ends at the
    last reading above it. ``longer_than`` is a ``timedelta``.
    """
    frame = query.dataframe("narrow", start=start, end=end, cast_value="float")
    if "point_id" not in frame.columns:
        return pl.DataFrame(schema={"stream": pl.Utf8, "start": _TIME, "end": _TIME,
                                    "duration": pl.Duration("us"), "peak": pl.Float64})
    exceed = pl.col("value_numeric") > limit
    return (
        frame.sort("time")
        .with_columns(exceed=exceed)
        .with_columns(run=(pl.col("exceed") != pl.col("exceed").shift(1)).fill_null(True).cum_sum().over("point_id"))
        .filter("exceed")
        .group_by("point_id", "run")
        .agg(start=pl.col("time").min(), end=pl.col("time").max(), peak=pl.col("value_numeric").max())
        .with_columns(duration=pl.col("end") - pl.col("start"))
        .filter(pl.col("duration") > longer_than)
        .select(pl.col("point_id").alias("stream"), "start", "end", "duration", "peak")
        .sort(["stream", "start"])
    )
