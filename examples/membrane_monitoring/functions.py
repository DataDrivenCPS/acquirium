"""Sequential and as-of calculations; this self-contained file is pinned by its content digest.

The first three are declared with ``sequential=True``. Their windows are
evaluated in event-time order, and ``context["previous"]`` holds the latest
earlier window that wrote outputs. Those outputs are the function's state:
whatever a function carries forward, it writes. ``residual`` reads the
permeability estimate as of each reading instead.
"""

import math

import polars as pl
from escapement import emit, empty, previous_value


def smoothed(frame, context):
    """Exponentially weighted mean with time constant ``tau_ns``, weighted by
    the time since the previous reading."""
    values = frame["value"].drop_nulls()
    if values.is_empty():
        return {"smoothed": empty()}
    value = values.mean()
    previous = context.get("previous")
    if previous is not None:
        elapsed = context["window_ns"] - previous["window_ns"]
        weight = math.exp(-elapsed / context["parameters"]["tau_ns"])
        value = weight * previous_value(context, "smoothed") + (1 - weight) * value
    return {"smoothed": emit(context, value)}


def drift(frame, context):
    """Upper CUSUM of conductivity above ``target`` plus ``slack``, and an
    alarm carrying the statistic while it exceeds ``limit``.

    The statistic returns to zero in normal operation, and a repair stops at
    the first window where it does."""
    values = frame["value"].drop_nulls()
    if values.is_empty():
        return {"cusum": empty(), "alarm": empty()}
    parameters = context["parameters"]
    excess = values.mean() - parameters["target"] - parameters["slack"]
    statistic = max(0.0, previous_value(context, "cusum", 0.0) + excess)
    alarm = statistic if statistic > parameters["limit"] else None
    return {"cusum": emit(context, statistic), "alarm": emit(context, alarm)}


def permeability(frame, context):
    """Recursive least-squares estimate of permeability in flux = A x TMP,
    from each window's mean flux and TMP. ``forgetting`` below 1 discounts
    older windows so the estimate follows fouling; the estimate's variance
    is state too, so it is written beside it."""
    flux = frame.filter(pl.col("role") == "flux")["value"].drop_nulls()
    tmp = frame.filter(pl.col("role") == "tmp")["value"].drop_nulls()
    if flux.is_empty() or tmp.is_empty() or tmp.mean() <= 0:
        return {"permeability": empty(), "variance": empty()}
    parameters = context["parameters"]
    forgetting = parameters["forgetting"]
    estimate = previous_value(context, "permeability", parameters["initial"])
    variance = previous_value(context, "variance", parameters["initial_variance"])
    x, y = tmp.mean(), flux.mean()
    gain = variance * x / (forgetting + x * variance * x)
    estimate += gain * (y - x * estimate)
    variance = (1 - gain * x) * variance / forgetting
    return {"permeability": emit(context, estimate), "variance": emit(context, variance)}


def residual(frame, context):
    """Expected flux from the latest permeability estimate, read as of each
    reading, and the measured flux's departure from it. Not sequential."""
    flux = frame.filter(pl.col("role") == "flux")["value"].drop_nulls()
    tmp = frame.filter(pl.col("role") == "tmp")["value"].drop_nulls()
    estimate = frame.filter(pl.col("role") == "permeability")["value"].drop_nulls()
    if flux.is_empty() or tmp.is_empty() or estimate.is_empty():
        return {"expected": empty(), "residual": empty()}
    expected = estimate[-1] * tmp.mean()
    return {
        "expected": emit(context, expected),
        "residual": emit(context, flux.mean() - expected),
    }
