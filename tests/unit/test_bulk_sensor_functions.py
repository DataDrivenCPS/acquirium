"""Ordinary and batch calculations must publish identical dated results."""

from datetime import datetime, timedelta, timezone

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from examples.bulk_ingest import batch_transforms, transforms
from acquirium.functions import emit

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
MINUTE = timedelta(minutes=1)


@pytest.mark.parametrize("name", [
    "snap_to_minute", "fill_gaps", "plant_turbidity",
    "high_turbidity", "sustained_high_turbidity", "plant_alarms",
])
def test_batch_and_ordinary_functions_agree_with_datetime_inputs(name):
    points = [(0, .2), (2, .8), (3, .4)]
    if name == "sustained_high_turbidity":
        points = [(0, 1.), (1, 1.), (2, 1.)]
    source = pl.DataFrame({
        "time": [START + minute * MINUTE for minute, _ in points],
        "value": [value for _, value in points],
        "context": ['{"group":"urn:bulk/loc-2"}'] * len(points),
    })
    frames, expected = [], {}
    # Calculate each minute independently to establish the expected result for the batch.
    for minute in range(4):
        output_time = START + minute * MINUTE
        if name == "fill_gaps":
            frame = source
        elif name == "sustained_high_turbidity":
            frame = source.filter(pl.col("time").is_between(output_time - 2 * MINUTE, output_time))
        else:
            frame = source.filter(pl.col("time") == output_time)
        context = {
            "output_time": output_time,
            "operator": {"window": {"lookback": timedelta(hours=2)}},
            "parameters": {"sustained_minutes": 2},
        }
        for port, result in getattr(transforms, name)(frame, context).items():
            if not isinstance(result, pl.DataFrame):
                result = emit(context, result)
            expected.setdefault(port, []).append(result)
        frames.append(frame.with_columns(pl.lit(output_time).alias("output_time")))
    # Tag the same inputs with their output times and run them as one batch.
    batch = pl.concat(frames)
    actual = getattr(batch_transforms, name)(batch, context)
    assert actual.keys() == expected.keys()
    for port in actual:
        assert_frame_equal(
            actual[port].sort("time"), pl.concat(expected[port]).sort("time"),
            check_exact=False,
        )
