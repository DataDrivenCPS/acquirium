"""Copy water-temperature history and fill short gaps by interpolation.

Check the app without contacting a server:

    acquirium app check scripts/apps/fill_short_temperature_gaps.py:FillShortTemperatureGaps

The app republishes the complete input history to a new derived stream. It
adds samples at the inferred sampling interval when two observed samples are
less than one hour apart, then linearly interpolates their values. Longer gaps
are preserved as gaps.
"""

from __future__ import annotations

from datetime import timedelta

import polars as pl

import acquirium as aq


# Do not invent values across a long outage; preserve that gap in the output.
MAX_GAP = timedelta(hours=1)


class FillShortTemperatureGaps(aq.App):
    """Fill sub-hour gaps while retaining every original temperature sample."""

    name = "fill-short-temperature-gaps"
    grouping = "per_match"
    # Gap detection needs the neighboring samples on both sides of a change.
    lookback = "all"
    # Build the derived stream from the existing history on first activation.
    backfill = True
    outputs = {
        "temperature": aq.output.stream(
            value_kind="numeric",
            unit="http://qudt.org/vocab/unit/DEG_C",
            quantity_kind="http://qudt.org/vocab/quantitykind/Temperature",
        ),
    }

    def build_query(self, plant):
        # Each matched temperature stream is filled independently.
        return plant.query().measurement(
            alias="temperature", quantity_kind="temperature"
        )

    def transform(self, inputs, output, context):
        # Read the full history in Celsius so interpolation never mixes units.
        frame = (
            inputs["temperature"]
            .in_unit("http://qudt.org/vocab/unit/DEG_C")
            .df()
            .select("time", "value")
            .sort("time")
        )
        if frame.height < 2:
            # There is no pair of samples from which to interpolate.
            output["temperature"] = frame
            return

        times = frame["time"].to_list()
        values = frame["value"].to_list()
        # The shortest observed interval is the cadence used for new samples.
        intervals = [right - left for left, right in zip(times, times[1:])]
        step = min(intervals)
        if step <= timedelta(0):
            raise ValueError("input timestamps must be strictly increasing")

        rows = []
        for left_time, left_value, right_time, right_value in zip(
            times, values, times[1:], values[1:]
        ):
            # Keep every original point, then add only missing cadence points.
            rows.append({"time": left_time, "value": left_value})
            gap = right_time - left_time
            if gap < MAX_GAP and gap > step:
                offset = step
                while left_time + offset < right_time:
                    # Linear interpolation preserves the endpoints' trend.
                    fraction = offset / gap
                    rows.append(
                        {
                            "time": left_time + offset,
                            "value": left_value
                            + (right_value - left_value) * fraction,
                        }
                    )
                    offset += step
        rows.append({"time": times[-1], "value": values[-1]})
        output["temperature"] = pl.DataFrame(rows).select("time", "value")
