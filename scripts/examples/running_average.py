"""One derived running-average stream for every selected temperature stream.

Check the app without contacting a server:

    acquirium app check scripts/examples/running_average.py:RunningAverage

Preview it against a running Acquirium server without registering streams,
writing observations, changing the graph, or calling webhooks:

    acquirium app run scripts/examples/running_average.py:RunningAverage \
        --dry-run \
        --server-url localhost \
        --server-port 8000 \
        --params '{"window": 5}'

Register (or replace) it and run continuously every 60 seconds:

    acquirium app run scripts/examples/running_average.py:RunningAverage \
        --replace \
        --keep-alive \
        --interval 60 \
        --params '{"window": 5}'

The selector below matches every registered temperature measurement. Change or
remove ``quantity_kind`` to adapt the example to the semantics in your graph.
"""

from __future__ import annotations

import polars as pl

from acquirium import AppContext, MappedApp, MappedStream, OutputTemplate


class RunningAverage(MappedApp):
    name = "running_average_example_ratio"
    version = "0.1"

    # Every point found under this query alias is transformed independently.
    input_alias = "sensor"

    # Fetch the newest 60 values of each input. MappedApp presents each input
    # frame to transform() in ascending timestamp order.
    fetch_limit = 60

    # Acquirium creates one output stream per matched input. Its stable identity
    # is derived from this app name, template name, and the input point URI.
    output = OutputTemplate(
        name="running_average",
        value_kind="numeric",
        unit="same_as_input",
    )

    def build_query(self, aq):
        return aq.query().measurement(
            alias=self.input_alias,
            quantity_kind="DimensionlessRatio",
        )

    def transform(self, stream: MappedStream, ctx: AppContext) -> pl.DataFrame:
        window = int(ctx.params.get("window", 5))
        if window <= 0:
            raise ValueError("window must be a positive integer")

        # min_samples=1 makes the example produce a preview even when a stream
        # has fewer than `window` observations. Returning tail(1) emits only the
        # newest calculation; change this to the full frame to backfill history.
        return (
            stream.values
            .with_columns(
                pl.col("value")
                .rolling_mean(window_size=window, min_samples=1)
                .alias("value")
            )
            .tail(1)
        )
