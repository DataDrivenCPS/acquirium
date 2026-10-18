# Example apps

These examples use `acquirium.App` API and can be checked
with the CLI before deployment:

```bash
acquirium app check scripts/apps/temperature_normalization.py:TemperatureNormalization
acquirium app check scripts/apps/fill_short_temperature_gaps.py:FillShortTemperatureGaps
acquirium app check scripts/apps/minute_temperature.py:MinuteTemperature
```

`TemperatureNormalization` converts each matched water-temperature stream to
Celsius. `FillShortTemperatureGaps` copies the complete Celsius history to a
derived stream and inserts linearly interpolated samples for gaps shorter than
one hour. It infers the sampling interval from the smallest observed interval;
gaps of one hour or more remain unchanged. `MinuteTemperature` averages each
stream into complete minutes and assigns the result as a polars LazyFrame,
which the runtime collects together with the other lazy outputs of its layer.
