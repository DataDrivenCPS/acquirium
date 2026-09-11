# Example apps

These examples use `acquirium.App` API and can be checked
with the CLI before deployment:

```bash
acquirium app check scripts/apps/temperature_normalization.py:TemperatureNormalization
acquirium app check scripts/apps/fill_short_temperature_gaps.py:FillShortTemperatureGaps
```

`TemperatureNormalization` converts each matched water-temperature stream to
Celsius. `FillShortTemperatureGaps` copies the complete Celsius history to a
derived stream and inserts linearly interpolated samples for gaps shorter than
one hour. It infers the sampling interval from the smallest observed interval;
gaps of one hour or more remain unchanged.
