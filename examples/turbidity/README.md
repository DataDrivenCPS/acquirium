# Turbidity alarm pipeline

```sh
uv run --locked python -m examples.turbidity.run
```

Run from the repository root. A temporary server runs twenty-five minutes of
readings from four locations through two apps declared in [apps.py](apps.py):
`turbidity-clean` drops fault codes and spikes, averages each minute, and fills
short gaps; `turbidity-alarm` reads the clean stream and raises an alarm when
turbidity stays above a location's limit for long enough. The functions are in
[calculations.py](calculations.py).

The cleaner declares `frame: pd.DataFrame` and receives Arrow-backed pandas
2.x. It uses resampling and linear interpolation. The alarm declares
`frame: pl.DataFrame` and receives Polars. The cleaner returns its calculated
series and declares `Output(replaces="raw", ...)`; normal measurement reads
follow it. Use `view="raw"` to read the original samples. The alarm uses
`emit` to return one reading per call.

Filter 3 is high with a fault code, a missing minute, and a spike; the alarm
fires. Late readings for the missing minute then clear the alarm, and the
snapshot taken before them still shows it. The
[tutorial](../../docs/tutorials/turbidity.md) explains each step.
