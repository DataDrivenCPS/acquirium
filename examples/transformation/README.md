# Convert a temperature stream

```sh
uv run --locked python examples/transformation/publish.py
```

Run from the repository root. The script starts a temporary server, converts
three Fahrenheit readings to Celsius, then corrects the middle reading. It checks
that the result changes from `[20, 25, 30]` to `[20, 20, 30]` and shuts down.

Follow [your first app](../../docs/tutorials/first-app.md) for the walkthrough.
[temperature_conversion.py](temperature_conversion.py) contains the calculation
and declaration; [publish.py](publish.py) supplies the data.

Pass `--host 127.0.0.1 --port 8000` to use an existing server with fresh demo data.
