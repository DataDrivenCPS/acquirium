# Temperature app

This is a small end-to-end app: Celsius input samples become a
derived Fahrenheit stream. It keeps the three pieces visible and separate:

- `temperature_conversion.py` declares the app.
- `acquirium.toml` deploys it when the server starts.
- `publish.py` registers an input stream, publishes samples, and prints the
  derived output.

Start an isolated local server in one terminal:

```bash
uv run acquirium server --config examples/transformation/acquirium.toml
```

Then run the example in another terminal:

```bash
uv run python examples/transformation/publish.py
```

The server writes its local graph and DuckDB data to
`examples/transformation/.data/`.
