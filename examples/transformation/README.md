# Temperature app

This app selects all measurement streams with unit `DEG_F` and publishes a
separate Celsius stream for each match. The example has three files:

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
