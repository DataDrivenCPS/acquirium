# Semantic selection with a native Python function

Start a server in a fresh directory with `exact_only=true`, then run:

```sh
uv run python examples/transformation/publish.py
```

The script registers a Fahrenheit stream, deploys the declaration returned by
`fahrenheit_to_celsius`, inserts observations, and queries derived Celsius data.
`temperature_conversion.py` contains both the semantic declaration factory and
the pure Escapement function. Source corrections and deletions repair the same
output timestamps through native epochs. No separate app process or scheduler
is required.

The function module must be accessible on the server. When configuring an app at
startup, use `temperature_conversion.py:fahrenheit_to_celsius` as its factory spec.
