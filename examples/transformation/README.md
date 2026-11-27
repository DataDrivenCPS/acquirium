# Sine-wave driver and Python threshold

Start the server and its driver from the repository root.

```sh
uv run --locked acquirium server --config examples/transformation/acquirium.toml
```

Follow [Build your first soft sensor](../../docs/tutorials/advanced/first-app.md)
to read the signal, declare the threshold app, and deploy it. The
[polling driver](publish.py) produces one reading per second. The
[threshold function](sine_threshold.py) keeps values above 0.5.

Stop the server with Ctrl-C. It stores readings under `.data/`.

[temperature_conversion.py](temperature_conversion.py) is a separate Python
calculation converting a registered Fahrenheit stream to Celsius.
