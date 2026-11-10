# A sine-wave driver and a Python threshold

Start the driver from the repository root:

```sh
uv run --locked acquirium server --config examples/transformation/acquirium.toml
```

In a Python session, deploy the calculation and read its results:

```python
import acquirium as aq
from examples.transformation.threshold_app import high_wave

client = aq.Acquirium(server_url="127.0.0.1", server_port=8000)
client.deploy_app(high_wave(client, {}))
client.query().measurement(uri="urn:example:sine-wave").dataframe().tail(8)
client.query().measurement(app="high-wave").dataframe().tail(3)
```

The driver generates one reading per second. The Python function keeps values
above 0.5. Stop the server with Ctrl-C; data is retained under `.data/`.

Follow [your first app](../../docs/tutorials/first-app.md) to write the function
and build its declaration. [publish.py](publish.py) is the polling driver;
[sine_threshold.py](sine_threshold.py) is the calculation;
[threshold_app.py](threshold_app.py) packages its declaration for deployment.

To deploy it at startup, add this to the example config and restart the server:

```toml
[[apps]]
spec = "threshold_app.py:high_wave"
```

[temperature_conversion.py](temperature_conversion.py) is a separate Python
calculation converting a registered Fahrenheit stream to Celsius.
