# Soft sensor actors

From the repository root, start the example with:

```sh
cd examples/actors
uv run --locked acquirium server
```

The server reads [acquirium.toml](acquirium.toml) in this folder. It starts
[driver.py](driver.py) to publish synthetic readings, then loads the
`alarm` declaration from [turbidity_alarm.py](turbidity_alarm.py) and the
`fit` declaration from [membrane_fit.py](membrane_fit.py).
The membrane file also defines the prediction app, which the server deploys
after registering the fitting actor's output.

Both actors run every four seconds in this example. The driver publishes
turbidity readings of 0.8 NTU, so the alarm fires once readings are available.
The driver also changes the simulated membrane's permeability from 2 to 4
after 20 seconds. A training window that spans the change may yield an
intermediate fit. Predictions follow as each new fit becomes effective.

While the server runs, inspect the streams in a Python session:

```python
import acquirium as aq

client = aq.Acquirium(server_url="127.0.0.1", server_port=8000)
print(client.stream("actor:turbidity-alarm", "alarm").dataframe().tail(5))
print(client.stream("actor:membrane-fit", "permeability").dataframe().tail(10))
print(client.query().measurement(app="predict-membrane-flux").dataframe().tail(10))
```

Stop the server with Ctrl-C. Readings and actor state persist in `.acquirium/`
inside this folder.
