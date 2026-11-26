---
title: Access data on a shared server
---

In this article we'll describe how to read data from an Acquirium server that someone else runs.
The person who runs the server gives you two things: its address, such as `https://plant.example.org/api`, and an access token.

The examples use a server that holds the seawater desalination plant of [5 - A Full Example](../tutorials/a-full-example.md).
Replace the address and the token with yours.

You only need the Python package on your computer.
This requires Acquirium 0.4.1 or newer, see [Install](../getting-started.md#install).

## Keeping the token out of the script

The access token is also called an API key.
Anyone who has it can read the data with your name, so it should not be written in a script or a notebook that you share.

Save it in an environment variable instead:

```bash
export ACQUIRIUM_TOKEN="paste-your-token-here"      # macOS and Linux
```
```powershell
$env:ACQUIRIUM_TOKEN = "paste-your-token-here"      # Windows PowerShell
```

The variable lasts until the terminal is closed.
Start Python or Jupyter from the same terminal.

## Connecting

`Acquirium` connects to a server that is already running.
It takes the address and the token:

```python
import os
from acquirium import Acquirium

acq = Acquirium(
    "https://plant.example.org/api",
    token=os.environ["ACQUIRIUM_TOKEN"],
)
print(acq.address)
```
```text
https://plant.example.org/api
```

The token is sent with every request.
Note that `aq.init()` is not used here.
`aq.init()` starts a server on your own computer, and the token will not work with it.

Be aware that the address should start with `https`.
With an `http` address the token is sent unencrypted, and Acquirium warns about it:

```text
UserWarning: The token is sent unencrypted to http://plant.example.org/api. Use an https address.
```

## Finding what is on the server

From here on, everything works as in the tutorials.
Printing a query shows the first five streams it selects:

```python
print(acq.query().measurement())
```
```text
Query
  data: measurement
32 matches, first 5:
┌─────────────────────────────────┬─────────────────────────────────┐
│ data                            ┆ data.label                      │
│ ---                             ┆ ---                             │
│ str                             ┆ str                             │
╞═════════════════════════════════╪═════════════════════════════════╡
│ wbs:intake-in-flow-rate-m3s     ┆ intake in flow rate m3s         │
│ wbs:P2-mechanical-power         ┆ P2 mechanical power             │
│ wbs:conn-cartridge-filtration-… ┆ conn cartridge filtration to S… │
│ wbs:conn-cartridge-filtration-… ┆ conn cartridge filtration to S… │
│ wbs:RO-in-flow-mass-water       ┆ RO in flow mass water           │
└─────────────────────────────────┴─────────────────────────────────┘
```

`options()` lists what is measured, with the number of streams for each:

```python
acq.query().measurement().options("quantity_kind")
```
```text
shape: (8, 2)
┌────────────────────────────────┬───────┐
│ quantity_kind                  ┆ count │
│ ---                            ┆ ---   │
│ str                            ┆ i64   │
╞════════════════════════════════╪═══════╡
│ quantitykind:MassFlowRate      ┆ 10    │
│ quantitykind:MassConcentration ┆ 6     │
│ quantitykind:Pressure          ┆ 6     │
│ quantitykind:Efficiency        ┆ 3     │
│ quantitykind:Power             ┆ 2     │
│ quantitykind:Temperature       ┆ 2     │
│ quantitykind:VolumeFlowRate    ┆ 2     │
│ quantitykind:Area              ┆ 1     │
└────────────────────────────────┴───────┘
```

[Explore a plant model](explore-a-model.md) continues from here.

## Reading data

Let's read the power of the pumps:

```python
q = acq.query().entity("pump").measurement(quantity_kind="power")
print(q)
```
```text
Query
  pump: entity, Pump
  pump_data: measurement, of pump, quantity_kind=Power
2 matches:
┌────────┬─────────────────────────┬─────────────────────┐
│ pump   ┆ pump_data               ┆ pump_data.label     │
│ ---    ┆ ---                     ┆ ---                 │
│ str    ┆ str                     ┆ str                 │
╞════════╪═════════════════════════╪═════════════════════╡
│ wbs:P2 ┆ wbs:P2-mechanical-power ┆ P2 mechanical power │
│ wbs:P1 ┆ wbs:P1-mechanical-power ┆ P1 mechanical power │
└────────┴─────────────────────────┴─────────────────────┘
```

```python
q.dataframe(start="2025-03-01", end="2025-03-01 03:00")
```
```text
shape: (4, 3)
┌─────────────────────────┬────────────────────────────────┬────────────────────────────────┐
│ time                    ┆ pump_data__P1 mechanical power ┆ pump_data__P2 mechanical power │
│ ---                     ┆ ---                            ┆ ---                            │
│ datetime[μs, UTC]       ┆ f64                            ┆ f64                            │
╞═════════════════════════╪════════════════════════════════╪════════════════════════════════╡
│ 2025-03-01 00:00:00 UTC ┆ 1.1748e6                       ┆ 110820.706632                  │
│ 2025-03-01 01:00:00 UTC ┆ 1.1752e6                       ┆ 111000.225434                  │
│ 2025-03-01 02:00:00 UTC ┆ 1.1535e6                       ┆ 100851.37612                   │
│ 2025-03-01 03:00:00 UTC ┆ 1.1490e6                       ┆ 106574.849974                  │
└─────────────────────────┴────────────────────────────────┴────────────────────────────────┘
```

Note that a shared server can hold years of data, and `dataframe()` without `start` and `end` downloads all of it for the selected streams.
Give a time range, and select the streams you need before reading.

## When the server refuses

The server checks the token on every request.
The text after `response body:` in the errors below is written by the person who runs the server, so yours can differ.

**The token is wrong or missing.**
`Acquirium` asks the server whether it is up when it connects, and retries for 60 seconds.
A refused token fails this check, so the error arrives after the wait and it is a `ConnectionError` with `401` in its message.
`health_timeout` shortens the wait:

```python
acq = Acquirium("https://plant.example.org/api", token="wrong", health_timeout=5)
```
```text
ConnectionError: Acquirium server at https://plant.example.org/api did not answer /health within 5s (last error: 401 Client Error: Unauthorized for url: https://plant.example.org/api/health; response body: missing or invalid access token). Is the server running? Start it with: acquirium server --config <config.toml>
```

Check that `ACQUIRIUM_TOKEN` is set in the terminal that started Python, and that the token was copied completely.

**The token is not allowed to make the call.**
A token can be limited to reading.
A call that writes then fails with `403`:

```python
acq.insert_timeseries("csv-data", "intake-in-flow-rate-m3s", [("2026-01-01T00:00:00Z", 1.0)])
```
```text
requests.exceptions.HTTPError: 403 Client Error: Forbidden for url: https://plant.example.org/api/insert_timeseries; response body: your access level does not allow this call
```

Ask the person who runs the server for a token that is allowed to write.

## Checking the token without Python

The token is sent in the `Authorization` header as `Bearer <token>`.
This is useful for testing a token from the terminal, with the [HTTP API](../reference/server/http-api.md):

```bash
curl -H "Authorization: Bearer $ACQUIRIUM_TOKEN" https://plant.example.org/api/health
```
```text
{"ok":true}
```

## If you run the server

**NOTE:** We're working on a built-in authentication system for Acquirium, which will be available in a future version.
Currently Acquirium does not check tokens itself.
Instead we use a reverse proxy, nginx, that is placed in front of the server and forwards only the requests with a known token.
Contact maintainers for more information.

