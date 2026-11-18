---
title: How drivers work
---

In this article we'll describe what happens inside the server when a driver runs: who calls its methods, where the values wait before they are inserted, what a driver remembers, and what happens on errors and restarts.

To write and start a driver, see [Writing a driver](../tutorials/advanced/writing-a-driver.md).
The classes, methods and config keys are in the [driver reference](../reference/drivers/_index.md).

## What the driver does and what the server does

Plant data comes from sources that behave differently: a historian that has to be asked, a broker that pushes values, a folder where files appear.
A driver is the code that knows one source: how to reach it, which streams it has and how to read their values.

Everything else is the same for every source, so the server does it: calling the driver on an interval, registering the streams, inserting the values in batches, saving the state and stopping the driver.
This keeps a driver short, and the server can run many drivers next to each other.

The design follows [sMAP](https://pythonhosted.org/Smap/en/2.0/tutorial.html), where a driver is also a small class per source that is listed in a config file.

## Where a driver runs

The server starts each driver in its own process.
The driver talks to the server with the same client a script uses, available as `self.aq`.
An error or a slow read in one driver does not stop the server or the other drivers.

- The drivers listed under `[[drivers]]` in the `acquirium.toml` of the server are started when the server is ready, one after another.
- `acquirium driver start` sends the drivers of a config file to a running server.

Note that the server only remembers the drivers of its own `acquirium.toml`.
A driver added with `acquirium driver start` has to be started again after the server restarts.

Be aware that the server imports the driver class, so the file has to be on the machine where the server runs.
The processes are started with Ray, and the first driver also starts Ray.
In our runs it took about half a minute from the start of the server until the first driver was running.

## The calls the server makes

The server calls `setup()` once, registers the declared streams, and then calls `tick()` immediately and after every `interval` seconds.
The base classes implement `tick()` for you:

- `PollingIngestDriver`: calls your `read()`, then inserts the values you added.
- `EventIngestDriver`: your callbacks add values at any time, and the tick inserts them.
- `FileIngestDriver`: finds the files of the folder, calls your `read()` for each file, and inserts what it returns.

The drivers are set up one at a time, so two drivers do not write to the graph at the same moment during `setup()`.
The full order of calls is in the [hooks reference](../reference/drivers/hooks.md#order-of-calls).

The server also checks if the graph has changed, every 10 seconds or every `interval` when that is longer, and calls `on_graph_change()` when it has.
This is useful for a driver that reads its streams from the plant model.

## Buffering

`add()` does not send anything.
It puts the value in a buffer in the memory of the driver, and the buffer is inserted with one request at the end of the tick.
A value is therefore visible in queries at most one `interval` after it was added.

Streams are registered the same way.
`declare()` only records the stream, and the new declarations are registered together before the next insert.

Note that the buffer holds at most 100,000 rows.
When the server cannot be reached for a long time and the buffer is full, `add()` raises `DriverBufferFull`.

## State

Every driver has `self.state`, a small key-value store that is saved to a JSON file after each change.
The file is in the `drivers` folder of the data folder.

For instance, this driver counts its reads:

```python
import random

import acquirium as aq


class TankDriver(aq.PollingIngestDriver):
    def setup(self):
        self.source_id = "tank"
        self.declare("level", unit="m", label="Tank level")
        self.reads = self.state.get("reads", 0)

    def read(self):
        self.reads += 1
        self.state.set("reads", self.reads)
        if self.reads == 3:
            raise RuntimeError("the sensor did not answer")
        self.add("level", 2.0 + random.random())
```

After the server was stopped, `.acquirium/drivers/TankDriver.json` contained:

```text
{
  "reads": 12
}
```

When the server started again, the driver continued from 13.

`FileIngestDriver` uses the state to remember how far each file was read.
The position of a file is saved after its rows are inserted.
If the driver stops between the two, the same rows are read again after the restart.
This does not create duplicates, because a row with the same stream and timestamp overwrites the old one.

Be aware that the buffer is not part of the state.
It is sent when the driver stops normally, but it is lost when the process is killed.

## Errors

The third read of the driver above raises an error.
The error is printed in the terminal of the server, and the driver tries again on the next interval:

```text
ERROR acquirium.driver.TankDriver tick error
Traceback (most recent call last):
  ...
  File ".../tank_driver.py", line 16, in read
    raise RuntimeError("the sensor did not answer")
RuntimeError: the sensor did not answer
```

`acquirium driver list` still shows the driver as `running`.

The other cases are handled as follows:

- An error in `setup()`: the driver is not started, and the start request reports the error.
- A failed insert, for example when the server is busy: the rows are put back in the buffer and sent again on the next tick.
- A file that cannot be read by a `FileIngestDriver`: the error is logged and the other files are still read.
- A driver process that ends with an error: `acquirium driver list` shows it as `failed`. It is not started again automatically.

## Stopping and restarting

`acquirium driver stop` ends the loop of the driver, calls its `stop()` and sends the rows still in the buffer.
Stopping the server with Ctrl+C does the same for every driver:

```text
INFO acquirium.driver.supervis Stopped driver 'TankDriver'
INFO:     Application shutdown complete.
```

A driver that has not finished after 10 seconds is ended by the server.

After a restart `setup()` runs again and the streams are registered again.
Registering a stream that already exists does not change its data, so a driver can be restarted at any time.

## Next

- [The stream lifecycle](stream-lifecycle.md) explains what registering a stream and inserting a row do in the server.
- [Driver status](../reference/drivers/configuration.md#driver-status) lists the states shown by `acquirium driver list`.
- [Persistent state](../reference/drivers/state.md) describes `self.state`.
