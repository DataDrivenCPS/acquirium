---
title: Starting and stopping
---

`acquirium.init()` is the usual way to get a client. It starts a server on this computer when none is running for the data directory, or connects to one that is. The `Acquirium` class connects to a server that is already running and never starts one.

| Item | Purpose |
|---|---|
| [`acquirium.init`](#acquiriuminit) | Return a client, starting a local server when needed |
| [`acquirium.shutdown`](#acquiriumshutdown) | Disconnect, and stop the server this process started |
| [`Acquirium`](#acquirium) | Connect to a server that is already running |
| [`Acquirium.address`](#acquiriumaddress) | Address of the server |
| [Other attributes](#other-attributes) | `client`, `study`, `insert_batch_rows` |

## `acquirium.init`

```python
acquirium.init(config=None, *, data_dir=None, address=None, exact_only=None,
               timeout=600.0)
```

Returns a client. Without `address`, it uses a server on this computer that stores its data in one data directory. If no server is running for that directory, `init` starts one and waits until it is ready.

Scripts that use the same data directory share one server. The script that started the server owns it. The server stops when that script calls `shutdown()` or exits, even if other scripts are still connected.

With no arguments, `init` reads `acquirium.toml` from the current directory when the file exists. Otherwise it uses the defaults: the data directory is `.acquirium` in the current directory, and text matching is exact only.

Calling `init` again in the same process with the same options returns the same client.

`init` prints its progress to standard error. The log of a server it started is at `<data_dir>/.runtime/server.log`.

**Parameters**

- `config` (`str` or `Path`, optional): path of a configuration file. Paths inside the file are relative to the file's directory. See the [server configuration](../server/configuration.md).
- `data_dir` (`str` or `Path`, optional): directory that holds the data. It is created when missing. When given, `acquirium.toml` in the current directory is not read and the defaults are used. It cannot be combined with `config`. Set `data_dir` in the configuration file instead.
- `address` (`str`, optional): URL of a server that is managed separately, such as `"http://10.0.0.5:8000"`. `init` connects to it and does not start anything. It cannot be combined with `config`, `data_dir` or `exact_only`.
- `exact_only` (`bool`, optional): when `True`, text such as `"gal/min"` is matched to a URI only by exact lookup, and no language model is loaded. When `False`, the server also matches by similarity. Defaults to the value in the configuration, which is `True` unless the file sets it.
- `timeout` (`float`, default `600.0`): seconds to wait for the server to be ready. Note that a server started with `exact_only=False` builds its text matching indexes on first start. This can take several minutes, so the wait is raised to at least one hour in that case.

**Returns**

An [`Acquirium`](#acquirium) client.

**Raises**

- `ValueError`: when `timeout` is not a positive number.
- `ValueError`: when `address` is combined with `config`, `data_dir` or `exact_only`, or `config` is combined with `data_dir`.
- `ValueError`: when `address` is not an `http` or `https` URL, or it contains a user name, a path or a query.
- `ValueError`: when the process already called `init` with different options. Call `shutdown()` first.
- `ValueError`: when the configuration sets `recreate = true`, `enabled = false` or more than one worker in `[server]`.
- `ValueError`: when a server is already running for the data directory with a different configuration or a different `exact_only`.
- `FileNotFoundError`: when `config` does not exist.
- `RuntimeError`: when the server it started exits before it is ready. The message gives the path of the log.
- `TimeoutError`: when the local server is not ready within `timeout` seconds.
- `ConnectionError`: when the server at `address` does not answer within `timeout` seconds.

**Example**

```python
import tempfile
import acquirium as aq

acq = aq.init(data_dir=tempfile.mkdtemp())
print(acq.graph_version())
aq.shutdown()
```

```text
Acquirium: starting a local server for /tmp/tmp1w8h9_s8
Acquirium: ready at http://127.0.0.1:50737
6
```

To connect to a server that runs elsewhere:

```python
acq = aq.init(address="http://127.0.0.1:8000")
```

## `acquirium.shutdown`

```python
acquirium.shutdown()
```

Closes the client that `init` returned. If this process started the server, the server is stopped too. The data stays on disk, and the next `init` with the same data directory finds it again.

A server that another script started, or one reached with `address`, keeps running. Calling `shutdown` when `init` was not called does nothing.

Python calls `shutdown` when the script exits, so calling it yourself is only needed to stop the server earlier or to call `init` with other options. Note that in a notebook this happens when the kernel exits, not when a cell finishes.

Be aware that the server keeps running when the script that started it is killed or crashes, because `shutdown` is not called in that case. The next `init` with the same data directory connects to that server and does not own it, so its `shutdown` does not stop the server. Such a server is stopped by ending its process.

**Returns**

`None`.

**Example**

```python
import acquirium as aq

acq = aq.init(data_dir="plant-data")
aq.shutdown()
acq.graph_version()
```

```text
requests.exceptions.ConnectionError: HTTPConnectionPool(host='127.0.0.1', port=50737): Max retries exceeded with url: /graph_version ...
```

## `Acquirium`

```python
Acquirium(server_url='127.0.0.1', server_port=8000, use_ssl=False,
          insert_batch_rows=50000, health_timeout=60.0)
```

Connects to a server that is already running. It does not start a server and `shutdown()` does not affect it. Use it when the server is started with `acquirium server`, or inside code that receives the host and port separately.

The constructor asks the server whether it is up and retries until `health_timeout` passes.

**Parameters**

- `server_url` (`str`, default `'127.0.0.1'`): host name or IP address of the server, without `http://` and without a port.
- `server_port` (`int`, default `8000`): port of the server.
- `use_ssl` (`bool`, default `False`): connect with `https` instead of `http`.
- `insert_batch_rows` (`int`, default `50000`): largest number of rows [`insert_timeseries_batch`](timeseries.md#acquiriuminsert_timeseries_batch) sends in one request.
- `health_timeout` (`float` or `None`, default `60.0`): seconds to wait for the server to answer. `None` or `0` skips the check.

**Raises**

- `ValueError`: when `insert_batch_rows` is zero or negative.
- `ConnectionError`: when the server does not answer within `health_timeout` seconds.

**Example**

```python
from acquirium import Acquirium

acq = Acquirium(server_port=1, health_timeout=1)
```

```text
ConnectionError: Acquirium server at http://127.0.0.1:1 did not answer /health within 1s (last error: ...). Is the server running? Start it with: acquirium server --config <config.toml>
```

## `Acquirium.address`

```python
Acquirium.address
```

The address of the server the client is connected to, as a string. This is useful for passing a server started by `init` to another program, since `init` picks a free port.

**Example**

```python
print(acq.address)
```

```text
http://127.0.0.1:50737
```

## Other attributes

- `Acquirium.client`: the lower-level HTTP client that the methods on these pages call. It has a few methods with no counterpart on `Acquirium`, such as `sparql_query` (see [Graph](graph.md#reading-the-graph)).
- `Acquirium.study`: the entry point for experiments, with `define`, `get` and `all`. See the [experiment reference](../experiments/_index.md).
- `Acquirium.insert_batch_rows` (`int`): the value given to the constructor. It can be changed at any time.
