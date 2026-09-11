# Start Acquirium from a script

`aq.init()` starts a local Acquirium server and returns a client connected to
it. It loads `acquirium.toml` from the current directory when present, or uses
local DuckDB and Oxigraph defaults when there is no config. You can also choose
the config file explicitly:

```python
import acquirium as aq

ac = aq.init("my-custom-config.toml")
# ... use the normal Acquirium client ...
aq.shutdown()
```

The file has the same format as the CLI's config, including `[server]`,
`[ontologies]`, and `[[drivers]]`. Relative storage and ontology paths resolve
against the config file's directory. To use local defaults and a chosen data
directory without loading a config, pass `data_dir=`:

```python
import acquirium as aq

ac = aq.init(data_dir="./analysis-data")
try:
    ac.insert_graph(
        '<urn:example:pump> <http://www.w3.org/2000/01/rdf-schema#label> "Feed pump" .',
        source_id="plant",
    )
    print(ac.query().entity(uri="urn:example:pump").metadata())
finally:
    aq.shutdown()
```

The server runs in a background process and listens only on `127.0.0.1`, using
the configured server port or an automatically chosen port if omitted or zero.
The client uses the same HTTP API as a client connected to a separately managed
server. `ac.address` gives the server URL.

## Share a runtime between scripts

Two scripts calling `aq.init(data_dir="./analysis-data")` connect to the same
server, provided the paths resolve to the same directory. A startup lock
prevents simultaneous calls from opening separate servers on the same stores.
Different data directories get independent Acquirium servers.

The script that starts the server owns its lifetime. Its `aq.shutdown()` stops
the server, even if other scripts are attached. An attached script's shutdown
only closes its own client connection. To keep a server running independently
of any script, start it with `acquirium server` and connect explicitly:

```python
ac = aq.init(address="http://127.0.0.1:8000")
print(ac.address)  # http://127.0.0.1:8000
# ... use ac ...
aq.shutdown()  # disconnects; the separately managed server keeps running
```

`address` must be a complete HTTP or HTTPS origin, such as
`https://acquirium.example.org` or `https://acquirium.example.org:8443`.
It does not start, configure, or stop the remote server, and cannot be combined
with `config`, `data_dir`, or `exact_only`.

Each Python process has one initialized client. Repeated `aq.init()` calls
with the same options return it; changing the destination requires
`aq.shutdown()` first. If an owner stops the server you were using, call
`shutdown()` and `init()` again to reconnect or start a new server.

## Storage and startup options

| Argument | Default | Meaning |
|---|---|---|
| `config` | `./acquirium.toml` if present | Optional first positional argument; an explicitly named file must exist |
| `data_dir` | config value, or `./.acquirium` | Explicit argument selects local defaults without config discovery; cannot be combined with `config` |
| `address` | none | Connect to an existing HTTP(S) server instead of managing a local one |
| `exact_only` | config value, otherwise none | Use `True` to disable embeddings; overrides the config when supplied |
| `timeout` | `600` | Seconds to wait for startup or attachment |

First startup loads bundled ontologies and, unless `exact_only=True`, builds
embedding indexes. This may take several minutes. Later startups reuse the
stored data and caches. Omitting `exact_only` when attaching accepts the
existing server's mode; an explicitly conflicting mode raises an error.

Local startup uses the standard server lifecycle, including Ray for drivers
and apps. Configured drivers start in the background after the server becomes
healthy. Inherited `ACQUIRIUM_*` environment settings do not override the chosen
config or redirect its storage. The Timescale backend still requires an
available PostgreSQL service and its configured DSN (or `PG_DSN`).

The local runtime binds to loopback even if the config specifies another host.
If `[driver]` specifies a server address, it must be local HTTP on the same
port as `[server]`. Use the CLI for a server that listens on other interfaces.
`aq.init()` rejects `recreate=true`, `enabled=false`, and multiple server
workers. `address` cannot be combined with a config or local startup options.

Scripts sharing a data directory must agree on the config. Changing the config
while the server is running raises an error on attachment; stop its owner and
initialize again to apply the change. Different configurations must not point
their graph or DuckDB paths at the same files under different data directories.

## Shutdown and failures

`aq.shutdown()` is safe to call more than once. It closes the client and, for
an owned server, requests graceful shutdown of its drivers, apps, and stores.
The data directory is retained. A later script can open it again.

Normal interpreter exit also calls `shutdown()` through Python's `atexit`
mechanism. In a notebook, this means kernel exit, not completion of a cell;
call `shutdown()` when you want to release the server earlier.

Forced termination, a process crash, or `os._exit()` can bypass exit handlers
and leave the server running. A subsequent script can attach to that server,
but does not take ownership of it. For guaranteed cleanup around a handled
exception, use `try/finally` as in the example above. This convenience API does
not install signal handlers or track the lifetimes of attached clients.

Startup failures and timeouts name the log at
`<data_dir>/.runtime/server.log`. A failed startup stops any server process
that call created. Shutdown waits up to 30 seconds for an owned process to
exit, then warns and kills it if necessary; forced shutdown may interrupt
in-flight work. Runtime discovery files live under `.runtime`; OS file locks
are released if the server crashes, allowing a subsequent startup to recover.
