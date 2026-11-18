---
title: Command line
---

The `acquirium` command starts the server and manages what runs on it.
With uv, run it as `uv run acquirium ...`.

| command | what it does |
|---|---|
| [`acquirium server`](#acquirium-server) | start the server, with the drivers and apps of the config file |
| [`acquirium driver start`](#acquirium-driver-start) | start the drivers of a config file on a running server |
| [`acquirium driver list`](#acquirium-driver-list) | list the drivers running on a server |
| [`acquirium driver stop`](#acquirium-driver-stop) | stop one driver |
| [`acquirium app list`](#acquirium-app-list) | list the deployed apps |
| [`acquirium app inspect`](#acquirium-app-inspect) | show the details of one deployed app |
| [`acquirium app check`](#acquirium-app-check) | run an app on stored data without saving anything |
| [`acquirium config init`](#acquirium-config-init) | write a starter `acquirium.toml` |
| [`acquirium example init`](#acquirium-example-init) | create the files of the example plant |
| [`acquirium clear`](#acquirium-clear) | erase the stored data of a data folder |

Every command accepts `--help`.

## Choosing the server

The `driver` and `app` commands talk to a server that is already running.
They take the same three options to find it:

| option | meaning |
|---|---|
| `--config`, `-c` `<path>` | an `acquirium.toml`; the address is read from its `[driver]` section (`server_url`, `server_port`) |
| `--server-url` `<str>` | server host. Default: `driver.server_url` of the config, else `127.0.0.1` |
| `--server-port` `<int>` | server port. Default: `driver.server_port` of the config, else `8000` |

## `acquirium server`

```bash
acquirium server [--config PATH] [--host HOST] [--port PORT] [--reload] [--workers N] [--verbose]
```

Starts the server, then the drivers and the apps listed in the config file.
The command keeps running until it is stopped with Ctrl+C.

| option | meaning |
|---|---|
| `--config`, `-c` `<path>` | path to `acquirium.toml` (see [server configuration](configuration.md)) |
| `--host` `<str>` | host to listen on; overrides `server.host` |
| `--port`, `-p` `<int>` | port to listen on; overrides `server.port` |
| `--reload` | restart the server when the code changes, for development |
| `--workers`, `-w` `<int>` | number of worker processes. It must be 1, because the graph store can only be opened by one process |
| `--verbose`, `-v` | print DEBUG logs of Acquirium |

If the config has `server.enabled = false`, no server is started.
The drivers of the config are sent to the server named in its `[driver]` section, as `acquirium driver start` does.

```bash
acquirium server --config acquirium.toml
```

## `acquirium driver start`

```bash
acquirium driver start CONFIG [--server-url HOST] [--server-port PORT]
```

Starts every `[[drivers]]` entry of `CONFIG` on a running server, then exits.
The drivers keep running on the server.

| argument | meaning |
|---|---|
| `CONFIG` | path to an `acquirium.toml` with `[[drivers]]` entries. Required |

The command exits with status 1 when the config has no drivers, when the server cannot be reached, or when a driver fails to start.

```bash
acquirium driver start acquirium.toml
```

## `acquirium driver list`

```bash
acquirium driver list [--config PATH] [--server-url HOST] [--server-port PORT]
```

Lists the drivers running on the server, or prints `No drivers running.`

## `acquirium driver stop`

```bash
acquirium driver stop --name NAME [--config PATH] [--server-url HOST] [--server-port PORT]
```

Stops one driver.

| option | meaning |
|---|---|
| `--name` `<str>` | the driver name, as shown by `acquirium driver list`. Required |

## `acquirium app list`

```bash
acquirium app list [--json] [--config PATH] [--server-url HOST] [--server-port PORT]
```

Lists the deployed apps with their status, grouping and number of bindings.
Apps that matched nothing or failed to plan are listed too, with their error.

| option | meaning |
|---|---|
| `--json` | print the raw result instead of the summary |

## `acquirium app inspect`

```bash
acquirium app inspect NAME [--json] [--config PATH] [--server-url HOST] [--server-port PORT]
```

Shows one deployed app: its entrypoint, settings, declared outputs, and for each binding the input and output streams and its progress.

| argument or option | meaning |
|---|---|
| `NAME` | the app name, as shown by `acquirium app list`. Required |
| `--json` | print the raw result instead of the summary |

## `acquirium app check`

```bash
acquirium app check SPEC [--params JSON] [--limit N] [--local] [--json]
                         [--config PATH] [--server-url HOST] [--server-port PORT]
```

Runs an app on the stored data and prints what it computed.
Nothing is saved: the app is not deployed, its streams are not created, and no progress is recorded.

| argument or option | meaning |
|---|---|
| `SPEC` | the app class, as `module:ClassName` or `./file.py:ClassName`. Required |
| `--params` `<str>` | constructor parameters as JSON, for example `'{"threshold": 3}'` |
| `--limit`, `-n` `<int>` | number of rows printed per output. Default `5`; `0` prints every row |
| `--local` | run the app in this terminal instead of on the server |
| `--json` | print the raw result instead of the summary |

Without `--local` the server runs the app, so the server must be able to import the class.
With `--local` the app runs in your terminal and reads its inputs from the server.
This is useful for debugging: `breakpoint()` opens in your terminal, and an error shows its traceback.

```bash
acquirium app check plant_apps:Celsius --local
```

## `acquirium config init`

```bash
acquirium config init [PATH] [--force]
```

Writes a starter `acquirium.toml` with a local server and one example driver.

| argument or option | meaning |
|---|---|
| `PATH` | the file to write, or a folder to put `acquirium.toml` in. Default `acquirium.toml` |
| `--force`, `-f` | overwrite the file if it exists |

## `acquirium example init`

```bash
acquirium example init [PATH] [--step MINUTES] [--force]
```

Creates a folder with the files of the example plant, a seawater desalination plant.
It does not start a server.

- `model.ttl` is the plant model.
- `data.csv` is the data of the plant for the year 2025, with a `time` column and one column per stream.

| argument or option | meaning |
|---|---|
| `PATH` | the folder to create. Default `example` |
| `--step` `<int>` | minutes between two rows of `data.csv`. Default `60` |
| `--force`, `-f` | overwrite the two files if they exist |

```bash
acquirium example init
```
```text
Wrote example/model.ttl
Wrote example/data.csv (8760 rows, 32 streams)
```

The files are used in [5 - A Full Example](../../tutorials/a-full-example.md).

## `acquirium clear`

```bash
acquirium clear [PATH] [--all] [--yes] [--config PATH]
```

Erases the stored data of a data folder: the graph, the readings and everything else in it.
The embeddings of the text matcher are kept, because they take long to build and do not depend on your data.
It does not start a server.

| argument or option | meaning |
|---|---|
| `PATH` | the data folder to clear. Default: `server.data_dir` of the config, else `.acquirium` in the current folder |
| `--all` | also erase the embeddings, by removing the whole folder |
| `--yes`, `-y` | do not ask for confirmation |
| `--config`, `-c` `<path>` | an `acquirium.toml`; without it, `./acquirium.toml` is used if it exists |

The command exits with status 1 and erases nothing when:

- a server is using the folder. Stop it first.
- the folder does not look like an Acquirium data folder.
- the confirmation is declined.

```bash
acquirium clear
```
```text
Erase the stored data in /home/you/my-plant/.acquirium? [y/N]: y
Cleared /home/you/my-plant/.acquirium
```

Note that with the `timescale` backend the readings are stored in Postgres, and this command keeps them.
To erase them, start the server once with `recreate = true` (see [server configuration](configuration.md)).
