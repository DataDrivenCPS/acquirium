---
title: Command line
---

The `acquirium app` commands check an app and report on deployed apps from a terminal. They talk to a running server. There is no command that deploys or removes an app; use [`deploy_app`](deploying.md#acquiriumdeploy_app), [`remove_app`](deploying.md#acquiriumremove_app) or the [`[[apps]]` table](deploying.md#the-apps-table) for that. The other `acquirium` commands are described in [the server CLI reference](../server/cli.md).

| Item | Purpose |
|---|---|
| [`acquirium app check`](#acquirium-app-check) | Run an app once on stored data and print the result, saving nothing |
| [`acquirium app list`](#acquirium-app-list) | List the deployed apps |
| [`acquirium app inspect`](#acquirium-app-inspect) | Show one deployed app in detail |

## Options shared by all three commands

| Option | Type | Default | Meaning |
|---|---|---|---|
| `--config`, `-c` | path | `./acquirium.toml` when it exists | Configuration file to read the server address from |
| `--server-url` | text | `[driver] server_url`, else `[server] host`, else `127.0.0.1` | Host of the server |
| `--server-port` | integer | `[driver] server_port`, else `[server] port`, else `8000` | Port of the server |
| `--json` | flag | off | Print the full result as JSON |
| `--help` | flag | | Show the help text |

Note that the address comes from the `[driver]` table of the configuration file, see [driver configuration](../drivers/configuration.md#connection-keys). `[driver] use_ssl = true` selects `https`.

## `acquirium app check`

```text
acquirium app check [OPTIONS] SPEC
```

Runs an app against the stored data and prints the matched input streams and the computed rows. Nothing is saved. It is the command form of [`check_app`](deploying.md#acquiriumcheck_app).

**Parameters**

- `SPEC`: the app class, as `module:ClassName` or `path/to/file.py:ClassName`. A file name must be a valid Python module name.
- `--params TEXT`: constructor parameters as a JSON object, for example `'{"factor": 3}'`.
- `--limit`, `-n INTEGER`: print only the first N rows of each output and the first N streams of each alias. `0` prints all of them. Default `5`.
- `--local`: run the app in this process and not on the server. The input data is read from the server. A `breakpoint()` in `transform` then opens in this terminal, and an error in `transform` is shown with its traceback.
- The [shared options](#options-shared-by-all-three-commands).

Without `--local`, the server imports the app. For a file `SPEC` the command tells the server the directory of the file. When the server runs on another machine and cannot read that directory, copy the file to the server or use `--local`.

**Returns**

Exit code `0` when every call succeeded. Exit code `1` when the app could not be loaded, the server could not be reached, the check was rejected, or a call reported an error.

**Example**

```bash
acquirium app check probe_app.py:Doubler -n 1 --params '{"factor": 10}'
```

```text
doubler: 2 input group(s) matched

[1] inputs
      input: 1 stream(s), 2 rows read
        - Tank temperature
    output 'doubled' -> doubler:doubled:37f7f05f808ecc3becb9f997b7a408d1179959718b25b4392d961d630a2d51d8 (numeric, 2 rows)
        2026-01-01T00:00:00+00:00  215.0
        … 1 more row(s); pass -n 0 for all of them

[2] inputs
      input: 1 stream(s), 2 rows read
        - flow
    output 'doubled' -> doubler:doubled:93a53f263192218dbb00f9749dd528071f2e41476cbea495c539deebbd78ca5e (numeric, 2 rows)
        2026-01-01T00:00:30+00:00  30.0
        … 1 more row(s); pass -n 0 for all of them
```

Each numbered block is one call the app would make. An error in a call is printed as `error: ...` in its block.

## `acquirium app list`

```text
acquirium app list [OPTIONS]
```

Prints one line for each deployed app: name, status, grouping and the number of calls it is split into. An app with a planning error gets a second line with the error. It is the command form of [`list_apps`](deploying.md#acquiriumlist_apps).

**Parameters**

- The [shared options](#options-shared-by-all-three-commands).

**Returns**

Exit code `0`, or `1` when the server cannot be reached.

**Example**

```bash
acquirium app list
```

```text
doubler  idle  grouping=per_match  bindings=2
total  idle  grouping=all_matches  bindings=1
```

The status values are explained in [Status values](deploying.md#status-values). Without deployed apps the command prints `No apps deployed.`

## `acquirium app inspect`

```text
acquirium app inspect [OPTIONS] NAME
```

Prints the settings, the declared outputs, the input and output streams and the progress of one deployed app. It is the command form of [`inspect_app`](deploying.md#acquiriuminspect_app).

**Parameters**

- `NAME`: name of the deployed app, as shown by `acquirium app list`.
- The [shared options](#options-shared-by-all-three-commands).

**Returns**

Exit code `0`. Exit code `1` when no app has this name or the server cannot be reached.

**Example**

```bash
acquirium app inspect total
```

```text
total: idle
Entrypoint: probe_app:Total
Grouping: all_matches
Plan: graph revision 10, current=True
every: 60s
lookback: 0s
lookahead: 0s
batch_delay: 0s
min_interval: disabled
backfill: True
parameters: {}
Declared outputs:
  sum: numeric, unit=(unspecified)
    schema: time: timestamp[us, tz=UTC] (non-null), value: float64 (non-null)
    metadata: {"data_source": null, "label": "Total", "medium": null, "properties": null, "property_uri": null, "quantity_kind": null, "stream_name": "total-sum", "substance": null, "unit": null, "value_kind": "numeric"}
Bindings: 1
  45d24406c4d680f9bb28d3cc9073606a621221d98c284b846eb535aeeecf013a: idle
    inputs: {"input": ["urn:acquirium#ad5dd93c-8801-5bbc-a45f-fa2060adc28d", "urn:acquirium#c640b603-534f-5e27-967d-ca110027a66c"]}
    outputs: {"sum": "urn:acquirium#3f08c47c-e0c9-588c-a297-2944be98ddc8"}
    revision: 3 / 3; last success: 2026-11-18T00:57:05.660678+00:00
```

`revision: 3 / 3` shows how far the app has processed and the latest version of the stored data. Equal numbers mean the app is up to date.
