---
title: Deploying
---

These client methods try out an app, hand it to the server, and report on it afterwards. An app can also be deployed from the configuration file with an `[[apps]]` entry. The examples use `acq = aq.init()`, see [Starting a client](../client/starting.md).

| Item | Purpose |
|---|---|
| [`Acquirium.check_app`](#acquiriumcheck_app) | Run an app once on stored data and return the result, saving nothing |
| [`Acquirium.deploy_app`](#acquiriumdeploy_app) | Deploy an app, or replace a deployed one |
| [`Acquirium.list_apps`](#acquiriumlist_apps) | List the deployed apps |
| [`Acquirium.inspect_app`](#acquiriuminspect_app) | Show one deployed app in detail |
| [`Acquirium.reprocess_app`](#acquiriumreprocess_app) | Compute a span of output times again |
| [`Acquirium.remove_app`](#acquiriumremove_app) | Remove a deployed app |
| [`Acquirium.app_dag`](#acquiriumapp_dag) | The deployed apps and their dependencies as a graph |
| [The `[[apps]]` table](#the-apps-table) | Deploy apps when the server starts |
| [Status values](#status-values) | What `idle`, `pending`, `failed` and the others mean |

The examples use an app class `Doubler` with the name `doubler` in a module `probe_app`. It multiplies every matched stream by `factor`, which defaults to `2.0`, and writes the result to the port `doubled`.

## Where the app code must be

The server runs the app, so the server must be able to import its module. Only the module name, the class name and a fingerprint of the module file are sent.

- Put the module in the directory of the server's configuration file, or install it as a package on the server.
- The module file on the server must be identical to the one on the client. A different file is rejected with a `digest mismatch` error.
- A class defined in a script that is run directly (module `__main__`) or inside a function cannot be deployed.

`check_app` is more lenient: it also tells the server the directory of the module, which works when client and server share a file system.

## `Acquirium.check_app`

```python
Acquirium.check_app(target, *, parameters=None, limit=None, search_path=None)
```

Runs an app on the server against all stored input data and returns what it computed. Nothing is saved: the app is not deployed and no output stream is created. Use it to confirm that the query matches the expected streams and that `transform` produces the expected values.

**Parameters**

- `target` (`type`): the app class, not an instance.
- `parameters` (`dict`, optional): keyword arguments for the app's constructor.
- `limit` (`int`, optional): return only the first `limit` rows of each output. By default every row is returned.
- `search_path` (`str`, optional): directory in which the server looks for the app's module. Defaults to the directory of the module file. Pass `""` to send none.

**Returns**

`dict` with these keys:

| Key | Meaning |
|---|---|
| `app` | Name of the app |
| `graph_revision` | Version of the graph the query ran on |
| `bindings` | One entry for each call the app would make |

Each entry of `bindings` has:

| Key | Meaning |
|---|---|
| `inputs` | For each alias, the matched streams with `stream_uri`, `label` and `unit` |
| `row` | The match of this call, or `None` for a call over all matches |
| `input_rows` | For each alias, the number of rows read |
| `read_window` | Start and end of the data that was read |
| `outputs` | For each port: `stream` (URI), `stream_name`, `value_kind`, `rows` (number of rows computed), `assigned` (whether `transform` assigned the port), `truncated` (whether `limit` cut the list) and `values` (a list of `{"time": ..., "value": ...}`) |
| `error` | `None`, or the error text. An exception raised in `transform` is reported here. `"no stored data for these inputs"` means the matched streams are empty |

**Raises**

- `ValueError`: when `target` is not an app class, declares no outputs, or is defined in `__main__`.
- `requests.HTTPError`: when the server rejects the request, for example because it cannot import the module, the query fails, or `limit` is negative. The message holds the reason.

**Example**

```python
from probe_app import Doubler

result = acq.check_app(Doubler, limit=2)
result["bindings"][0]["outputs"]
```

```text
{'doubled': {'stream': 'urn:acquirium#3f944465-d988-5dde-9466-cabe4a668b98',
  'stream_name': 'doubler:doubled:37f7f05f808ecc3becb9f997b7a408d1179959718b25b4392d961d630a2d51d8',
  'value_kind': 'numeric',
  'rows': 4,
  'assigned': True,
  'truncated': True,
  'values': [{'time': '2025-12-31T00:00:00+00:00', 'value': 40.0},
   {'time': '2026-01-01T00:00:00+00:00', 'value': 43.0}]}}
```

To step through `transform` with a debugger, run the check in your own process with [`acquirium app check --local`](cli.md#acquirium-app-check).

## `Acquirium.deploy_app`

```python
Acquirium.deploy_app(target, *, parameters=None)
```

Deploys an app. The server stores the definition, runs the query, and from then on calls `transform` whenever the inputs change. The deployment survives a restart of the server.

Deploying a name that is already deployed replaces the definition. The output streams and the record of what was already processed are kept.

**Parameters**

- `target` (`type`): the app class, not an instance.
- `parameters` (`dict`, optional): keyword arguments for the app's constructor. They are stored with the deployment and must be JSON values.

**Returns**

`dict`, for example `{'ok': True, 'name': 'doubler', 'status': 'deployed'}`.

**Raises**

- `ValueError`: when `target` is not an app class, declares no outputs, has no valid `grouping`, or is defined in `__main__`.
- `TypeError`: when an entry of `outputs` was not made with `aq.output.stream` or `aq.output.named`.
- `requests.HTTPError`: when the server rejects the deployment. Reasons include: the module cannot be imported, the module file differs, `build_query` does not return a query, a named output is combined with `"per_match"` and several matches, two apps write the same stream, or apps read each other's outputs in a circle.

Note that a rejected deployment changes nothing. An app that was deployed under the same name before keeps running with its old definition.

**Example**

```python
acq.deploy_app(Doubler, parameters={"factor": 3.0})
```

```text
{'ok': True, 'name': 'doubler', 'status': 'deployed'}
```

## `Acquirium.list_apps`

```python
Acquirium.list_apps()
```

Lists every deployed app, including apps whose query matches nothing and apps with an error.

**Returns**

`dict` with the key `apps`, a list with one entry per app:

| Key | Meaning |
|---|---|
| `name` | Name of the app |
| `entrypoint` | Module and class, as `module:Class` |
| `grouping` | `per_match` or `all_matches` |
| `status` | See [Status values](#status-values) |
| `binding_count` | Number of calls the app is split into: one per match for `per_match`, one for `all_matches` |
| `binding_statuses` | How many of them are in each status |
| `graph_revision` | Version of the graph the app was last planned on |
| `plan_current` | `False` while the graph has changed and the app has not been planned again yet |
| `error` | `None`, or the planning error |

**Example**

```python
acq.list_apps()["apps"][0]
```

```text
{'name': 'doubler',
 'entrypoint': 'probe_app:Doubler',
 'grouping': 'per_match',
 'status': 'idle',
 'binding_count': 2,
 'binding_statuses': {'idle': 2},
 'graph_revision': 13,
 'plan_current': True,
 'error': None}
```

## `Acquirium.inspect_app`

```python
Acquirium.inspect_app(name)
```

Returns the details of one deployed app: its settings, its outputs, the streams it reads and writes, and its progress.

**Parameters**

- `name` (`str`): name of the deployed app.

**Returns**

`dict` with the key `app`. It holds the keys of a [`list_apps`](#acquiriumlist_apps) entry, and:

| Key | Meaning |
|---|---|
| `definition` | The stored definition: `name`, `entrypoint`, `executable_digest`, `grouping`, `parameters`, `backfill`, `outputs`, and the durations `every`, `lookback`, `lookahead`, `batch_delay`, `min_interval` in microseconds. `lookback` may be `"all"` |
| `output_schemas` | For each port, the type of the `time` and `value` columns |
| `bindings` | One entry for each call the app is split into |

Each entry of `bindings` has:

| Key | Meaning |
|---|---|
| `binding_signature` | Identifier of the entry |
| `inputs` | For each alias, the URIs of the input streams |
| `outputs` | For each port, the URI of the output stream |
| `status` | See [Status values](#status-values) |
| `consumed_revision`, `current_revision` | How far the app has processed, and the latest version of the stored data. Equal numbers mean the app is up to date |
| `last_success` | Time of the last successful call since the server started, or `None` |
| `error` | `None`, or the error of the last failed call |
| `lookback`, `backfill` | The settings in use |

**Raises**

- `requests.HTTPError`: with status 404 when no app has this name.

**Example**

```python
info = acq.inspect_app("doubler")["app"]
info["definition"]["parameters"], info["bindings"][0]["outputs"]
```

```text
({'factor': 3.0}, {'doubled': 'urn:acquirium#3f944465-d988-5dde-9466-cabe4a668b98'})
```

## `Acquirium.reprocess_app`

```python
Acquirium.reprocess_app(name, start, end)
```

Asks the server to compute the outputs of an app again for a span of time. The work is done in the background. With `every` set, the span is widened to whole buckets.

**Parameters**

- `name` (`str`): name of the deployed app.
- `start` (`datetime`): start of the span. A datetime without a time zone is read as UTC.
- `end` (`datetime`): end of the span.

**Returns**

`dict`, for example `{'ok': True, 'name': 'doubler', 'status': 'reprocessing', 'bindings': 2}`.

**Raises**

- `requests.HTTPError`: with status 404 when no app has this name, and with status 400 when `end` is before `start`.

**Example**

```python
from datetime import datetime, timezone

acq.reprocess_app(
    "doubler",
    datetime(2026, 1, 1, tzinfo=timezone.utc),
    datetime(2026, 1, 2, tzinfo=timezone.utc),
)
```

```text
{'ok': True, 'name': 'doubler', 'status': 'reprocessing', 'bindings': 2}
```

## `Acquirium.remove_app`

```python
Acquirium.remove_app(name)
```

Removes a deployed app. The server stops running it and forgets what it had processed.

Note that the values already written to the output streams stay in storage and can still be read by stream URI. The output streams no longer appear in queries.

**Parameters**

- `name` (`str`): name of the deployed app.

**Returns**

`dict`, for example `{'ok': True, 'name': 'doubler', 'status': 'removed'}`.

**Raises**

- `requests.HTTPError`: with status 404 when no app has this name.

**Example**

```python
acq.remove_app("doubler")
```

```text
{'ok': True, 'name': 'doubler', 'status': 'removed'}
```

## `Acquirium.app_dag`

```python
Acquirium.app_dag()
```

Returns the deployed apps as a directed graph. Each node is one entry of `bindings` as in [`inspect_app`](#acquiriuminspect_app). An edge from A to B means that B reads a stream that A writes.

**Returns**

`networkx.DiGraph`. Nodes are keyed by `binding_signature` and carry the attributes `application_name`, `inputs`, `outputs`, `lookback`, `backfill`, `consumed_revision`, `current_revision`, `status`, `last_success` and `error`. Edges carry `stream_uris`, the list of streams passed along.

**Example**

```python
graph = acq.app_dag()
graph.number_of_nodes(), graph.number_of_edges()
```

```text
(3, 0)
```

## The `[[apps]]` table

Apps listed in the server's configuration file are deployed each time the server starts, after the configured drivers. See [the server configuration reference](../server/configuration.md).

```toml
[[apps]]
spec   = "tank_apps.py:Scale"
factor = 3.0
```

| Key | Type | Default | Meaning |
|---|---|---|---|
| `spec` | `str` | required | The app class, as `file.py:ClassName` or `package.module:ClassName`. A relative file path is resolved against the directory of the configuration file. An entry without `spec` is skipped with a warning |
| `name` | `str` | none | Used only in the server's log lines. It does not rename the app; the name comes from the class |
| any other key | | | Passed to the app's constructor as a keyword argument |

Note that a file is imported under its file name without `.py`. That name must be a valid Python module name and must not be the name of an installed module or of another app file.

`spec` may also name a function in place of a class. The server calls it as `function(client, parameters)`, where `client` is an `Acquirium` client and `parameters` holds the other keys of the entry. The function returns an app class, a list of app classes, or `None` after deploying apps itself. Classes it returns are deployed without constructor parameters.

An entry that fails is written to the server log. The server and the other entries are not affected.

Three keys of the `[server]` table control how apps are run:

| Key | Type | Default | Meaning |
|---|---|---|---|
| `materialization_workers` | `int` | `2` | Largest number of `transform` calls that run at the same time |
| `materialization_poll_seconds` | `float` | `0.25` | Seconds the server waits before looking for new work when there was none |
| `materialization_error_log_seconds` | `float` | `30.0` | Shortest time between two log lines that report failing apps |

## Status values

Each entry of `bindings` has one of these statuses:

| Status | Meaning |
|---|---|
| `idle` | Up to date |
| `pending` | Stored data has changed since the last call. The change may be in streams the app does not read |
| `running` | A call is in progress |
| `reprocessing` | Work requested by `reprocess_app`, or a large backfill, is in progress |
| `waiting` | An app whose output this one reads has failed |
| `failed` | The last call raised an error |

The status of an app is derived from them:

| Status | Meaning |
|---|---|
| `failed` | The app could not be planned, for example because its module cannot be imported, or one of its entries failed |
| `planning` | The graph changed and the app has not been planned again yet |
| `running`, `reprocessing`, `waiting`, `pending`, `idle` | The first of these that any entry has, in this order |
| `no_matches` | The query matches no stream |
