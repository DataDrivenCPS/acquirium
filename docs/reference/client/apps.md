---
title: Apps
---

An app is a class that computes new streams from existing ones on the server. The client checks, deploys and inspects apps. This page lists the client methods only. Writing an app, and the content of the dictionaries these methods return, are described in the [app reference](../apps/_index.md).

| Item | Purpose |
|---|---|
| [`Acquirium.check_app`](#acquiriumcheck_app) | Run an app without saving anything |
| [`Acquirium.deploy_app`](#acquiriumdeploy_app) | Deploy an app |
| [`Acquirium.list_apps`](#acquiriumlist_apps) | List deployed apps |
| [`Acquirium.inspect_app`](#acquiriuminspect_app) | Details of one deployed app |
| [`Acquirium.reprocess_app`](#acquiriumreprocess_app) | Recompute a time range of an app's output |
| [`Acquirium.remove_app`](#acquiriumremove_app) | Remove a deployed app |
| [`Acquirium.app_dag`](#acquiriumapp_dag) | The deployed apps as a dependency graph |

Every method on this page except `app_dag` returns a `dict` whose `ok` key is `True`. `inspect_app`, `reprocess_app` and `remove_app` raise `requests.exceptions.HTTPError` when no deployed app has the given name.

## `Acquirium.check_app`

```python
Acquirium.check_app(target, *, parameters=None, limit=None, search_path=None)
```

Runs an app against the stored data and returns what it computed. Nothing is saved: the app is not deployed and its output streams are not created. This is useful for confirming that an app matches the streams you expect before deploying it.

**Parameters**

- `target` (class): the app, a subclass of `acquirium.App`. Pass the class, not an instance.
- `parameters` (`dict`, optional): values for the app's parameters.
- `limit` (`int`, optional): keep only the first `limit` rows of each output. By default every computed row is returned.
- `search_path` (`str`, optional): directory in which the server looks for the module that defines the app. Defaults to the directory of that module's file, which works when the server runs on the same computer. Pass `""` to send none.

**Returns**

A `dict` with `ok` and the computed outputs.

**Raises**

- `ValueError`: when `target` is not an `App` class.
- `requests.exceptions.HTTPError`: when the server cannot load or run the app.

## `Acquirium.deploy_app`

```python
Acquirium.deploy_app(target, *, parameters=None)
```

Deploys an app. The server then runs it as new values arrive in its input streams.

**Parameters**

- `target` (class): the app, a subclass of `acquirium.App`.
- `parameters` (`dict`, optional): values for the app's parameters.

**Returns**

A `dict`:

```python
{'ok': True, 'name': '<app name>', 'status': 'deployed'}
```

**Raises**

- `ValueError`: when `target` is not an `App` class.
- `requests.exceptions.HTTPError`: when the server rejects the app.

## `Acquirium.list_apps`

```python
Acquirium.list_apps()
```

Lists every deployed app, including apps that match no stream and apps that failed.

**Returns**

A `dict` whose `apps` key holds one summary per app. With no app deployed:

```python
{'ok': True, 'apps': []}
```

## `Acquirium.inspect_app`

```python
Acquirium.inspect_app(name)
```

Returns the details of one deployed app: its settings, its output streams and its progress.

**Parameters**

- `name` (`str`): name of the deployed app.

**Returns**

A `dict` whose `app` key holds the details.

**Example**

```python
acq.inspect_app("daily_mean")
```

```text
requests.exceptions.HTTPError: 404 Client Error: Not Found for url: http://127.0.0.1:50737/apps/daily_mean; response body: unknown app 'daily_mean'
```

## `Acquirium.reprocess_app`

```python
Acquirium.reprocess_app(name, start, end)
```

Recomputes the output of a deployed app for a time range. Values that arrive later are processed as before.

**Parameters**

- `name` (`str`): name of the deployed app.
- `start` (`datetime` or `str`): start of the range. A text such as `"2025-01-01"` is accepted. A time without a timezone is read as UTC.
- `end` (`datetime` or `str`): end of the range.

**Returns**

A `dict` with `ok` and a summary of what was scheduled.

## `Acquirium.remove_app`

```python
Acquirium.remove_app(name)
```

Removes a deployed app.

**Parameters**

- `name` (`str`): name of the deployed app.

**Returns**

A `dict`:

```python
{'ok': True, 'name': '<app name>', 'status': 'removed'}
```

## `Acquirium.app_dag`

```python
Acquirium.app_dag()
```

Returns the deployed apps as a directed graph that shows which app reads the output of which. This call needs the `networkx` package.

**Returns**

A `networkx.DiGraph`. Each node is one app bound to its matched streams. Each edge has a `stream_uris` attribute, the list of streams that the source node writes and the target node reads. With no app deployed the graph is empty.
