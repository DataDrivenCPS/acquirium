---
title: Persistent state
---

Every driver has `self.state`, a small key-value store that is saved to a JSON file after each change. It survives a restart of the driver or the server. This is useful for remembering how far a source has been read.

| Item | Purpose |
|---|---|
| [`DriverState`](#driverstate) | The class of `self.state` |
| [`DriverState.get`](#driverstateget) | Read a value |
| [`DriverState.set`](#driverstateset) | Write a value |
| [`DriverState.update`](#driverstateupdate) | Write several values |
| [`DriverState.delete`](#driverstatedelete) | Remove a key |
| [`DriverState.keys`](#driverstatekeys) | List the keys |
| [`DriverState.clear`](#driverstateclear) | Remove every key |

## `DriverState`

```python
DriverState(state_file)
```

Key-value store backed by one JSON file. A driver does not create it; the base class does and assigns it to `self.state`. It can be used from several threads.

**Parameters**

- `state_file` (`Path`): the JSON file. Missing parent directories are created on the first write.

The file of a driver is `<data_dir>/drivers/<identifier>.json`, where `<data_dir>` is [`Driver.data_dir()`](helpers.md#driverdata_dir). The identifier is the configuration key `driver_id` when it is set, otherwise the class name of the driver.

Note that two `[[drivers]]` entries that use the same class share one state file unless each sets its own `driver_id`. See [Configuration](configuration.md#keys-read-for-every-driver).

Note that `DriverState` is not a `dict`. `self.state["key"]` does not work; use `get` and `set`. `"key" in self.state` works. A file that cannot be read as a JSON object is treated as empty.

`from acquirium import DriverState` gives the module, not the class. Import the class with `from acquirium.DriverState import DriverState`.

**Example**

```python
d.state.set("last", 5)
d.state.get("last"), d.state.get("missing", 0), d.state.keys(), "last" in d.state
```

```text
(5, 0, ['last'], True)
```

## `DriverState.get`

```python
DriverState.get(key, default=None)
```

Returns the value stored under `key`.

**Parameters**

- `key` (`str`): the key to read.
- `default` (any): returned when the key does not exist.

**Returns**

The stored value, or `default`.

## `DriverState.set`

```python
DriverState.set(key, value)
```

Stores `value` under `key` and saves the file.

**Parameters**

- `key` (`str`): the key to write.
- `value` (any JSON value): the value to store. A value that JSON cannot represent is saved as its text form.

**Returns**

`None`.

## `DriverState.update`

```python
DriverState.update(data)
```

Stores several keys at once and saves the file.

**Parameters**

- `data` (`dict`): keys and values to store.

**Returns**

`None`.

## `DriverState.delete`

```python
DriverState.delete(key)
```

Removes `key` and saves the file.

**Parameters**

- `key` (`str`): the key to remove.

**Returns**

`bool`: `True` when the key existed.

## `DriverState.keys`

```python
DriverState.keys()
```

Returns every key in the store.

**Returns**

`list[str]`.

## `DriverState.clear`

```python
DriverState.clear()
```

Removes every key and saves the file.

**Returns**

`None`.
