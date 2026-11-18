---
title: Codecs and values
---

This page covers how Python values become stored data. Codecs save whole Python objects as bytes for `object` variables. Value handlers teach recording to convert your own types to JSON. `timestamp` normalizes times to UTC. All of these names are exported from `acquirium`.

| Item | Description |
|---|---|
| [`Codec`](#codec) | The interface a codec implements |
| [`PickleCodec`](#picklecodec) | Save objects with Python's pickle |
| [`CloudpickleCodec`](#cloudpicklecodec) | Save objects with cloudpickle |
| [`JsonCodec`](#jsoncodec) | Save a JSON document as a file |
| [`BytesCodec`](#bytescodec) | Save bytes unchanged |
| [Value conversion](#value-conversion) | How recorded values and metadata become JSON |
| [`register_value_handler`](#register_value_handler) | Convert your own type to JSON when it is recorded |
| [`timestamp`](#timestamp) | Parse a time and convert it to UTC |

A codec is used in two places. Declare an object variable with [`builder.object(codec=...)`](declaring-variables.md#builderobject), and read the object back with [`Observation.load`](reading.md#observationload). The server stores the bytes and never decodes them. A codec runs only in the Python process that records or loads.

## `Codec`

```python
class Codec(Protocol):
    media_type: str
    def dumps(self, value: Any) -> bytes: ...
    def loads(self, data: bytes) -> Any: ...
```

The interface of a codec. Any class with these members is a codec. It does not need to inherit from `Codec`.

| Member | Meaning |
|---|---|
| `media_type` | A non-empty string with the content type of the bytes, such as `"application/json"` |
| `dumps(value)` | Return the object as `bytes` |
| `loads(data)` | Rebuild the object from `bytes` |
| `suffix` (optional) | A string added to the variable label to form the stored filename, such as `".json"` |

Four rules apply to a codec class:

- Pass the class, not an instance. The declaration saves the import path of the class.
- Define the class at the top level of a module, so that `load()` can import it. A class defined inside a function is rejected.
- The class is built with no arguments. To configure a codec, write a subclass.
- Keep the class in a module that other scripts can import. A class defined in the script you run is saved as `__main__:ClassName`, which another script cannot import.

Where the declared codec cannot be imported, pass a codec to `load(codec=...)`.

**Example**

A codec that saves a model through a specification the model can be rebuilt from:

```python
import json
import acquirium as aq

class Tank:
    def __init__(self, speed=45.0):
        self.speed = speed

class TankCodec:
    media_type = "application/json"
    suffix = ".json"

    def dumps(self, tank):
        return json.dumps({"speed": tank.speed}).encode()

    def loads(self, data):
        return Tank(**json.loads(data))

acq = aq.init()
study = acq.study.define("tank")
spec = study.output("spec").object(codec=TankCodec)

run = study.start()
spec.record(Tank(speed=45.0))
run.finish()

restored = run.observations(spec).latest().load()
print(restored.speed)
```

```text
45.0
```

## `PickleCodec`

```python
aq.PickleCodec()
```

Saves any picklable object with Python's `pickle`, at the highest protocol. Its `media_type` is `"application/x-python-pickle"` and its `suffix` is `".pkl"`.

Note that loading a pickle runs code named in its bytes. Load pickles only from people you trust. A pickle also loads only where the classes it names can still be imported, usually with the same library versions that wrote it.

| Method | Behaviour |
|---|---|
| `dumps(value)` | Returns `pickle.dumps(value)` |
| `loads(data)` | Returns `pickle.loads(data)` |

**Example**

```python
model = study.output("model").object(codec=aq.PickleCodec)

run = study.start()
print(model.record({"weights": [1, 2, 3]}))
run.finish()
print(run.observations(model).latest().load())
```

```text
{'digest': 'b2327a1a3eafcb64769f0401dc13c846c1eb769fbde8640484e4d76f9f29d3a7', 'filename': 'model.pkl', 'byte_length': 35}
{'weights': [1, 2, 3]}
```

## `CloudpickleCodec`

```python
aq.CloudpickleCodec()
```

Saves objects with `cloudpickle`. It also saves lambdas and classes defined in a notebook, which `pickle` cannot. It is a subclass of [`PickleCodec`](#picklecodec) with the same `media_type` and `suffix`, and the same trust and version limits.

| Method | Behaviour |
|---|---|
| `dumps(value)` | Returns `cloudpickle.dumps(value)`. Raises `ImportError` when the `cloudpickle` package is not installed |
| `loads(data)` | Returns `pickle.loads(data)`. Loading needs `cloudpickle` installed for anything `pickle` alone could not have saved |

**Example**

```python
function = study.output("objective").object(codec=aq.CloudpickleCodec)
```

## `JsonCodec`

```python
aq.JsonCodec()
```

Saves a JSON document as a file. Its `media_type` is `"application/json"` and its `suffix` is `".json"`. This is useful for a large document nobody filters on, such as a full model specification. A `json` variable keeps its value inside the observation instead, where `observation.value` and result tables show it.

| Method | Behaviour |
|---|---|
| `dumps(value)` | Converts the value to JSON (see [value conversion](#value-conversion)) and returns compact UTF-8 bytes with sorted keys |
| `loads(data)` | Returns `json.loads(data)` |

**Example**

```python
codec = aq.JsonCodec()
data = codec.dumps({"b": 1, "a": (1, 2)})
print(data)
print(codec.loads(data))
```

```text
b'{"a":[1,2],"b":1}'
{'a': [1, 2], 'b': 1}
```

## `BytesCodec`

```python
aq.BytesCodec()
```

Saves bytes exactly as given and loads them back as `bytes`. Its `media_type` is `"application/octet-stream"` and its `suffix` is empty. It is also useful with `load(codec=aq.BytesCodec)` to get the raw bytes of any file, table or object observation.

| Method | Behaviour |
|---|---|
| `dumps(value)` | Returns `bytes(value)`. Raises `TypeError` when the value is not `bytes`, `bytearray` or `memoryview` |
| `loads(data)` | Returns `data` |

**Example**

```python
codec = aq.BytesCodec()
print(codec.dumps(bytearray(b"ab")))
codec.dumps("ab")
```

```text
b'ab'
TypeError: BytesCodec records bytes, not str
```

## Value conversion

Scalar, text, JSON and log values, and the metadata of runs, variables and annotations, are converted to JSON before they are sent. The same conversion applies to every one of them.

| Python value | Stored as |
|---|---|
| `None`, `str`, `bool`, `int` | Unchanged |
| `float` | Unchanged. NaN and infinity raise `ValueError` |
| `dict` | A JSON object. A key that is not a string raises `TypeError` |
| `list`, `tuple` | A JSON array |
| NumPy scalar | A Python number |
| NumPy array | A JSON array |
| `datetime` | An ISO 8601 string in UTC. A value without a timezone is read as UTC |
| `date`, `time` | An ISO 8601 string |
| Any other type | The result of its registered handler. Without a handler, `TypeError` is raised |

Note that the conversion goes one way. A recorded `datetime` or tuple is read back as a string or a list.

**Example**

```python
import datetime as dt
import numpy as np

settings = study.output("settings").json()
run = study.start()
settings.record({"gain": np.float64(1.5), "steps": np.array([1, 2]), "when": dt.datetime(2026, 1, 1)})
print(run.observations(settings).latest().value)
run.finish()
```

```text
{'gain': 1.5, 'steps': [1, 2], 'when': '2026-01-01T00:00:00+00:00'}
```

## `register_value_handler`

```python
aq.register_value_handler(cls, handler)
```

Registers a function that converts instances of `cls` to JSON when they are recorded. The handler also applies to subclasses of `cls`. Registering a class again replaces its handler. The registration lasts for the current Python process.

**Parameters**

- `cls` (`type`): the class to convert.
- `handler` (callable): a function that takes one instance and returns a JSON value. The result is converted again, so it may contain NumPy values, dates or other handled types.

**Returns**

`None`.

**Raises**

- `TypeError`: when `cls` is not a class.

**Example**

```python
class Money:
    def __init__(self, amount, currency):
        self.amount, self.currency = amount, currency

aq.register_value_handler(Money, lambda value: {"amount": value.amount, "currency": value.currency})

price = study.output("price").json()
run = study.start()
price.record(Money(5, "USD"))
print(run.observations(price).latest().value)
run.finish()
```

```text
{'amount': 5, 'currency': 'USD'}
```

Without the handler, the same `record()` call raises:

```text
TypeError: value is a Money, which cannot be recorded as JSON; convert it first or register a handler with acquirium.register_value_handler()
```

## `timestamp`

```python
aq.timestamp(value)
```

Parses a time and returns it in UTC. The experiment methods use it for every time argument, such as `occurred_at`, `start`, `end` and `at`.

**Parameters**

- `value` (`datetime | str`): a `datetime`, or an ISO 8601 string such as `"2026-01-01T12:00:00+02:00"`. A value without a timezone is read as UTC.

**Returns**

A timezone-aware `datetime` in UTC.

**Raises**

- `ValueError`: when a string is not ISO 8601.

**Example**

```python
print(aq.timestamp("2026-01-01T12:00:00"))
print(aq.timestamp("2026-01-01T12:00:00+02:00"))
```

```text
2026-01-01 12:00:00+00:00
2026-01-01 10:00:00+00:00
```
