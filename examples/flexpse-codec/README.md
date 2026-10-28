# Saving flex-pse models with Acquirium

Save a whole flex-pse model with an Acquirium experiment run, and build it
back later. The model is stored as one file attached to the run. Acquirium
does not index the variables inside it, so a model with thousands of
variables costs one recording, not thousands of declarations.

## How it works

An Acquirium `object` variable saves any Python object through a codec: a
class with a `media_type`, `dumps(value) -> bytes`, and `loads(data)`.
`flexpse_codec.FlexModelCodec` is a codec built on flex-pse's own round trip,
from the [Round-trip a model](https://github.com/flex-pse/flex-pse/blob/main/docs/how_to/round_trip_a_model.md)
guide:

- `dumps(model)` calls `emit_model` and saves the spec as a zip. Value lists
  longer than the spec's inline limit go in `data/*.json.gz` beside it.
  Tariff and demand-response files the costing block reads are copied into
  `inputs/`, so the file rebuilds the model on another machine.
- `loads(data)` unpacks the zip and calls `build_model(load_spec(...))`.

```python
from flexpse_codec import FlexModelCodec

model_state = study.output("model").object(codec=FlexModelCodec)

experiment = study.start()
model_state.record(m)
experiment.finish()

restored = experiment.observations(model_state).latest().load()
```

A saved model is flex-pse's spec, not a pickle, so a later flex-pse that can
migrate the spec's `schema_version` can still load it. Loading runs no code
stored in the file, though it does import and call the extension functions
the spec names.

## Run the example

From this directory:

```bash
uv sync
uv run python record_model.py
uv run python load_model.py
```

`record_model.py` starts a local Acquirium server, builds the tank and pump
model from the round-trip guide, saves it with a run, loads it back, and
checks it with `assert_models_equivalent`. `load_model.py` opens the Study in
a new session and builds the latest saved model again. Data stays in
`.data/`. The environment pins flex-pse to the commit the example was tested
against.

## Things to know

- **Strict by default.** `dumps()` raises when the spec would not rebuild the
  same model, for instance with a custom constraint, a surrogate object, or a
  hand-written objective. To save such models anyway, subclass the codec and
  set `strict = False`; the loaded model will lack those parts.
- **Extensions must import.** A spec names an extension as `module:function`.
  Keep extensions in a module, as `plant_extensions.py` does here, and make
  sure it imports wherever the model is loaded.
- **The codec must import too.** Acquirium saves the codec's import path,
  `flexpse_codec:FlexModelCodec`, and `load()` imports it. Run from this
  directory, or move `FlexModelCodec` into a package you install.
- **Loaded models keep their inputs.** `load()` unpacks into
  `$TMPDIR/flexpse-models/<sha256>/`, and a loaded model's costing block
  reads its files from there, so the model can be saved again. Set
  `directory` in a subclass to unpack somewhere else.
- **Identical models are stored once.** The zip has fixed timestamps and
  order, so saving the same model twice gives the same bytes, and Acquirium
  stores them once.

## See also

- [Object variables and codecs](../../docs/reference/experiments.md#object-variables-and-codecs)
- [Save objects to restore later](../../docs/how-to/record-experiments.md#save-objects-to-restore-later)
