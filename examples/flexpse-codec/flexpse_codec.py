"""An Acquirium codec that saves a flex-pse model and builds it back.

    model_state = study.output("model").object(codec=FlexModelCodec)
    model_state.record(m)
    restored = experiment.observations(model_state).latest().load()

The codec stores the model as flex-pse's own spec, the format `emit_model`
writes and `build_model` reads, rather than as a pickle, so a saved model
loads with a later flex-pse that can migrate its `schema_version`.
"""
from __future__ import annotations

import io
import json
import shutil
import tempfile
import warnings
import zipfile
from hashlib import sha256
from pathlib import Path

import flexops
from flexcore.config.io import dump_spec, load_spec
from flexcore.exceptions import FlexEmitWarning

SPEC = "spec.json"


class FlexModelCodec:
    """Save a model as its spec, zipped with every file the spec names.

    `emit_model` writes value lists longer than its inline limit to
    `.json.gz` files beside the spec. The codec also copies in the tariff
    and demand-response files the costing block reads, so the bytes rebuild
    the model wherever flex-pse and the model's extension modules import.
    Identical models give identical bytes, which Acquirium stores once.

    Acquirium rebuilds a codec from its class, so configure it by
    subclassing and overriding these attributes.
    """

    media_type = "application/zip"
    suffix = ".flexpse.zip"
    # Refuse a model whose spec would not rebuild it exactly (a custom
    # constraint, a surrogate object, a hand-written objective), rather than
    # saving a model that loads back different.
    strict = True
    # Where load() unpacks a saved model. A loaded model's costing block keeps
    # paths to the files here, so the model can be saved again.
    directory = Path(tempfile.gettempdir()) / "flexpse-models"

    def dumps(self, model) -> bytes:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with warnings.catch_warnings():
                if self.strict:
                    warnings.simplefilter("error", FlexEmitWarning)
                spec = flexops.emit_model(model, data_dir=root / "data", relative_to=root)
            dump_spec(spec, root / SPEC)
            document = json.loads((root / SPEC).read_text())
            _bundle_inputs(document, root, _base_dir(model))
            (root / SPEC).write_text(json.dumps(document, indent=2) + "\n")
            return _zip(root)

    def loads(self, data: bytes):
        # Unpack once per saved model, under its digest; the contents never
        # change, so a second load reuses the folder.
        folder = self.directory / sha256(data).hexdigest()
        if not (folder / SPEC).is_file():
            self.directory.mkdir(parents=True, exist_ok=True)
            staging = Path(tempfile.mkdtemp(dir=self.directory))
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                archive.extractall(staging)
            try:
                staging.rename(folder)
            except OSError:
                # Another load unpacked the same model first.
                shutil.rmtree(staging)
        return flexops.build_model(load_spec(folder / SPEC))


def _base_dir(model) -> Path:
    """The directory the model's relative file paths were read against."""
    spec = getattr(model, "_flex_spec", None)
    base = getattr(spec, "_base_dir", None)
    return Path(base) if base is not None else Path.cwd()


def _bundle_inputs(document: dict, root: Path, base: Path) -> None:
    """Copy every file the spec reads into `root`, and point the spec at the copies."""
    copied: dict[Path, str] = {}

    def localize(path: str) -> str:
        relative = not Path(path).is_absolute()
        if relative and (root / path).is_file():
            return path  # a list emit_model wrote beside the spec
        source = base / path if relative else Path(path)
        source = source.resolve()
        if source not in copied:
            target = Path("inputs") / f"{len(copied)}-{source.name}"
            (root / target).parent.mkdir(exist_ok=True)
            shutil.copyfile(source, root / target)
            copied[source] = target.as_posix()
        return copied[source]

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key in {"$source", "events_source"} and isinstance(value, str):
                    node[key] = localize(value)
                elif key == "tariff_source" and value is not None:
                    if isinstance(value, str):
                        node[key] = localize(value)
                    elif isinstance(value, list):
                        node[key] = [localize(item) for item in value]
                    else:
                        node[key] = {name: localize(item) for name, item in value.items()}
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(document)


def _zip(root: Path) -> bytes:
    """Zip `root` with fixed names, order, and timestamps."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for path in sorted(p for p in root.rglob("*") if p.is_file()):
            info = zipfile.ZipInfo(path.relative_to(root).as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    return buffer.getvalue()
