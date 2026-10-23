"""Recipes: named query shortcuts bound to a client.

A recipe file is a plain Python module. Every public top-level function it
defines whose first parameter is named ``acq`` is a recipe; underscore names,
imported functions and functions with another first parameter are helpers and
are skipped. A module constant ``ACQUIRIUM = ">=0.4"`` (a PEP 440 specifier)
pins the acquirium versions the file was written for; loading it under any
other version raises :class:`RecipeError`.

The files in this package ship with acquirium and load on first use of
``acq.recipes``. ``acq.recipes.add(path)`` loads more from a local file::

    acq.recipes.add("my_recipes.py")
    acq.recipes.neighbors(uri)          # calls neighbors(acq, uri)
    print(acq.recipes.source("neighbors"))
"""

from __future__ import annotations

import functools
import importlib.metadata
import importlib.util
import inspect
import itertools
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterator

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import Version

VERSION_CONSTANT = "ACQUIRIUM"
_BUILTIN_DIR = Path(__file__).parent
_load_counter = itertools.count(1)


class RecipeError(ValueError):
    """A recipe file could not be added: version mismatch, name collision or malformed metadata."""


@dataclass(frozen=True)
class Recipe:
    name: str
    func: Callable[..., Any]
    origin: str

    @property
    def description(self) -> str:
        doc = inspect.getdoc(self.func)
        return doc.splitlines()[0] if doc else ""


def _installed_version() -> Version:
    return Version(importlib.metadata.version("acquirium"))


def _check_version(module: ModuleType, origin: str) -> None:
    spec = getattr(module, VERSION_CONSTANT, None)
    if spec is None:
        return
    try:
        allowed = SpecifierSet(str(spec), prereleases=True)
    except InvalidSpecifier as exc:
        raise RecipeError(f"{origin}: {VERSION_CONSTANT} = {spec!r} is not a valid version specifier") from exc
    installed = _installed_version()
    if installed not in allowed:
        raise RecipeError(
            f"{origin} requires acquirium {allowed}; {installed} is installed"
        )


def recipes_in(module: ModuleType) -> dict[str, Callable[..., Any]]:
    """The recipe functions a module defines, by name, in definition order."""
    found: dict[str, Callable[..., Any]] = {}
    for name, obj in vars(module).items():
        if name.startswith("_") or not inspect.isfunction(obj):
            continue
        if obj.__module__ != module.__name__:
            continue
        params = list(inspect.signature(obj).parameters)
        if params and params[0] == "acq":
            found[name] = obj
    return found


def _load_file(path: Path) -> ModuleType:
    if path.suffix != ".py" or not path.is_file():
        raise RecipeError(f"{path} is not a Python file")
    name = f"acquirium.recipes._loaded.{path.stem}_{next(_load_counter)}"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bind(func: Callable[..., Any], acq: Any) -> Callable[..., Any]:
    @functools.wraps(func)
    def bound(*args: Any, **kwargs: Any) -> Any:
        return func(acq, *args, **kwargs)

    sig = inspect.signature(func)
    bound.__signature__ = sig.replace(parameters=list(sig.parameters.values())[1:])  # type: ignore[attr-defined]
    return bound


class Recipes:
    """The recipes available on one client, each callable with ``acq`` already bound."""

    def __init__(self, acq: Any, *, builtin: bool = True):
        self._acq = acq
        self._recipes: dict[str, Recipe] = {}
        if builtin:
            for path in sorted(_BUILTIN_DIR.glob("[!_]*.py")):
                self.add(path)

    # ---------- loading ----------

    def add(self, source: str | Path | ModuleType, *, replace: bool = False) -> list[str]:
        """Register the recipes a local file or an imported module defines.

        Returns the names registered. A name already present raises
        :class:`RecipeError` unless ``replace=True``; so does a file whose
        ``ACQUIRIUM`` specifier excludes the installed version.
        """
        if isinstance(source, ModuleType):
            module, origin = source, source.__name__
        else:
            path = Path(source).expanduser().resolve()
            module, origin = _load_file(path), str(path)
            if path.parent == _BUILTIN_DIR:
                origin = f"acquirium.recipes.{path.stem}"
        _check_version(module, origin)
        found = recipes_in(module)
        for name in found:
            if hasattr(type(self), name):
                raise RecipeError(f"{origin}: {name!r} is reserved and cannot be a recipe name")
            existing = self._recipes.get(name)
            if existing is not None and not replace:
                raise RecipeError(
                    f"recipe {name!r} from {origin} is already registered from "
                    f"{existing.origin}; pass replace=True to override it"
                )
        for name, func in found.items():
            self._recipes[name] = Recipe(name, func, origin)
        return list(found)

    # ---------- inspection ----------

    def source(self, name: str) -> str:
        """The source code of a recipe, for reading or copying."""
        return inspect.getsource(self._recipe(name).func)

    def _recipe(self, name: str) -> Recipe:
        try:
            return self._recipes[name]
        except KeyError:
            available = ", ".join(sorted(self._recipes)) or "none"
            raise AttributeError(f"no recipe named {name!r}; available: {available}") from None

    def __getattr__(self, name: str) -> Callable[..., Any]:
        if name.startswith("_"):
            raise AttributeError(name)
        return _bind(self._recipe(name).func, self._acq)

    def __dir__(self) -> list[str]:
        return sorted(self._recipes) + ["add", "source"]

    def __contains__(self, name: object) -> bool:
        return name in self._recipes

    def __iter__(self) -> Iterator[Recipe]:
        return iter(self._recipes.values())

    def __len__(self) -> int:
        return len(self._recipes)

    def __repr__(self) -> str:
        if not self._recipes:
            return "Recipes: none loaded"
        width = max(len(r.name) for r in self._recipes.values())
        lines = [f"Recipes ({len(self._recipes)}):"]
        for r in self._recipes.values():
            lines.append(f"  {r.name:<{width}}  {r.description}  [{r.origin}]")
        return "\n".join(lines)


__all__ = ["Recipe", "RecipeError", "Recipes", "recipes_in", "VERSION_CONSTANT"]
