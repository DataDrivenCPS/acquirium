"""Old names of renamed arguments, keys, columns and attributes.

``ref`` became ``stream`` and ``point`` became ``property``. The client
interface still accepts the old names:

- ``point`` names are plain aliases of the ``property`` names.
- ``ref`` names are deprecated. Using one emits a ``FutureWarning`` that
  names its replacement.
"""
from __future__ import annotations

import functools
import inspect
import warnings
from typing import Any, Callable, Mapping, TypeVar

# old name -> new name, accepted silently
ALIASES = {
    "point_uri": "property_uri",
    "point_uris": "property_uris",
}
# old name -> new name, accepted with a FutureWarning
DEPRECATED = {
    "ref_uri": "stream_uri",
    "ref_uris": "stream_uris",
    "ref_name": "stream_name",
    "ref_names": "stream_names",
    "include_ref": "include_stream",
    "include_ref_uris": "include_stream_uris",
}
OLD_NAMES = {**ALIASES, **DEPRECATED}
_NEW_NAMES = frozenset(OLD_NAMES.values())

T = TypeVar("T")


def warn_deprecated(old: str, new: str, *, stacklevel: int = 3) -> None:
    warnings.warn(
        f"'{old}' is deprecated and will be removed in a future version; use '{new}' instead",
        FutureWarning,
        stacklevel=stacklevel,
    )


def new_names(mapping: Mapping[str, Any], *, stacklevel: int = 3, renames: Mapping[str, str] = OLD_NAMES) -> dict[str, Any]:
    """Return ``mapping`` with every old key replaced by its new name."""
    if renames.keys().isdisjoint(mapping):
        return dict(mapping)
    out: dict[str, Any] = {}
    for key, value in mapping.items():
        new = renames.get(key)
        if new is None:
            out[key] = value
            continue
        if new in mapping:
            raise TypeError(f"got both '{key}' and '{new}'; use '{new}' only")
        if key not in ALIASES:
            warn_deprecated(key, new, stacklevel=stacklevel + 1)
        out[new] = value
    return out


def _wrap(func: Callable, renames: Mapping[str, str]) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if kwargs and not renames.keys().isdisjoint(kwargs):
            kwargs = new_names(kwargs, stacklevel=3, renames=renames)
        return func(*args, **kwargs)
    return wrapper


def _renames_for(func: Callable) -> dict[str, str]:
    try:
        parameters = inspect.signature(func).parameters
    except (TypeError, ValueError):
        return {}
    return {old: new for old, new in OLD_NAMES.items() if new in parameters and old not in parameters}


def _old_attribute(old: str, new: str) -> property:
    def getter(self):
        if old not in ALIASES:
            warn_deprecated(old, new)
        return getattr(self, new)
    return property(getter)


def accepts_old_names(target: T) -> T:
    """Let a function, or every public method of a class, take the old names.

    A keyword argument given under an old name is passed on under its new
    name. A class also gets a read-only attribute for the old name of each
    renamed field it declares.
    """
    if not inspect.isclass(target):
        renames = _renames_for(target)
        return _wrap(target, renames) if renames else target
    for name, member in list(vars(target).items()):
        if name.startswith("_") and name != "__init__":
            continue
        kind = type(member)
        func = member.__func__ if kind in (staticmethod, classmethod) else member
        if not inspect.isfunction(func):
            continue
        renames = _renames_for(func)
        if not renames:
            continue
        wrapped = _wrap(func, renames)
        setattr(target, name, kind(wrapped) if kind in (staticmethod, classmethod) else wrapped)
    fields = set(getattr(target, "__annotations__", ())) | set(getattr(target, "_fields", ()))
    for old, new in OLD_NAMES.items():
        if new in fields and old not in vars(target):
            setattr(target, old, _old_attribute(old, new))
    return target
