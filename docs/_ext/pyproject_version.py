"""Make the package version from pyproject.toml available to pages as ``{{ acquirium_version }}``."""

import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).parents[2] / "pyproject.toml"


def _set_version(app, config):
    version = tomllib.loads(PYPROJECT.read_text())["project"]["version"]
    config.myst_substitutions = {**(config.myst_substitutions or {}), "acquirium_version": version}


def setup(app):
    app.connect("config-inited", _set_version)
    return {"parallel_read_safe": True}
