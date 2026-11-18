"""Starter files written by the CLI.

``acquirium.toml`` is the config that ``acquirium config init`` copies into
the user's directory. It ships inside the wheel as package data.
"""

from importlib import resources


def starter_config() -> str:
    """Return the text of the starter ``acquirium.toml``."""
    return resources.files(__name__).joinpath("acquirium.toml").read_text(encoding="utf-8")
