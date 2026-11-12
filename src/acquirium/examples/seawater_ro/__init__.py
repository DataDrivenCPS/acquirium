"""A seawater reverse-osmosis desalination plant.

The model follows the WaterTAP seawater RO flowsheet: intake, pretreatment,
a reverse-osmosis stage with a pressure exchanger, and post-treatment.
"""

from pathlib import Path

model_path: Path = Path(__file__).parent / "model.ttl"
"""The plant model, a Turtle file."""

__all__ = ["model_path"]
