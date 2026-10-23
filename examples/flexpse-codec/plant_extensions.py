"""Extensions the example model uses.

A spec names an extension as "module:function", so a saved model loads only
where this module imports. Keep extensions in a module, not in the script
that builds the model.
"""
import flexops
import pyomo.environ as pyo


@flexops.extension
def end_full(model, *, tank: str, fraction: float):
    """End the horizon with the tank at least `fraction` full."""
    block = model.find_component(tank)
    last = model.time_block.time_index.last()
    block.end_full = pyo.Constraint(expr=block.volume[last] >= fraction * block.capacity)
