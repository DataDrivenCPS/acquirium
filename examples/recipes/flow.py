"""Recipes that walk the flow of water through the plant."""

ACQUIRIUM = ">=0.4.0a8"


def nearest_measurement(acq, cls=None, *, uri=None, direction="downstream", max_depth=10, **where):
    """The closest matching stream up or down the flow from an entity.

    ``cls`` is a class name or URI, ``uri`` pins one instance. The search
    moves place by place along the flow and stops at the first place that
    holds a match; the keyword filters decide what counts as a match, so
    ``quantity_kind="turbidity"`` skips places with other readings. Ties
    within a place all survive. ``max_depth`` counts entities passed.
    """
    return (
        acq.query()
        .entity(cls, uri=uri, alias="entity")
        .measurement(alias="stream", direction=direction, max_depth=max_depth, **where)
    )


def across(acq, cls=None, *, uri=None, max_depth=10, **where):
    """The nearest matching stream entering and leaving an entity, as two queries.

    Returns ``(upstream, downstream)``; compare their data to see what the
    entity does to the quantity, inlet against outlet.
    """
    return (
        nearest_measurement(acq, cls, uri=uri, direction="upstream", max_depth=max_depth, **where),
        nearest_measurement(acq, cls, uri=uri, direction="downstream", max_depth=max_depth, **where),
    )
