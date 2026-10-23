"""Recipes about equipment, treatment steps and systems."""

ACQUIRIUM = ">=0.4.0a8"


def equipment_in(acq, process):
    """Equipment performing a treatment step, with the step shown.

    ``process`` is free text or a URI from the process taxonomy. The result
    is a Query positioned on the equipment, so ``.measurement()`` continues
    to its streams.
    """
    return acq.query().entity("equipment", alias="unit").where(process=process).include("process")


def measurements_of(acq, cls=None, *, uri=None, **where):
    """Streams on an entity class, or on one entity, with quantity kind and unit columns.

    ``cls`` is a class name or URI (``"pump"``, ``"ultrafiltration unit"``);
    ``uri`` pins one instance instead. Keyword arguments filter the streams
    like ``where()``.
    """
    return (
        acq.query()
        .entity(cls, uri=uri, alias="entity")
        .measurement(alias="stream", **where)
        .include("quantity_kind", "unit")
    )


def units_in(acq, system):
    """The distinct units of measure on the equipment of a system, sorted.

    ``system`` is the system's URI. Equipment is reached through any
    relation up to three hops, so subsystems count.
    """
    q = (
        acq.query()
        .entity(uri=system, alias="system")
        .related("equipment", alias="equipment")
        .measurement(alias="stream")
        .include("unit")
    )
    return sorted(set(q.metadata()["stream.unit"].drop_nulls().to_list()))
