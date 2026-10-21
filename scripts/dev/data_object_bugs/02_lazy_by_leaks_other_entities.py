"""Bug 2: ``by()`` on a lazy DataObject leaks other entities into each group.

Cause: the lazy branch of ``DataObject.by()`` groups bindings by entity but
appends each ``BindingInfo`` unchanged, so a binding whose point hangs off two
basins carries both basins' contexts into both sub-objects. When a sub-object
materializes it emits one row per context, and the frame for basin 1 contains
rows tagged basin 2 (and vice versa). ``metadata()``, raw narrow output and a
nested ``by()`` on the sub-object all show the foreign entity. The eager branch
filters the tall frame, so the same call after ``dataframe()`` is correct.
Fix: the lazy branch gives each sub-object a copy of the binding whose
``entity_contexts`` are restricted to the contexts matching that entity.
"""
from datetime import datetime, timezone, timedelta
from tempfile import TemporaryDirectory

from rdflib import URIRef
import acquirium

TTL = """
@prefix ex: <urn:ex/> .
@prefix s223: <http://data.ashrae.org/standard223#> .
ex:basin_1 a ex:Basin ; s223:hasProperty ex:cl_1 , ex:shared .
ex:basin_2 a ex:Basin ; s223:hasProperty ex:cl_2 , ex:shared .
ex:cl_1   a s223:QuantifiableObservableProperty .
ex:cl_2   a s223:QuantifiableObservableProperty .
ex:shared a s223:QuantifiableObservableProperty .
"""
t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
rows = [(t0 + timedelta(hours=i), float(i)) for i in range(3)]

with TemporaryDirectory() as data_dir:
    ac = acquirium.init(data_dir=data_dir)
    try:
        ac.insert_graph(TTL, source_id="plant")
        ac.register_datasource("src")
        ac.register_streams([
            {"source_id": "src", "ref_name": n, "point_uri": f"urn:ex/{n}", "value_kind": "numeric"}
            for n in ("cl_1", "cl_2", "shared")
        ])
        ac.insert_timeseries_batch("src", {n: rows for n in ("cl_1", "cl_2", "shared")})

        q = ac.query().entity(URIRef("urn:ex/Basin"), alias="basin").measurement(alias="m")

        print("lazy parent:")
        bad = False
        for basin, sub in q.data().by("basin"):
            seen = sub.dataframe(shape="narrow", compact=False)["entity__basin"].unique().to_list()
            print(f"  {basin} -> rows tagged {seen}")
            bad |= seen != [basin]

        print("materialized parent:")
        d = q.data(); d.dataframe()
        for basin, sub in d.by("basin"):
            seen = sub.dataframe(shape="narrow", compact=False)["entity__basin"].unique().to_list()
            print(f"  {basin} -> rows tagged {seen}")

        assert not bad, "lazy by() sub-objects contain rows of other entities"
    finally:
        acquirium.shutdown()
