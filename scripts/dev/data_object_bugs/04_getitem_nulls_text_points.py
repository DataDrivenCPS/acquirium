"""Bug 4: ``data[alias]`` returns null for text points when the alias also has
numeric points.

Cause: ``__getitem__``, ``iter()`` and ``latest()`` call
``_restore_single_value_column`` on the alias subset as a whole. That helper
picks ``value_numeric`` as the ``value`` column when *any* row in the frame has
a number, so in an alias mixing a chlorine sensor and a status enumeration the
status rows come back with ``value = null``. Nothing warns; the text is simply
gone. A text-only alias works, which is why this hides in mixed plants. Fix:
``_restore_single_value_column`` now folds a frame that holds both kinds into a
String ``value`` column (numbers rendered as text) instead of nulling the text.
Numeric-only frames keep a Float64 column, text-only frames a String column, so
the dtype tells the caller whether the alias is mixed.
"""
from datetime import datetime, timezone, timedelta
from tempfile import TemporaryDirectory

from rdflib import URIRef
import acquirium

TTL = """
@prefix ex: <urn:ex/> .
@prefix s223: <http://data.ashrae.org/standard223#> .
ex:basin_1 a ex:Basin ; s223:hasProperty ex:cl , ex:status .
ex:cl     a s223:QuantifiableObservableProperty .
ex:status a s223:EnumeratedObservableProperty .
"""
t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
times = [t0 + timedelta(hours=i) for i in range(3)]

with TemporaryDirectory() as data_dir:
    ac = acquirium.init(data_dir=data_dir)
    try:
        ac.insert_graph(TTL, source_id="plant")
        ac.register_datasource("src")
        ac.register_streams([
            {"source_id": "src", "ref_name": "cl", "point_uri": "urn:ex/cl", "value_kind": "numeric"},
            {"source_id": "src", "ref_name": "status", "point_uri": "urn:ex/status", "value_kind": "text"},
        ])
        ac.insert_timeseries_batch("src", {
            "cl": [(t, 1.0 + i) for i, t in enumerate(times)],
            "status": [(t, f"state{i}") for i, t in enumerate(times)],
        })

        data = ac.query().entity(URIRef("urn:ex/Basin"), alias="basin").measurement(alias="m").data()
        frame = data["m"]
        print(frame)
        status_values = frame.filter(frame["point_uri"] == "urn:ex/status")["value"].to_list()
        print("status values via data['m']:", status_values)
        print("latest('m'):", data.latest("m").to_dicts())
        assert status_values == ["state0", "state1", "state2"], "text point values were nulled"
    finally:
        acquirium.shutdown()
