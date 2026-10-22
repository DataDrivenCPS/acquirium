"""Bug 3: ``convert_to()`` on a lazy DataObject applies the conversion twice.

Cause: ``convert_to()`` on an unmaterialized object records the pending
conversion ``(alias, from_uri, to_uri)`` *and* rewrites the binding's
``property_unit`` to ``to_uri``. When ``_materialize`` later runs, the
auto-conversion step sees ``ref_unit != property_unit`` and converts the raw
rows from the ref unit straight to the target. The pending step then converts
them again from the old property unit to the target. Any stream whose external
reference carries a unit (every stream registered with a ``unit`` that differs
from the point's) is scaled twice. The eager path converts the already
auto-converted frame once, so ``d["flow"]; d.convert_to(...)`` is right while
``d.convert_to(...)["flow"]`` is wrong. Fix: the lazy path leaves the binding's
``property_unit`` alone; ``_materialize`` auto-converts to the annotated unit,
applies the pending conversions once, and then records the target unit on the
bindings. ``units()`` overlays pending targets so it reports the requested unit
before and after materialization, and chained ``convert_to()`` calls start from
the previous target.
"""
from datetime import datetime, timezone, timedelta
from tempfile import TemporaryDirectory

from rdflib import URIRef
import acquirium

TTL = """
@prefix ex: <urn:ex/> .
@prefix qudt: <http://qudt.org/schema/qudt/> .
@prefix unit: <http://qudt.org/vocab/unit/> .
@prefix s223: <http://data.ashrae.org/standard223#> .
ex:pump_1 a ex:Pump ; s223:hasProperty ex:flow .
ex:flow a s223:QuantifiableObservableProperty ; qudt:hasUnit unit:L-PER-MIN .
"""
ML_PER_MIN = "http://qudt.org/vocab/unit/MilliL-PER-MIN"
L_PER_SEC = "http://qudt.org/vocab/unit/L-PER-SEC"
t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
raw_ml_per_min = [1000.0, 2000.0, 3000.0]          # = 1, 2, 3 L/min

with TemporaryDirectory() as data_dir:
    ac = acquirium.init(data_dir=data_dir)
    try:
        ac.insert_graph(TTL, source_id="plant")
        ac.register_datasource("src")
        # The stream stores mL/min while the point is annotated L/min, so the
        # external reference gets its own unit and data access auto-converts.
        ac.register_streams([{"source_id": "src", "ref_name": "flow", "point_uri": "urn:ex/flow",
                              "value_kind": "numeric", "unit": URIRef(ML_PER_MIN)}])
        ac.insert_timeseries("src", "flow", [(t0 + timedelta(hours=i), v) for i, v in enumerate(raw_ml_per_min)])

        q = ac.query().entity(URIRef("urn:ex/Pump"), alias="pump").measurement(alias="flow")

        eager = q.data()
        eager["flow"]                                    # materialize first
        eager_vals = eager.convert_to(L_PER_SEC)["flow"]["value"].to_list()
        lazy_vals = q.data().convert_to(L_PER_SEC)["flow"]["value"].to_list()
        expected = [v / 1000 / 60 for v in raw_ml_per_min]

        print("expected L/s:", expected)
        print("eager   L/s:", eager_vals)
        print("lazy    L/s:", lazy_vals)
        assert all(abs(a - b) < 1e-12 for a, b in zip(eager_vals, expected))
        assert all(abs(a - b) < 1e-12 for a, b in zip(lazy_vals, expected)), "lazy convert_to converted twice"
    finally:
        acquirium.shutdown()
