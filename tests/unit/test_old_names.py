"""The old ``ref``/``point`` names still work on the client interface."""
import warnings

import polars as pl
import pytest

from acquirium.Client.streams import Stream
from acquirium.Materialization.models import OutputSpec, StreamDescriptor
from acquirium.internals._names import accepts_old_names, new_names
from acquirium.internals.models import compute_ref_uri, compute_stream_uri


def test_point_names_are_plain_aliases():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert new_names({"point_uri": "urn:p", "label": "x"}) == {"property_uri": "urn:p", "label": "x"}
        assert OutputSpec(value_kind="numeric", point_uri="urn:p").property_uri == "urn:p"
        assert StreamDescriptor("urn:s", property_uri="urn:p").point_uri == "urn:p"


def test_ref_names_warn_and_map_to_stream_names():
    with pytest.warns(FutureWarning, match="'ref_name' is deprecated.*use 'stream_name'"):
        assert new_names({"source_id": "s", "ref_name": "flow"}) == {"source_id": "s", "stream_name": "flow"}
    with pytest.warns(FutureWarning, match="use 'stream_uri'"):
        assert StreamDescriptor(ref_uri="urn:s").stream_uri == "urn:s"
    with pytest.warns(FutureWarning, match="use 'stream_uri'"):
        assert Stream("urn:s").ref_uri == "urn:s"
    with pytest.warns(FutureWarning, match="use 'compute_stream_uri'"):
        assert compute_ref_uri("s", "flow") == compute_stream_uri("s", "flow")


def test_old_and_new_name_together_is_an_error():
    with pytest.raises(TypeError, match="use 'stream_name' only"):
        new_names({"ref_name": "a", "stream_name": "b"})


def test_methods_take_old_keyword_arguments():
    @accepts_old_names
    class Thing:
        def read(self, stream_uri, *, property_uri=None, include_stream=False):
            return stream_uri, property_uri, include_stream

    with pytest.warns(FutureWarning) as caught:
        assert Thing().read(ref_uri="urn:s", point_uri="urn:p", include_ref=True) == ("urn:s", "urn:p", True)
    assert sorted(str(w.message).split("'")[1] for w in caught) == ["include_ref", "ref_uri"]
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert Thing().read("urn:s", property_uri="urn:p") == ("urn:s", "urn:p", False)


def test_measurement_takes_ref_as_the_old_name_of_stream():
    from acquirium.Client.explore.core import Query

    new = Query(client=None).measurement(alias="m", stream="urn:test#s").to_sparql()
    with pytest.warns(FutureWarning, match="use 'stream'"):
        old = Query(client=None).measurement(alias="m", ref="urn:test#s").to_sparql()
    assert old == new


def test_driver_observations_take_the_ref_name_column():
    from acquirium.Drivers.Driver import IngestDriver

    frame = pl.DataFrame({"ts": ["2026-01-01T00:00:00Z"], "ref_name": ["flow"], "value": [1.0]})
    class Bare(IngestDriver):
        def setup(self): ...
        def tick(self): ...

    driver = Bare.__new__(Bare)
    with pytest.warns(FutureWarning, match="use 'stream_name'"):
        assert "stream_name" in driver.normalize_observations(frame).columns
