"""Pattern matching on the test model through the explore builder.

Row counts are properties of ``tests/test_model.ttl``; each test says which
shape it exercises.
"""
import pytest
from conftest import ACQUIRIUM_TEST_SERVER_HOST, ACQUIRIUM_TEST_SERVER_PORT
from acquirium import Acquirium
from acquirium.internals.internals_namespaces import *


@pytest.fixture
def acquirium_client():
    acq = Acquirium(
        server_url=ACQUIRIUM_TEST_SERVER_HOST,
        server_port=ACQUIRIUM_TEST_SERVER_PORT,
        use_ssl=False,
    )
    acq.insert_graph_file("tests/test_model.ttl", source_id="plant")
    return acq


#### ENTITY ####

def test_entity(acquirium_client: Acquirium):
    acq = acquirium_client
    entity_A = acq.query().entity(ACQUIRIUM_NS.A, alias="a")
    assert len(entity_A.query_graph.nodes) == 1
    assert len(entity_A.query_graph.edges) == 0

    result_A = entity_A.execute()
    assert set(result_A) == {"columns", "rows", "datatypes"}
    assert len(result_A["rows"]) == 4

    entity_B = acq.query().entity(ACQUIRIUM_NS.B, alias="b")
    assert len(entity_B.execute()["rows"]) == 2
    result_B_df = entity_B.metadata()
    assert len(result_B_df) == 2
    assert result_B_df.columns == ["b"]

    # a second, unconnected entity is a cross product
    entity_AC = entity_A.entity(ACQUIRIUM_NS.C, alias="c")
    assert len(entity_AC.query_graph.nodes) == 2
    assert len(entity_AC.query_graph.edges) == 0
    assert len(entity_AC.execute()["rows"]) == 3 * 4
    result_AC_df = entity_AC.metadata()
    assert len(result_AC_df) == 3 * 4
    assert set(result_AC_df.columns) == {"a", "c"}


#### RELATED ####

def test_related_single_predicate_single_hop(acquirium_client: Acquirium):
    acq = acquirium_client
    related = (acq.query().entity(ACQUIRIUM_NS.A, alias="a")
               .related(ACQUIRIUM_NS.C, via=[ACQUIRIUM_NS.x], alias="c"))
    assert len(related.query_graph.nodes) == 2
    assert len(related.query_graph.edges) == 1

    result = related.metadata()
    assert len(result) == 1
    assert set(result.columns) == {"a", "c"}


def test_related_single_predicate_two_hops(acquirium_client: Acquirium):
    acq = acquirium_client
    related = (acq.query().entity(ACQUIRIUM_NS.A, alias="a")
               .related(ACQUIRIUM_NS.C, via=[ACQUIRIUM_NS.x], alias="c", max_depth=2, nearest=False))
    result = related.metadata()
    assert len(result) == 3
    assert set(result.columns) == {"a", "c"}


def test_related_two_predicates_single_hop(acquirium_client: Acquirium):
    acq = acquirium_client
    related = (acq.query().entity(ACQUIRIUM_NS.B, alias="b")
               .related(ACQUIRIUM_NS.A, via=[ACQUIRIUM_NS.z, ACQUIRIUM_NS.w], alias="a"))
    result = related.metadata()
    assert len(result) == 3
    assert set(result.columns) == {"a", "b"}


def test_related_any_predicate_single_hop(acquirium_client: Acquirium):
    acq = acquirium_client
    related = (acq.query().entity(ACQUIRIUM_NS.B, alias="b")
               .related(ACQUIRIUM_NS.A, alias="a", max_depth=1, nearest=False))
    result = related.metadata()
    assert len(result) == 3
    assert set(result.columns) == {"a", "b"}


def test_related_any_predicate_multi_hop(acquirium_client: Acquirium):
    acq = acquirium_client
    entity_A = acq.query().entity(ACQUIRIUM_NS.A, alias="a")

    result = entity_A.related(ACQUIRIUM_NS.B, alias="b", max_depth=2, nearest=False).metadata()
    assert len(result) == 4
    assert set(result.columns) == {"a", "b"}

    result2 = entity_A.related(ACQUIRIUM_NS.C, alias="c", max_depth=3, nearest=False).metadata()
    assert len(result2) == 6
    assert set(result2.columns) == {"a", "c"}


def test_related_chained_from_pointer(acquirium_client: Acquirium):
    acq = acquirium_client
    related = (acq.query().entity(ACQUIRIUM_NS.A, alias="a")
               .related(ACQUIRIUM_NS.B, alias="b", max_depth=1, nearest=False))
    assert len(related.metadata()) == 3

    related_2 = related.related(ACQUIRIUM_NS.C, alias="c", max_depth=1, nearest=False)
    assert len(related_2.query_graph.nodes) == 3
    assert len(related_2.query_graph.edges) == 2
    result2 = related_2.metadata()
    assert len(result2) == 4
    assert set(result2.columns) == {"a", "b", "c"}


def test_related_from_alias_single_hop(acquirium_client: Acquirium):
    acq = acquirium_client
    related = (acq.query().entity(ACQUIRIUM_NS.A, alias="a")
               .related(ACQUIRIUM_NS.B, alias="b", max_depth=1, nearest=False))
    related_2 = related.related(ACQUIRIUM_NS.E, frm="a", alias="e", max_depth=1, nearest=False)
    assert len(related_2.query_graph.nodes) == 3
    assert len(related_2.query_graph.edges) == 2
    result2 = related_2.metadata()
    assert len(result2) == 2
    assert set(result2.columns) == {"a", "b", "e"}


def test_related_from_alias_multi_hop(acquirium_client: Acquirium):
    acq = acquirium_client
    related = (acq.query().entity(ACQUIRIUM_NS.A, alias="a")
               .related(ACQUIRIUM_NS.B, alias="b", max_depth=2, nearest=False))
    assert len(related.metadata()) == 4

    related_2 = related.related(ACQUIRIUM_NS.E, frm="a", alias="e", max_depth=3, nearest=False)
    result2 = related_2.metadata()
    assert len(result2) == 8
    assert set(result2.columns) == {"a", "b", "e"}


def test_unconnected_entity_after_related(acquirium_client: Acquirium):
    acq = acquirium_client
    related = (acq.query().entity(ACQUIRIUM_NS.A, alias="a")
               .related(ACQUIRIUM_NS.B, alias="b", max_depth=2, nearest=False))
    related_2 = related.entity(ACQUIRIUM_NS.C, alias="c")
    assert len(related_2.query_graph.nodes) == 3
    assert len(related_2.query_graph.edges) == 1
    result2 = related_2.metadata()
    assert len(result2) == 12
    assert set(result2.columns) == {"a", "b", "c"}


def test_related_from_alias_two_hops_any(acquirium_client: Acquirium):
    acq = acquirium_client
    related = (acq.query().entity(ACQUIRIUM_NS.A, alias="a")
               .related(ACQUIRIUM_NS.B, alias="b", max_depth=1, nearest=False)
               .related(ACQUIRIUM_NS.C, frm="a", alias="c", max_depth=2, nearest=False))
    assert len(related.query_graph.nodes) == 3
    assert len(related.query_graph.edges) == 2
    result = related.metadata()
    assert len(result) == 5
    assert set(result.columns) == {"a", "b", "c"}
