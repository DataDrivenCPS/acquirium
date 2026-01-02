import pytest
from acquirium import Acquirium
from acquirium.internals.internals_namespaces import *
from acquirium.Client.query import Query
import shutil

@pytest.fixture
def acquirium_client():
    """Fixture to create an Acquirium client for testing."""
    acq = Acquirium(
        server_url="localhost",
        server_port=8000,
        use_ssl=False,
        lexicon_path="ontologies/lexicon.json",
    )

    acq.insert_graph("tests/test_model.ttl")
    return acq

#### FIND ENTITY TESTS ####

def test_find_entity(acquirium_client:Acquirium):
    """Test the find_entity method of Acquirium."""
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")
    
    assert entity_A is not None
    assert len(entity_A.query_graph.nodes) == 1
    assert len(entity_A.query_graph.edges) == 0

    result_A = entity_A.execute()

    assert result_A is not None
    assert len(result_A) == 2  
    assert len(result_A['rows']) == 4

    entity_B : Query = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")

    assert entity_B is not None
    assert len(entity_B.query_graph.nodes) == 1
    assert len(entity_B.query_graph.edges) == 0
    
    result_B = entity_B.execute()

    assert result_B is not None
    assert len(result_B) == 2
    assert len(result_B['rows']) == 2

    result_B_df = entity_B.metadata()
    assert result_B_df is not None
    assert len(result_B_df) == 2
    assert 'b' in result_B_df.columns
    assert len(result_B_df.columns) == 1


    entity_A = entity_A.find_entity(_class=ACQUIRIUM_NS.C, alias="c")

    assert entity_A is not None
    assert len(entity_A.query_graph.nodes) == 2
    assert len(entity_A.query_graph.edges) == 0

    result_AC = entity_A.execute()

    assert result_AC is not None
    assert len(result_AC) == 2
    assert len(result_AC['rows']) == 3*4

    result_AC_df = entity_A.metadata()

    assert result_AC_df is not None
    assert len(result_AC_df) == 3*4
    assert 'c' in result_AC_df.columns and 'a' in result_AC_df.columns
    assert len(result_AC_df.columns) == 2

#### FIND RELATED TESTS ####

def test_find_related_1(acquirium_client:Acquirium):
    """Test the find_related method of Acquirium.
       This test checks the basic functionality of finding related entities.
       Given single predicate and single hop.
    """
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")

    related_entities = entity_A.find_related(
        _class = ACQUIRIUM_NS.C,
        predicates=[ACQUIRIUM_NS.x],
        alias="c"
    )
    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 1
    assert 'a' in result.columns and 'c' in result.columns
    assert len(result.columns) == 2

def test_find_related_2(acquirium_client:Acquirium):
    '''
    Test the find_related method of Acquirium.
    This test checks the basic functionality of finding related entities.
    Given single predicate and multiple hops.
    '''
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")
    
    related_entities = entity_A.find_related(
        _class = ACQUIRIUM_NS.C,
        predicates=[ACQUIRIUM_NS.x],
        alias="c",
        hops = 2,
        multi_hop_predicates = True
    )
    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 3
    assert 'a' in result.columns and 'c' in result.columns
    assert len(result.columns) == 2

def test_find_related_3(acquirium_client:Acquirium):
    '''
    Test the find_related method of Acquirium.
    This test checks the basic functionality of finding related entities.
    Given multiple predicate and single hops.
    '''
    acq = acquirium_client
    entity_B : Query = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    
    related_entities = entity_B.find_related(
        _class = ACQUIRIUM_NS.A,
        predicates=[ACQUIRIUM_NS.z, ACQUIRIUM_NS.w],
        alias="a"
    )

    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 3
    assert 'a' in result.columns and 'b' in result.columns
    assert len(result.columns) == 2

def test_find_related_4(acquirium_client:Acquirium):
    '''
    Test the find_related method of Acquirium.
    This test checks the basic functionality of finding related entities.
    Given no predicate and single hop.
    '''
    acq = acquirium_client
    entity_B : Query = acq.find_entity(_class=ACQUIRIUM_NS.B, alias="b")
    
    related_entities = entity_B.find_related(
        _class = ACQUIRIUM_NS.A,
        alias="a",
        hops = 1
    )

    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 3
    assert 'a' in result.columns and 'b' in result.columns
    assert len(result.columns) == 2


def test_find_related_5(acquirium_client:Acquirium):
    '''
    Test the find_related method of Acquirium.
    This test checks the basic functionality of finding related entities.
    Given no predicate and multi hop.
    '''
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")

    related_entities = entity_A.find_related(
        _class = ACQUIRIUM_NS.B,
        alias="b",
        hops = 2
    )

    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 4
    assert 'a' in result.columns and 'b' in result.columns
    assert len(result.columns) == 2

    related_entities_2 = entity_A.find_related(
        _class = ACQUIRIUM_NS.C,
        alias="c",
        hops = 3
    )
    assert related_entities_2 is not None
    assert len(related_entities_2.query_graph.nodes) == 2
    assert len(related_entities_2.query_graph.edges) == 1

    result2 = related_entities_2.metadata()
    assert result2 is not None
    assert len(result2) == 6
    assert 'a' in result2.columns and 'c' in result2.columns
    assert len(result2.columns) == 2

def test_find_related_6(acquirium_client:Acquirium):
    '''
    Test the find_related method of Acquirium.
    This test checks the basic functionality of finding related entities.
    Multiple calls using current pointer, single hop
    '''
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")

    related_entities = entity_A.find_related(
        _class = ACQUIRIUM_NS.B,
        alias="b",
        hops = 1
    )

    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 3
    assert 'a' in result.columns and 'b' in result.columns
    assert len(result.columns) == 2

    related_entities_2 = related_entities.find_related(
        _class = ACQUIRIUM_NS.C,
        alias="c",
        hops = 1
    )
    assert related_entities_2 is not None
    assert len(related_entities_2.query_graph.nodes) == 3
    assert len(related_entities_2.query_graph.edges) == 2

    result2 = related_entities_2.metadata()
    assert result2 is not None
    assert len(result2) == 4
    assert 'a' in result2.columns and 'c' in result2.columns and 'b' in result2.columns
    assert len(result2.columns) == 3

def test_find_related_7(acquirium_client:Acquirium):
    '''
    Test the find_related method of Acquirium.
    This test checks the basic functionality of finding related entities.
    Multiple calls using from param, single hop
    '''
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")

    related_entities = entity_A.find_related(
        _class = ACQUIRIUM_NS.B,
        alias="b",
        hops = 1
    )

    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 3
    assert 'a' in result.columns and 'b' in result.columns
    assert len(result.columns) == 2

    related_entities_2 = related_entities.find_related(
        _class = ACQUIRIUM_NS.E,
        _from = "a",
        alias="e",
        hops = 1
    )
    assert related_entities_2 is not None
    assert len(related_entities_2.query_graph.nodes) == 3
    assert len(related_entities_2.query_graph.edges) == 2

    result2 = related_entities_2.metadata()
    assert result2 is not None
    assert len(result2) == 2
    assert 'a' in result2.columns and 'e' in result2.columns and 'b' in result2.columns
    assert len(result2.columns) == 3

def test_find_related_8(acquirium_client:Acquirium):
    '''
    Test the find_related method of Acquirium.
    This test checks the basic functionality of finding related entities.
    Multiple calls using from param, multi hop
    '''
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")

    related_entities = entity_A.find_related(
        _class = ACQUIRIUM_NS.B,
        alias="b",
        hops = 2
    )

    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 4
    assert 'a' in result.columns and 'b' in result.columns
    assert len(result.columns) == 2

    related_entities_2 = related_entities.find_related(
        _class = ACQUIRIUM_NS.E,
        _from = "a",
        alias="e",
        hops = 3
    )
    assert related_entities_2 is not None
    assert len(related_entities_2.query_graph.nodes) == 3
    assert len(related_entities_2.query_graph.edges) == 2

    result2 = related_entities_2.metadata()
    assert result2 is not None
    assert len(result2) == 8
    assert 'a' in result2.columns and 'e' in result2.columns and 'b' in result2.columns
    assert len(result2.columns) == 3

def test_find_related_9(acquirium_client:Acquirium):
    '''
    Test the find_related method of Acquirium.
    This test checks the basic functionality of finding related entities.
    Multiple calls using from param, multi hop
    also unconnected find_entity after find_related
    '''
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")

    related_entities = entity_A.find_related(
        _class = ACQUIRIUM_NS.B,
        alias="b",
        hops = 2
    )

    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 4
    assert 'a' in result.columns and 'b' in result.columns
    assert len(result.columns) == 2

    related_entities_2 = related_entities.find_entity(
        _class = ACQUIRIUM_NS.C,
        alias="c"
        )
    assert related_entities_2 is not None
    assert len(related_entities_2.query_graph.nodes) == 3
    assert len(related_entities_2.query_graph.edges) == 1

    result2 = related_entities_2.metadata()
    assert result2 is not None
    assert len(result2) == 12
    assert 'a' in result2.columns and 'c' in result2.columns and 'b' in result2.columns
    assert len(result2.columns) == 3

#### RELATE TO TESTS ####

def test_relate_to_1(acquirium_client:Acquirium):
    '''
    Test the relate_to method of Acquirium.
    This test checks the basic functionality of relating entities.
    Given single predicate and single hop.
    '''
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")
    entity_C : Query = acq.find_entity(_class=ACQUIRIUM_NS.C, alias="c")

    related_entities = entity_A.relate_to(
        other = entity_C,
        hops = 1,
        predicates = [ACQUIRIUM_NS.x]
    )
    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 2
    assert len(related_entities.query_graph.edges) == 1

    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 1
    assert 'a' in result.columns and 'c' in result.columns
    assert len(result.columns) == 2

def test_relate_to_2(acquirium_client:Acquirium):
    '''
    Test the relate_to method of Acquirium.
    This test checks the basic functionality of relating entities.
    Given from parameter and single hop.
    '''
    acq = acquirium_client
    entity_A : Query = acq.find_entity(_class=ACQUIRIUM_NS.A, alias="a")
    entity_AB = entity_A.find_related(
        _class = ACQUIRIUM_NS.B,
        alias="b",
        hops = 1
    )
    entity_C : Query = acq.find_entity(_class=ACQUIRIUM_NS.C, alias="c")
    related_entities = entity_AB.relate_to(
        other = entity_C,
        _from = "a",
        hops = 2,
    )
    assert related_entities is not None
    assert len(related_entities.query_graph.nodes) == 3
    assert len(related_entities.query_graph.edges) == 2
    
    result = related_entities.metadata()
    assert result is not None
    assert len(result) == 5
    assert 'a' in result.columns and 'c' in result.columns and 'b' in result.columns
    assert len(result.columns) == 3


    