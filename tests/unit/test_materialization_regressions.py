from dataclasses import replace
from datetime import timedelta

import polars as pl

import pytest

from acquirium.Materialization.planner import Deployment
from acquirium.Storage.duckdb_store import DuckDBStore
from tests.unit.test_incremental_materialization import LineageCopy, LineageGraph
from tests.unit.test_incremental_materialization import Copy, NOW, _port
from acquirium.Materialization import App, ApplicationGraph, Binding, StreamDescriptor
from acquirium.Materialization import align
from acquirium.Materialization import TimeWindow
from acquirium.Materialization import output
from acquirium.Materialization.planner import BindingPlanner


class SumFleet(LineageCopy):
    name = 'sum-fleet'
    grouping = 'all_matches'
    outputs = {'out': output.named('fleet-sum', value_kind='numeric')}

    def transform(self, inputs, output, context):
        output['out'] = inputs['input'].df().group_by('time').agg(pl.col('value').sum())




class FleetGraph(LineageGraph):
    def __init__(self, refs):
        super().__init__()
        self.refs = refs

    def sparql_query(self, query, **kwargs):
        return {'columns': ['v0', 'ext0'],
                'rows': [[ref + ':point', ref] for ref in self.refs]}




def test_grouping_is_independent_of_output_naming():
    class GeneratedAggregate(SumFleet):
        grouping = 'all_matches'
        outputs = {'out': output.stream(value_kind='numeric')}
    # Durable entrypoints cannot be local classes; verify the declaration
    # independently with a replaced deployment targeting the importable app.
    declaration = replace(Deployment.from_class(SumFleet), outputs=GeneratedAggregate.outputs)
    dag, _ = BindingPlanner(FleetGraph(['urn:a', 'urn:b'])).compile([declaration], 1)
    assert len(dag.bindings) == 1
    assert len(dag.bindings[0].inputs['input']) == 2


def test_ambiguous_entity_matches_are_rejected():
    class Ambiguous(FleetGraph):
        def sparql_query(self, query, **kwargs):
            return {'columns': ['v0', 'ext0'], 'rows': [['urn:p1', 'urn:a'], ['urn:p2', 'urn:a']]}
    with pytest.raises(ValueError, match='different entity bindings'):
        BindingPlanner(Ambiguous([])).compile([Deployment.from_class(LineageCopy)], 1)


def test_app_instantiation_requires_an_explicit_valid_grouping():
    class MissingGrouping(App):
        outputs = {'out': output.stream(value_kind='numeric')}

    class InvalidGrouping(App):
        grouping = 'together'
        outputs = {'out': output.stream(value_kind='numeric')}

    with pytest.raises(ValueError, match="MissingGrouping.grouping must be explicitly set.*got None"):
        MissingGrouping()
    with pytest.raises(ValueError, match="InvalidGrouping.grouping must be explicitly set.*got 'together'"):
        InvalidGrouping()


def test_deployment_roundtrip_requires_grouping():
    import json
    declaration = replace(Deployment.from_class(SumFleet), lookahead=timedelta(seconds=2))
    assert Deployment.from_json(declaration.to_json()) == declaration
    payload = json.loads(declaration.to_json())
    payload.pop('grouping')
    with pytest.raises(ValueError, match="grouping must be explicitly set.*got None"):
        Deployment.from_json(json.dumps(payload))
    payload['unexpected_setting'] = 2
    with pytest.raises(ValueError, match='unknown deployment fields'):
        Deployment.from_json(json.dumps(payload))


def test_http_deployment_uses_explicit_grouping_and_rejects_unknown_fields():
    import json
    from pydantic import ValidationError
    from acquirium.Server.app import AppRegistration
    payload = json.loads(Deployment.from_class(SumFleet).to_json())
    payload.pop('grouping')
    with pytest.raises(ValidationError):
        AppRegistration.model_validate(payload)
    with pytest.raises(ValidationError):
        AppRegistration.model_validate({**payload, 'grouping': None})
    with pytest.raises(ValidationError):
        AppRegistration.model_validate({**payload, 'grouping': 'together'})
    with pytest.raises(ValidationError):
        AppRegistration.model_validate({**payload, 'unexpected_setting': 1})




def stored(store, ref):
    return [row for batch in store.timeseries(ref, value_mode='numeric') for row in batch.to_pylist()]


class MinuteMean(Copy):
    grouping = 'per_match'
    every = '1m'

    def transform(self, inputs, output, context):
        output['out'] = align(inputs, '1m').rename({'source': 'value'})


class RollingMean(Copy):
    grouping = 'per_match'
    lookback = '1m'

    def transform(self, inputs, output, context):
        output['out'] = inputs['source'].df().sort('time').select(
            'time', pl.col('value').rolling_mean_by('time', window_size='1m'))






def copy_binding(name, source='urn:input'):
    return Binding(name, 'digest', {'source': (StreamDescriptor(source),)},
                   {'out': _port(ref=f'urn:{name}')})






