---
title: Experiments
---

The experiment interface records what a calculation used and produced. A study holds named variables. Each execution of the study is a run, and each recorded value is an observation of one variable in one run. The interface is reached from the client as `acq.study`.

| Page | Covers |
|---|---|
| [Studies and runs](studies-and-runs.md) | Creating studies, starting and ending runs, deleting, storage use |
| [Declaring variables](declaring-variables.md) | Inputs, outputs and logs of each kind, and listing them |
| [Recording values](recording.md) | Recording values, files, tables, objects and time series in a run |
| [Stream annotations](annotations.md) | Linking intervals of existing streams to a run |
| [Selecting and comparing runs](selecting-runs.md) | Filtering runs and comparing their scalar values |
| [Reading recorded data](reading.md) | Reading observations back as values, tables, samples and objects |
| [Model adapters](model-adapters.md) | Declaring and recording many values of a model at once |
| [Codecs and values](codecs-and-values.md) | Saving Python objects, converting values to JSON, timestamps |

A short session looks like this:

```python
import acquirium as aq

acq = aq.init()
study = acq.study.define("pump tuning")
speed = study.input("speed").scalar(unit="Hz")
cost = study.output("cost").scalar(unit="USD")

run = study.start({"scenario": "baseline"})
speed.record(45)
cost.record(12.5)
run.finish()

print(run.observations(cost).latest().value)
```

```text
12.5
```

## Errors

Checks made in your Python process raise built-in exceptions such as `ValueError`, `TypeError` and `RuntimeError`. Checks made by the server raise `requests.HTTPError`. The message ends with the reason given by the server, and the status code is 400 for a request that is not valid, 404 for something that does not exist and 409 for a delete that is refused.

## Studies and runs

| Item | Description |
|---|---|
| [`acq.study.define`](studies-and-runs.md#acqstudydefine) | Create a study, or reopen the one with that name |
| [`acq.study.get`](studies-and-runs.md#acqstudyget) | Reopen a saved study by name or ID |
| [`acq.study.all`](studies-and-runs.md#acqstudyall) | List every study |
| [`acq.study.usage`](studies-and-runs.md#acqstudyusage) | Count and size what all studies store |
| [`acq.study.clear`](studies-and-runs.md#acqstudyclear) | Delete every study and run |
| [`Study`](studies-and-runs.md#study) | Attributes of a study |
| [`Study.start`](studies-and-runs.md#studystart) | Start a run |
| [`Study.resume`](studies-and-runs.md#studyresume) | Continue a run that is still running |
| [`Study.usage`](studies-and-runs.md#studyusage) | Count and size what one study stores |
| [`Study.delete`](studies-and-runs.md#studydelete) | Delete a study with its declarations and runs |
| [`Experiment`](studies-and-runs.md#experiment) | Attributes of a run |
| [`Experiment.finish`](studies-and-runs.md#experimentfinish) | Mark a run as succeeded |
| [`Experiment.fail`](studies-and-runs.md#experimentfail) | Mark a run as failed and save the error |
| [`Experiment.as_dict`](studies-and-runs.md#experimentas_dict) | Return the run as a dictionary |
| [`Experiment.delete`](studies-and-runs.md#experimentdelete) | Delete a finished run and what it recorded |

## Declaring variables

| Item | Description |
|---|---|
| [`Study.input`](declaring-variables.md#studyinput) | Start declaring an input |
| [`Study.output`](declaring-variables.md#studyoutput) | Start declaring an output |
| [`Study.log`](declaring-variables.md#studylog) | Declare a log variable for events |
| [`builder.scalar`](declaring-variables.md#builderscalar) | A number, with an optional unit |
| [`builder.text`](declaring-variables.md#buildertext) | A string |
| [`builder.json`](declaring-variables.md#builderjson) | Structured JSON |
| [`builder.file`](declaring-variables.md#builderfile) | A copy of a file |
| [`builder.table`](declaring-variables.md#buildertable) | A table |
| [`builder.object`](declaring-variables.md#builderobject) | A Python object saved through a codec |
| [`builder.timeseries`](declaring-variables.md#buildertimeseries) | Numeric samples over time |
| [`ExperimentVariable`](declaring-variables.md#experimentvariable) | Attributes of a declared variable |
| [`Study.variables`](declaring-variables.md#studyvariables) | Every declared variable of the study |
| [`Study.variables.where`](declaring-variables.md#studyvariableswhere) | Filter variables |
| [`Study.variables.all`](declaring-variables.md#studyvariablesall) | Return the selected variables as a list |
| [`Study.variables.frame`](declaring-variables.md#studyvariablesframe) | Return the selected variables as a table |
| [`Study.variables.get`](declaring-variables.md#studyvariablesget) | Look up one variable with a default |
| [`Study.variables.query`](declaring-variables.md#studyvariablesquery) | Query variable metadata |

## Recording values

| Item | Description |
|---|---|
| [`ExperimentVariable.record`](recording.md#experimentvariablerecord) | Record a value of any kind |
| [`ExperimentVariable.set`](recording.md#experimentvariableset) | Record a scalar, text, JSON or log value |
| [`ExperimentVariable.append`](recording.md#experimentvariableappend) | Record a log event |
| [`ExperimentVariable.attach`](recording.md#experimentvariableattach) | Record a file |
| [`ExperimentVariable.add`](recording.md#experimentvariableadd) | Record time-series rows |
| [`Experiment.output`](recording.md#experimentoutput) | Record outputs by label on a run |

## Stream annotations

| Item | Description |
|---|---|
| [`Experiment.annotate_stream`](annotations.md#experimentannotate_stream) | Link a stream interval to a run |
| [`Experiment.stream_annotations`](annotations.md#experimentstream_annotations) | Find the annotations of one run |
| [`Study.stream_annotations`](annotations.md#studystream_annotations) | Find the annotations of one study |
| [`acq.study.stream_annotations`](annotations.md#acqstudystream_annotations) | Find annotations across all studies |
| [`StreamAnnotation`](annotations.md#streamannotation) | Attributes of an annotation |
| [`StreamAnnotation.close`](annotations.md#streamannotationclose) | Set the end of an open annotation |
| [`StreamAnnotation.dataframe`](annotations.md#streamannotationdataframe) | Read the samples in the interval |

## Selecting and comparing runs

| Item | Description |
|---|---|
| [`Study.experiments`](selecting-runs.md#studyexperiments) | The runs of a study |
| [`ExperimentCollection.where`](selecting-runs.md#experimentcollectionwhere) | Filter runs |
| [`ExperimentCollection.all`](selecting-runs.md#experimentcollectionall) | Return the selected runs as a list |
| [`ExperimentCollection.frame`](selecting-runs.md#experimentcollectionframe) | Return the selected runs as a table |
| [`ExperimentCollection.get`](selecting-runs.md#experimentcollectionget) | Look up one run by ID |
| [`ExperimentCollection.scalars`](selecting-runs.md#experimentcollectionscalars) | Compare scalar values across runs |
| [`ExperimentCollection.query`](selecting-runs.md#experimentcollectionquery) | Query run metadata |
| [`ExperimentCollection.usage`](selecting-runs.md#experimentcollectionusage) | Count and size what the selected runs store |
| [`ExperimentCollection.delete`](selecting-runs.md#experimentcollectiondelete) | Delete the selected runs |

## Reading recorded data

| Item | Description |
|---|---|
| [`Experiment.observations`](reading.md#experimentobservations) | The observations of one run |
| [`ExperimentVariable.observations`](reading.md#experimentvariableobservations) | The observations of one variable across runs |
| [`ExperimentCollection.observations`](reading.md#experimentcollectionobservations) | The observations of a selection of runs |
| [`ObservationCollection.all`](reading.md#observationcollectionall) | Return the observations as a list |
| [`ObservationCollection.frame`](reading.md#observationcollectionframe) | Return the observations as a table |
| [`ObservationCollection.latest`](reading.md#observationcollectionlatest) | Return the last observation |
| [`Observation`](reading.md#observation) | Attributes of an observation |
| [`Observation.dataframe`](reading.md#observationdataframe) | Read time-series samples or a table |
| [`Observation.read_bytes`](reading.md#observationread_bytes) | Read the bytes of a file, table or object |
| [`Observation.load`](reading.md#observationload) | Rebuild a recorded object |
| [`Observation.as_dict`](reading.md#observationas_dict) | Return the observation as a dictionary |

## Model adapters

| Item | Description |
|---|---|
| [`adapters.define`](model-adapters.md#adaptersdefine) | Create an empty adapter for any Python model |
| [`adapters.pyomo`](model-adapters.md#adapterspyomo) | Create an empty adapter for a Pyomo model |
| [`AdapterDefinition`](model-adapters.md#adapterdefinition) | The adapter builder and its `input` and `output` namespaces |
| [`AdapterDefinition.scalar`](model-adapters.md#adapterdefinitionscalar) | Declare a number |
| [`AdapterDefinition.text`](model-adapters.md#adapterdefinitiontext) | Declare a string |
| [`AdapterDefinition.json`](model-adapters.md#adapterdefinitionjson) | Declare a JSON value |
| [`AdapterDefinition.table`](model-adapters.md#adapterdefinitiontable) | Declare a table |
| [`AdapterDefinition.object`](model-adapters.md#adapterdefinitionobject) | Declare an object saved through a codec |
| [`AdapterDefinition.timeseries`](model-adapters.md#adapterdefinitiontimeseries) | Declare a value sampled over time |
| [`AdapterDefinition.add`](model-adapters.md#adapterdefinitionadd) | Register a prepared `Component` |
| [`AdapterDefinition.frame`](model-adapters.md#adapterdefinitionframe) | List the declarations as a table |
| [`AdapterDefinition.preview`](model-adapters.md#adapterdefinitionpreview) | Read every value locally |
| [`AdapterDefinition.freeze`](model-adapters.md#adapterdefinitionfreeze) | Return a copy that accepts no more declarations |
| [`Study.declare_from`](model-adapters.md#studydeclare_from) | Declare an adapter's variables in a study |
| [`DeclaredVariables`](model-adapters.md#declaredvariables) | The variables declared from an adapter |
| [`DeclaredVariables.record_values`](model-adapters.md#declaredvariablesrecord_values) | Read and record every value that is not a time series |
| [`DeclaredVariables.sample`](model-adapters.md#declaredvariablessample) | Read and record one sample of every time series |
| [`DeclaredVariables.preview`](model-adapters.md#declaredvariablespreview) | Read the selected values locally |
| [`DeclaredVariables.frame`](model-adapters.md#declaredvariablesframe) | List the selected variables as a table |
| [`Component`](model-adapters.md#component) | The description of one declared value |
| [`ModelAdapter`](model-adapters.md#modeladapter) | The protocol a custom adapter implements |

## Codecs and values

| Item | Description |
|---|---|
| [`Codec`](codecs-and-values.md#codec) | The interface a codec implements |
| [`PickleCodec`](codecs-and-values.md#picklecodec) | Save objects with Python's pickle |
| [`CloudpickleCodec`](codecs-and-values.md#cloudpicklecodec) | Save objects with cloudpickle |
| [`JsonCodec`](codecs-and-values.md#jsoncodec) | Save a JSON document as a file |
| [`BytesCodec`](codecs-and-values.md#bytescodec) | Save bytes unchanged |
| [Value conversion](codecs-and-values.md#value-conversion) | How recorded values and metadata become JSON |
| [`register_value_handler`](codecs-and-values.md#register_value_handler) | Convert your own type to JSON when it is recorded |
| [`timestamp`](codecs-and-values.md#timestamp) | Parse a time and convert it to UTC |
