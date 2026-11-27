---
title: How experiments work
---

In this article we'll describe how Acquirium stores experiments, and why the experiment interface is built the way it is.
The steps for using it are in the tutorial [Running experiments](../tutorials/advanced/experiments.md).

## Studies, variables, runs and observations

An experiment record has four parts.

- A **study** is one question. It holds the declarations that all its runs share.
- A **variable** is a named input, output or log of the study. It has a kind, such as scalar, text or timeseries, and it can have a unit.
- A **run** is one execution of the study. It has a status, a start time, an end time and a description of your own.
- An **observation** is one value recorded for one variable in one run.

The variables are declared on the study and not on the run.
This is why runs can be compared: every run records the same variable under the same label, the same kind and the same unit.
Declaring a label again with a different kind or unit is an error.
Declaring a new variable while a run is active is allowed, but it gives a warning, because the earlier runs do not have a value for it.

A variable can be recorded several times in a run.
Each `record()` call adds a new observation with a sequence number, and earlier observations are never changed.
`latest()` gives the last one.

## The active run

`variable.record(value)` does not take a run.
The value goes to the active run of the study object, which is the run created by `study.start()` or `study.resume()`.
This is useful for passing variables to your own functions without passing the run along.

A study object has one active run at a time.
Two runs can be open at the same time when each one is started from its own study object, for instance from two scripts.

An observation is saved at the moment it is recorded.
If the script stops with an error, the values recorded so far are kept, and the run stays open until `fail()` or `finish()` is called on it.
A run that has ended does not accept new values.

## Storage

Experiments are stored on the server, next to the plant data.

- Scalar, text, JSON and log values are stored directly in the database of the server.
- Files, tables and objects are stored as files in the same database. A table is saved as a Parquet file, and an object is saved by its codec. Each file is identified by the hash of its content, so two runs that record the same file share one copy.
- Timeseries values are stored as streams, in the same place as the streams of the plant. The run keeps the `stream_uri` and the time range of what it recorded.

The descriptions of studies, variables and runs are also written to the metadata graph.
This is why `where()` accepts the same expressions as a plant query, and why a unit such as `"kW"` is matched in the same way as the unit of a stream.

A backup of the experiments needs both the database and the metadata graph.
With a local server started by `aq.init()`, both are in the `.acquirium` folder.

## Timeseries of a run and the plant

Each run writes its timeseries to streams of its own, with the `source_id` `experiment/<run_id>`.
Two runs can therefore record different values at the same times without overwriting each other.

These streams are listed by `acq.find_all_data()`, and `acq.query().measurement(run_id=...)` selects the streams of one run.

`observed=` on a timeseries variable names the property of the plant model that the values describe.
It is a description of the run's stream, and it does not connect the stream to the property.
A plant query on the property returns only the measurements of the plant, so simulated values are never mixed with measured values.
The runs that describe a property are found with `acq.query().measurement(observed=property_uri)`.

Scalar, text, JSON, file, table and object values are not streams.
They are read through the study only.

## Plant data used by a run

A run can refer to data that is already in Acquirium in three ways.

- `run.annotate_stream()` links a time range of a stream to the run. No variable is needed, and the end of the range can be left open and closed later.
- Recording `stream.between(start, end)` to a timeseries variable stores the stream and the time range as a named input.
- Recording the rows, such as `stream.between(start, end).dataframe()`, stores a copy of the values in a stream of the run.

The first two store a reference and not the values.
Reading them later returns what the stream holds at that moment, including corrections made after the run.
The third one is a fixed copy.
Use it when the run must be reproducible with exactly the data it used.

## Deleting

A run cannot be edited after it ends, but it can be deleted.
Deleting a run removes its observations, its stream annotations, the streams it wrote and the files that no other run uses.
The streams of the plant are not changed, including the ones the run annotated or referred to.

Two cases are refused:

- a run that is still running,
- a run whose stream is recorded as an input of another run, unless both runs are deleted together.

`usage()` on a study or on a selection of runs counts what is stored and estimates its size.
The size of files is exact.
The size of samples and values is an estimate, and it is not the size on disk.

## Next

- [Record experiments](../how-to/record-experiments.md), [Record a whole model](../how-to/record-a-whole-model.md) and [Compare experiment runs](../how-to/compare-experiment-runs.md) show the steps.
- The [experiment reference](../reference/experiments/_index.md) describes every method.
