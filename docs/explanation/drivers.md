---
title: Drivers
---

This is not a guide to authoring drivers; for that go to
[building drivers](../how-to/drivers.md).
This is an explanation of why drivers exist in acquirium and where the design
comes from.

## Why drivers

Plant data comes from heterogeneous sources: a SCADA historian, a lab
spreadsheet dropped into a folder every morning, an MQTT broker, a process
simulation, a single instrument on a serial line.
They differ in three ways that matter to us.

- **Pull or push.** Some sources have to be polled (a historian, a file
  directory, a simulation), others push readings to you when they feel like it
  (a broker, a callback-based SDK).
- **Shape.** Some deliver one reading at a time, others a whole frame per
  visit; some name streams by column, others by topic or tag.
- **Continuity.** Ingestion is not a one-off import.
  A plant produces data as long as it runs, so the process reading it has to
  keep running too, survive restarts, and pick up where it left off.

We could ask every source to be adapted to a common format and then imported.
Instead, we accept the differences and put a small piece of code, a driver,
between each source and the server.
A driver knows one source: how to reach it, which streams it produces, and
how to turn what it reads into `(stream, timestamp, value)` observations.
Everything that is the same for every source, scheduling, registration,
batching, insertion, state and shutdown, is owned by the platform, so a
driver stays small and the platform can run many of them side by side.

This split is also what ties data to meaning.
A driver declares its streams against points in the semantic model, so the
moment a reading arrives it is already attached to the equipment, the unit
and the quantity it describes, and every query in this documentation can find
it.

**TODO:** edge deployment, where a driver runs next to the source and pushes to a remote server

## Where the design comes from

The driver model is borrowed from
[sMAP](https://pythonhosted.org/Smap/en/2.0/tutorial.html), the Simple
Measurement and Actuation Profile from UC Berkeley.
In sMAP, a driver is a small Python class per source: it declares its
timeseries in `setup()`, is started by the daemon, and reports readings with
`add()`, while a configuration file lists the drivers to run and an archiver
stores what they report.
