---
title: Drivers
---

This is not a guide to authoring drivers; for that go to
[your first driver](../tutorials/first-driver.md) and the [driver reference](../reference/drivers.md).
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

## Drivers and the engine

Two loops run side by side. A driver reads its source, batches what it read
and inserts it. The engine's processor takes accepted writes from its inbox,
applies them in epochs, runs the apps they affect and publishes the result.
The [Escapement inbox](escapement-core.md#writes) sits between them, and it is
what keeps them apart.

An insert returns once the server has accepted the write: the request is on
disk in the inbox, durable but not yet visible. The driver does not wait for
the epoch that publishes it. So how fast a driver can send is set by the
acceptance round trip (the HTTP request, normalizing the batch and committing
it to the inbox, typically a few tens of milliseconds), not by how long apps
take to run. A slow function delays when readings become visible, not how fast
drivers deliver them; the processor catches up on its own.

The loops meet at three points:

- **Acceptance hands over responsibility.** Until a write is accepted, the
  reading is held by its source or by the driver. Afterwards the server holds
  it, through crashes and restarts. A source that can redeliver, such as an
  MQTT broker, is therefore acknowledged at acceptance, never on receipt.
- **A full inbox pushes back.** The inbox holds `max_pending_requests`
  accepted writes (100,000 by default). If publication falls that far behind,
  inserts get 503, and drivers keep their rows and retry. This is the only way
  the engine slows a driver down.
- **They share a store.** Admission and the processor commit to the same
  database, so heavy publication can lengthen acceptance somewhat, though not
  enough to matter in the measurements so far.

A driver's throughput is the rows it sends per insert divided by the round
trip. Event drivers gather readings for up to 0.1 s before inserting, and they
send the next batch as soon as the last is accepted. The MQTT driver keeps up
to 65,535 messages unacknowledged at once, so the broker keeps sending while
an insert is in flight. Against a local server one MQTT driver keeps up with
about 20,000 messages a second.

Between the source and publication, a reading is in one of these places:

| Stage | Held by | Survives |
|---|---|---|
| Not yet accepted, from a broker | the broker, until acknowledged | a driver crash; a broker restart only if the broker persists its queues |
| Not yet accepted, from a polled source | the driver's memory | nothing, though it is held only from the read until its insert is accepted |
| Accepted, not yet published | the inbox | a server crash or restart; it is processed at the next start |
| Published | the store | everything |

Drivers run as children of the server, so a server restart stops them too.
Accepted writes wait in the inbox, but while the drivers are down a broker
has to queue what arrives, and its own limits decide how much it keeps. See
[broker settings](../reference/drivers.md#broker-settings).

## Where the design comes from

The driver model is borrowed from
[sMAP](https://pythonhosted.org/Smap/en/2.0/tutorial.html), the Simple
Measurement and Actuation Profile from UC Berkeley.
In sMAP, a driver is a small Python class per source: it declares its
timeseries in `setup()`, is started by the daemon, and reports readings with
`add()`, while a configuration file lists the drivers to run and an archiver
stores what they report.
