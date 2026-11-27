---
title: Drivers
---

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

## Drivers and the engine

Two loops run side by side. A driver reads its source, batches what it read
and inserts it. The engine's processor takes accepted writes from its inbox,
applies them in epochs, runs the apps they affect and publishes the result.
The [Escapement inbox](escapement-core.md#writes) sits between them, and it is
what keeps them apart.

An insert returns once the server has accepted the write into its durable
inbox. The driver can then send another batch while apps process the accepted
readings. A slow calculation delays publication; drivers continue sending until
the inbox reaches its limit.

The loops meet at three points:

- **Acceptance hands over responsibility.** Until a write is accepted, the
  reading is held by its source or by the driver. Afterwards the server holds
  it, through crashes and restarts. A source that can redeliver, such as an
  MQTT broker, is therefore acknowledged at acceptance, never on receipt.
- **A full inbox pushes back.** The inbox holds `max_pending_requests`
  accepted writes (100,000 by default). If publication falls that far behind,
  inserts get 503, and drivers keep their rows and retry.
- **They share a store.** Admission and publication commit to the same database,
  so heavy publication can also lengthen the acceptance round trip.

Between the source and publication, a reading is in one of these places:

| Stage | Held by | Survives |
|---|---|---|
| Not yet accepted, from a broker | the broker, until acknowledged | a driver crash; a broker restart only if the broker persists its queues |
| Not yet accepted, from a polled source | the driver's memory | nothing, though it is held only from the read until its insert is accepted |
| Accepted, not yet published | the inbox | a server crash or restart; it is processed at the next start |
| Published | the store | a server crash or restart; recovery from storage loss requires a backup |

Drivers run as children of the server, so a server restart stops them too.
Accepted writes wait in the inbox, but while the drivers are down a broker
has to queue what arrives, and its own limits decide how much it keeps. See
[broker settings](../reference/drivers/_index.md#broker-settings).

Start with [File ingestion driver](../tutorials/advanced/writing-a-driver.md) to write one.
The [driver reference](../reference/drivers/_index.md) lists hooks and configuration.
