# Materialization runtime

The authoritative design is [`materialization_epochs.md`](materialization_epochs.md).

The runtime now has one durable topology-epoch reconciler.  Graph publication
creates an immutable resolved epoch; claim recovery, staged range work, atomic
dependency-component seals, supersession, state-revision pinning, and
compaction are described in that document.
