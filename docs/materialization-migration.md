# Updating materialization apps

Update clients and servers together. Existing stored deployments are readable:
missing grouping is inferred using the old output declarations, and the old
coalesce/max_delay values become their equivalent fixed batch delay.

For source modules being redeployed:

- Replace `aq.output.per_row(...)` with `aq.output.stream(...)`.
- Set `grouping = "all_matches"` on fleet aggregates. The default is
  `"per_match"`; output naming does not select grouping.
- Set `every = "1m"` (or the appropriate bucket size) on resampling apps.
  Then use `aq.align(inputs)`.
- Replace `coalesce` and `max_delay` with `batch_delay`.
  Its timer runs from the first pending change and does not restart.
- Review `lookback` and `lookahead` as input dependencies. Corrections expand
  the affected output interval in the opposite direction as well.
- Assign an empty typed table when previous results should disappear.
  Leave a port unassigned only when it should remain unchanged.
- Use `reprocess_app(name, start, end)` to repair history after code changes.

Generated and explicitly named stream identities are unchanged. Existing
binding progress is preserved across code edits. The new
`materialization_work` table is created additively on startup; no timeseries
data needs to be dropped. It records pending bounded work and is updated
atomically with output publication.
The lineage table gains an additive context fingerprint column so query-match
changes also trigger retained output repair after a restart.

This upgrade assumes the branch's revision-frontier schema already exists
(`binding_progress.progress_key`, `timeseries.last_revision`, and
`timeseries.deleted`). It does not migrate older pre-materialization databases.

After upgrading, reprocess retained history for bucketed calculations that may
have published partial boundary buckets, and filtered outputs that may have
retained obsolete rows. Take a normal database backup before a production
upgrade. Existing stored data is retained unless an assigned replacement
explicitly recomputes its interval.
