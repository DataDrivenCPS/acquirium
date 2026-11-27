---
title: Storage upgrades and backups
---

Moving from the previous Acquirium storage engine to Escapement requires a
**new data directory and new database locations**. The new server starts with
empty stores. Keep the old installation and its data for export.

## Move to Escapement

1. Stop the old server. Back up its configuration, data directory, and any
   graph or databases stored elsewhere.
2. In the new configuration, set `data_dir` to a new path and leave
   `recreate = false`. If you set `graph_path`, `engine_path`, or
   `experiments_path` explicitly, give them new locations too. A fresh
   PostgreSQL deployment should use a new database.
3. Remove the old `timeseries_backend`, `duckdb_path`, and `pg_dsn` settings
   if present. If you set them through the server's environment, remove
   `ACQUIRIUM_TIMESERIES_BACKEND`, `ACQUIRIUM_DUCKDB_PATH`, or `PG_DSN` there.
   Then [start the server with the new configuration](cli.md#acquirium-server).

If you need the old readings or experiment records, use the old Acquirium
version with its original stores or a backup to export them.

```{warning}
`recreate = true` erases the configured data directory and clears the
experiment database, including one configured outside that directory.
Use a new path when moving to Escapement.
```

## Backup and restore

For an Escapement deployment, stop the server before taking a backup. Save
the configuration and copy the whole `data_dir`. If any of these settings
point outside the directory, back up those locations too.

| Setting | What it holds |
| --- | --- |
| `graph_path` | The plant model, stream registrations, and other graph metadata |
| `engine_path` | Raw readings, derived readings, and samples recorded by experiments |
| `experiments_path` | Experiment definitions, runs, observations, files, and logs |

For a PostgreSQL location, take a database backup. If multiple settings use
the same database, back it up once. See [server configuration](configuration.md)
for the default paths and environment overrides.

To restore, stop the server and restore the directory and databases from the
same backup. Point the configuration at the restored locations and start with
`recreate = false`. An experiment record can refer to samples in the readings
database and metadata in the graph, so the stores need to match.

## Schema upgrades

For a compatible experiment database, Acquirium applies pending schema changes
at startup and records them in `schema_migrations`. A failed change rolls back.
An unfamiliar migration version or name stops startup. The move to Escapement
uses the fresh-storage process above.

To add a schema change, append a consecutively numbered `Migration` in
`src/acquirium/Storage/migrations.py`. Keep existing entries unchanged. Use
the supplied transaction and test the upgrade, rollback, and reopen on DuckDB
and PostgreSQL.
