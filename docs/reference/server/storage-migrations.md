---
title: Storage upgrades and backups
---

For this pre-1.0 rollout, start Acquirium with **fresh Escapement storage**.
Use a new data directory and new database locations. Keep the old
installation and its stores for historical access.

(move-to-escapement)=
## Start with fresh storage

For a new installation, begin at step 2.

1. Stop the old server. Back up its configuration, data directory, and any
   graph or databases stored elsewhere.
2. In the new configuration, set `data_dir` to an unused path, such as
   `.acquirium-escapement`, and leave `recreate = false`. If you set
   `graph_path`, `engine_path`, or
   `experiments_path` explicitly, give them new locations too. A fresh
   PostgreSQL deployment should use a new database.
3. Remove the old `timeseries_backend`, `duckdb_path`, and `pg_dsn` settings
   if present. If you set them through the server's environment, remove
   `ACQUIRIUM_TIMESERIES_BACKEND`, `ACQUIRIUM_DUCKDB_PATH`, or `PG_DSN` there.
4. [Start the server with the new configuration](cli.md#acquirium-server).
   Load the plant model and start ingestion for the streams you want to use.
   Verify that you can read a new sample before deploying soft sensors.

Use the old Acquirium version to read historical records from its original
stores or a backup.

```{warning}
`recreate = true` erases the configured data directory and clears the
experiment database, including one configured outside that directory.
Use a new path when moving to Escapement.
```

## Backup and restore

These commands cover Escapement deployments. Run them in Bash on a machine
that can read the server's files, and restore with the same Acquirium version
that made the backup.

### Back up the files

Stop Acquirium and wait for it to exit. For a server running in a terminal,
press Ctrl+C in that terminal. Keep it stopped until the file copies and any
PostgreSQL dumps are complete. The graph, readings, and experiment records
refer to each other and need to come from the same stopped server.

From the directory containing `acquirium.toml`, run the following. Replace
`data_dir` below if your server uses a different path, including one set
through `ACQUIRIUM_DATA_DIR`. Choose an existing `backup_root` outside
`data_dir`.

```bash
data_dir="$PWD/.acquirium"
backup_root="$PWD"
backup_dir=$(mktemp -d "$backup_root/acquirium-backup.XXXXXX") &&
  cp -aL "$PWD/acquirium.toml" "$backup_dir/acquirium.toml" &&
  cp -aL "$data_dir/." "$backup_dir/data" &&
  printf 'Backup directory: %s\n' "$backup_dir"
```

This copies the default graph and DuckDB databases along with saved app
functions. The `-L` option copies the targets of symbolic links. If you
configured any of the following outside `data_dir`, run the matching lines
too, substituting your actual paths.

```bash
cp -aL /srv/acquirium/graph "$backup_dir/graph"
cp -aL /srv/acquirium/readings.duckdb "$backup_dir/readings.duckdb"
cp -aL /srv/acquirium/experiments.duckdb "$backup_dir/experiments.duckdb"
```

Those paths correspond to `graph_path`, `engine_path`, and `experiments_path`.
Use the next step for any database setting that is a PostgreSQL URL. If all
your stores are local files, skip that step. Keep the backup directory together
and copy it to separate storage before relying on it for recovery.

### Back up PostgreSQL, if configured

Use PostgreSQL client tools compatible with your server. These commands
back up an entire database. With Acquirium still stopped, substitute the
connection URL from your configuration, including any port and TLS settings.

```bash
source_db='postgresql://acquirium@db.example.org/acquirium'
pg_dump --format=custom --file="$backup_dir/acquirium.dump" "$source_db"
```

If `engine_path` and `experiments_path` use the same database, dump it once.
If they use different databases, run the command for each and use a different
filename for each dump. [PostgreSQL backup command reference](https://www.postgresql.org/docs/current/app-pgdump.html)

### Restore into a new location

Set `backup_dir` to the directory printed during backup. Copy its data to a
new directory and create a configuration for checking the restore without
starting drivers or configured apps. If `18000` is taken, use an available port
in the configuration and in the checks below.

```bash
backup_dir="/absolute/path/to/acquirium-backup.REPLACE_ME"
restore_dir=$(mktemp -d "$PWD/acquirium-restore.XXXXXX") &&
  cp -a "$backup_dir/data" "$restore_dir/data" &&
cat > "$restore_dir/restore.toml" <<'EOF'
[server]
host = "127.0.0.1"
port = 18000
data_dir = "data"
exact_only = true
recreate = false
EOF
```

For each external file or directory you backed up, run its matching command.

```bash
cp -a "$backup_dir/graph" "$restore_dir/graph"
cp -a "$backup_dir/readings.duckdb" "$restore_dir/readings.duckdb"
cp -a "$backup_dir/experiments.duckdb" "$restore_dir/experiments.duckdb"
```

For the files you copied, add the corresponding settings under `[server]`
in `restore.toml`.

```toml
graph_path = "graph"
engine_path = "readings.duckdb"
experiments_path = "experiments.duckdb"
```

For a PostgreSQL dump, create an empty database and restore into it. Replace
the host, user, and restore database name below. If an administrator runs
`createdb`, have them set `--owner=acquirium` using your actual server role.
Run `pg_restore` as that role so it owns the restored tables.

```bash
maintenance_db='postgresql://acquirium@db.example.org/postgres'
restored_db='postgresql://acquirium@db.example.org/acquirium_restore'
createdb --maintenance-db="$maintenance_db" acquirium_restore &&
  pg_restore --single-transaction --no-owner --no-privileges \
    --dbname="$restored_db" "$backup_dir/acquirium.dump"
```

In `restore.toml`, set `engine_path` and/or `experiments_path` to the restored
database URLs. Restore separate dumps into separate databases. If you set
`engine_schema` or a PostgreSQL `search_path` before, keep it the same for
the restored database. [PostgreSQL restore command reference](https://www.postgresql.org/docs/current/app-pgrestore.html)

Start the restore server. The `env` options below keep storage overrides
from the original server out of this check. If you run Acquirium through uv,
use `uv run acquirium server` from its project directory in the last line.

```bash
env -u ACQUIRIUM_DATA_DIR -u ACQUIRIUM_GRAPH_PATH \
    -u ACQUIRIUM_ENGINE_PATH -u ACQUIRIUM_ENGINE_SCHEMA \
    -u ACQUIRIUM_EXPERIMENTS_PATH ACQUIRIUM_RECREATE=false \
    acquirium server --config "$restore_dir/restore.toml"
```

In a second terminal, check that the server answers.

```bash
curl -fsS http://127.0.0.1:18000/health
```

It should return `{"ok":true}`. Then open a Python session and substitute a
source ID and stream name you expect in the backup.

```python
import acquirium as aq

client = aq.Acquirium(server_url="127.0.0.1", server_port=18000)
print(client.stream("gateway", "temperature").dataframe(limit=5))
```

If you recorded experiments, check a run too. Replace `REPLACE_WITH_RUN_ID`.

```bash
curl -fsS http://127.0.0.1:18000/experiments/runs/REPLACE_WITH_RUN_ID
```

After checking the restore, stop the test server. To switch to the restored
stores, stop the original server if you restarted it. Copy the saved
`acquirium.toml` beside your original config under a new name. Set its storage
paths to the restored locations, using absolute paths for local files. Set
`recreate = false` and update any storage environment overrides before
starting it.

## Schema upgrades

For a compatible experiment database, Acquirium applies pending schema changes
at startup and records them in `schema_migrations`. A failed change rolls back.
An unfamiliar migration version or name stops startup. The move to Escapement
uses the fresh-storage process above.

To add a schema change, append a consecutively numbered `Migration` in
`src/acquirium/Storage/migrations.py`. Keep existing entries unchanged. Use
the supplied transaction and test the upgrade, rollback, and reopen on DuckDB
and PostgreSQL.
