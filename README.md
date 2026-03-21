# pgmon

A PostgreSQL monitoring TUI inspired by `pg_activity`.

## Features

- Real-time views of:
  - `pg_stat_activity`
  - `pg_stat_database`
  - `pg_locks`
  - `pg_stat_io` (PostgreSQL 16+)
  - `pg_stat_statements` (if extension exists)
- Pg-activity-inspired `Activity` dashboard with sampled TPS/DML/temp rates, session counts, and worker/process summaries
- Activity subviews for interesting sessions, active, waiting, blocking, and idle in transaction backends
- Interactive TUI (Tabs, Table navigation)
- Configurable refresh rate and top-N rows.

## Installation

```bash
cargo build --release
```

## Usage

```bash
pgmon --dsn "postgresql://user:password@localhost:5432/postgres"

# Specific home view and sort
pgmon --dsn "..." --home-view statements --sort total_time --top-n 20 --refresh-ms 2000

# Fail faster on unreachable hosts
pgmon --connect-timeout-ms 1500

# Save selected queries into a persistent directory
pgmon --query-output-dir "$HOME/.local/share/pgmon/queries"

# Or rely on PGMON_DSN / ~/.pgpass
PGMON_DSN="postgresql://postgres@localhost/postgres" pgmon
```

## CLI Options

- `-d, --dsn <STRING>`: PostgreSQL connection string (optional if `PGMON_DSN` or `.pgpass` is available)
- `--connect-timeout-ms <u64>`: Connection timeout in milliseconds (default: 3000)
- `--query-output-dir <PATH>`: Directory used when saving selected queries with `Enter`
- `-r, --refresh-ms <u64>`: Refresh interval (default: 1000)
- `-n, --top-n <u32>`: Rows to show (default: 10)
- `--home-view <activity|statements>`: Initial view
- `-v`: Verbose logging

If no DSN is provided, `pgmon` falls back to `PGMON_DSN` and then to the first usable entry in `PGPASSFILE` or `~/.pgpass`.

In the Database view, press `Enter` on a selected database row to browse schemas and tables for that database, and press `Esc` to return to the summary view.
In the Statements view, press `Enter` to save the selected SQL text and `i` to inspect statement timings and the full query in a detail modal.
In the Activity view, use `g`, `a`, `w`, `b`, and `t` to switch between interesting, active, waiting, blocking, and idle-in-transaction session subviews.
