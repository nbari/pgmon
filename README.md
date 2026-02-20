# pgmon

A PostgreSQL monitoring TUI inspired by `pg_activity`.

## Features

- Real-time views of:
  - `pg_stat_activity`
  - `pg_stat_database`
  - `pg_locks`
  - `pg_stat_io` (PostgreSQL 16+)
  - `pg_stat_statements` (if extension exists)
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
```

## CLI Options

- `-d, --dsn <STRING>`: PostgreSQL connection string (required)
- `-r, --refresh-ms <u64>`: Refresh interval (default: 1000)
- `-n, --top-n <u32>`: Rows to show (default: 10)
- `--home-view <activity|statements>`: Initial view
- `-v`: Verbose logging
