[![Test & Build](https://github.com/nbari/pgmon/actions/workflows/build.yml/badge.svg)](https://github.com/nbari/pgmon/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/nbari/pgmon/graph/badge.svg?token=SK0EWR2QH5)](https://codecov.io/gh/nbari/pgmon)
[![Crates.io](https://img.shields.io/crates/v/pgmon.svg)](https://crates.io/crates/pgmon)
[![License](https://img.shields.io/crates/l/pg_exporter.svg)](LICENSE)

# pgmon

A PostgreSQL monitoring TUI inspired by `pg_activity`.

<p align="center">
  <a href="./pgmon.png">
    <img src="./pgmon.png" alt="pgmon screenshot" width="800">
  </a>
</p>

## Features

- Real-time views of:
  - `pg_stat_activity`
  - `pg_stat_replication` / `pg_replication_slots`
  - `pg_stat_database`
  - `pg_locks`
  - `pg_stat_io` (PostgreSQL 16+)
  - `pg_stat_statements` (if extension exists)
- Pg-activity-inspired `Activity` dashboard with sampled TPS/DML/temp rates, session counts, and worker/process summaries
- Activity subviews for active, waiting, blocking, and idle in transaction backends
- Contextual in-app help overlay (`?`) with current-view shortcuts and metric explanations
- Built-in themes plus runtime theme switching and config validation with `pgmon check-config`
- Interactive TUI (Tabs, Table navigation)
- Configurable refresh rate and top-N rows.

## Installation

```bash
cargo build --release
```

## Usage

```bash
pgmon --dsn "postgresql://user:password@localhost:5432/postgres"

# Connect using an alias from pgmon.yaml or pgmon.yml
pgmon prod

# Load a specific config file
pgmon --config ./pgmon.yaml prod

# Use the config's default connection
pgmon --config ./pgmon.yaml

# Validate config loading and connection resolution without starting the TUI
pgmon check-config --config ./pgmon.yaml prod

# Specific home view and sort
pgmon --dsn "..." --home-view statements --sort total_time --top-n 20 --refresh-ms 2000

# Fail faster on unreachable hosts
pgmon --connect-timeout-ms 1500

# Save selected queries into a persistent directory
pgmon --query-output-dir "$HOME/.local/share/pgmon/queries"

# Or rely on PGMON_DSN / ~/.pgpass
PGMON_DSN="postgresql://postgres@localhost/postgres" pgmon
```

## Configuration

`pgmon` supports a `pgmon.yaml` or `pgmon.yml` configuration file for connection aliases, UI preferences, built-in theme selection, custom named themes, and per-view colors.

If the config file does not define any `connections`, `pgmon` still starts normally and falls back to `PGMON_DSN`, then `.pgpass`.

The file is looked for in the following locations (in order):
1.  Path passed with `-c, --config`
2.  Current working directory (`./pgmon.yaml`, then `./pgmon.yml`)
3.  User's configuration directory (`~/.config/pgmon/pgmon.yaml`, then `~/.config/pgmon/pgmon.yml` on Linux/macOS)

An example config is available at the repository root as [`pgmon.yaml`](./pgmon.yaml).

### Example `pgmon.yaml`

```yaml
# Optional default alias used when no positional alias is passed
default_connection: local

# Optional theme selection
# Built-in themes: calibrachoa, sky, mint, retro
theme: my_theme

# Named connections
connections:
  local:
    dsn: "postgresql://postgres:postgres@localhost:5432/postgres"
  prod:
    dsn: "postgresql://pgmon@prod.example.com/postgres"
  staging:
    dsn: "host=staging-db dbname=postgres user=pgmon password=secret"

# Optional custom theme templates
themes:
  my_theme:
    ui:
      header_border_color: "#95a8b8"
      footer_border_color: "#98aaa4"
    views:
      settings:
        colors:
          value: "#b4a9b7"

# Global UI preferences
# Top-level values override the selected theme
ui:
  show_controls: true # Set to false to hide the Controls section
  default_export_format: "csv" # or "json"

# Customize UI colors for specific views
# Top-level values override the selected theme
views:
  settings:
    colors:
      value: "#a8a0b4" # Optional muted override for the 'Value' column
```

Built-in themes are available even without a config file: `calibrachoa`, `sky`, `mint`, and `retro`. Colors can be specified by name (e.g., "red", "green", "blue", "yellow", "cyan", "magenta", "white", "black") or as `#RRGGBB` hex values for softer palettes. The `my_theme` example above is only illustrative; you can remove it entirely and select a built-in theme by name.
Connection aliases are selected with `pgmon <alias>`. If `default_connection` is set, `pgmon` can start without a positional alias and will use that configured target. If `--dsn` is also provided, the explicit DSN takes precedence over both the positional alias and `default_connection`.
Current config support is limited to the keys above; `PGMON_DSN_*` environment switching remains future work.

When themes are available, press `T` inside the TUI to switch between them at runtime. Theme switching is applied immediately for the current session; edit `pgmon.yaml` or `pgmon.yml` if you want the new theme to persist across restarts. If a custom YAML theme uses the same name as a built-in theme, the custom definition overrides the built-in one.

## CLI Options

- `-d, --dsn <STRING>`: PostgreSQL connection string (optional if `PGMON_DSN` or `.pgpass` is available)
- `[ALIAS]`: Optional connection alias from `pgmon.yaml` or `pgmon.yml`
- `-c, --config <PATH>`: Explicit path to `pgmon.yaml` or `pgmon.yml`
- `--connect-timeout-ms <u64>`: Connection timeout in milliseconds (default: 3000)
- `--query-output-dir <PATH>`: Directory used when saving selected queries with `Enter`
- `-r, --refresh-ms <u64>`: Refresh interval (default: 1000)
- `-n, --top-n <u32>`: Rows to show (default: 10)
- `--home-view <activity|statements>`: Initial view
- `-s, --sort <total_time|mean_time|calls>`: Statements sort column (default: `total_time`)
- `-v`: Verbose logging

Connection precedence is: explicit `--dsn`, then positional alias from `pgmon.yaml` or `pgmon.yml`, then `default_connection`, then `PGMON_DSN`, then the first usable entry in `PGPASSFILE` or `~/.pgpass`. If no aliases are configured at all, the resolution simply continues to `PGMON_DSN` and `.pgpass`.

## TUI Shortcuts

- `1`-`8`: switch tabs
- `h` / `l`: previous or next tab
- `j` / `k`: move selection up or down
- `/`: search or filter the current view
- `e`: export the current table as CSV or JSON in `Activity` and `Statements`
- `m`: cycle the Activity chart between Connections, TPS, DML/s, Temp Bytes/s, and Growth Bytes/s
- `T`: open the theme picker when built-in or custom themes are available
- `?`: open contextual in-app help for the current view
- `q`: quit or close the current modal

## Config Validation

Use `pgmon check-config` to validate configuration loading and the effective connection resolution without starting the TUI.

Examples:

```bash
pgmon check-config
pgmon check-config prod
pgmon check-config --config ./pgmon.yaml prod
pgmon check-config --dsn "postgresql://postgres@localhost/postgres"
```

The report includes:
- which config file was loaded, or whether built-in defaults are being used
- configured aliases, default connection, and active theme
- invalid color or alias/default-connection issues
- the effective connection source (`--dsn`, alias, `PGMON_DSN`, or `.pgpass`)
- a safe connection target summary without printing passwords

## Connection & Capability Status

- The Activity summary now shows the current connection target, observed refresh latency, and last successful refresh state.
- The footer highlights offline/reconnect state when background refreshes fail and shows a slow-link indicator when refreshes exceed the configured interval.
- `Statements`, `IO`, and `Replication` now show explicit capability panels when `pg_stat_statements`, `pg_stat_io`, or replication settings are unavailable instead of rendering synthetic placeholder rows.
- On slower or remote links, `pgmon` reduces extra metadata round trips by caching capability checks and uses the observed refresh latency to avoid overly aggressive background polling.

## Query Inspection

`pgmon` has two query-inspection flows:

- In `Statements`, press `i` on the selected row to open a detail modal with the database name, full SQL text, and aggregated timing counters from `pg_stat_statements`.
- In `Activity`, press `i` on the selected session to inspect the current query text for that backend and optionally run `EXPLAIN (ANALYZE, BUFFERS)`.

Inside the query detail modal:

- `Enter` saves the SQL text to `--query-output-dir` or the system temp directory.
- `x` runs `EXPLAIN (ANALYZE, BUFFERS)` only for queries opened from `Activity`.

### Normalized Query Limitation

`pg_stat_statements` often stores normalized SQL such as:

```sql
SELECT * FROM accounts WHERE id = $1
```

Those placeholders do not include actual runtime values, so `pgmon` cannot safely execute `EXPLAIN ANALYZE` for them.

For that reason, `Statements` is now an inspect-only view for query text and aggregated timings. Its detail modal does not offer `EXPLAIN ANALYZE`, because the SQL usually represents a normalized statement shape rather than one executable statement with real values.

When a normalized query is opened from `Activity`:

- the modal shows a warning
- the `x` action is marked as unavailable
- pressing `x` keeps the modal open and shows a notice instead of sending the query to PostgreSQL

If you need an execution plan for a normalized statement, copy the SQL and replace `$1`, `$2`, and similar placeholders with real literal values in a separate session before running `EXPLAIN`.

## View Notes

- In the Database view, press `Enter` on a selected database row to browse schemas and tables for that database, and press `Esc` to return to the summary view.
- In the Activity view, use `a`, `w`, `b`, and `t` to switch between active, waiting, blocking, and idle-in-transaction session subviews.
- In the Activity view, press `m` to cycle the chart between Connections, TPS, DML/s, Temp Bytes/s, and Growth Bytes/s without triggering a refresh.
- Press `?` in any main view to open contextual help with that view's shortcuts, important limitations, and brief metric explanations.
