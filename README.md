[![Test & Build](https://github.com/nbari/pgmon/actions/workflows/build.yml/badge.svg)](https://github.com/nbari/pgmon/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/nbari/pgmon/graph/badge.svg?token=SK0EWR2QH5)](https://codecov.io/gh/nbari/pgmon)
[![Crates.io](https://img.shields.io/crates/v/pgmon.svg)](https://crates.io/crates/pgmon)
[![License](https://img.shields.io/crates/l/pg_exporter.svg)](LICENSE)

# pgmon

A PostgreSQL monitoring TUI inspired by `pg_activity`.

Supports PostgreSQL 14 and newer.

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
- TLS/SSL connections via rustls, including `sslmode=verify-full` and custom root certificates
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
- `-n, --top-n <u32>`: Rows to show, 0 = all (default: 0)
- `--home-view <activity|statements>`: Initial view
- `-s, --sort <total_time|mean_time|calls>`: Statements sort column (default: `total_time`)
- `-v`: Verbose logging

Connection precedence is: explicit `--dsn`, then positional alias from `pgmon.yaml` or `pgmon.yml`, then `default_connection`, then `PGMON_DSN`, then the first usable entry in `PGPASSFILE` or `~/.pgpass`. If no aliases are configured at all, the resolution simply continues to `PGMON_DSN` and `.pgpass`.

## TLS / SSL

`pgmon` is built with TLS support (rustls + ring), so encrypted connections work
out of the box. TLS parameters are read from the DSN and follow libpq naming:

```bash
# Encrypt, but do not verify the server certificate (see the table below)
pgmon --dsn "postgresql://user@db.example.com/postgres?sslmode=require"

# Encrypt and fully verify the certificate and hostname
pgmon --dsn "postgresql://user@db.example.com/postgres?sslmode=verify-full"

# Verify against a private CA
pgmon --dsn "postgresql://user@db.example.com/postgres?sslmode=verify-full&sslrootcert=/etc/ssl/certs/my-ca.crt"

# Client certificate authentication
pgmon --dsn "postgresql://user@db.example.com/postgres?sslmode=verify-full&sslcert=/path/client.crt&sslkey=/path/client.key"
```

Supported parameters: `sslmode`, `sslrootcert`, `sslcert`, and `sslkey`. They
work in key/value DSNs too (`host=db.example.com sslmode=require`), and the
`PGSSLMODE`, `PGSSLROOTCERT`, `PGSSLCERT`, and `PGSSLKEY` environment variables
apply when the DSN does not set them.

The TLS connection is made by sqlx, so a few modes behave differently from
`psql`:

| `sslmode` | Encrypted | Server certificate checked | Differences from libpq |
| --- | --- | --- | --- |
| `disable` | no | no | none |
| `allow` | only when the server rejects the unencrypted attempt | when TLS is used and `sslrootcert` names an existing file | tries without TLS first and, after an authorization error, once more with TLS, as libpq does |
| `prefer` (default) | when the server offers and accepts TLS | when `sslrootcert` names an existing file | if the TLS attempt fails (handshake, certificate check, or an authorization error), retries once without TLS, as libpq does |
| `require` | yes | no, unless `sslrootcert` names an existing file | with one, pgmon applies `verify-ca`, as libpq does; a missing file leaves the certificate unverified, also as in libpq |
| `verify-ca` | yes | chain only, not the hostname | any publicly trusted certificate is accepted, because the compiled-in root store is trusted too (see below) |
| `verify-full` | yes | chain and hostname | a publicly trusted certificate for the host name is accepted even when `sslrootcert` names a private CA |

Use `verify-full` whenever the server identity matters.

More differences:

- libpq retries `prefer` and `allow` after any server error; pgmon retries only
  after an authorization error (SQLSTATE class 28, such as a `pg_hba.conf`
  rejection), so an unrelated failure like an unknown database is not attempted
  twice.
- A connection that drops or resets during the TLS handshake is not retried
  without TLS, because sqlx reports it exactly like a drop after the handshake,
  which libpq does not retry either. Handshake failures the server reports,
  such as an unsupported TLS version, are retried.
- An `sslrootcert` that exists but cannot be read makes `prefer` connect without
  TLS and makes `require` and stricter modes fail, as in libpq.
- `sslcert` and `sslkey` must be given together and both files must exist;
  libpq ignores a client certificate file that does not exist.
- `sslrootcert` is trusted **in addition to** the compiled-in root store
  (bundled Mozilla roots, or the host trust store), not instead of it as in
  libpq. sqlx offers no way to trust a private CA alone.
- `sslrootcert=system` is rejected with an error, because sqlx would read it as
  a file named `system`. Omit `sslrootcert` and use `verify-full` to verify
  against the compiled-in root store.
- libpq's default `~/.postgresql/root.crt` is not read, so pass `sslrootcert`
  explicitly.
- When a key/value DSN sets both `host` and `hostaddr`, `verify-full` checks the
  certificate against the `hostaddr` IP address rather than the host name.

> [!IMPORTANT]
> The default `sslmode` is `prefer`, which silently falls back to an
> unencrypted connection when the server does not offer TLS or rejects the TLS
> attempt, even after a failed certificate check. Set `sslmode` to
> `require` or higher whenever encryption is not optional — otherwise a
> misconfigured server yields a plaintext session rather than an error.

By default the trusted root certificates are the Mozilla set bundled into the
binary (`webpki-roots`), which keeps the static musl releases self-contained.
Point `sslrootcert` at your CA file for internal certificates, or build against
the host trust store instead:

```bash
cargo build --release --no-default-features --features tls-rustls-ring-native-roots
```

To confirm which TLS backend a given binary was built with:

```bash
pgmon --version          # long version: includes commit hash and TLS line
pgmon -V                 # short version: version number only
pgmon check-config       # reports backend and root certificate source
```

Releases up to and including 0.7.1 were built without any TLS backend and could
not connect to servers requiring TLS ([#6](https://github.com/nbari/pgmon/issues/6)).

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
- the effective `sslmode` for that target, with a warning when it can connect unencrypted, does not check the hostname, trusts more than `sslrootcert`, or names a missing `sslrootcert` file
- the TLS backend compiled into the binary and where it loads root certificates from

## Connection & Capability Status

- The Activity summary now shows the current connection target, observed refresh latency, and last successful refresh state.
- The footer highlights offline/reconnect state when background refreshes fail and shows a slow-link indicator when refreshes exceed the configured interval.
- `Statements`, `IO`, and `Replication` now show explicit capability panels when `pg_stat_statements`, `pg_stat_io`, or replication settings are unavailable instead of rendering synthetic placeholder rows.
- On slower or remote links, `pgmon` reduces extra metadata round trips by caching capability checks and uses the observed refresh latency to avoid overly aggressive background polling.

## Query Inspection

`pgmon` has two query-inspection flows:

- In `Statements`, press `i` on the selected row to open an info modal with the database name, full SQL text, and aggregated timing counters from `pg_stat_statements`.
- In `Activity`, press `i` on the selected session to open an info modal for that backend query and optionally run `EXPLAIN`.

Inside the query info modal:

- `Enter` saves the SQL text to `--query-output-dir` or the system temp directory.
- `x` runs safe `EXPLAIN` from both `Activity` and `Statements`.
- `pgmon` also shows whether `auto_explain` is enabled and, when needed, how to enable it for real execution plans in PostgreSQL logs.
- Unsupported SQL is refused before it reaches PostgreSQL; pgmon only explains single `SELECT`/`INSERT`/`UPDATE`/`DELETE`/`MERGE` statements.

### Parameterized Query Limitation

`pg_stat_statements` often stores normalized SQL such as:

```sql
SELECT * FROM accounts WHERE id = $1
```

Those placeholders do not include actual runtime values, so `pgmon` cannot produce a value-specific plan for them.

On PostgreSQL 16+, placeholders use `EXPLAIN (GENERIC_PLAN, VERBOSE, SETTINGS)` instead of executing the statement. This is safe, but it is intentionally value-agnostic: real bind values can change row estimates and even the chosen plan nodes.

On PostgreSQL 14 and 15, `GENERIC_PLAN` is not available yet, so pgmon leaves parameterized statements as info-only and shows a notice instead of attempting explain.

When PostgreSQL cannot infer one or more parameter types for a generic plan:

- `pgmon` shows an error instead of executing the query
- add explicit casts in the SQL or replace placeholders with real literals outside `pgmon`

`pgmon` also refuses multi-statement SQL in explain mode so trailing statements in a batch cannot execute accidentally.

All plans shown by `pgmon` are estimated plans gathered from a fresh session. Session-local settings, temp objects, prepared statements, or different bind values can still make the real runtime plan differ.

## View Notes

- In the Database view, press `Enter` on a selected database row to browse schemas and tables for that database, and press `Esc` to return to the summary view.
- In the Activity view, use `i` to open query info for the selected backend, and use `a`, `w`, `b`, and `t` to switch between active, waiting, blocking, and idle-in-transaction session subviews.
- In the Activity view, press `m` to cycle the chart between Connections, TPS, DML/s, Temp Bytes/s, and Growth Bytes/s without triggering a refresh.
- Press `?` in any main view to open contextual help with that view's shortcuts, important limitations, and brief metric explanations.
