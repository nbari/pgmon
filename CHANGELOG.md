# Changelog

All notable changes to this project will be documented in this file.

## [0.4.0] - 2026-03-22

### Added
- **Configuration & Validation**: Added `pgmon.yaml` / `pgmon.yml` support for aliases, default connections, export defaults, custom themes, and a new `pgmon check-config` command to validate config loading and connection resolution without starting the TUI.
- **Themes**: Added built-in themes (`calibrachoa`, `sky`, `mint`, `retro`) plus runtime theme switching from inside the TUI.
- **Contextual Help Overlay**: Added `?` to open per-view in-app help with shortcuts, metric definitions, and capability/Explain limitations.
- **Database Admin Actions**: Added context-aware `VACUUM ANALYZE`, `REINDEX`, and table-definition inspection from the Database tree view.
- **Activity Chart Switching**: Added `m` to cycle the Activity chart between Connections, TPS, DML/s, Temp Bytes/s, and Growth Bytes/s.

### Fixed
- **Explain Availability**: Restricted `EXPLAIN (ANALYZE, BUFFERS)` to query inspection opened from `Activity`, and prevented execution for normalized SQL without literal values.
- **Statements and Export UX**: Removed unavailable actions from views where they created noise, including `Explain` from `Statements` and export from non-primary views.
- **Replication Empty State**: Reworked the Replication view to show a clear capability/status panel instead of misaligned empty panels when replication is not enabled.
- **Reconnect Behavior**: Kept the last successful data on screen after transient refresh failures, added retry backoff, and preserved a manual reconnect path.
- **Capability Messaging**: Replaced synthetic placeholder rows with explicit availability panels for `pg_stat_statements`, `pg_stat_io`, and replication features.

### Changed
- **Activity Summary & Footer Layout**: Simplified the Activity summary metadata row and reduced footer duplication so connection identity, offline state, and slow-link signals are easier to scan.
- **Connection Workflow**: Added alias-based startup, explicit config-path loading, default-connection fallback, and support for both `.yaml` and `.yml` config discovery.
- **Documentation Refresh**: Expanded the README to cover config aliases, themes, `check-config`, contextual help, chart metrics, and current query-inspection behavior.

## [0.3.0] - 2026-03-21

### Added
- **New Replication View (shortcut 8)**: Added a dedicated read-only replication dashboard showing WAL senders, standby receiver status, and replication slot state.

### Fixed
- **Statements Sorting Contract**: Aligned the CLI and runtime behavior so `--sort` now consistently supports `total_time`, `mean_time`, and `calls`.
- **Waiting Session Accuracy**: Corrected Activity waiting counts so blocking sessions are no longer misreported as waiting.
- **Replica Query Labels**: Replaced raw `START_REPLICATION SLOT ...` activity text with compact replica labels across Activity previews.
- **PostgreSQL Compatibility**: Extended `pg_stat_statements` compatibility to tolerate installations where block timing columns are absent while still supporting newer timing column names.

### Changed
- **Default Statements Sort**: Changed the default statements sort mode to `total_time`.
- **Documentation Cleanup**: Removed stale references to the unimplemented `g`/`interesting` Activity subview and updated keybinding/docs for the Replication tab.

## [0.2.0] - 2026-03-21

### Added
- **New Tools View (shortcut 6)**: Integrated administrative actions like terminating idle sessions, canceling long-running queries, and resetting statistics with a secure confirmation workflow.
- **New Settings View (shortcut 7)**: Full scrollable access to every PostgreSQL configuration parameter (`pg_settings`) organized by category.
- **Fuzzy Search (shortcut /)**: Real-time subsequence matching for all tables, allowing quick navigation (e.g., searching `mxcon` for `max_connections`).
- **Dynamic Refresh Interval (shortcut r)**: Interactive modal to change monitoring frequency (0.5s to 10s) on the fly.
- **Dynamic Top-N Limit (shortcut n)**: Toggle display limits between 10, 20, 50, 100, or All rows.
- **Vim Motion Support**: Added standard `j` and `k` keys for navigating all tables and session lists.
- **Enhanced Visuals**: Implemented a consistent "Identity-White / Metric-Gray" aesthetic across all views with functional color-coding (Red/Yellow/Green) for critical performance indicators.
- **Blocking Chain Detection**: The Locks view now identifies blocking vs. blocked PIDs and calculates wait durations.

### Fixed
- **Postgres Version Compatibility**: Automatically detect and support different `pg_stat_statements` column names (handling the v12 vs v13+ naming changes).
- **PostgreSQL Connection Reuse**: Refactored background worker to reuse client connections, drastically reducing network and database overhead.
- **Robust DSN Parsing**: Fixed summarization of connection strings containing query parameters or quoted values with spaces.
- **Client IP Formatting**: Correctly extract host addresses from `inet` types without network masks.
- **TUI Navigation**: Standardized row selection and fixed bugs where navigation keys would become non-functional in certain views.
- **Modal Layout**: Redesigned popups to handle massive SQL statements and high-resolution screens without hiding action hints.

### Changed
- **Architectural Refactoring**: Split monolithic files into a clean, modular directory structure (`src/tui/app/` and `src/tui/ui/`) for easier maintenance.
- **Symmetrical Activity Layout**: Reorganized the Activity Summary into a balanced 2x2 grid for better scanability.
- **Restored Unit Tests**: Fully recovered and expanded the unit test suite to 19 tests, covering core logic and data shaping.
