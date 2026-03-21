# TODO

Upcoming features and improvements for `pgmon`.

## TUI & UX
- [ ] **Metric Switching in Charts**: Add the ability to toggle the main chart between different metrics (e.g., Connections, TPS, IO Throughput).
- [ ] **Table Export**: Implement a shortcut (e.g., `e`) to export the current (potentially filtered) table view to a CSV or JSON file.
- [ ] **Mouse Support**: Enable basic mouse interactions for tab switching and row selection.
- [ ] **Custom Themes**: Allow users to define their own color schemes in a config file.

## Administration & Monitoring
- [ ] **Extended Admin Tools**: Add context-aware tools like `VACUUM ANALYZE` or `REINDEX` for the selected table in the Database Tree view.
- [ ] **Replication Monitoring**: Add a specialized view for WAL replication lag and replication slot status.
- [ ] **Deadlock Visualizer**: A more detailed view of waiting transactions and the exact circular dependencies.
- [ ] **Explain Analyze**: Integrate the ability to run `EXPLAIN (ANALYZE, BUFFERS)` on a selected query from the Statements view.

## Configuration & Integration
- [ ] **Configuration File**: Support for `pgmon.yaml` to save default settings, DSN aliases, and UI preferences.
- [ ] **Environment Variable Support**: Better handling of multiple `PGMON_DSN_*` variables for quick switching between environments.
- [ ] **Remote Execution**: Improve performance and stability when monitoring remote instances over slow high-latency links.
