# TODO

Upcoming features and improvements for `pgmon`.

## Recently Completed
- [x] **Replication Monitoring**: Added a dedicated read-only Replication view for WAL senders, standby receiver status, and replication slots.
- [x] **Statements Sort Contract Cleanup**: Aligned CLI/runtime sorting with supported `total_time`, `mean_time`, and `calls` modes.
- [x] **Activity Waiting Accuracy**: Corrected waiting-session counts so blockers are no longer reported as waiting.
- [x] **Replica Query Presentation**: Show human-friendly replica labels instead of raw `START_REPLICATION SLOT ...` text in the Activity UI.

## TUI & UX
- [ ] **Metric Switching in Charts**: Add the ability to toggle the main chart between different metrics (e.g., Connections, TPS, IO Throughput).
- [ ] **Table Export**: Implement a shortcut (e.g., `e`) to export the current (potentially filtered) table view to a CSV or JSON file.
- [ ] **Mouse Support**: Enable basic mouse interactions for tab switching and row selection.
- [ ] **Custom Themes**: Allow users to define their own color schemes in a config file.

## Administration & Monitoring
- [ ] **Extended Admin Tools**: Add context-aware tools like `VACUUM ANALYZE` or `REINDEX` for the selected table in the Database Tree view.
- [ ] **Deadlock Visualizer**: A more detailed view of waiting transactions and the exact circular dependencies.
- [ ] **Explain Analyze**: Integrate the ability to run `EXPLAIN (ANALYZE, BUFFERS)` on a selected query from the Statements view.

## Configuration & Integration
- [ ] **Configuration File**: Support for `pgmon.yaml` to save default settings, DSN aliases, and UI preferences.
- [ ] **Environment Variable Support**: Better handling of multiple `PGMON_DSN_*` variables for quick switching between environments.
- [ ] **Remote Execution**: Improve performance and stability when monitoring remote instances over slow high-latency links.
