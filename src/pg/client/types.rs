//! Shared `PostgreSQL` client data types.

use chrono::{DateTime, Utc};

/// Availability state for optional `PostgreSQL` monitoring features.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) enum CapabilityStatus {
    /// The capability has not been checked yet in this session.
    #[default]
    Unknown,
    /// The capability is available and the view can be populated normally.
    Available,
    /// The capability is unavailable together with a user-facing reason.
    Unavailable(String),
}

impl CapabilityStatus {
    pub(super) fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable(reason.into())
    }
}

/// Safe in-app planning modes used by `pgmon` query diagnosis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExplainMode {
    /// Use a regular estimated plan for executable SQL text.
    Estimated,
    /// Use `PostgreSQL`'s generic planner path for parameterized SQL with placeholders.
    GenericEstimated,
}

impl ExplainMode {
    /// Human-readable title shown by the explain modal.
    pub(crate) const fn title(self) -> &'static str {
        match self {
            Self::Estimated => "Estimated Plan",
            Self::GenericEstimated => "Generic Estimated Plan",
        }
    }
}

/// Connection-scoped capabilities and server metadata needed by the TUI.
#[derive(Debug, Clone, Default)]
pub(crate) struct ConnectionMeta {
    pub(crate) server_version_num: i32,
    pub(crate) io_capability: CapabilityStatus,
    pub(crate) statements_capability: CapabilityStatus,
    pub(crate) replication_capability: CapabilityStatus,
}

#[derive(Debug, Clone)]
pub(crate) struct ActivitySnapshot {
    pub(crate) summary: ActivitySummarySnapshot,
    pub(crate) process: ActivityProcessSnapshot,
    pub(crate) checkpoint: ActivityCheckpointSnapshot,
    pub(crate) sessions: Vec<ActivitySession>,
}

/// Checkpoint activity counters, I/O figures, and related settings, used to show
/// whether checkpoints are being driven by `checkpoint_timeout` (`timed`) or by
/// `max_wal_size` (`requested`), and how much I/O they cause.
#[derive(Debug, Clone, Default)]
pub(crate) struct ActivityCheckpointSnapshot {
    pub(crate) timed: i64,
    pub(crate) requested: i64,
    /// Buffers (8 KiB pages) written by checkpoints since `stats_reset`.
    pub(crate) buffers_written: i64,
    /// Cumulative time spent writing checkpoint buffers, in milliseconds.
    pub(crate) write_time_ms: f64,
    /// Cumulative time spent syncing checkpoint files (`fsync`), in milliseconds.
    pub(crate) sync_time_ms: f64,
    /// When the checkpoint statistics were last reset.
    pub(crate) stats_reset: Option<DateTime<Utc>>,
    pub(crate) checkpoint_timeout_seconds: i64,
    pub(crate) max_wal_size_mb: i64,
    pub(crate) completion_target: f64,
    /// `min_wal_size`, in megabytes (as `PostgreSQL` reports the setting).
    pub(crate) min_wal_size_mb: i64,
    /// `wal_segment_size`, in bytes. Large segments (the default is 16 MiB) inflate
    /// the WAL "distance" consumed by each forced segment switch, which can trip
    /// WAL-threshold checkpoints even when almost no real WAL is written.
    pub(crate) wal_segment_size_bytes: i64,
    /// Total WAL generated since `pg_stat_wal` was last reset, in bytes.
    pub(crate) wal_bytes: i64,
    /// Seconds elapsed since `pg_stat_wal` was last reset, used with `wal_bytes` to
    /// derive the WAL generation rate.
    pub(crate) wal_elapsed_seconds: f64,
    /// Whether the server is a standby (`pg_is_in_recovery()`). On a standby the
    /// counters describe **restartpoints**, not checkpoints, which changes wording.
    pub(crate) in_recovery: bool,
    /// Seconds since the last completed checkpoint/restartpoint (control file).
    /// `None` when unknown (e.g. the role lacks `pg_monitor`). A value far larger
    /// than `checkpoint_timeout` indicates a lagging or stalled checkpointer.
    pub(crate) seconds_since_last_checkpoint: Option<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct ActivitySummarySnapshot {
    pub(crate) server_version: String,
    pub(crate) postmaster_start: DateTime<Utc>,
    pub(crate) database_count: i64,
    pub(crate) total_database_bytes: i64,
    pub(crate) cache_hit_pct: f64,
    pub(crate) rollback_pct: f64,
    pub(crate) total_commits: i64,
    pub(crate) total_rollbacks: i64,
    pub(crate) total_xacts: i64,
    pub(crate) total_inserts: i64,
    pub(crate) total_updates: i64,
    pub(crate) total_deletes: i64,
    pub(crate) total_returned: i64,
    pub(crate) total_temp_files: i64,
    pub(crate) total_temp_bytes: i64,
    pub(crate) max_connections: i64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ActivityProcessSnapshot {
    pub(crate) worker_total: i64,
    pub(crate) max_worker_processes: i64,
    pub(crate) logical_workers: i64,
    pub(crate) max_logical_workers: i64,
    pub(crate) parallel_workers: i64,
    pub(crate) max_parallel_workers: i64,
    pub(crate) autovacuum_workers: i64,
    pub(crate) max_autovacuum_workers: i64,
    pub(crate) wal_senders: i64,
    pub(crate) max_wal_senders: i64,
    pub(crate) wal_receivers: i64,
    pub(crate) replication_slots: i64,
    pub(crate) max_replication_slots: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ActivitySession {
    pub(crate) pid: String,
    pub(crate) backend_type: String,
    pub(crate) xmin: String,
    pub(crate) database: String,
    pub(crate) application: String,
    pub(crate) user: String,
    pub(crate) client: String,
    pub(crate) duration_seconds: i64,
    pub(crate) wait_info: String,
    pub(crate) state: String,
    pub(crate) query: String,
    pub(crate) blocked_by_count: i64,
    pub(crate) blocked_count: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ActivityDetail {
    pub(crate) pid: String,
    pub(crate) usename: String,
    pub(crate) application_name: String,
    pub(crate) client_addr: String,
    pub(crate) client_port: String,
    pub(crate) backend_start: String,
    pub(crate) state: String,
    pub(crate) wait_event_type: String,
    pub(crate) wait_event: String,
    pub(crate) xact_start: String,
    pub(crate) state_change: String,
    pub(crate) query: String,
    pub(crate) blocking_pids: String,
    pub(crate) blockers: Vec<Vec<String>>,
    pub(crate) locks: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub(crate) struct AutoExplainInfo {
    pub(crate) summary: String,
    pub(crate) hint: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ReplicationSnapshot {
    pub(crate) capability: CapabilityStatus,
    pub(crate) receiver_summary: Option<String>,
    pub(crate) senders: Vec<ReplicationSender>,
    pub(crate) slots: Vec<ReplicationSlot>,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicationSender {
    pub(crate) pid: String,
    pub(crate) user: String,
    pub(crate) application: String,
    pub(crate) client: String,
    pub(crate) state: String,
    pub(crate) sync_state: String,
    pub(crate) slot_name: String,
    pub(crate) sent_lag_bytes: i64,
    pub(crate) write_lag_bytes: i64,
    pub(crate) flush_lag_bytes: i64,
    pub(crate) replay_lag_bytes: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicationSlot {
    pub(crate) slot_name: String,
    pub(crate) slot_type: String,
    pub(crate) active: String,
    pub(crate) active_pid: String,
    pub(crate) restart_lsn: String,
    pub(crate) confirmed_flush_lsn: String,
    pub(crate) wal_status: String,
}
