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
    pub(crate) sessions: Vec<ActivitySession>,
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
