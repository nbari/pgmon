use crate::pg::client::{ActivityProcessSnapshot, ActivitySession, CapabilityStatus, ExplainMode};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Tab {
    Activity,
    Database,
    Locks,
    IO,
    Statements,
    Replication,
    Settings,
    Tools,
}

impl Tab {
    pub fn next(self) -> Self {
        match self {
            Self::Activity => Self::Database,
            Self::Database => Self::Locks,
            Self::Locks => Self::IO,
            Self::IO => Self::Statements,
            Self::Statements => Self::Replication,
            Self::Replication => Self::Settings,
            Self::Settings => Self::Tools,
            Self::Tools => Self::Activity,
        }
    }

    pub fn prev(self) -> Self {
        match self {
            Self::Activity => Self::Tools,
            Self::Database => Self::Activity,
            Self::Locks => Self::Database,
            Self::IO => Self::Locks,
            Self::Statements => Self::IO,
            Self::Replication => Self::Statements,
            Self::Settings => Self::Replication,
            Self::Tools => Self::Settings,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Activity => "Activity",
            Self::Database => "Database",
            Self::Locks => "Locks",
            Self::IO => "IO",
            Self::Statements => "Statements",
            Self::Replication => "Replication",
            Self::Settings => "Settings",
            Self::Tools => "Tools",
        }
    }
}

#[derive(Clone, Default)]
pub struct DashboardStats {
    pub summary: ActivityDisplaySummary,
    pub sessions: Vec<ActivitySession>,
    pub chart_history: ActivityChartHistory,
}

/// Metrics that can be rendered in the Activity chart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ActivityChartMetric {
    #[default]
    Connections,
    Tps,
    Dml,
    TempBytesPerSec,
    GrowthBytesPerSec,
}

impl ActivityChartMetric {
    pub fn next(self) -> Self {
        match self {
            Self::Connections => Self::Tps,
            Self::Tps => Self::Dml,
            Self::Dml => Self::TempBytesPerSec,
            Self::TempBytesPerSec => Self::GrowthBytesPerSec,
            Self::GrowthBytesPerSec => Self::Connections,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Connections => "Connections",
            Self::Tps => "TPS",
            Self::Dml => "DML/s",
            Self::TempBytesPerSec => "Temp Bytes/s",
            Self::GrowthBytesPerSec => "Growth Bytes/s",
        }
    }
}

/// Session-scoped sampled history for the Activity chart.
#[derive(Clone, Default)]
pub struct ActivityChartHistory {
    pub connections: VecDeque<(i64, i64, i64)>,
    pub tps: VecDeque<f64>,
    pub inserts_per_sec: VecDeque<f64>,
    pub updates_per_sec: VecDeque<f64>,
    pub deletes_per_sec: VecDeque<f64>,
    pub temp_bytes_per_sec: VecDeque<f64>,
    pub growth_bytes_per_sec: VecDeque<f64>,
}

#[derive(Debug, Clone)]
pub struct LoadingState {
    pub message: String,
    pub started_at: Instant,
}

#[derive(Debug, Clone)]
pub struct ErrorState {
    pub title: String,
    pub details: String,
}

/// Connection metadata shown when background refreshes are failing.
#[derive(Debug, Clone)]
pub struct OfflineState {
    /// Most recent refresh error summarized for footer display.
    pub last_error: String,
    /// Number of consecutive failed refresh attempts.
    pub failed_attempts: u32,
    /// Earliest time a background reconnect may be attempted.
    pub next_retry_at: Instant,
    /// Timestamp of the last successful refresh, if any.
    pub last_successful_refresh_at: Option<Instant>,
}

/// Current connection health for the running TUI session.
#[derive(Debug, Clone, Default)]
pub enum ConnectionStatus {
    /// Background refreshes are succeeding.
    #[default]
    Online,
    /// Background refreshes are failing and the app is retrying.
    Offline(OfflineState),
}

/// Capability availability for optional monitoring views.
#[derive(Debug, Clone, Default)]
pub struct CapabilitySummary {
    /// Availability of the `Statements` view.
    pub statements: CapabilityStatus,
    /// Availability of the `IO` view.
    pub io: CapabilityStatus,
    /// Availability of replication monitoring.
    pub replication: CapabilityStatus,
}

/// Connection target and recent refresh/error timings shown in the UI.
#[derive(Debug, Clone)]
pub struct ConnectionHealthState {
    /// Human-readable target derived from the DSN without exposing secrets.
    pub target: String,
    /// Duration of the most recent successful background refresh.
    pub last_refresh_duration: Option<Duration>,
    /// Completion timestamp of the most recent successful background refresh.
    pub last_refresh_at: Option<Instant>,
    /// Summary of the most recent background refresh failure.
    pub last_error: Option<String>,
    /// Timestamp of the most recent background refresh failure.
    pub last_error_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivitySubview {
    Active,
    Waiting,
    Blocking,
    IdleInTransaction,
}

impl ActivitySubview {
    pub fn label(self) -> &'static str {
        match self {
            Self::Active => "a",
            Self::Waiting => "w",
            Self::Blocking => "b",
            Self::IdleInTransaction => "t",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionCounts {
    pub total: i64,
    pub active: i64,
    pub idle: i64,
    pub idle_in_transaction: i64,
    pub idle_in_transaction_aborted: i64,
    pub waiting: i64,
}

#[derive(Debug, Clone)]
pub struct ActivityRates {
    pub tps: String,
    pub inserts_per_sec: String,
    pub updates_per_sec: String,
    pub deletes_per_sec: String,
    pub tuples_returned_per_sec: String,
    pub temp_files_per_sec: String,
    pub temp_bytes_per_sec: String,
    pub growth_bytes_per_sec: String,
}

impl Default for ActivityRates {
    fn default() -> Self {
        Self {
            tps: "-".to_string(),
            inserts_per_sec: "-".to_string(),
            updates_per_sec: "-".to_string(),
            deletes_per_sec: "-".to_string(),
            tuples_returned_per_sec: "-".to_string(),
            temp_files_per_sec: "-".to_string(),
            temp_bytes_per_sec: "-".to_string(),
            growth_bytes_per_sec: "-".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActivityDisplaySummary {
    pub server_version: String,
    pub uptime_seconds: i64,
    pub database_count: i64,
    pub total_database_bytes: i64,
    pub cache_hit_pct: f64,
    pub rollback_pct: f64,
    pub total_commits: i64,
    pub total_rollbacks: i64,
    pub session_counts: SessionCounts,
    pub rates: ActivityRates,
    pub process: ActivityProcessSnapshot,
    pub max_connections: i64,
}

impl Default for ActivityDisplaySummary {
    fn default() -> Self {
        Self {
            server_version: String::new(),
            uptime_seconds: 0,
            database_count: 0,
            total_database_bytes: 0,
            cache_hit_pct: 0.0,
            rollback_pct: 0.0,
            total_commits: 0,
            total_rollbacks: 0,
            session_counts: SessionCounts::default(),
            rates: ActivityRates::default(),
            process: ActivityProcessSnapshot::default(),
            max_connections: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseView {
    Summary,
    Tables { database: String },
}

#[derive(Debug, Clone)]
pub struct NoticeState {
    pub message: String,
    pub created_at: Instant,
}

#[derive(Debug, Clone)]
pub struct QueryDetailState {
    pub query: String,
    pub database: String,
    pub source: QueryDetailSource,
    /// Safe explain mode selected for this query text.
    pub explain_mode: ExplainMode,
    /// Optional reason explain is currently unavailable for this query.
    pub explain_unavailable_reason: Option<String>,
    pub stats: Option<QueryStats>,
    pub activity_detail: Option<ActivityDetail>,
    /// Summary of the current `auto_explain` state for the selected backend database.
    pub auto_explain_summary: String,
    /// Optional hint that explains how to enable or improve `auto_explain`.
    pub auto_explain_hint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryDetailSource {
    Activity,
    Statements,
}

#[derive(Debug, Clone)]
pub struct ActivityDetail {
    pub pid: String,
    pub usename: String,
    pub application_name: String,
    pub client_addr: String,
    pub client_port: String,
    pub backend_start: String,
    pub state: String,
    pub wait_event_type: String,
    pub wait_event: String,
    pub xact_start: String,
    pub state_change: String,
    pub query: String,
    pub blocking_pids: String,
    pub blockers: Vec<Vec<String>>,
    pub locks: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct QueryStats {
    pub total_time: String,
    pub mean_time: String,
    pub calls: String,
    pub read_time: String,
    pub write_time: String,
}

#[derive(Debug, Clone)]
pub struct ExplainPlanState {
    pub plan: Vec<String>,
    /// Safe explain mode used to generate this plan.
    pub explain_mode: ExplainMode,
    /// Summary of the current `auto_explain` state for the inspected query.
    pub auto_explain_summary: String,
    /// Optional hint that explains how to enable or improve `auto_explain`.
    pub auto_explain_hint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TableDefinitionState {
    pub schema: String,
    pub name: String,
    pub columns: Vec<Vec<String>>,
    pub indexes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DeadlockGraphState {
    pub graph: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ConfirmActionState {
    pub title: String,
    pub description: String,
    pub query: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    Normal,
    Search,
}

#[derive(Debug, Clone)]
pub struct RefreshIntervalState {
    pub options: Vec<u64>,
    pub selected_index: usize,
}

#[derive(Debug, Clone)]
pub struct TopNState {
    pub options: Vec<u32>,
    pub selected_index: usize,
}

#[derive(Debug, Clone)]
pub struct ThemeState {
    /// Available theme names loaded from configuration.
    pub options: Vec<String>,
    /// Currently highlighted theme in the picker modal.
    pub selected_index: usize,
}

#[derive(Debug, Clone)]
pub struct HelpSection {
    pub heading: String,
    pub lines: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct HelpState {
    pub title: String,
    pub sections: Vec<HelpSection>,
}
