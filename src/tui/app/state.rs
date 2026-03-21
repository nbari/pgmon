use crate::pg::client::{ActivityProcessSnapshot, ActivitySession};
use std::{collections::VecDeque, time::Instant};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Tab {
    Activity,
    Database,
    Locks,
    IO,
    Statements,
    Tools,
    Settings,
}

#[derive(Clone, Default)]
pub struct DashboardStats {
    pub summary: ActivityDisplaySummary,
    pub sessions: Vec<ActivitySession>,
    pub conn_history: VecDeque<(i64, i64, i64)>,
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
pub struct StatementDetailState {
    pub query: String,
    pub total_time: String,
    pub mean_time: String,
    pub calls: String,
    pub read_time: String,
    pub write_time: String,
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
