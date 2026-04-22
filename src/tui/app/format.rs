use super::state::{
    ActivityDisplaySummary, ActivityRates, ActivitySubview, DatabaseView, SessionCounts, Tab,
};
use crate::pg::client::{
    ActivityProcessSnapshot, ActivitySession, ActivitySummarySnapshot, ReplicationSender,
    ReplicationSlot,
};
use chrono::Utc;

pub(crate) fn table_header_cells(tab: Tab, database_view: &DatabaseView) -> Vec<&'static str> {
    match tab {
        Tab::Activity => vec![
            "PID", "XMIN", "Database", "App", "User", "Client", "Time+", "Waiting", "State",
            "Query",
        ],
        Tab::Database => match database_view {
            DatabaseView::Summary => vec![
                "DB",
                "Backends",
                "Commits",
                "Rollbacks",
                "Hit %",
                "Temp Bytes",
                "Deadlocks",
                "Reset",
            ],
            DatabaseView::Tables { .. } => vec!["Node", "Type", "Children/Rows", "Size"],
        },
        Tab::Locks => vec![
            "Blocking PID",
            "Blocked PID",
            "User",
            "Target",
            "Mode",
            "Time(s)",
            "Query",
        ],
        Tab::IO => vec![
            "Backend",
            "Object",
            "Context",
            "Reads",
            "Writes",
            "Time Read(ms)",
            "Time Write(ms)",
        ],
        Tab::Statements => vec![
            "DB",
            "Query",
            "Total(ms)",
            "Mean(ms)",
            "Calls",
            "Read(ms)",
            "Write(ms)",
        ],
        Tab::Tools => vec!["Action", "Description"],
        Tab::Replication => vec![
            "PID",
            "User",
            "App",
            "Client",
            "State",
            "Sync",
            "Slot",
            "Sent Lag",
            "Write Lag",
            "Flush Lag",
            "Replay Lag",
        ],
        Tab::Settings => vec!["Name", "Value", "Unit", "Category", "Description"],
    }
}

pub(crate) fn format_duration_hms(duration_seconds: i64) -> String {
    let duration_seconds = duration_seconds.max(0);
    let hours = duration_seconds / 3600;
    let minutes = (duration_seconds % 3600) / 60;
    let seconds = duration_seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

pub(crate) fn format_uptime(uptime_seconds: i64) -> String {
    let uptime_seconds = uptime_seconds.max(0);
    let days = uptime_seconds / 86_400;
    let hours = (uptime_seconds % 86_400) / 3600;
    let minutes = (uptime_seconds % 3600) / 60;

    if days > 0 {
        format!("{days}d {hours}h {minutes}m")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m")
    }
}

pub(crate) fn format_bytes(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];

    let negative = bytes < 0;
    let absolute = u128::from(bytes.unsigned_abs());
    let mut unit_index = 0usize;
    let mut divisor = 1u128;

    while unit_index + 1 < UNITS.len() && absolute >= divisor * 1024 {
        divisor *= 1024;
        unit_index += 1;
    }

    let unit = UNITS.get(unit_index).copied().unwrap_or("B");
    let sign = if negative { "-" } else { "" };

    if unit_index == 0 {
        format!("{sign}{absolute} {unit}")
    } else {
        let scaled_tenths = absolute.saturating_mul(10) / divisor;
        let whole = scaled_tenths / 10;
        let tenths = scaled_tenths % 10;
        format!("{sign}{whole}.{tenths} {unit}")
    }
}

pub(crate) fn activity_wait_cell(session: &ActivitySession) -> String {
    if session.blocked_count > 0 {
        format!("blocking {}", session.blocked_count)
    } else if session.blocked_by_count > 0 {
        format!("blocked by {}", session.blocked_by_count)
    } else if session.wait_info.is_empty() {
        "-".to_string()
    } else {
        session.wait_info.clone()
    }
}

pub(crate) fn activity_state_cell(session: &ActivitySession) -> String {
    if session.blocked_count > 0 {
        format!("blocking | {}", session.state)
    } else {
        session.state.clone()
    }
}

pub(crate) fn sender_to_row(sender: &ReplicationSender) -> Vec<String> {
    vec![
        sender.pid.clone(),
        sender.user.clone(),
        sender.application.clone(),
        sender.client.clone(),
        sender.state.clone(),
        sender.sync_state.clone(),
        sender.slot_name.clone(),
        format_bytes(sender.sent_lag_bytes),
        format_bytes(sender.write_lag_bytes),
        format_bytes(sender.flush_lag_bytes),
        format_bytes(sender.replay_lag_bytes),
    ]
}

pub(crate) fn slot_to_row(slot: &ReplicationSlot) -> Vec<String> {
    vec![
        slot.slot_name.clone(),
        slot.slot_type.clone(),
        slot.active.clone(),
        slot.active_pid.clone(),
        slot.restart_lsn.clone(),
        slot.confirmed_flush_lsn.clone(),
        slot.wal_status.clone(),
    ]
}

pub(crate) struct ActivityCounterSample {
    pub(crate) total_xacts: i64,
    pub(crate) total_inserts: i64,
    pub(crate) total_updates: i64,
    pub(crate) total_deletes: i64,
    pub(crate) total_returned: i64,
    pub(crate) total_temp_files: i64,
    pub(crate) total_temp_bytes: i64,
    pub(crate) total_database_bytes: i64,
    pub(crate) sample_time: std::time::Instant,
}

impl From<&ActivitySummarySnapshot> for ActivityCounterSample {
    fn from(summary: &ActivitySummarySnapshot) -> Self {
        Self {
            total_xacts: summary.total_xacts,
            total_inserts: summary.total_inserts,
            total_updates: summary.total_updates,
            total_deletes: summary.total_deletes,
            total_returned: summary.total_returned,
            total_temp_files: summary.total_temp_files,
            total_temp_bytes: summary.total_temp_bytes,
            total_database_bytes: summary.total_database_bytes,
            sample_time: std::time::Instant::now(),
        }
    }
}

pub(crate) fn build_activity_summary(
    summary: &ActivitySummarySnapshot,
    process: &ActivityProcessSnapshot,
    session_counts: SessionCounts,
    previous_sample: Option<&ActivityCounterSample>,
) -> ActivityDisplaySummary {
    let uptime_seconds = Utc::now()
        .signed_duration_since(summary.postmaster_start)
        .num_seconds();

    ActivityDisplaySummary {
        server_version: summary.server_version.clone(),
        uptime_seconds,
        database_count: summary.database_count,
        total_database_bytes: summary.total_database_bytes,
        cache_hit_pct: summary.cache_hit_pct,
        rollback_pct: summary.rollback_pct,
        total_commits: summary.total_commits,
        total_rollbacks: summary.total_rollbacks,
        session_counts,
        rates: compute_activity_rates(previous_sample, summary),
        process: process.clone(),
        max_connections: summary.max_connections,
    }
}

pub(crate) fn count_sessions(sessions: &[ActivitySession]) -> SessionCounts {
    let mut counts = SessionCounts {
        total: sessions.len().try_into().unwrap_or(i64::MAX),
        ..Default::default()
    };

    for session in sessions {
        match session.state.as_str() {
            "active" => counts.active += 1,
            "idle" => counts.idle += 1,
            "idle in transaction" => counts.idle_in_transaction += 1,
            "idle in transaction (aborted)" => counts.idle_in_transaction_aborted += 1,
            _ => {}
        }
        if is_waiting_session(session) {
            counts.waiting += 1;
        }
    }

    counts
}

fn compute_activity_rates(
    previous: Option<&ActivityCounterSample>,
    current: &ActivitySummarySnapshot,
) -> ActivityRates {
    let Some(prev) = previous else {
        return ActivityRates::default();
    };

    let elapsed_secs = prev.sample_time.elapsed().as_secs_f64();
    if elapsed_secs <= 0.0 {
        return ActivityRates::default();
    }

    ActivityRates {
        tps: format_counter_rate(prev.total_xacts, current.total_xacts, elapsed_secs),
        inserts_per_sec: format_counter_rate(
            prev.total_inserts,
            current.total_inserts,
            elapsed_secs,
        ),
        updates_per_sec: format_counter_rate(
            prev.total_updates,
            current.total_updates,
            elapsed_secs,
        ),
        deletes_per_sec: format_counter_rate(
            prev.total_deletes,
            current.total_deletes,
            elapsed_secs,
        ),
        tuples_returned_per_sec: format_counter_rate(
            prev.total_returned,
            current.total_returned,
            elapsed_secs,
        ),
        temp_files_per_sec: format_counter_rate(
            prev.total_temp_files,
            current.total_temp_files,
            elapsed_secs,
        ),
        temp_bytes_per_sec: format_counter_rate(
            prev.total_temp_bytes,
            current.total_temp_bytes,
            elapsed_secs,
        ),
        growth_bytes_per_sec: format_counter_rate(
            prev.total_database_bytes,
            current.total_database_bytes,
            elapsed_secs,
        ),
    }
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn format_counter_rate(prev: i64, current: i64, elapsed_secs: f64) -> String {
    let diff = current.saturating_sub(prev);
    if diff <= 0 {
        return "0".to_string();
    }

    let rate = diff as f64 / elapsed_secs;
    if rate < 10.0 {
        format!("{rate:.1}")
    } else {
        format!("{}", rate.round() as i64)
    }
}

pub(crate) fn sort_statement_rows(mut rows: Vec<Vec<String>>, sort: &str) -> Vec<Vec<String>> {
    let column = match sort {
        "total_time" => 1,
        "mean_time" => 2,
        "calls" => 3,
        _ => return rows,
    };

    rows.sort_by(|a, b| {
        let a_val = a
            .get(column)
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let b_val = b
            .get(column)
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        b_val
            .partial_cmp(&a_val)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

pub(crate) fn limit_rows(mut rows: Vec<Vec<String>>, top_n: u32) -> Vec<Vec<String>> {
    rows.truncate(top_n as usize);
    rows
}

pub(crate) fn filter_activity_sessions(
    sessions: &[ActivitySession],
    subview: ActivitySubview,
) -> Vec<ActivitySession> {
    sessions
        .iter()
        .filter(|s| match subview {
            ActivitySubview::Active => s.state == "active" || s.blocked_count > 0,
            ActivitySubview::Waiting => is_waiting_session(s),
            ActivitySubview::Blocking => s.blocked_count > 0,
            ActivitySubview::IdleInTransaction => s.state.starts_with("idle in transaction"),
        })
        .cloned()
        .collect()
}

pub(crate) fn sorted_activity_sessions(
    mut sessions: Vec<ActivitySession>,
    subview: ActivitySubview,
) -> Vec<ActivitySession> {
    sessions.sort_by(|a, b| {
        if subview == ActivitySubview::Blocking {
            let block_cmp = b.blocked_count.cmp(&a.blocked_count);
            if block_cmp != std::cmp::Ordering::Equal {
                return block_cmp;
            }
        }
        b.duration_seconds
            .cmp(&a.duration_seconds)
            .then_with(|| a.pid.cmp(&b.pid))
    });
    sessions
}

pub(crate) fn limit_activity_sessions(
    mut sessions: Vec<ActivitySession>,
    top_n: u32,
) -> Vec<ActivitySession> {
    sessions.truncate(top_n as usize);
    sessions
}

pub(crate) fn is_fuzzy_match(text: &str, query: &str) -> bool {
    let text_lower = text.to_lowercase();
    let mut text_chars = text_lower.chars();
    for q_char in query.to_lowercase().chars() {
        if !text_chars.any(|t_char| t_char == q_char) {
            return false;
        }
    }
    true
}

pub(crate) fn format_activity_query(session: &ActivitySession) -> String {
    if session.backend_type != "walsender" {
        return session.query.clone();
    }

    replication_slot_name(&session.query).map_or_else(
        || "replica".to_string(),
        |slot_name| format!("replica {slot_name}"),
    )
}

fn replication_slot_name(query: &str) -> Option<&str> {
    let prefix = "START_REPLICATION SLOT \"";
    let remainder = query.strip_prefix(prefix)?;
    let slot_end = remainder.find('"')?;
    remainder.get(..slot_end)
}

fn is_waiting_session(session: &ActivitySession) -> bool {
    session.blocked_by_count > 0 || (!session.wait_info.is_empty() && session.state == "active")
}
