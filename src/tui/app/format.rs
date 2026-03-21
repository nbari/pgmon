use super::state::{ActivityDisplaySummary, ActivityRates, ActivitySubview, SessionCounts};
use crate::pg::client::{ActivityProcessSnapshot, ActivitySession, ActivitySummarySnapshot};
use chrono::Utc;

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
        if session.wait_info.starts_with("Lock") || session.blocked_count > 0 {
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
            ActivitySubview::Waiting => {
                s.blocked_by_count > 0 || (!s.wait_info.is_empty() && s.state == "active")
            }
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
