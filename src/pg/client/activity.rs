//! Activity and diagnostic fetchers for `PgClient`.

use super::{
    ActivityDetail, ActivityProcessSnapshot, ActivitySession, ActivitySnapshot,
    ActivitySummarySnapshot, AutoExplainInfo, PgClient,
};
use crate::pg::queries::{
    ACTIVITY_BLOCKING_QUERY, ACTIVITY_DETAIL_QUERY, ACTIVITY_LOCKS_QUERY, ACTIVITY_PROCESS_QUERY,
    ACTIVITY_SESSIONS_QUERY, ACTIVITY_SUMMARY_QUERY, AUTO_EXPLAIN_STATUS_QUERY,
};
use anyhow::Result;
use chrono::{DateTime, Utc};

impl PgClient {
    pub fn fetch_activity_snapshot(&mut self) -> Result<ActivitySnapshot> {
        let summary_row = self.client.query_one(ACTIVITY_SUMMARY_QUERY, &[])?;
        let process_row = self.client.query_one(ACTIVITY_PROCESS_QUERY, &[])?;
        let session_rows = self.client.query(ACTIVITY_SESSIONS_QUERY, &[])?;

        let sessions = session_rows
            .into_iter()
            .map(|row| {
                Ok(ActivitySession {
                    pid: row.try_get::<_, String>(0)?,
                    backend_type: row.try_get::<_, String>(1)?,
                    xmin: row.try_get::<_, String>(2)?,
                    database: row.try_get::<_, String>(3)?,
                    application: row.try_get::<_, String>(4)?,
                    user: row.try_get::<_, String>(5)?,
                    client: row.try_get::<_, String>(6)?,
                    duration_seconds: row.try_get::<_, i64>(7)?.max(0),
                    wait_info: row.try_get::<_, String>(8)?,
                    state: row.try_get::<_, String>(9)?,
                    query: row.try_get::<_, String>(10)?,
                    blocked_by_count: row.try_get::<_, i64>(11)?,
                    blocked_count: row.try_get::<_, i64>(12)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ActivitySnapshot {
            summary: ActivitySummarySnapshot {
                server_version: summary_row.try_get::<_, String>(0)?,
                postmaster_start: summary_row.try_get::<_, DateTime<Utc>>(1)?,
                database_count: summary_row.try_get::<_, i64>(2)?,
                total_database_bytes: summary_row.try_get::<_, i64>(3)?,
                cache_hit_pct: summary_row.try_get::<_, f64>(4)?,
                rollback_pct: summary_row.try_get::<_, f64>(5)?,
                total_commits: summary_row.try_get::<_, i64>(6)?,
                total_rollbacks: summary_row.try_get::<_, i64>(7)?,
                total_xacts: summary_row.try_get::<_, i64>(8)?,
                total_inserts: summary_row.try_get::<_, i64>(9)?,
                total_updates: summary_row.try_get::<_, i64>(10)?,
                total_deletes: summary_row.try_get::<_, i64>(11)?,
                total_returned: summary_row.try_get::<_, i64>(12)?,
                total_temp_files: summary_row.try_get::<_, i64>(13)?,
                total_temp_bytes: summary_row.try_get::<_, i64>(14)?,
                max_connections: summary_row.try_get::<_, i64>(15)?,
            },
            process: ActivityProcessSnapshot {
                worker_total: process_row.try_get::<_, i64>(0)?,
                max_worker_processes: process_row.try_get::<_, i64>(1)?,
                logical_workers: process_row.try_get::<_, i64>(2)?,
                max_logical_workers: process_row.try_get::<_, i64>(3)?,
                parallel_workers: process_row.try_get::<_, i64>(4)?,
                max_parallel_workers: process_row.try_get::<_, i64>(5)?,
                autovacuum_workers: process_row.try_get::<_, i64>(6)?,
                max_autovacuum_workers: process_row.try_get::<_, i64>(7)?,
                wal_senders: process_row.try_get::<_, i64>(8)?,
                max_wal_senders: process_row.try_get::<_, i64>(9)?,
                wal_receivers: process_row.try_get::<_, i64>(10)?,
                replication_slots: process_row.try_get::<_, i64>(11)?,
                max_replication_slots: process_row.try_get::<_, i64>(12)?,
            },
            sessions,
        })
    }

    pub fn fetch_activity_detail(&mut self, pid: i32) -> Result<ActivityDetail> {
        let row = self.client.query_one(ACTIVITY_DETAIL_QUERY, &[&pid])?;
        let blockers = self
            .client
            .query(ACTIVITY_BLOCKING_QUERY, &[&pid])?
            .into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, String>(1)?,
                    row.try_get::<_, String>(2)?,
                    row.try_get::<_, String>(3)?,
                    row.try_get::<_, String>(4)?,
                ])
            })
            .collect::<Result<Vec<_>>>()?;

        let locks = self
            .client
            .query(ACTIVITY_LOCKS_QUERY, &[&pid])?
            .into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, String>(1)?,
                    row.try_get::<_, String>(2)?,
                    row.try_get::<_, String>(3)?,
                ])
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ActivityDetail {
            pid: row.try_get::<_, String>(0)?,
            usename: row.try_get::<_, String>(1)?,
            application_name: row.try_get::<_, String>(2)?,
            client_addr: row.try_get::<_, String>(3)?,
            client_port: row.try_get::<_, String>(4)?,
            backend_start: row.try_get::<_, String>(5)?,
            state: row.try_get::<_, String>(6)?,
            wait_event_type: row.try_get::<_, String>(7)?,
            wait_event: row.try_get::<_, String>(8)?,
            xact_start: row.try_get::<_, String>(9)?,
            state_change: row.try_get::<_, String>(10)?,
            query: row.try_get::<_, String>(11)?,
            blocking_pids: row.try_get::<_, String>(12)?,
            blockers,
            locks,
        })
    }

    pub(crate) fn fetch_auto_explain_info(&mut self) -> Result<AutoExplainInfo> {
        let row = self.client.query_one(AUTO_EXPLAIN_STATUS_QUERY, &[])?;
        let log_min_duration = row.try_get::<_, Option<String>>(0)?;
        let log_analyze = row
            .try_get::<_, Option<String>>(1)?
            .unwrap_or_else(|| "off".to_string());
        let log_buffers = row
            .try_get::<_, Option<String>>(2)?
            .unwrap_or_else(|| "off".to_string());
        let log_format = row
            .try_get::<_, Option<String>>(3)?
            .unwrap_or_else(|| "text".to_string());

        if let Some(log_min_duration) = log_min_duration {
            if log_min_duration == "-1" {
                return Ok(AutoExplainInfo {
                    summary: "auto_explain: loaded but disabled (log_min_duration = -1)"
                        .to_string(),
                    hint: Some(
                        "Set auto_explain.log_min_duration to 0ms or a threshold and consider auto_explain.log_analyze = on for real execution plans."
                            .to_string(),
                    ),
                });
            }

            let mut hints = Vec::new();
            if log_analyze != "on" {
                hints.push(
                    "Set auto_explain.log_analyze = on to capture real execution timing."
                        .to_string(),
                );
            }
            if log_format != "json" {
                hints.push(format!("Current auto_explain.log_format is {log_format}."));
            }

            return Ok(AutoExplainInfo {
                summary: format!(
                    "auto_explain: enabled (log_min_duration = {log_min_duration}, log_analyze = {log_analyze}, log_buffers = {log_buffers}, log_format = {log_format})"
                ),
                hint: (!hints.is_empty()).then(|| hints.join(" ")),
            });
        }

        Ok(AutoExplainInfo {
            summary: "auto_explain: not loaded".to_string(),
            hint: Some(
                "A DBA can preload auto_explain for new sessions via session_preload_libraries or shared_preload_libraries, then set auto_explain.log_min_duration and optionally auto_explain.log_analyze = on."
                    .to_string(),
            ),
        })
    }
}
