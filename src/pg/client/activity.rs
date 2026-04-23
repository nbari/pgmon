//! Activity and diagnostic fetchers for `PgClient`.

use super::{
    ActivityDetail, ActivityProcessSnapshot, ActivitySession, ActivitySnapshot,
    ActivitySummarySnapshot, AutoExplainInfo, DbResult, PgClient, PgClientConnection,
};
use crate::pg::queries::{
    ACTIVITY_BLOCKING_QUERY, ACTIVITY_DETAIL_QUERY, ACTIVITY_LOCKS_QUERY, ACTIVITY_PROCESS_QUERY,
    ACTIVITY_SESSIONS_QUERY, ACTIVITY_SUMMARY_QUERY, AUTO_EXPLAIN_STATUS_QUERY,
};
use chrono::{DateTime, Utc};
use sqlx::Row;

impl PgClient {
    pub(crate) async fn fetch_activity_snapshot(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<ActivitySnapshot> {
        let summary_row = sqlx::query(ACTIVITY_SUMMARY_QUERY)
            .fetch_one(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        let process_row = sqlx::query(ACTIVITY_PROCESS_QUERY)
            .fetch_one(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        let session_rows = sqlx::query(ACTIVITY_SESSIONS_QUERY)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;

        let sessions = session_rows
            .iter()
            .map(map_activity_session)
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)?;

        Ok(ActivitySnapshot {
            summary: map_activity_summary(&summary_row)
                .map_err(super::connect::classify_query_error)?,
            process: map_activity_process(&process_row)
                .map_err(super::connect::classify_query_error)?,
            sessions,
        })
    }

    pub(crate) async fn fetch_activity_detail(
        &self,
        connection: &mut PgClientConnection,
        pid: i32,
    ) -> DbResult<ActivityDetail> {
        let row = sqlx::query(ACTIVITY_DETAIL_QUERY)
            .bind(pid)
            .fetch_one(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        let blockers = sqlx::query(ACTIVITY_BLOCKING_QUERY)
            .bind(pid)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?
            .into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<String, _>(0)?,
                    row.try_get::<String, _>(1)?,
                    row.try_get::<String, _>(2)?,
                    row.try_get::<String, _>(3)?,
                    row.try_get::<String, _>(4)?,
                ])
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)?;
        let locks = sqlx::query(ACTIVITY_LOCKS_QUERY)
            .bind(pid)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?
            .into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<String, _>(0)?,
                    row.try_get::<String, _>(1)?,
                    row.try_get::<String, _>(2)?,
                    row.try_get::<String, _>(3)?,
                ])
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)?;

        Ok(ActivityDetail {
            pid: row
                .try_get::<String, _>(0)
                .map_err(super::connect::classify_query_error)?,
            usename: row
                .try_get::<String, _>(1)
                .map_err(super::connect::classify_query_error)?,
            application_name: row
                .try_get::<String, _>(2)
                .map_err(super::connect::classify_query_error)?,
            client_addr: row
                .try_get::<String, _>(3)
                .map_err(super::connect::classify_query_error)?,
            client_port: row
                .try_get::<String, _>(4)
                .map_err(super::connect::classify_query_error)?,
            backend_start: row
                .try_get::<String, _>(5)
                .map_err(super::connect::classify_query_error)?,
            state: row
                .try_get::<String, _>(6)
                .map_err(super::connect::classify_query_error)?,
            wait_event_type: row
                .try_get::<String, _>(7)
                .map_err(super::connect::classify_query_error)?,
            wait_event: row
                .try_get::<String, _>(8)
                .map_err(super::connect::classify_query_error)?,
            xact_start: row
                .try_get::<String, _>(9)
                .map_err(super::connect::classify_query_error)?,
            state_change: row
                .try_get::<String, _>(10)
                .map_err(super::connect::classify_query_error)?,
            query: row
                .try_get::<String, _>(11)
                .map_err(super::connect::classify_query_error)?,
            blocking_pids: row
                .try_get::<String, _>(12)
                .map_err(super::connect::classify_query_error)?,
            blockers,
            locks,
        })
    }

    pub(crate) async fn fetch_auto_explain_info(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<AutoExplainInfo> {
        let row = sqlx::query(AUTO_EXPLAIN_STATUS_QUERY)
            .fetch_one(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        let log_min_duration = row
            .try_get::<Option<String>, _>(0)
            .map_err(super::connect::classify_query_error)?;
        let log_analyze = row
            .try_get::<Option<String>, _>(1)
            .map_err(super::connect::classify_query_error)?
            .unwrap_or_else(|| "off".to_string());
        let log_buffers = row
            .try_get::<Option<String>, _>(2)
            .map_err(super::connect::classify_query_error)?
            .unwrap_or_else(|| "off".to_string());
        let log_format = row
            .try_get::<Option<String>, _>(3)
            .map_err(super::connect::classify_query_error)?
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

fn map_activity_session(row: &sqlx::postgres::PgRow) -> Result<ActivitySession, sqlx::Error> {
    Ok(ActivitySession {
        pid: row.try_get::<String, _>(0)?,
        backend_type: row.try_get::<String, _>(1)?,
        xmin: row.try_get::<String, _>(2)?,
        database: row.try_get::<String, _>(3)?,
        application: row.try_get::<String, _>(4)?,
        user: row.try_get::<String, _>(5)?,
        client: row.try_get::<String, _>(6)?,
        duration_seconds: row.try_get::<i64, _>(7)?.max(0),
        wait_info: row.try_get::<String, _>(8)?,
        state: row.try_get::<String, _>(9)?,
        query: row.try_get::<String, _>(10)?,
        blocked_by_count: row.try_get::<i64, _>(11)?,
        blocked_count: row.try_get::<i64, _>(12)?,
    })
}

fn map_activity_summary(
    row: &sqlx::postgres::PgRow,
) -> Result<ActivitySummarySnapshot, sqlx::Error> {
    Ok(ActivitySummarySnapshot {
        server_version: row.try_get::<String, _>(0)?,
        postmaster_start: row.try_get::<DateTime<Utc>, _>(1)?,
        database_count: row.try_get::<i64, _>(2)?,
        total_database_bytes: row.try_get::<i64, _>(3)?,
        cache_hit_pct: row.try_get::<f64, _>(4)?,
        rollback_pct: row.try_get::<f64, _>(5)?,
        total_commits: row.try_get::<i64, _>(6)?,
        total_rollbacks: row.try_get::<i64, _>(7)?,
        total_xacts: row.try_get::<i64, _>(8)?,
        total_inserts: row.try_get::<i64, _>(9)?,
        total_updates: row.try_get::<i64, _>(10)?,
        total_deletes: row.try_get::<i64, _>(11)?,
        total_returned: row.try_get::<i64, _>(12)?,
        total_temp_files: row.try_get::<i64, _>(13)?,
        total_temp_bytes: row.try_get::<i64, _>(14)?,
        max_connections: row.try_get::<i64, _>(15)?,
    })
}

fn map_activity_process(
    row: &sqlx::postgres::PgRow,
) -> Result<ActivityProcessSnapshot, sqlx::Error> {
    Ok(ActivityProcessSnapshot {
        worker_total: row.try_get::<i64, _>(0)?,
        max_worker_processes: row.try_get::<i64, _>(1)?,
        logical_workers: row.try_get::<i64, _>(2)?,
        max_logical_workers: row.try_get::<i64, _>(3)?,
        parallel_workers: row.try_get::<i64, _>(4)?,
        max_parallel_workers: row.try_get::<i64, _>(5)?,
        autovacuum_workers: row.try_get::<i64, _>(6)?,
        max_autovacuum_workers: row.try_get::<i64, _>(7)?,
        wal_senders: row.try_get::<i64, _>(8)?,
        max_wal_senders: row.try_get::<i64, _>(9)?,
        wal_receivers: row.try_get::<i64, _>(10)?,
        replication_slots: row.try_get::<i64, _>(11)?,
        max_replication_slots: row.try_get::<i64, _>(12)?,
    })
}
