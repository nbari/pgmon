use crate::pg::conninfo::describe_connection_target;
use crate::pg::queries::{
    ACTIVITY_PROCESS_QUERY, ACTIVITY_SESSIONS_QUERY, ACTIVITY_SUMMARY_QUERY, DATABASE_QUERY,
    DATABASE_TREE_QUERY, IO_QUERY, LOCKS_QUERY, REPLICATION_RECEIVER_QUERY,
    REPLICATION_SENDERS_QUERY, REPLICATION_SLOTS_QUERY, SETTINGS_QUERY,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use postgres::{Client, Config, NoTls};
use std::time::Duration;

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

#[derive(Debug, Clone, Default)]
pub(crate) struct ReplicationSnapshot {
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

pub struct PgClient {
    client: Client,
}

impl std::fmt::Debug for PgClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgClient").finish()
    }
}

impl PgClient {
    pub fn new(dsn: &str, connect_timeout_ms: u64) -> Result<Self> {
        let config = config_from_dsn(dsn, connect_timeout_ms)?;
        Self::connect(&config, dsn)
    }

    pub fn for_database(dsn: &str, connect_timeout_ms: u64, database: &str) -> Result<Self> {
        let mut config = config_from_dsn(dsn, connect_timeout_ms)?;
        config.dbname(database);
        Self::connect(&config, dsn)
    }

    pub fn fetch_database_stats(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(DATABASE_QUERY, &[])?;
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, Option<String>>(0)?.unwrap_or_default(),
                    row.try_get::<_, i32>(1)?.to_string(),
                    row.try_get::<_, i64>(2)?.to_string(),
                    row.try_get::<_, i64>(3)?.to_string(),
                    format!("{:.1}%", row.try_get::<_, f64>(4)?),
                    row.try_get::<_, i64>(5)?.to_string(),
                    row.try_get::<_, i64>(6)?.to_string(),
                    row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(7)?
                        .map(|t| t.to_rfc3339())
                        .unwrap_or_default(),
                ])
            })
            .collect()
    }

    pub fn fetch_locks(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(LOCKS_QUERY, &[])?;
        let result: Result<Vec<Vec<String>>> = rows
            .into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, String>(1)?,
                    row.try_get::<_, String>(2)?,
                    row.try_get::<_, String>(3)?,
                    row.try_get::<_, String>(4)?,
                    row.try_get::<_, i64>(5)?.to_string(),
                    row.try_get::<_, String>(6)?,
                ])
            })
            .collect();

        let mut data = result?;
        if data.is_empty() {
            data.push(vec![
                String::new(),
                String::new(),
                String::new(),
                "No active locks found".to_string(),
                String::new(),
                String::new(),
                String::new(),
            ]);
        }
        Ok(data)
    }

    pub fn fetch_io_stats(&mut self) -> Result<Vec<Vec<String>>> {
        if !self.view_exists("pg_stat_io")? {
            return Ok(vec![vec![
                "pg_stat_io not available (PG 16+ required)".to_string(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            ]]);
        }
        let rows = self.client.query(IO_QUERY, &[])?;
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, Option<String>>(0)?.unwrap_or_default(),
                    row.try_get::<_, Option<String>>(1)?.unwrap_or_default(),
                    row.try_get::<_, Option<String>>(2)?.unwrap_or_default(),
                    row.try_get::<_, i64>(3)?.to_string(),
                    row.try_get::<_, i64>(4)?.to_string(),
                    row.try_get::<_, f64>(5)?.to_string(),
                    row.try_get::<_, f64>(6)?.to_string(),
                ])
            })
            .collect()
    }

    pub fn fetch_statements(&mut self) -> Result<Vec<Vec<String>>> {
        if !self.extension_exists("pg_stat_statements")? {
            return Ok(vec![vec!["pg_stat_statements not installed".to_string()]]);
        }

        let total_time_column = if self.column_exists("pg_stat_statements", "total_exec_time")? {
            "total_exec_time"
        } else {
            "total_time"
        };
        let mean_time_column = if self.column_exists("pg_stat_statements", "mean_exec_time")? {
            "mean_exec_time"
        } else {
            "mean_time"
        };
        let blk_read_time_expr =
            if self.column_exists("pg_stat_statements", "shared_blk_read_time")? {
                "COALESCE(shared_blk_read_time, 0)::float8"
            } else {
                "0::float8"
            };
        let blk_write_time_expr =
            if self.column_exists("pg_stat_statements", "shared_blk_write_time")? {
                "COALESCE(shared_blk_write_time, 0)::float8"
            } else {
                "0::float8"
            };
        let query = build_statements_query(
            total_time_column,
            mean_time_column,
            blk_read_time_expr,
            blk_write_time_expr,
        );

        let rows = match self.client.query(query.as_str(), &[]) {
            Ok(r) => r,
            Err(e) => {
                if e.to_string().contains("shared_preload_libraries") {
                    return Ok(vec![vec![
                        "pg_stat_statements library not loaded".to_string(),
                    ]]);
                }
                return Err(e.into());
            }
        };
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, f64>(1)?.to_string(),
                    row.try_get::<_, f64>(2)?.to_string(),
                    row.try_get::<_, i64>(3)?.to_string(),
                    row.try_get::<_, f64>(4)?.to_string(),
                    row.try_get::<_, f64>(5)?.to_string(),
                ])
            })
            .collect()
    }

    pub fn fetch_replication_snapshot(&mut self) -> Result<ReplicationSnapshot> {
        let receiver_summary =
            if let Some(row) = self.client.query_opt(REPLICATION_RECEIVER_QUERY, &[])? {
                let status = row.try_get::<_, String>(0)?;
                let sender_host = row.try_get::<_, String>(1)?;
                let sender_port = row.try_get::<_, String>(2)?;
                let slot_name = row.try_get::<_, String>(3)?;
                let latest_end_lsn = row.try_get::<_, String>(4)?;
                Some(format_receiver_summary(
                    &status,
                    &sender_host,
                    &sender_port,
                    &slot_name,
                    &latest_end_lsn,
                ))
            } else {
                None
            };

        let senders = self
            .client
            .query(REPLICATION_SENDERS_QUERY, &[])?
            .into_iter()
            .map(|row| {
                Ok(ReplicationSender {
                    pid: row.try_get::<_, String>(0)?,
                    user: row.try_get::<_, String>(1)?,
                    application: row.try_get::<_, String>(2)?,
                    client: row.try_get::<_, String>(3)?,
                    state: row.try_get::<_, String>(4)?,
                    sync_state: row.try_get::<_, String>(5)?,
                    slot_name: row.try_get::<_, String>(6)?,
                    sent_lag_bytes: row.try_get::<_, i64>(7)?,
                    write_lag_bytes: row.try_get::<_, i64>(8)?,
                    flush_lag_bytes: row.try_get::<_, i64>(9)?,
                    replay_lag_bytes: row.try_get::<_, i64>(10)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let slots = self
            .client
            .query(REPLICATION_SLOTS_QUERY, &[])?
            .into_iter()
            .map(|row| {
                Ok(ReplicationSlot {
                    slot_name: row.try_get::<_, String>(0)?,
                    slot_type: row.try_get::<_, String>(1)?,
                    active: row.try_get::<_, String>(2)?,
                    active_pid: row.try_get::<_, String>(3)?,
                    restart_lsn: row.try_get::<_, String>(4)?,
                    confirmed_flush_lsn: row.try_get::<_, String>(5)?,
                    wal_status: row.try_get::<_, String>(6)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ReplicationSnapshot {
            receiver_summary,
            senders,
            slots,
        })
    }

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

    pub fn fetch_database_tree(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(DATABASE_TREE_QUERY, &[])?;
        rows.into_iter()
            .map(|row| {
                let depth = row.try_get::<_, i32>(5)?;
                let label = match depth {
                    0 => row.try_get::<_, String>(0)?,
                    _ => format!(
                        "  {}",
                        row.try_get::<_, Option<String>>(1)?.unwrap_or_default()
                    ),
                };
                Ok(vec![
                    label,
                    row.try_get::<_, String>(2)?,
                    row.try_get::<_, i64>(3)?.to_string(),
                    row.try_get::<_, String>(4)?,
                ])
            })
            .collect()
    }

    pub fn fetch_settings(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(SETTINGS_QUERY, &[])?;
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, String>(1)?,
                    row.try_get::<_, String>(2)?,
                    row.try_get::<_, String>(3)?,
                    row.try_get::<_, String>(4)?,
                ])
            })
            .collect()
    }

    pub fn execute_query(&mut self, query: &str) -> Result<u64> {
        let result = self.client.execute(query, &[])?;
        Ok(result)
    }

    fn extension_exists(&mut self, name: &str) -> Result<bool> {
        let row = self
            .client
            .query_opt("SELECT 1 FROM pg_extension WHERE extname = $1", &[&name])?;
        Ok(row.is_some())
    }

    fn view_exists(&mut self, name: &str) -> Result<bool> {
        let row = self
            .client
            .query_opt("SELECT 1 FROM pg_views WHERE viewname = $1", &[&name])?;
        Ok(row.is_some())
    }

    fn column_exists(&mut self, table_name: &str, column_name: &str) -> Result<bool> {
        let row = self.client.query_opt(
            "SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
            &[&table_name, &column_name],
        )?;
        Ok(row.is_some())
    }

    fn connect(config: &Config, dsn: &str) -> Result<Self> {
        let client = config.connect(NoTls).with_context(|| {
            format!(
                "Failed to connect to Postgres using {}",
                describe_connection_target(dsn)
            )
        })?;
        Ok(Self { client })
    }
}

fn config_from_dsn(dsn: &str, connect_timeout_ms: u64) -> Result<Config> {
    let mut config: Config = dsn.parse().with_context(|| {
        format!(
            "Failed to parse Postgres connection settings for {}",
            describe_connection_target(dsn)
        )
    })?;
    config.connect_timeout(Duration::from_millis(connect_timeout_ms));
    Ok(config)
}

fn format_receiver_summary(
    status: &str,
    sender_host: &str,
    sender_port: &str,
    slot_name: &str,
    latest_end_lsn: &str,
) -> String {
    let mut parts = vec![format!("Receiver: {status}")];

    if !sender_host.is_empty() {
        if sender_port.is_empty() {
            parts.push(format!("source={sender_host}"));
        } else {
            parts.push(format!("source={sender_host}:{sender_port}"));
        }
    }
    if !slot_name.is_empty() {
        parts.push(format!("slot={slot_name}"));
    }
    if !latest_end_lsn.is_empty() {
        parts.push(format!("latest_end_lsn={latest_end_lsn}"));
    }

    parts.join(" | ")
}

fn build_statements_query(
    total_time_column: &str,
    mean_time_column: &str,
    blk_read_time_expr: &str,
    blk_write_time_expr: &str,
) -> String {
    format!(
        r"
SELECT 
    COALESCE(regexp_replace(query, '\s+', ' ', 'g'), '') as query, 
    COALESCE({total_time_column}, 0)::float8 as total_time, 
    COALESCE({mean_time_column}, 0)::float8 as mean_time, 
    COALESCE(calls, 0)::bigint as calls, 
    {blk_read_time_expr} as blk_read_time, 
    {blk_write_time_expr} as blk_write_time
FROM pg_stat_statements
ORDER BY {total_time_column} DESC
LIMIT 500
"
    )
}

#[cfg(test)]
mod tests {
    use super::{build_statements_query, format_receiver_summary};

    #[test]
    fn test_build_statements_query_uses_exec_time_columns() {
        let query = build_statements_query(
            "total_exec_time",
            "mean_exec_time",
            "COALESCE(shared_blk_read_time, 0)::float8",
            "COALESCE(shared_blk_write_time, 0)::float8",
        );

        assert!(query.contains("COALESCE(total_exec_time, 0)::float8 as total_time"));
        assert!(query.contains("COALESCE(mean_exec_time, 0)::float8 as mean_time"));
        assert!(query.contains("ORDER BY total_exec_time DESC"));
    }

    #[test]
    fn test_build_statements_query_falls_back_when_block_timing_columns_are_missing() {
        let query = build_statements_query(
            "total_exec_time",
            "mean_exec_time",
            "0::float8",
            "0::float8",
        );

        assert!(query.contains("0::float8 as blk_read_time"));
        assert!(query.contains("0::float8 as blk_write_time"));
    }

    #[test]
    fn test_format_receiver_summary_omits_empty_fields() {
        let summary = format_receiver_summary("streaming", "replica", "5432", "", "");

        assert_eq!(summary, "Receiver: streaming | source=replica:5432");
    }
}
