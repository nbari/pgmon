use crate::pg::conninfo::describe_connection_target;
use crate::pg::queries::{
    ACTIVITY_PROCESS_QUERY, ACTIVITY_SESSIONS_QUERY, ACTIVITY_SUMMARY_QUERY, DATABASE_QUERY,
    DATABASE_TREE_QUERY, IO_QUERY, LOCKS_QUERY, SETTINGS_QUERY, STATEMENTS_QUERY,
    STATEMENTS_QUERY_V12,
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

        let has_exec_time = self.column_exists("pg_stat_statements", "total_exec_time")?;
        let query = if has_exec_time {
            STATEMENTS_QUERY
        } else {
            STATEMENTS_QUERY_V12
        };

        let rows = match self.client.query(query, &[]) {
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

    pub fn fetch_activity_snapshot(&mut self) -> Result<ActivitySnapshot> {
        let summary_row = self.client.query_one(ACTIVITY_SUMMARY_QUERY, &[])?;
        let process_row = self.client.query_one(ACTIVITY_PROCESS_QUERY, &[])?;
        let session_rows = self.client.query(ACTIVITY_SESSIONS_QUERY, &[])?;

        let sessions = session_rows
            .into_iter()
            .map(|row| {
                Ok(ActivitySession {
                    pid: row.try_get::<_, String>(0)?,
                    xmin: row.try_get::<_, String>(1)?,
                    database: row.try_get::<_, String>(2)?,
                    application: row.try_get::<_, String>(3)?,
                    user: row.try_get::<_, String>(4)?,
                    client: row.try_get::<_, String>(5)?,
                    duration_seconds: row.try_get::<_, i64>(6)?.max(0),
                    wait_info: row.try_get::<_, String>(7)?,
                    state: row.try_get::<_, String>(8)?,
                    query: row.try_get::<_, String>(9)?,
                    blocked_by_count: row.try_get::<_, i64>(10)?,
                    blocked_count: row.try_get::<_, i64>(11)?,
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
