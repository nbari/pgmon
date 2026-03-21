use crate::pg::conninfo::describe_connection_target;
use crate::pg::queries::{
    ACTIVITY_PROCESS_QUERY, ACTIVITY_SESSIONS_QUERY, ACTIVITY_SUMMARY_QUERY, DATABASE_QUERY,
    DATABASE_TREE_QUERY, IO_QUERY, LOCKS_QUERY, STATEMENTS_QUERY,
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
        Ok(rows
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0).unwrap_or_default(),
                    row.get::<_, i32>(1).to_string(),
                    row.get::<_, i64>(2).to_string(),
                    row.get::<_, i64>(3).to_string(),
                    format!("{:.1}%", row.get::<_, f64>(4)),
                    row.get::<_, i64>(5).to_string(),
                    row.get::<_, i64>(6).to_string(),
                    row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(7)
                        .map(|t| t.to_rfc3339())
                        .unwrap_or_default(),
                ]
            })
            .collect())
    }

    pub fn fetch_locks(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(LOCKS_QUERY, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0).unwrap_or_default(),
                    row.get::<_, Option<String>>(1).unwrap_or_default(),
                    row.get::<_, i64>(2).to_string(),
                    row.get::<_, i64>(3).to_string(),
                ]
            })
            .collect())
    }

    pub fn fetch_io_stats(&mut self) -> Result<Vec<Vec<String>>> {
        if !self.view_exists("pg_stat_io")? {
            return Ok(vec![vec![
                "pg_stat_io not available (PG 16+ required)".to_string(),
            ]]);
        }
        let rows = self.client.query(IO_QUERY, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0).unwrap_or_default(),
                    row.get::<_, i64>(1).to_string(),
                    row.get::<_, i64>(2).to_string(),
                    row.get::<_, f64>(3).to_string(),
                    row.get::<_, f64>(4).to_string(),
                ]
            })
            .collect())
    }

    pub fn fetch_statements(&mut self) -> Result<Vec<Vec<String>>> {
        if !self.extension_exists("pg_stat_statements")? {
            return Ok(vec![vec!["pg_stat_statements not installed".to_string()]]);
        }
        let rows = match self.client.query(STATEMENTS_QUERY, &[]) {
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
        Ok(rows
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, String>(0),
                    row.get::<_, f64>(1).to_string(),
                    row.get::<_, f64>(2).to_string(),
                    row.get::<_, i64>(3).to_string(),
                    row.get::<_, f64>(4).to_string(),
                    row.get::<_, f64>(5).to_string(),
                ]
            })
            .collect())
    }

    pub fn fetch_activity_snapshot(&mut self) -> Result<ActivitySnapshot> {
        let summary_row = self.client.query_one(ACTIVITY_SUMMARY_QUERY, &[])?;
        let process_row = self.client.query_one(ACTIVITY_PROCESS_QUERY, &[])?;
        let session_rows = self.client.query(ACTIVITY_SESSIONS_QUERY, &[])?;

        let sessions = session_rows
            .into_iter()
            .map(|row| ActivitySession {
                pid: row.get::<_, String>(0),
                xmin: row.get::<_, String>(1),
                database: row.get::<_, String>(2),
                application: row.get::<_, String>(3),
                user: row.get::<_, String>(4),
                client: row.get::<_, String>(5),
                duration_seconds: row.get::<_, i64>(6).max(0),
                wait_info: row.get::<_, String>(7),
                state: row.get::<_, String>(8),
                query: row.get::<_, String>(9),
                blocked_by_count: row.get::<_, i64>(10),
                blocked_count: row.get::<_, i64>(11),
            })
            .collect();

        Ok(ActivitySnapshot {
            summary: ActivitySummarySnapshot {
                server_version: summary_row.get::<_, String>(0),
                postmaster_start: summary_row.get::<_, DateTime<Utc>>(1),
                database_count: summary_row.get::<_, i64>(2),
                total_database_bytes: summary_row.get::<_, i64>(3),
                cache_hit_pct: summary_row.get::<_, f64>(4),
                rollback_pct: summary_row.get::<_, f64>(5),
                total_xacts: summary_row.get::<_, i64>(8),
                total_inserts: summary_row.get::<_, i64>(9),
                total_updates: summary_row.get::<_, i64>(10),
                total_deletes: summary_row.get::<_, i64>(11),
                total_returned: summary_row.get::<_, i64>(12),
                total_temp_files: summary_row.get::<_, i64>(13),
                total_temp_bytes: summary_row.get::<_, i64>(14),
                max_connections: summary_row.get::<_, i64>(15),
            },
            process: ActivityProcessSnapshot {
                worker_total: process_row.get::<_, i64>(0),
                max_worker_processes: process_row.get::<_, i64>(1),
                logical_workers: process_row.get::<_, i64>(2),
                max_logical_workers: process_row.get::<_, i64>(3),
                parallel_workers: process_row.get::<_, i64>(4),
                max_parallel_workers: process_row.get::<_, i64>(5),
                autovacuum_workers: process_row.get::<_, i64>(6),
                max_autovacuum_workers: process_row.get::<_, i64>(7),
                wal_senders: process_row.get::<_, i64>(8),
                max_wal_senders: process_row.get::<_, i64>(9),
                wal_receivers: process_row.get::<_, i64>(10),
                replication_slots: process_row.get::<_, i64>(11),
                max_replication_slots: process_row.get::<_, i64>(12),
            },
            sessions,
        })
    }

    pub fn fetch_database_tree(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(DATABASE_TREE_QUERY, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let depth = row.get::<_, i32>(5);
                let label = match depth {
                    0 => row.get::<_, String>(0),
                    _ => format!("  {}", row.get::<_, Option<String>>(1).unwrap_or_default()),
                };
                vec![
                    label,
                    row.get::<_, String>(2),
                    row.get::<_, i64>(3).to_string(),
                    row.get::<_, String>(4),
                ]
            })
            .collect())
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
