use crate::pg::conninfo::describe_connection_target;
use crate::pg::queries::{
    ACTIVITY_BLOCKING_QUERY, ACTIVITY_DETAIL_QUERY, ACTIVITY_LOCKS_QUERY, ACTIVITY_PROCESS_QUERY,
    ACTIVITY_SESSIONS_QUERY, ACTIVITY_SUMMARY_QUERY, DATABASE_QUERY, DATABASE_TREE_QUERY, IO_QUERY,
    LOCKS_QUERY, REPLICATION_RECEIVER_QUERY, REPLICATION_SENDERS_QUERY, REPLICATION_SLOTS_QUERY,
    SETTINGS_QUERY,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use postgres::{Client, Config, NoTls};
use std::time::Duration;

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
    fn unavailable(reason: impl Into<String>) -> Self {
        Self::Unavailable(reason.into())
    }
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

pub struct PgClient {
    client: Client,
    capability_cache: CapabilityCache,
}

#[derive(Debug, Default)]
struct CapabilityCache {
    io: Option<CapabilityStatus>,
    statements: Option<CapabilityStatus>,
    replication: CapabilityStatus,
    statements_query: Option<String>,
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

    pub fn execute_admin_action(&mut self, query: &str) -> Result<()> {
        self.client.simple_query(query)?;
        Ok(())
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
        if let CapabilityStatus::Unavailable(_) = self.ensure_io_capability()? {
            return Ok(Vec::new());
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
        let Some(query) = self.ensure_statements_query()? else {
            return Ok(Vec::new());
        };

        let rows = match self.client.query(query.as_str(), &[]) {
            Ok(r) => r,
            Err(e) => {
                if e.to_string().contains("shared_preload_libraries") {
                    self.capability_cache.statements = Some(CapabilityStatus::unavailable(
                        "pg_stat_statements extension exists, but shared_preload_libraries does not load it.",
                    ));
                    self.capability_cache.statements_query = None;
                    return Ok(Vec::new());
                }
                return Err(e.into());
            }
        };
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(6)?,
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

    pub fn fetch_explain_plan(&mut self, query: &str) -> Result<Vec<String>> {
        use postgres::SimpleQueryMessage;
        // Simple query protocol is used here because it doesn't try to parse parameters ($1, $2)
        // on the client side, allowing us to send the raw query string to Postgres.
        let explain_query = format!("EXPLAIN (ANALYZE, BUFFERS) {query}");
        let messages = self.client.simple_query(&explain_query)?;
        let mut plan = Vec::new();
        for msg in messages {
            if let SimpleQueryMessage::Row(row) = msg {
                plan.push(row.get(0).unwrap_or_default().to_string());
            }
        }
        Ok(plan)
    }

    pub fn fetch_table_definition(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Result<(Vec<Vec<String>>, Vec<String>)> {
        let col_query = r"
            SELECT 
                column_name, 
                data_type, 
                COALESCE(character_maximum_length::text, ''), 
                is_nullable, 
                COALESCE(column_default, '')
            FROM information_schema.columns 
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        ";

        let columns = self
            .client
            .query(col_query, &[&schema, &table])?
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

        let idx_query = r"
            SELECT pg_get_indexdef(indexrelid)
            FROM pg_index
            WHERE indrelid = ($1 || '.' || $2)::regclass
        ";

        let quoted_schema = format!("\"{schema}\"");
        let quoted_table = format!("\"{table}\"");

        let indexes = self
            .client
            .query(idx_query, &[&quoted_schema, &quoted_table])?
            .into_iter()
            .map(|row| row.try_get::<_, String>(0))
            .collect::<Result<Vec<_>, _>>()?;

        Ok((columns, indexes))
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

        let capability = detect_replication_capability(
            &mut self.client,
            receiver_summary.as_ref(),
            &senders,
            &slots,
        )?;
        self.capability_cache.replication = capability.clone();

        Ok(ReplicationSnapshot {
            capability,
            receiver_summary,
            senders,
            slots,
        })
    }

    pub(crate) fn io_capability(&self) -> CapabilityStatus {
        self.capability_cache.io.clone().unwrap_or_default()
    }

    pub(crate) fn statements_capability(&self) -> CapabilityStatus {
        self.capability_cache.statements.clone().unwrap_or_default()
    }

    pub(crate) fn replication_capability(&self) -> CapabilityStatus {
        self.capability_cache.replication.clone()
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
                    row.try_get::<_, String>(0)?, // schema
                    row.try_get::<_, Option<String>>(1)?.unwrap_or_default(), // table
                    depth.to_string(),
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
        Ok(Self {
            client,
            capability_cache: CapabilityCache::default(),
        })
    }

    fn ensure_io_capability(&mut self) -> Result<CapabilityStatus> {
        if let Some(status) = self.capability_cache.io.as_ref() {
            return Ok(status.clone());
        }

        let status = if self.view_exists("pg_stat_io")? {
            CapabilityStatus::Available
        } else {
            CapabilityStatus::unavailable(
                "pg_stat_io is not available on this server (PostgreSQL 16+ required).",
            )
        };
        self.capability_cache.io = Some(status.clone());
        Ok(status)
    }

    fn ensure_statements_query(&mut self) -> Result<Option<String>> {
        if let Some(status) = self.capability_cache.statements.as_ref()
            && matches!(status, CapabilityStatus::Unavailable(_))
        {
            return Ok(None);
        }

        if let Some(query) = self.capability_cache.statements_query.as_ref() {
            self.capability_cache.statements = Some(CapabilityStatus::Available);
            return Ok(Some(query.clone()));
        }

        if !self.extension_exists("pg_stat_statements")? {
            self.capability_cache.statements = Some(CapabilityStatus::unavailable(
                "pg_stat_statements is not installed in the current database.",
            ));
            return Ok(None);
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
                "COALESCE(s.shared_blk_read_time, 0)::float8"
            } else {
                "0::float8"
            };
        let blk_write_time_expr =
            if self.column_exists("pg_stat_statements", "shared_blk_write_time")? {
                "COALESCE(s.shared_blk_write_time, 0)::float8"
            } else {
                "0::float8"
            };

        let query = build_statements_query(
            total_time_column,
            mean_time_column,
            blk_read_time_expr,
            blk_write_time_expr,
        );
        self.capability_cache.statements_query = Some(query.clone());
        self.capability_cache.statements = Some(CapabilityStatus::Available);
        Ok(Some(query))
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

fn detect_replication_capability(
    client: &mut Client,
    receiver_summary: Option<&String>,
    senders: &[ReplicationSender],
    slots: &[ReplicationSlot],
) -> Result<CapabilityStatus> {
    if receiver_summary.is_some() || !senders.is_empty() || !slots.is_empty() {
        return Ok(CapabilityStatus::Available);
    }

    let settings = client.query_one(
        "SELECT current_setting('max_wal_senders')::bigint, current_setting('max_replication_slots')::bigint, pg_is_in_recovery()",
        &[],
    )?;
    let max_wal_senders = settings.try_get::<_, i64>(0)?;
    let max_replication_slots = settings.try_get::<_, i64>(1)?;
    let in_recovery = settings.try_get::<_, bool>(2)?;

    if in_recovery || max_wal_senders > 0 || max_replication_slots > 0 {
        Ok(CapabilityStatus::Available)
    } else {
        Ok(CapabilityStatus::unavailable(
            "Replication is disabled on this server (max_wal_senders=0, max_replication_slots=0).",
        ))
    }
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
    COALESCE(regexp_replace(s.query, '\s+', ' ', 'g'), '') as query, 
    COALESCE(s.{total_time_column}, 0)::float8 as total_time, 
    COALESCE(s.{mean_time_column}, 0)::float8 as mean_time, 
    COALESCE(s.calls, 0)::bigint as calls, 
    {blk_read_time_expr} as blk_read_time, 
    {blk_write_time_expr} as blk_write_time,
    COALESCE(d.datname, '') as datname
FROM pg_stat_statements s
LEFT JOIN pg_database d ON d.oid = s.dbid
ORDER BY s.{total_time_column} DESC
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
            "COALESCE(s.shared_blk_read_time, 0)::float8",
            "COALESCE(s.shared_blk_write_time, 0)::float8",
        );

        assert!(query.contains("COALESCE(s.total_exec_time, 0)::float8 as total_time"));
        assert!(query.contains("COALESCE(s.mean_exec_time, 0)::float8 as mean_time"));
        assert!(query.contains("ORDER BY s.total_exec_time DESC"));
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
        assert!(!query.contains("s.0::float8"));
    }

    #[test]
    fn test_format_receiver_summary_omits_empty_fields() {
        let summary = format_receiver_summary("streaming", "replica", "5432", "", "");

        assert_eq!(summary, "Receiver: streaming | source=replica:5432");
    }
}
