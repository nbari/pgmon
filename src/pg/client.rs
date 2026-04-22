use crate::pg::conninfo::describe_connection_target;
use crate::pg::queries::{
    ACTIVITY_BLOCKING_QUERY, ACTIVITY_DETAIL_QUERY, ACTIVITY_LOCKS_QUERY, ACTIVITY_PROCESS_QUERY,
    ACTIVITY_SESSIONS_QUERY, ACTIVITY_SUMMARY_QUERY, AUTO_EXPLAIN_STATUS_QUERY, DATABASE_QUERY,
    DATABASE_TREE_QUERY, IO_QUERY, LOCKS_QUERY, REPLICATION_RECEIVER_QUERY,
    REPLICATION_SENDERS_QUERY, REPLICATION_SLOTS_QUERY, SETTINGS_QUERY,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use pg_query::NodeRef;
use postgres::{Client, Config, NoTls};
use std::time::Duration;

pub(crate) const MIN_SUPPORTED_SERVER_VERSION_NUM: i32 = 140_000;
const GENERIC_PLAN_MIN_SERVER_VERSION_NUM: i32 = 160_000;

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

/// Safe in-app planning modes used by `pgmon` query diagnosis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExplainMode {
    /// Use a regular estimated plan for executable SQL text.
    Estimated,
    /// Use `PostgreSQL`'s generic planner path for parameterized SQL with placeholders.
    GenericEstimated,
}

impl ExplainMode {
    /// Human-readable title shown by the explain modal.
    pub(crate) const fn title(self) -> &'static str {
        match self {
            Self::Estimated => "Estimated Plan",
            Self::GenericEstimated => "Generic Estimated Plan",
        }
    }

    fn statement_prefix(self) -> &'static str {
        match self {
            Self::Estimated => "EXPLAIN (VERBOSE, SETTINGS)",
            Self::GenericEstimated => "EXPLAIN (GENERIC_PLAN, VERBOSE, SETTINGS)",
        }
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

#[derive(Debug, Clone)]
pub(crate) struct AutoExplainInfo {
    pub(crate) summary: String,
    pub(crate) hint: Option<String>,
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
    server_version_num: i32,
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

    pub(crate) const fn server_version_num(&self) -> i32 {
        self.server_version_num
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

    pub fn fetch_explain_plan(&mut self, query: &str, mode: ExplainMode) -> Result<Vec<String>> {
        validate_explain_query(query, mode, Some(self.server_version_num))?;
        let explain_query = format!("{} {query}", mode.statement_prefix());
        match mode {
            ExplainMode::Estimated => {
                let rows = self.client.query(&explain_query, &[])?;
                rows.into_iter()
                    .map(|row| row.try_get::<_, String>(0).map_err(anyhow::Error::from))
                    .collect()
            }
            ExplainMode::GenericEstimated => {
                use postgres::SimpleQueryMessage;

                // Simple query protocol is used here because it doesn't try to parse parameters
                // ($1, $2) on the client side, allowing us to send the raw query string to
                // Postgres after we have verified it contains only one statement.
                let messages = match self.client.simple_query(&explain_query) {
                    Ok(messages) => messages,
                    Err(error)
                        if error.to_string().contains("could not determine data type")
                            || error
                                .to_string()
                                .contains("could not determine polymorphic type") =>
                    {
                        return Err(anyhow::anyhow!(
                            "Generic estimated plan failed because PostgreSQL could not infer one or more parameter types. Add explicit casts or replace placeholders with real literals outside pgmon."
                        ));
                    }
                    Err(error) => return Err(error.into()),
                };
                let mut plan = Vec::new();
                for msg in messages {
                    if let SimpleQueryMessage::Row(row) = msg {
                        plan.push(row.get(0).unwrap_or_default().to_string());
                    }
                }
                Ok(plan)
            }
        }
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
        let mut client = config.connect(NoTls).with_context(|| {
            format!(
                "Failed to connect to Postgres using {}",
                describe_connection_target(dsn)
            )
        })?;
        let version_row = client.query_one(
            "SELECT current_setting('server_version_num')::int, current_setting('server_version')",
            &[],
        )?;
        let server_version_num = version_row.try_get::<_, i32>(0)?;
        let server_version = version_row.try_get::<_, String>(1)?;
        if server_version_num < MIN_SUPPORTED_SERVER_VERSION_NUM {
            return Err(anyhow::anyhow!(
                "pgmon requires PostgreSQL 14 or newer; connected server is PostgreSQL {server_version}."
            ));
        }
        Ok(Self {
            client,
            capability_cache: CapabilityCache::default(),
            server_version_num,
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

pub(crate) fn supports_generic_explain(server_version_num: i32) -> bool {
    server_version_num >= GENERIC_PLAN_MIN_SERVER_VERSION_NUM
}

pub(crate) fn analyze_explain_query(
    query: &str,
    server_version_num: Option<i32>,
) -> Result<ExplainMode> {
    let parsed = pg_query::parse(query).map_err(|error| {
        anyhow::anyhow!(
            "Explain is only available for a single PostgreSQL statement that pgmon can parse safely: {error}"
        )
    })?;

    if parsed.protobuf.stmts.len() != 1 {
        return Err(anyhow::anyhow!(
            "Explain only supports a single SQL statement. Multiple statements are refused to avoid executing trailing SQL."
        ));
    }

    let nodes = parsed.protobuf.nodes();
    let Some((top_level_node, _, _, _)) = nodes.first() else {
        return Err(anyhow::anyhow!(
            "Explain is unavailable because PostgreSQL returned an empty parse tree for this statement."
        ));
    };

    let explain_mode = match top_level_node {
        NodeRef::SelectStmt(_)
        | NodeRef::InsertStmt(_)
        | NodeRef::UpdateStmt(_)
        | NodeRef::DeleteStmt(_)
        | NodeRef::MergeStmt(_) => {
            if nodes
                .iter()
                .any(|(node, _, _, _)| matches!(node, NodeRef::ParamRef(_)))
            {
                ExplainMode::GenericEstimated
            } else {
                ExplainMode::Estimated
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Explain is only available for single SELECT, INSERT, UPDATE, DELETE, or MERGE statements."
            ));
        }
    };

    if explain_mode == ExplainMode::GenericEstimated
        && let Some(server_version_num) = server_version_num
        && !supports_generic_explain(server_version_num)
    {
        return Err(anyhow::anyhow!(
            "Generic estimated plans require PostgreSQL 16+; this server is PostgreSQL {}.",
            major_server_version(server_version_num)
        ));
    }

    Ok(explain_mode)
}

pub(crate) fn validate_explain_query(
    query: &str,
    mode: ExplainMode,
    server_version_num: Option<i32>,
) -> Result<()> {
    let actual_mode = analyze_explain_query(query, server_version_num)?;
    if actual_mode != mode {
        return Err(anyhow::anyhow!(
            "Explain mode changed after parsing the SQL. Reopen the query info modal and try again."
        ));
    }
    Ok(())
}

fn major_server_version(server_version_num: i32) -> i32 {
    server_version_num / 10_000
}

#[cfg(test)]
mod tests {
    use super::{
        ExplainMode, PgClient, analyze_explain_query, build_statements_query, config_from_dsn,
        format_receiver_summary, supports_generic_explain, validate_explain_query,
    };
    use anyhow::Result;
    use postgres::{Client, NoTls};
    use std::time::{SystemTime, UNIX_EPOCH};

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

    #[test]
    fn test_supports_generic_explain_requires_postgresql_16() {
        assert!(!supports_generic_explain(150_000));
        assert!(supports_generic_explain(160_000));
    }

    #[test]
    fn test_analyze_explain_query_accepts_single_select() -> Result<()> {
        let mode = analyze_explain_query("SELECT 1", Some(160_000))?;

        assert_eq!(mode, ExplainMode::Estimated);
        Ok(())
    }

    #[test]
    fn test_analyze_explain_query_detects_placeholder_params_from_parse_tree() -> Result<()> {
        let mode = analyze_explain_query("SELECT * FROM accounts WHERE id = $1", Some(160_000))?;

        assert_eq!(mode, ExplainMode::GenericEstimated);
        Ok(())
    }

    #[test]
    fn test_analyze_explain_query_ignores_placeholder_text_in_literals_and_comments() -> Result<()>
    {
        let mode = analyze_explain_query(
            "SELECT '$1' AS literal /* $2 */ -- $3\nFROM pg_catalog.pg_class",
            Some(160_000),
        )?;

        assert_eq!(mode, ExplainMode::Estimated);
        Ok(())
    }

    #[test]
    fn test_analyze_explain_query_rejects_utility_statement() {
        let result = analyze_explain_query("SET application_name = 'pgmon'", Some(160_000));

        assert!(result.is_err());
        let message = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("SELECT, INSERT, UPDATE, DELETE, or MERGE"));
    }

    #[test]
    fn test_analyze_explain_query_rejects_nested_explain_statement() {
        let result = analyze_explain_query("EXPLAIN SELECT 1", Some(160_000));

        assert!(result.is_err());
        let message = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("SELECT, INSERT, UPDATE, DELETE, or MERGE"));
    }

    #[test]
    fn test_analyze_explain_query_rejects_create_table_as_statement() {
        let result = analyze_explain_query(
            "CREATE TABLE explain_review AS SELECT 1 AS id",
            Some(160_000),
        );

        assert!(result.is_err());
        let message = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("SELECT, INSERT, UPDATE, DELETE, or MERGE"));
    }

    #[test]
    fn test_validate_explain_query_rejects_multiple_statements() {
        let result = validate_explain_query(
            "SELECT 1; DELETE FROM accounts",
            ExplainMode::Estimated,
            Some(160_000),
        );

        assert!(result.is_err());
        let message = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("single SQL statement"));
    }

    #[test]
    fn test_validate_explain_query_rejects_generic_plan_on_postgresql_15() {
        let result = validate_explain_query(
            "SELECT * FROM accounts WHERE id = $1",
            ExplainMode::GenericEstimated,
            Some(150_000),
        );

        assert!(result.is_err());
        let message = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("PostgreSQL 16+"));
    }

    #[test]
    fn test_validate_explain_query_rejects_mode_mismatch() {
        let result = validate_explain_query(
            "SELECT * FROM accounts WHERE id = $1",
            ExplainMode::Estimated,
            Some(160_000),
        );

        assert!(result.is_err());
        let message = result
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(message.contains("Explain mode changed"));
    }

    #[test]
    fn test_explain_safety_live_insert_does_not_mutate_rows() -> Result<()> {
        let Some(dsn) = live_test_dsn() else {
            return Ok(());
        };
        let table_name = unique_test_table_name("insert");
        let mut control_client = connect_test_client(&dsn)?;
        control_client.batch_execute(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL);"
        ))?;

        let mut explain_client = PgClient::new(&dsn, 5_000)?;
        let query = format!("INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10)");
        let plan = explain_client.fetch_explain_plan(&query, ExplainMode::Estimated)?;

        assert!(!plan.is_empty());
        assert_eq!(table_row_count(&mut control_client, &table_name)?, 0);

        control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
        Ok(())
    }

    #[test]
    fn test_explain_safety_live_update_does_not_mutate_rows() -> Result<()> {
        let Some(dsn) = live_test_dsn() else {
            return Ok(());
        };
        let table_name = unique_test_table_name("update");
        let mut control_client = connect_test_client(&dsn)?;
        control_client.batch_execute(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10);"
        ))?;

        let mut explain_client = PgClient::new(&dsn, 5_000)?;
        let query = format!("UPDATE \"{table_name}\" SET value = 20 WHERE id = 1");
        let plan = explain_client.fetch_explain_plan(&query, ExplainMode::Estimated)?;

        assert!(!plan.is_empty());
        assert_eq!(table_value(&mut control_client, &table_name, 1)?, 10);

        control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
        Ok(())
    }

    #[test]
    fn test_explain_safety_live_delete_does_not_mutate_rows() -> Result<()> {
        let Some(dsn) = live_test_dsn() else {
            return Ok(());
        };
        let table_name = unique_test_table_name("delete");
        let mut control_client = connect_test_client(&dsn)?;
        control_client.batch_execute(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10);"
        ))?;

        let mut explain_client = PgClient::new(&dsn, 5_000)?;
        let query = format!("DELETE FROM \"{table_name}\" WHERE id = 1");
        let plan = explain_client.fetch_explain_plan(&query, ExplainMode::Estimated)?;

        assert!(!plan.is_empty());
        assert_eq!(table_row_count(&mut control_client, &table_name)?, 1);

        control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
        Ok(())
    }

    #[test]
    fn test_explain_safety_live_modifying_cte_select_does_not_mutate_rows() -> Result<()> {
        let Some(dsn) = live_test_dsn() else {
            return Ok(());
        };
        let table_name = unique_test_table_name("cte");
        let mut control_client = connect_test_client(&dsn)?;
        control_client.batch_execute(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10);"
        ))?;

        let mut explain_client = PgClient::new(&dsn, 5_000)?;
        let query = format!(
            "WITH deleted AS (DELETE FROM \"{table_name}\" WHERE id = 1 RETURNING id) SELECT * FROM deleted"
        );
        let plan = explain_client.fetch_explain_plan(&query, ExplainMode::Estimated)?;

        assert!(!plan.is_empty());
        assert_eq!(table_row_count(&mut control_client, &table_name)?, 1);

        control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
        Ok(())
    }

    #[test]
    fn test_explain_safety_live_generic_insert_does_not_mutate_rows() -> Result<()> {
        let Some(dsn) = live_test_dsn() else {
            return Ok(());
        };
        let mut control_client = connect_test_client(&dsn)?;
        let server_version_num = current_server_version_num(&mut control_client)?;
        if !supports_generic_explain(server_version_num) {
            return Ok(());
        }

        let table_name = unique_test_table_name("generic");
        control_client.batch_execute(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL);"
        ))?;

        let mut explain_client = PgClient::new(&dsn, 5_000)?;
        let query =
            format!("INSERT INTO \"{table_name}\" (id, value) VALUES ($1::integer, $2::integer)");
        let plan = explain_client.fetch_explain_plan(&query, ExplainMode::GenericEstimated)?;

        assert!(!plan.is_empty());
        assert_eq!(table_row_count(&mut control_client, &table_name)?, 0);

        control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
        Ok(())
    }

    fn live_test_dsn() -> Option<String> {
        std::env::var("PGMON_TEST_DSN")
            .ok()
            .filter(|dsn| !dsn.trim().is_empty())
    }

    fn unique_test_table_name(suffix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        format!("pgmon_explain_safety_{suffix}_{nanos}")
    }

    fn connect_test_client(dsn: &str) -> Result<Client> {
        let config = config_from_dsn(dsn, 5_000)?;
        Ok(config.connect(NoTls)?)
    }

    fn current_server_version_num(client: &mut Client) -> Result<i32> {
        let row = client.query_one("SELECT current_setting('server_version_num')::int", &[])?;
        Ok(row.try_get::<_, i32>(0)?)
    }

    fn table_row_count(client: &mut Client, table_name: &str) -> Result<i64> {
        let row = client.query_one(&format!("SELECT COUNT(*) FROM \"{table_name}\""), &[])?;
        Ok(row.try_get::<_, i64>(0)?)
    }

    fn table_value(client: &mut Client, table_name: &str, id: i32) -> Result<i32> {
        let row = client.query_one(
            &format!("SELECT value FROM \"{table_name}\" WHERE id = $1"),
            &[&id],
        )?;
        Ok(row.try_get::<_, i32>(0)?)
    }
}
