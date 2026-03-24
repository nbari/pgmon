use super::state::{ActivityDetail, DatabaseView, Tab};
use crate::pg::client::{ActivitySnapshot, PgClient, ReplicationSnapshot};
use anyhow::Result;

#[derive(Debug)]
pub enum RefreshPayload {
    Activity(Box<ActivitySnapshot>, PgClient),
    Replication(Box<ReplicationSnapshot>, PgClient),
    Table(Vec<Vec<String>>, PgClient),
    Explain(Vec<String>),
    TableDefinition(String, String, Vec<Vec<String>>, Vec<String>),
    ActivityDetail(ActivityDetail),
}

pub fn load_refresh_payload(
    client: Option<PgClient>,
    dsn: &str,
    connect_timeout_ms: u64,
    tab: Tab,
    database_view: &DatabaseView,
) -> Result<RefreshPayload> {
    let mut client = match client {
        Some(c) => c,
        None => PgClient::new(dsn, connect_timeout_ms)?,
    };

    match tab {
        Tab::Activity => {
            let snapshot = client.fetch_activity_snapshot()?;
            Ok(RefreshPayload::Activity(Box::new(snapshot), client))
        }
        Tab::Database => match database_view {
            DatabaseView::Summary => {
                let data = client.fetch_database_stats()?;
                Ok(RefreshPayload::Table(data, client))
            }
            DatabaseView::Tables { database } => {
                let mut client = PgClient::for_database(dsn, connect_timeout_ms, database)?;
                let data = client.fetch_database_tree()?;
                Ok(RefreshPayload::Table(data, client))
            }
        },
        Tab::Locks => {
            let data = client.fetch_locks()?;
            Ok(RefreshPayload::Table(data, client))
        }
        Tab::IO => {
            let data = client.fetch_io_stats()?;
            Ok(RefreshPayload::Table(data, client))
        }
        Tab::Statements => {
            let data = client.fetch_statements()?;
            Ok(RefreshPayload::Table(data, client))
        }
        Tab::Replication => {
            let snapshot = client.fetch_replication_snapshot()?;
            Ok(RefreshPayload::Replication(Box::new(snapshot), client))
        }
        Tab::Settings => {
            let data = client.fetch_settings()?;
            Ok(RefreshPayload::Table(data, client))
        }
        Tab::Tools => {
            let tools_data = vec![
                vec![
                    "Terminate Idle in Transaction (> 5m)".to_string(),
                    "This will kill backend processes that have been idle in a transaction for more than 5 minutes.".to_string(),
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state LIKE 'idle in transaction%' AND now() - state_change > interval '5 minutes';".to_string(),
                ],
                vec![
                    "Terminate all Idle Sessions".to_string(),
                    "This will kill all backend processes that are currently in 'idle' state (excluding your current connection).".to_string(),
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle' AND pid <> pg_backend_pid();".to_string(),
                ],
                vec![
                    "Cancel Long-Running Queries (> 5m)".to_string(),
                    "This will cancel active queries running for more than 5 minutes without killing the connection.".to_string(),
                    "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE state = 'active' AND now() - query_start > interval '5 minutes' AND pid <> pg_backend_pid();".to_string(),
                ],
                vec![
                    "Terminate Blocking Sessions".to_string(),
                    "This will kill sessions that are actively blocking other queries from executing.".to_string(),
                    "SELECT pg_terminate_backend(blocking_pid) FROM (SELECT unnest(pg_blocking_pids(pid)) AS blocking_pid FROM pg_stat_activity WHERE wait_event_type = 'Lock') AS blockers GROUP BY blocking_pid;".to_string(),
                ],
                vec![
                    "Reset Statistics".to_string(),
                    "This will reset database statistics (pg_stat_database) for the current database.".to_string(),
                    "SELECT pg_stat_reset();".to_string(),
                ],
            ];
            Ok(RefreshPayload::Table(tools_data, client))
        }
    }
}
