use super::state::{ActivityDetail, DatabaseView, ExplainPlanState, Tab};
use crate::pg::client::{
    ActivitySnapshot, AutoExplainInfo, ConnectionMeta, DbError, DbExecutor, DbResult, ExplainMode,
    PgClient, PgClientConnection, ReplicationSnapshot,
};
use std::{future::Future, pin::Pin, time::Duration};
use tokio::time::timeout;

type BoxDbFuture<'a, T> = Pin<Box<dyn Future<Output = DbResult<T>> + Send + 'a>>;

#[derive(Debug)]
pub(crate) struct ActivityInspectionPayload {
    pub(crate) detail: ActivityDetail,
    pub(crate) auto_explain: AutoExplainInfo,
}

#[derive(Debug)]
pub enum RefreshPayload {
    Activity(Box<ActivitySnapshot>, ConnectionMeta),
    Replication(Box<ReplicationSnapshot>, ConnectionMeta),
    Table(Vec<Vec<String>>, ConnectionMeta),
    Explain(ExplainPlanState),
    TableDefinition(String, String, Vec<Vec<String>>, Vec<String>),
    ActivityDetail(Box<ActivityInspectionPayload>),
}

pub async fn load_refresh_payload(
    db: DbExecutor,
    dsn: &str,
    connect_timeout_ms: u64,
    refresh_ms: u64,
    tab: Tab,
    database_view: &DatabaseView,
) -> DbResult<RefreshPayload> {
    let client = db.clone().client(dsn, connect_timeout_ms).await?;

    match tab {
        Tab::Activity => {
            let query_client = client.clone();
            let snapshot = run_refresh_query(&client, refresh_ms, move |connection| {
                Box::pin(async move { query_client.fetch_activity_snapshot(connection).await })
            })
            .await?;
            Ok(RefreshPayload::Activity(
                Box::new(snapshot),
                client.connection_meta(),
            ))
        }
        Tab::Database => match database_view {
            DatabaseView::Summary => {
                let query_client = client.clone();
                let data = run_refresh_query(&client, refresh_ms, move |connection| {
                    Box::pin(async move { query_client.fetch_database_stats(connection).await })
                })
                .await?;
                Ok(RefreshPayload::Table(data, client.connection_meta()))
            }
            DatabaseView::Tables { database } => {
                let database_client = db
                    .clone()
                    .client_for_database(dsn, connect_timeout_ms, database)
                    .await?;
                let query_client = database_client.clone();
                let data = run_refresh_query(&database_client, refresh_ms, move |connection| {
                    Box::pin(async move { query_client.fetch_database_tree(connection).await })
                })
                .await?;
                Ok(RefreshPayload::Table(
                    data,
                    database_client.connection_meta(),
                ))
            }
        },
        Tab::Locks => {
            let query_client = client.clone();
            let data = run_refresh_query(&client, refresh_ms, move |connection| {
                Box::pin(async move { query_client.fetch_locks(connection).await })
            })
            .await?;
            Ok(RefreshPayload::Table(data, client.connection_meta()))
        }
        Tab::IO => {
            let query_client = client.clone();
            let data = run_refresh_query(&client, refresh_ms, move |connection| {
                Box::pin(async move { query_client.fetch_io_stats(connection).await })
            })
            .await?;
            Ok(RefreshPayload::Table(data, client.connection_meta()))
        }
        Tab::Statements => {
            let query_client = client.clone();
            let data = run_refresh_query(&client, refresh_ms, move |connection| {
                Box::pin(async move { query_client.fetch_statements(connection).await })
            })
            .await?;
            Ok(RefreshPayload::Table(data, client.connection_meta()))
        }
        Tab::Replication => {
            let query_client = client.clone();
            let snapshot = run_refresh_query(&client, refresh_ms, move |connection| {
                Box::pin(async move { query_client.fetch_replication_snapshot(connection).await })
            })
            .await?;
            Ok(RefreshPayload::Replication(
                Box::new(snapshot),
                client.connection_meta(),
            ))
        }
        Tab::Settings => {
            let query_client = client.clone();
            let data = run_refresh_query(&client, refresh_ms, move |connection| {
                Box::pin(async move { query_client.fetch_settings(connection).await })
            })
            .await?;
            Ok(RefreshPayload::Table(data, client.connection_meta()))
        }
        Tab::Tools => Ok(RefreshPayload::Table(
            tools_table_data(),
            client.connection_meta(),
        )),
    }
}

pub(crate) async fn load_explain_payload(
    db: DbExecutor,
    dsn: String,
    timeout_ms: u64,
    database: String,
    query: String,
    explain_mode: ExplainMode,
) -> DbResult<RefreshPayload> {
    let client = db.client_for_database(&dsn, timeout_ms, &database).await?;
    let mut connection = TimedConnection::new(client.acquire().await?);
    let query_client = client.clone();
    let result = timeout(Duration::from_millis(timeout_ms.max(1)), async {
        let plan = query_client
            .fetch_explain_plan(connection.connection(), &query, explain_mode)
            .await?;
        let auto_explain = query_client
            .fetch_auto_explain_info(connection.connection())
            .await?;
        Ok::<_, DbError>((plan, auto_explain))
    })
    .await;

    let (plan, auto_explain) = match result {
        Ok(inner) => {
            connection.disarm();
            inner.map_err(|error| {
                let prefix = match explain_mode {
                    ExplainMode::Estimated => {
                        "Explain failed. pgmon only collects a planner estimate and does not execute the query."
                    }
                    ExplainMode::GenericEstimated => {
                        "Generic estimated plan failed. PostgreSQL may need explicit casts or real literals to infer placeholder types."
                    }
                };
                DbError::fatal(format!("{prefix} {error}"))
            })?
        }
        Err(_) => return Err(DbError::Timeout),
    };

    Ok(RefreshPayload::Explain(ExplainPlanState {
        plan,
        explain_mode,
        auto_explain_summary: auto_explain.summary,
        auto_explain_hint: auto_explain.hint,
    }))
}

pub(crate) async fn load_activity_detail_payload(
    db: DbExecutor,
    dsn: String,
    timeout_ms: u64,
    database: String,
    pid: i32,
) -> DbResult<RefreshPayload> {
    let client = db.client_for_database(&dsn, timeout_ms, &database).await?;
    let mut connection = TimedConnection::new(client.acquire().await?);
    let query_client = client.clone();
    let result = timeout(Duration::from_millis(timeout_ms.max(1)), async {
        let detail = query_client
            .fetch_activity_detail(connection.connection(), pid)
            .await?;
        let auto_explain = query_client
            .fetch_auto_explain_info(connection.connection())
            .await?;
        Ok::<_, DbError>((detail, auto_explain))
    })
    .await;

    let (detail, auto_explain) = match result {
        Ok(inner) => {
            connection.disarm();
            inner?
        }
        Err(_) => return Err(DbError::Timeout),
    };

    Ok(RefreshPayload::ActivityDetail(Box::new(
        ActivityInspectionPayload {
            detail: ActivityDetail {
                pid: detail.pid,
                usename: detail.usename,
                application_name: detail.application_name,
                client_addr: detail.client_addr,
                client_port: detail.client_port,
                backend_start: detail.backend_start,
                state: detail.state,
                wait_event_type: detail.wait_event_type,
                wait_event: detail.wait_event,
                xact_start: detail.xact_start,
                state_change: detail.state_change,
                query: detail.query,
                blocking_pids: detail.blocking_pids,
                blockers: detail.blockers,
                locks: detail.locks,
            },
            auto_explain,
        },
    )))
}

pub(crate) async fn execute_admin_action_payload(
    db: DbExecutor,
    dsn: String,
    timeout_ms: u64,
    query: String,
) -> DbResult<RefreshPayload> {
    let client = db.client(&dsn, timeout_ms).await?;
    let mut connection = TimedConnection::new(client.acquire().await?);
    let query_client = client.clone();
    let result = timeout(Duration::from_millis(timeout_ms.max(1)), async {
        query_client
            .execute_admin_action(connection.connection(), &query)
            .await
    })
    .await;

    match result {
        Ok(inner) => {
            connection.disarm();
            inner?;
        }
        Err(_) => return Err(DbError::Timeout),
    }

    Ok(RefreshPayload::Table(Vec::new(), client.connection_meta()))
}

pub(crate) async fn load_table_definition_payload(
    db: DbExecutor,
    dsn: String,
    timeout_ms: u64,
    database: String,
    schema: String,
    table: String,
) -> DbResult<RefreshPayload> {
    let client = db.client_for_database(&dsn, timeout_ms, &database).await?;
    let mut connection = TimedConnection::new(client.acquire().await?);
    let query_client = client.clone();
    let result = timeout(Duration::from_millis(timeout_ms.max(1)), async {
        query_client
            .fetch_table_definition(connection.connection(), &schema, &table)
            .await
    })
    .await;

    let (columns, indexes) = match result {
        Ok(inner) => {
            connection.disarm();
            inner?
        }
        Err(_) => return Err(DbError::Timeout),
    };

    Ok(RefreshPayload::TableDefinition(
        schema, table, columns, indexes,
    ))
}

async fn run_refresh_query<T, F>(client: &PgClient, refresh_ms: u64, operation: F) -> DbResult<T>
where
    F: for<'a> FnOnce(&'a mut PgClientConnection) -> BoxDbFuture<'a, T>,
{
    let result = timeout(refresh_budget(refresh_ms), async {
        // Keep connection checkout inside the refresh budget so a saturated pool
        // degrades like a timed refresh instead of stretching the UI cycle.
        let mut connection = TimedConnection::new(client.acquire().await?);
        let inner = operation(connection.connection()).await;
        connection.disarm();
        inner
    })
    .await;
    match result {
        Ok(inner) => inner,
        Err(_) => Err(DbError::Timeout),
    }
}

fn refresh_budget(refresh_ms: u64) -> Duration {
    Duration::from_millis(refresh_ms.saturating_sub(50).max(100))
}

struct TimedConnection {
    connection: PgClientConnection,
    close_on_drop: bool,
}

impl TimedConnection {
    fn new(connection: PgClientConnection) -> Self {
        Self {
            connection,
            close_on_drop: true,
        }
    }

    fn connection(&mut self) -> &mut PgClientConnection {
        &mut self.connection
    }

    fn disarm(&mut self) {
        self.close_on_drop = false;
    }
}

impl Drop for TimedConnection {
    fn drop(&mut self) {
        if self.close_on_drop {
            self.connection.close_on_drop();
        }
    }
}

fn tools_table_data() -> Vec<Vec<String>> {
    vec![
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
    ]
}
