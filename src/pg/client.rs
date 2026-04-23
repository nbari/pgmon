mod activity;
mod connect;
mod error;
mod explain;
mod metadata;
mod replication;
mod runtime;
mod statements;
mod types;

use sqlx::{PgPool, Postgres, pool::PoolConnection};
use std::sync::{Arc, Mutex, MutexGuard};

pub(crate) use self::error::{DbError, DbResult};
pub(crate) use self::explain::analyze_explain_query;
pub(crate) use self::runtime::{DbExecutor, DbRuntime};
pub(crate) use self::types::{
    ActivityDetail, ActivityProcessSnapshot, ActivitySession, ActivitySnapshot,
    ActivitySummarySnapshot, AutoExplainInfo, CapabilityStatus, ConnectionMeta, ExplainMode,
    ReplicationSender, ReplicationSlot, ReplicationSnapshot,
};

pub(crate) const MIN_SUPPORTED_SERVER_VERSION_NUM: i32 = 140_000;

pub(crate) type PgClientConnection = PoolConnection<Postgres>;

#[derive(Clone)]
pub struct PgClient {
    pool: PgPool,
    state: Arc<Mutex<ClientState>>,
}

#[derive(Debug)]
struct ClientState {
    server_version_num: i32,
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
    fn new(pool: PgPool, server_version_num: i32) -> Self {
        Self {
            pool,
            state: Arc::new(Mutex::new(ClientState {
                server_version_num,
                capability_cache: CapabilityCache::default(),
            })),
        }
    }

    pub(crate) async fn acquire(&self) -> DbResult<PgClientConnection> {
        self.pool.acquire().await.map_err(|error| {
            DbError::transient(format!(
                "Failed to acquire a PostgreSQL connection from the pool: {error}"
            ))
        })
    }

    pub(crate) fn connection_meta(&self) -> ConnectionMeta {
        let state = self.state();
        ConnectionMeta {
            server_version_num: state.server_version_num,
            io_capability: state.capability_cache.io.clone().unwrap_or_default(),
            statements_capability: state
                .capability_cache
                .statements
                .clone()
                .unwrap_or_default(),
            replication_capability: state.capability_cache.replication.clone(),
        }
    }

    pub(crate) fn server_version_num(&self) -> i32 {
        self.state().server_version_num
    }

    fn state(&self) -> MutexGuard<'_, ClientState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
