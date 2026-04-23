//! Tokio runtime and pool-registry helpers for the database layer.

use super::{
    DbError, DbResult, PgClient,
    connect::{
        PoolKey, classify_connect_error, connect_timeout, current_server_version_num,
        prepare_connection_target,
    },
};
use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};
use tokio::runtime::{Builder, Handle, Runtime};

const PRIMARY_POOL_MAX_CONNECTIONS: u32 = 3;
const DATABASE_POOL_MAX_CONNECTIONS: u32 = 1;
const IDLE_POOL_TTL: Duration = Duration::from_mins(2);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_mins(1);
const POOL_MAX_LIFETIME: Duration = Duration::from_mins(30);

#[derive(Clone)]
pub(crate) struct DbExecutor {
    handle: Handle,
    registry: PoolRegistry,
}

pub(crate) struct DbRuntime {
    _runtime: Runtime,
    executor: DbExecutor,
}

#[derive(Clone, Default)]
struct PoolRegistry {
    entries: Arc<Mutex<HashMap<PoolKey, PoolEntry>>>,
}

#[derive(Clone)]
struct PoolEntry {
    client: PgClient,
    database_specific: bool,
    last_used: Instant,
}

impl DbRuntime {
    /// Build the dedicated async runtime used for all database work.
    pub(crate) fn new() -> Result<Self> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("pgmon-db")
            .enable_all()
            .build()
            .context("Failed to initialize pgmon's database runtime")?;
        let handle = runtime.handle().clone();

        Ok(Self {
            _runtime: runtime,
            executor: DbExecutor {
                handle,
                registry: PoolRegistry::default(),
            },
        })
    }

    pub(crate) fn executor(&self) -> DbExecutor {
        self.executor.clone()
    }

    #[cfg(test)]
    #[allow(clippy::used_underscore_binding)]
    pub(crate) fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self._runtime.block_on(future)
    }
}

impl DbExecutor {
    pub(crate) fn spawn_request<T>(
        &self,
        tx: mpsc::Sender<DbResult<T>>,
        request: impl FnOnce() -> DbResult<T> + Send + 'static,
    ) where
        T: Send + 'static,
    {
        let registry = self.registry.clone();
        std::thread::spawn(move || {
            let result = request();
            registry.evict_idle_database_pools();
            let _ = tx.send(result);
        });
    }

    pub(crate) fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self.handle.block_on(future)
    }

    pub(crate) async fn client(self, dsn: &str, connect_timeout_ms: u64) -> DbResult<PgClient> {
        self.registry
            .get_or_create(dsn, connect_timeout_ms, None)
            .await
    }

    pub(crate) async fn client_for_database(
        self,
        dsn: &str,
        connect_timeout_ms: u64,
        database: &str,
    ) -> DbResult<PgClient> {
        if database.is_empty() {
            return self.client(dsn, connect_timeout_ms).await;
        }

        self.registry
            .get_or_create(dsn, connect_timeout_ms, Some(database))
            .await
    }

    #[cfg(test)]
    pub(crate) fn pool_count(&self) -> usize {
        self.registry.pool_count()
    }
}

impl PoolRegistry {
    async fn get_or_create(
        &self,
        dsn: &str,
        connect_timeout_ms: u64,
        database_override: Option<&str>,
    ) -> DbResult<PgClient> {
        self.evict_idle_database_pools();

        let database_specific = database_override.is_some_and(|database| !database.is_empty());
        let target = prepare_connection_target(dsn, database_override)?;

        if let Some(client) = self.lookup(&target.key) {
            return Ok(client);
        }

        let pool = tokio::time::timeout(
            connect_timeout(connect_timeout_ms),
            pool_options(connect_timeout_ms, database_specific)
                .connect_with(target.options.clone()),
        )
        .await
        .map_err(|_| DbError::Timeout)?
        .map_err(|error| classify_connect_error(&target.target_summary, error))?;
        let server_version_num = current_server_version_num(&pool, connect_timeout_ms).await?;
        let client = PgClient::from_pool(pool, server_version_num);

        Ok(self.insert_or_get(target.key, &client, database_specific))
    }

    fn lookup(&self, key: &PoolKey) -> Option<PgClient> {
        let mut entries = self.entries();
        let entry = entries.get_mut(key)?;
        entry.last_used = Instant::now();
        Some(entry.client.clone())
    }

    fn insert_or_get(&self, key: PoolKey, client: &PgClient, database_specific: bool) -> PgClient {
        let mut entries = self.entries();
        let entry = entries.entry(key).or_insert_with(|| PoolEntry {
            client: client.clone(),
            database_specific,
            last_used: Instant::now(),
        });
        entry.last_used = Instant::now();
        entry.client.clone()
    }

    fn evict_idle_database_pools(&self) {
        let now = Instant::now();
        let mut entries = self.entries();
        entries.retain(|_, entry| {
            !entry.database_specific
                || now.saturating_duration_since(entry.last_used) <= IDLE_POOL_TTL
        });
    }

    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<PoolKey, PoolEntry>> {
        match self.entries.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[cfg(test)]
    fn pool_count(&self) -> usize {
        self.entries().len()
    }
}

fn pool_options(connect_timeout_ms: u64, database_specific: bool) -> PgPoolOptions {
    let max_connections = if database_specific {
        DATABASE_POOL_MAX_CONNECTIONS
    } else {
        PRIMARY_POOL_MAX_CONNECTIONS
    };
    let min_connections = u32::from(!database_specific);

    PgPoolOptions::new()
        .min_connections(min_connections)
        .max_connections(max_connections)
        .acquire_timeout(connect_timeout(connect_timeout_ms))
        .idle_timeout(POOL_IDLE_TIMEOUT)
        .max_lifetime(POOL_MAX_LIFETIME)
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::DbRuntime;

    #[test]
    fn test_db_runtime_initializes() {
        let runtime = match DbRuntime::new() {
            Ok(runtime) => runtime,
            Err(error) => panic!("runtime should initialize: {error:#}"),
        };

        assert_eq!(runtime.executor().pool_count(), 0);
    }
}
