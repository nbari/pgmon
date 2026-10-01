//! Tokio runtime and pool-registry helpers for the database layer.

use super::{
    DbError, DbResult, PgClient,
    connect::{
        PoolKey, PreparedConnectionTarget, classify_connect_error, connect_timeout,
        current_server_version_num, prepare_connection_target, retries_with_tls_fallback,
        tls_fallback_detail,
    },
};
use anyhow::{Context, Result};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
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

        let pool = connect_pool(&target, connect_timeout_ms, database_specific).await?;
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

/// Open the pool for `target`, retrying once with the other TLS choice where
/// libpq would.
///
/// Only the first attempt's failure can trigger the retry, and a timeout never
/// does. When the retry also fails or times out, its outcome is reported with
/// the first error appended.
async fn connect_pool(
    target: &PreparedConnectionTarget,
    connect_timeout_ms: u64,
    database_specific: bool,
) -> DbResult<PgPool> {
    let first_error = match open_pool(
        target.options.clone(),
        connect_timeout_ms,
        database_specific,
    )
    .await?
    {
        Ok(pool) => return Ok(pool),
        Err(error) => error,
    };

    let Some(fallback) = target
        .tls_fallback
        .as_ref()
        .filter(|fallback| retries_with_tls_fallback(fallback.mode, &first_error))
    else {
        return Err(classify_connect_error(&target.target_summary, first_error));
    };

    let detail = tls_fallback_detail(fallback.mode, fallback.options.get_ssl_mode(), &first_error);
    match open_pool(
        fallback.options.clone(),
        connect_timeout_ms,
        database_specific,
    )
    .await
    {
        Ok(Ok(pool)) => Ok(pool),
        Ok(Err(retry_error)) => {
            Err(classify_connect_error(&target.target_summary, retry_error).with_detail(&detail))
        }
        // The first attempt already failed for a concrete reason, so report it
        // rather than a bare timeout, which the TUI treats as a slow refresh.
        Err(_) => Err(DbError::transient(format!(
            "Failed to connect to Postgres using {}: the retry timed out ({detail})",
            target.target_summary
        ))),
    }
}

/// One pool connection attempt, keeping sqlx's error for [`connect_pool`].
async fn open_pool(
    options: PgConnectOptions,
    connect_timeout_ms: u64,
    database_specific: bool,
) -> DbResult<Result<PgPool, sqlx::Error>> {
    tokio::time::timeout(
        connect_timeout(connect_timeout_ms),
        pool_options(connect_timeout_ms, database_specific).connect_with(options),
    )
    .await
    .map_err(|_| DbError::Timeout)
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
    use super::{DbError, DbRuntime, connect_pool, prepare_connection_target};
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread::{self, JoinHandle},
        time::Duration,
    };

    /// The last four bytes of a PostgreSQL `SSLRequest` (request code 80877103).
    const SSL_REQUEST_CODE: [u8; 4] = [0x04, 0xd2, 0x16, 0x2f];
    /// Sent by the test to stop the fake server once the client is done.
    const STOP: [u8; 8] = *b"STOPSTOP";

    /// How the fake server answers one connection.
    #[derive(Clone, Copy)]
    enum Reply {
        /// Accept the TLS request, then answer the handshake with non-TLS bytes.
        TlsThenGarbage,
        /// Accept the TLS request, then close during the handshake.
        TlsThenClose,
        /// Refuse TLS with `N` and close.
        RefuseTls,
        /// Refuse TLS with `N`, then reject the startup with this SQLSTATE.
        RefuseTlsThenError(&'static str),
        /// Reject a plaintext startup with this SQLSTATE.
        Error(&'static str),
        /// Read the request and then answer nothing until the client gives up.
        Stall,
    }

    /// A one-shot fake PostgreSQL server that answers connections with
    /// `replies` in order and records whether each connection opened with a
    /// TLS request (`"tls"`) or a plaintext startup (`"startup"`).
    fn fake_server(replies: Vec<Reply>) -> (u16, JoinHandle<Vec<&'static str>>) {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(error) => panic!("fake server should bind: {error}"),
        };
        let port = match listener.local_addr() {
            Ok(address) => address.port(),
            Err(error) => panic!("fake server should have an address: {error}"),
        };

        let handle = thread::spawn(move || {
            let mut seen = Vec::new();
            let mut replies = replies.into_iter();
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let mut head = [0_u8; 8];
                if stream.read_exact(&mut head).is_err() {
                    continue;
                }
                if head == STOP {
                    break;
                }
                let tls_request = head.ends_with(&SSL_REQUEST_CODE);
                seen.push(if tls_request { "tls" } else { "startup" });
                let Some(reply) = replies.next() else { break };
                if !tls_request {
                    skip_message_body(&mut stream, &head);
                }
                answer(&mut stream, reply);
            }
            seen
        });

        (port, handle)
    }

    fn answer(stream: &mut TcpStream, reply: Reply) {
        let mut client_hello = [0_u8; 4096];
        match reply {
            Reply::TlsThenGarbage => {
                let _ = stream.write_all(b"S");
                let _ = stream.read(&mut client_hello);
                let _ = stream.write_all(b"this is not a TLS record");
                thread::sleep(Duration::from_millis(200));
            }
            Reply::TlsThenClose => {
                let _ = stream.write_all(b"S");
                let _ = stream.read(&mut client_hello);
            }
            Reply::RefuseTls => {
                let _ = stream.write_all(b"N");
                thread::sleep(Duration::from_millis(200));
            }
            Reply::RefuseTlsThenError(code) => {
                let _ = stream.write_all(b"N");
                let mut length = [0_u8; 4];
                if stream.read_exact(&mut length).is_ok() {
                    skip_message_body(stream, &length);
                }
                let _ = stream.write_all(&error_response(code));
            }
            Reply::Error(code) => {
                let _ = stream.write_all(&error_response(code));
            }
            Reply::Stall => thread::sleep(Duration::from_millis(1500)),
        }
    }

    /// Read the rest of a startup message whose first bytes are `head`.
    fn skip_message_body(stream: &mut TcpStream, head: &[u8]) {
        let mut length = [0_u8; 4];
        for (slot, byte) in length.iter_mut().zip(head) {
            *slot = *byte;
        }
        let total = usize::try_from(u32::from_be_bytes(length)).unwrap_or(0);
        let remaining = total.saturating_sub(head.len());
        let mut body = vec![0_u8; remaining];
        let _ = stream.read_exact(&mut body);
    }

    /// A FATAL `ErrorResponse` with SQLSTATE `code`.
    fn error_response(code: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for (field, value) in [
            (b'S', "FATAL"),
            (b'V', "FATAL"),
            (b'C', code),
            (b'M', "rejected by the fake server"),
        ] {
            body.push(field);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);

        let length = match u32::try_from(body.len() + 4) {
            Ok(length) => length,
            Err(error) => panic!("error response should fit in a message: {error}"),
        };
        let mut message = vec![b'E'];
        message.extend_from_slice(&length.to_be_bytes());
        message.extend(body);
        message
    }

    /// Connect to a fake server with the DSN query `params`, then return
    /// pgmon's error and what each connection the server saw opened with.
    fn connect_to_fake_server(params: &str, replies: Vec<Reply>) -> (DbError, Vec<&'static str>) {
        connect_to_fake_server_within(params, replies, 3000)
    }

    /// [`connect_to_fake_server`] with a connect timeout per attempt.
    fn connect_to_fake_server_within(
        params: &str,
        replies: Vec<Reply>,
        connect_timeout_ms: u64,
    ) -> (DbError, Vec<&'static str>) {
        let (port, server) = fake_server(replies);
        let dsn = format!("postgresql://pgmon@127.0.0.1:{port}/postgres?{params}");
        let target = match prepare_connection_target(&dsn, None) {
            Ok(target) => target,
            Err(error) => panic!("test DSN should parse: {error}"),
        };
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => panic!("test runtime should build: {error}"),
        };

        // Database-specific pools keep no idle connection, so sqlx's pool
        // maintenance does not open extra connections behind the test's back.
        let result = runtime.block_on(connect_pool(&target, connect_timeout_ms, true));
        if let Ok(mut stop) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = stop.write_all(&STOP);
        }
        let Ok(seen) = server.join() else {
            panic!("fake server thread should not panic");
        };
        match result {
            Ok(_) => panic!("the fake server never accepts a session"),
            Err(error) => (error, seen),
        }
    }

    #[test]
    fn test_connect_pool_prefer_retries_without_tls_after_tls_failure() {
        let (error, seen) = connect_to_fake_server(
            "sslmode=prefer",
            vec![Reply::TlsThenGarbage, Reply::Error("3D000")],
        );

        assert_eq!(seen, vec!["tls", "startup"]);
        let message = error.to_string();
        assert!(message.contains("rejected by the fake server"), "{message}");
        assert!(
            message.contains("retried with sslmode=disable after sslmode=prefer failed"),
            "{message}"
        );
    }

    #[test]
    fn test_connect_pool_prefer_does_not_retry_a_dropped_connection() {
        // rustls reports a drop during the handshake exactly like one after it,
        // and libpq does not retry the latter.
        let (error, seen) = connect_to_fake_server("sslmode=prefer", vec![Reply::TlsThenClose]);

        assert_eq!(seen, vec!["tls"]);
        assert!(!error.to_string().contains("retried"), "{error}");
    }

    #[test]
    fn test_connect_pool_prefer_retries_without_tls_after_authorization_failure() {
        let (_, seen) = connect_to_fake_server(
            "sslmode=prefer",
            vec![Reply::RefuseTlsThenError("28000"), Reply::Error("3D000")],
        );

        assert_eq!(seen, vec!["tls", "startup"]);
    }

    #[test]
    fn test_connect_pool_prefer_does_not_retry_unrelated_server_error() {
        let (error, seen) =
            connect_to_fake_server("sslmode=prefer", vec![Reply::RefuseTlsThenError("3D000")]);

        assert_eq!(seen, vec!["tls"]);
        assert!(!error.to_string().contains("retried"));
    }

    #[test]
    fn test_connect_pool_allow_retries_with_tls_after_authorization_failure() {
        let (error, seen) = connect_to_fake_server(
            "sslmode=allow",
            vec![Reply::Error("28000"), Reply::RefuseTls],
        );

        assert_eq!(seen, vec!["startup", "tls"]);
        let message = error.to_string();
        assert!(message.contains("server does not support TLS"), "{message}");
        assert!(message.contains("after sslmode=allow failed"), "{message}");
    }

    #[test]
    fn test_connect_pool_strict_modes_do_not_retry() {
        let (_, seen) = connect_to_fake_server("sslmode=require", vec![Reply::RefuseTls]);
        assert_eq!(seen, vec!["tls"]);

        let (_, seen) = connect_to_fake_server("sslmode=disable", vec![Reply::Error("28000")]);
        assert_eq!(seen, vec!["startup"]);
    }

    #[test]
    fn test_db_runtime_initializes() {
        let runtime = match DbRuntime::new() {
            Ok(runtime) => runtime,
            Err(error) => panic!("runtime should initialize: {error:#}"),
        };

        assert_eq!(runtime.executor().pool_count(), 0);
    }

    #[test]
    fn test_connect_pool_prefer_with_root_cert_falls_back_when_server_has_no_tls() {
        let params = concat!(
            "sslmode=prefer&sslrootcert=",
            env!("CARGO_MANIFEST_DIR"),
            "/Cargo.toml"
        );
        let (error, seen) =
            connect_to_fake_server(params, vec![Reply::RefuseTls, Reply::Error("3D000")]);

        assert_eq!(seen, vec!["tls", "startup"]);
        assert!(
            error
                .to_string()
                .contains("retried with sslmode=disable after sslmode=prefer failed"),
            "{error}"
        );
    }

    #[test]
    fn test_connect_pool_reports_first_error_when_retry_times_out() {
        let (error, seen) = connect_to_fake_server_within(
            "sslmode=allow",
            vec![Reply::Error("28000"), Reply::Stall],
            500,
        );

        // sqlx's acquire timeout usually fires first ("pool timed out"); pgmon's
        // own timeout is the backstop ("the retry timed out"). Either way the
        // failure is transient and names the first attempt.
        assert_eq!(seen, vec!["startup", "tls"]);
        assert!(matches!(error, DbError::Transient(_)), "{error:?}");
        let message = error.to_string();
        assert!(message.contains("timed out"), "{message}");
        assert!(
            message.contains("retried with sslmode=require after sslmode=allow failed")
                && message.contains("rejected by the fake server"),
            "{message}"
        );
    }
}
