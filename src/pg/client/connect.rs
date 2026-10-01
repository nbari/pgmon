//! Connection helpers for `PgClient`.

use super::{DbError, DbResult, MIN_SUPPORTED_SERVER_VERSION_NUM, PgClient};
use crate::pg::conninfo::describe_connection_target;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs::File,
    hash::{Hash, Hasher},
    path::Path,
    str::FromStr,
    time::Duration,
};
use tokio::time::timeout;
use url::{Url, form_urlencoded};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct PoolKey {
    pub(super) host: String,
    pub(super) hostaddr: Option<String>,
    pub(super) port: u16,
    pub(super) database: String,
    pub(super) user: String,
    pub(super) socket: Option<String>,
    pub(super) ssl_mode: String,
    /// The first attempt's `sslmode` and the TLS retry's, if any. They depend on
    /// whether the root certificate file exists right now, so a file that
    /// appears or disappears while pgmon runs leads to a new pool rather than
    /// one connected under the old decision.
    pub(super) ssl_attempts: String,
    pub(super) ssl_root_cert: Option<String>,
    pub(super) ssl_cert: Option<String>,
    pub(super) ssl_key: Option<String>,
    pub(super) options: Option<String>,
    pub(super) application_name: Option<String>,
    pub(super) target_session_attrs: Option<String>,
    pub(super) password_fingerprint: Option<u64>,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedConnectionTarget {
    pub(super) key: PoolKey,
    pub(super) options: PgConnectOptions,
    /// libpq's one retry with the other TLS choice, for `prefer` and `allow`
    /// only; see [`tls_fallback_mode`].
    pub(super) tls_fallback: Option<TlsFallback>,
    pub(super) target_summary: String,
}

/// The retry libpq makes after a failed `prefer` or `allow` attempt.
#[derive(Debug, Clone)]
pub(super) struct TlsFallback {
    /// The mode whose retry rule applies: `prefer` or `allow`.
    pub(super) mode: PgSslMode,
    /// Connection options for the retry.
    pub(super) options: PgConnectOptions,
}

/// Connection parameter names sqlx accepts for the TLS root certificate.
const ROOT_CERT_PARAMS: [&str; 3] = ["sslrootcert", "ssl-root-cert", "ssl-ca"];

/// The `sslmode` a DSN asks for and the one pgmon connects with, for
/// `check-config`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SslModeResolution {
    /// Mode from the DSN, or from `PGSSLMODE` when the DSN sets none, in libpq
    /// spelling.
    pub(crate) requested: &'static str,
    /// Mode pgmon applies after [`effective_ssl_mode`], in libpq spelling.
    pub(crate) effective: &'static str,
    /// Ways the effective settings may not protect the session, most
    /// important first.
    pub(crate) warnings: Vec<String>,
}

/// The root certificate sqlx will load, classified the way libpq decides
/// whether `sslmode=require` verifies the server.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RootCert {
    /// No root certificate, or an empty value.
    Unset,
    /// libpq's special value `system`, which sqlx would read as a file name.
    System,
    /// Inline PEM or a path to an existing file.
    Available,
    /// A path that does not exist.
    Missing(String),
    /// A path that exists but cannot be read as a file, such as one without
    /// read permission or a directory.
    Unreadable(String),
}

/// Where the root certificate value sqlx will use comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RootCertSetting {
    /// A root parameter in the DSN, which sqlx always reads as a file path.
    Dsn(String),
    /// `PGSSLROOTCERT`, which sqlx also accepts as inline PEM.
    Env(String),
}

impl PgClient {
    pub(super) fn from_pool(pool: PgPool, server_version_num: i32) -> Self {
        Self::new(pool, server_version_num)
    }

    pub(crate) async fn execute_admin_action(
        &self,
        connection: &mut super::PgClientConnection,
        query: &str,
    ) -> DbResult<()> {
        sqlx::raw_sql(sqlx::AssertSqlSafe(query))
            .execute(connection.as_mut())
            .await
            .map(|_| ())
            .map_err(classify_query_error)
    }
}

pub(super) async fn current_server_version_num(
    pool: &PgPool,
    connect_timeout_ms: u64,
) -> DbResult<i32> {
    let budget = connect_timeout(connect_timeout_ms);
    let mut connection = timeout(budget, pool.acquire())
        .await
        .map_err(|_| DbError::Timeout)?
        .map_err(|error| {
            DbError::transient(format!(
                "Failed to acquire a PostgreSQL connection from the pool: {error}"
            ))
        })?;

    let row = timeout(
        budget,
        sqlx::query(
            "SELECT current_setting('server_version_num')::int, current_setting('server_version')",
        )
        .fetch_one(connection.as_mut()),
    )
    .await
    .map_err(|_| DbError::Timeout)?
    .map_err(classify_connect_error_from_sqlx)?;

    let server_version_num =
        sqlx::Row::try_get::<i32, _>(&row, 0).map_err(classify_connect_error_from_sqlx)?;
    let server_version =
        sqlx::Row::try_get::<String, _>(&row, 1).map_err(classify_connect_error_from_sqlx)?;

    if server_version_num < MIN_SUPPORTED_SERVER_VERSION_NUM {
        return Err(DbError::fatal(format!(
            "pgmon requires PostgreSQL 14 or newer; connected server is PostgreSQL {server_version}."
        )));
    }

    Ok(server_version_num)
}

pub(super) fn connect_timeout(connect_timeout_ms: u64) -> Duration {
    Duration::from_millis(connect_timeout_ms.max(1))
}

pub(super) fn prepare_connection_target(
    dsn: &str,
    database_override: Option<&str>,
) -> DbResult<PreparedConnectionTarget> {
    let (mut options, params, root_cert) = parse_connect_options(dsn)?;
    if let Some(database) = database_override {
        options = options.database(database);
    }
    let ssl_mode = effective_ssl_mode(options.get_ssl_mode(), &root_cert);
    let tls_fallback = tls_fallback_mode(ssl_mode, &root_cert).map(|mode| TlsFallback {
        mode: ssl_mode,
        options: options.clone().ssl_mode(mode),
    });
    options = options.ssl_mode(first_attempt_ssl_mode(ssl_mode, &root_cert));

    // Key on the libpq-level mode as well as the attempts: a `prefer` pool may
    // have fallen back to plaintext and must never be reused for `verify-ca`.
    let key = build_pool_key(
        &options,
        &params,
        database_override,
        ssl_mode,
        tls_fallback
            .as_ref()
            .map(|fallback| fallback.options.get_ssl_mode()),
    );

    Ok(PreparedConnectionTarget {
        key,
        options,
        tls_fallback,
        target_summary: describe_connection_target(dsn),
    })
}

/// Resolve the `sslmode` pgmon will use for `dsn` without connecting.
///
/// # Errors
///
/// Returns an error when `dsn` cannot be parsed into connection settings or
/// uses a root certificate setting pgmon does not support.
pub(crate) fn resolve_ssl_mode(dsn: &str) -> DbResult<SslModeResolution> {
    let (options, _, root_cert) = parse_connect_options(dsn)?;
    let requested = options.get_ssl_mode();
    let effective = effective_ssl_mode(requested, &root_cert);

    Ok(SslModeResolution {
        requested: ssl_mode_name(requested),
        effective: ssl_mode_name(effective),
        warnings: ssl_mode_warning(effective)
            .into_iter()
            .chain(root_cert_warning(effective, &root_cert))
            .collect(),
    })
}

/// Parse `dsn`, URL or key/value form, into sqlx options, its raw parameters,
/// and the root certificate sqlx will load.
fn parse_connect_options(
    dsn: &str,
) -> DbResult<(PgConnectOptions, BTreeMap<String, String>, RootCert)> {
    let url = if looks_like_postgres_url(dsn) {
        dsn.to_string()
    } else {
        conninfo_to_url(dsn)?
    };

    let parsed = Url::parse(&url).map_err(|error| {
        DbError::fatal(format!(
            "Failed to parse PostgreSQL connection URL: {error}"
        ))
    })?;
    let params = url_query_params(&parsed);
    let options = PgConnectOptions::from_str(&url).map_err(|error| {
        DbError::fatal(format!(
            "Failed to parse Postgres connection settings for {}: {error}",
            describe_connection_target(dsn)
        ))
    })?;

    let root_cert = classify_root_cert(
        root_cert_setting(&parsed, std::env::var("PGSSLROOTCERT").ok()).as_ref(),
    );
    if root_cert == RootCert::System {
        return Err(DbError::fatal(format!(
            "Unsupported Postgres connection settings for {}: sslrootcert=system is not \
             supported, because sqlx would read it as a file named \"system\". Remove \
             sslrootcert and use sslmode=verify-full, which verifies against the {}.",
            describe_connection_target(dsn),
            crate::tls::root_store()
        )));
    }

    Ok((options, params, root_cert))
}

/// Apply libpq's rule that `sslmode=require` verifies the server certificate
/// when a root certificate is available.
///
/// libpq treats `require` as `verify-ca` whenever the root CA file exists.
/// sqlx skips certificate verification for `require` and ignores
/// `sslrootcert`, so without this a DSN copied from `psql` would silently drop
/// its CA check. Every other mode is returned unchanged.
const fn effective_ssl_mode(requested: PgSslMode, root_cert: &RootCert) -> PgSslMode {
    match (requested, root_cert) {
        // An unreadable file still exists, so libpq still tries to verify and
        // then fails; sqlx does the same once it is asked to verify.
        (PgSslMode::Require, RootCert::Available | RootCert::Unreadable(_)) => PgSslMode::VerifyCa,
        (other, _) => other,
    }
}

/// The `sslmode` of the first connection attempt for the libpq-level `mode`.
///
/// libpq verifies the certificate chain whenever the root CA file exists and
/// TLS is used, including under `prefer`, where a failed check then falls back
/// to plaintext through [`tls_fallback_mode`]. sqlx's `prefer` never verifies,
/// so pgmon starts `prefer` as `verify-ca` in that case.
///
/// An existing but unreadable root file makes libpq's TLS setup fail before the
/// handshake, after which `prefer` continues without TLS; pgmon goes straight
/// to that outcome, since sqlx would fail reading the file with an I/O error
/// it cannot tell apart from a network failure.
const fn first_attempt_ssl_mode(mode: PgSslMode, root_cert: &RootCert) -> PgSslMode {
    match (mode, root_cert) {
        (PgSslMode::Prefer, RootCert::Available) => PgSslMode::VerifyCa,
        (PgSslMode::Prefer, RootCert::Unreadable(_)) => PgSslMode::Disable,
        (other, _) => other,
    }
}

/// The `sslmode` libpq retries with when a connection in `mode` fails.
///
/// libpq tries `prefer` with TLS and, if that attempt fails, once more
/// without it; it tries `allow` without TLS and, if the server rejects that,
/// once more with it. sqlx does neither, so pgmon performs the retry itself.
/// The TLS retry for `allow` verifies the chain when a root certificate is
/// available, as `require` does.
const fn tls_fallback_mode(mode: PgSslMode, root_cert: &RootCert) -> Option<PgSslMode> {
    match (mode, root_cert) {
        (PgSslMode::Allow, _) => Some(effective_ssl_mode(PgSslMode::Require, root_cert)),
        // With an unreadable root file the first attempt is already without
        // TLS; see `first_attempt_ssl_mode`.
        (PgSslMode::Prefer, RootCert::Unreadable(_))
        | (
            PgSslMode::Disable | PgSslMode::Require | PgSslMode::VerifyCa | PgSslMode::VerifyFull,
            _,
        ) => None,
        (PgSslMode::Prefer, _) => Some(PgSslMode::Disable),
    }
}

/// Whether a connection attempt in `mode` that failed with `error` gets
/// libpq's retry with the other TLS choice.
///
/// `prefer` retries after a TLS failure, including a server without TLS or a
/// failed certificate check when its first attempt verifies the chain, or an
/// authorization error, which is how `pg_hba.conf` rules that treat TLS and
/// plaintext differently show up.
/// `allow` retries only after an authorization error, since its first attempt
/// does not use TLS. Network errors before any TLS exchange, such as a refused
/// connection, and unrelated server errors, such as an unknown database, are
/// not retried: the other TLS choice cannot change their outcome.
pub(super) fn retries_with_tls_fallback(mode: PgSslMode, error: &sqlx::Error) -> bool {
    match mode {
        PgSslMode::Prefer => is_tls_failure(error) || is_authorization_failure(error),
        PgSslMode::Allow => is_authorization_failure(error),
        PgSslMode::Disable | PgSslMode::Require | PgSslMode::VerifyCa | PgSslMode::VerifyFull => {
            false
        }
    }
}

/// Whether `error` came from setting up TLS rather than from the network or
/// the server.
///
/// sqlx reports TLS setup problems, including a server without TLS, as
/// `Error::Tls`, and rustls reports a failed handshake, such as a TLS alert from
/// the server or a failed certificate check, as `InvalidData`. A dropped or
/// reset connection is not counted: rustls returns the same bare error for one
/// during the handshake and one after it, and libpq does not retry the latter.
fn is_tls_failure(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Tls(_) => true,
        sqlx::Error::Io(io_error) => io_error.kind() == std::io::ErrorKind::InvalidData,
        _ => false,
    }
}

/// Whether the server rejected the session with SQLSTATE class 28 (invalid
/// authorization), which covers `pg_hba.conf` rejections and failed password or
/// certificate authentication.
fn is_authorization_failure(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(database_error) => database_error
            .code()
            .is_some_and(|code| code.starts_with("28")),
        _ => false,
    }
}

/// Note appended to the final error when the TLS retry also failed, so both
/// attempts are visible.
pub(super) fn tls_fallback_detail(
    requested: PgSslMode,
    fallback: PgSslMode,
    first_error: &sqlx::Error,
) -> String {
    format!(
        "retried with sslmode={} after sslmode={} failed: {first_error}",
        ssl_mode_name(fallback),
        ssl_mode_name(requested)
    )
}

/// The root certificate value sqlx will use: the last root parameter in the
/// URL, otherwise `PGSSLROOTCERT`.
///
/// sqlx starts from the environment and lets every root parameter it parses
/// overwrite the previous value, so a DSN value, even an empty one, hides the
/// environment. libpq's default `~/.postgresql/root.crt` is not consulted
/// because sqlx never loads it.
fn root_cert_setting(url: &Url, env_root_cert: Option<String>) -> Option<RootCertSetting> {
    url.query_pairs()
        .filter(|(key, _)| ROOT_CERT_PARAMS.contains(&key.as_ref()))
        .last()
        .map(|(_, value)| RootCertSetting::Dsn(value.into_owned()))
        .or_else(|| env_root_cert.map(RootCertSetting::Env))
}

/// Classify a root certificate setting as libpq would for `sslmode=require`.
///
/// libpq decides with `stat`, so a path that exists counts even when it cannot
/// be read; such a path is kept apart as `Unreadable`.
///
/// Empty values count as unset: under `require` sqlx never reads the file, so
/// they must not trigger the `verify-ca` upgrade. Inline PEM counts only from
/// `PGSSLROOTCERT`, where sqlx applies the PEM heuristic of its
/// `CertificateInput`; a DSN value is always read as a file path.
fn classify_root_cert(setting: Option<&RootCertSetting>) -> RootCert {
    let (value, inline_pem_allowed) = match setting {
        Some(RootCertSetting::Dsn(value)) => (value.as_str(), false),
        Some(RootCertSetting::Env(value)) => (value.as_str(), true),
        None => return RootCert::Unset,
    };
    if value.is_empty() {
        return RootCert::Unset;
    }
    if value == "system" {
        return RootCert::System;
    }

    let trimmed = value.trim();
    if inline_pem_allowed && trimmed.starts_with("-----BEGIN") && trimmed.ends_with("-----") {
        return RootCert::Available;
    }

    let path = Path::new(value);
    match path.metadata() {
        Err(_) => RootCert::Missing(value.to_string()),
        Ok(metadata) if metadata.is_file() && File::open(path).is_ok() => RootCert::Available,
        Ok(_) => RootCert::Unreadable(value.to_string()),
    }
}

/// libpq spelling of an sqlx `sslmode`.
const fn ssl_mode_name(mode: PgSslMode) -> &'static str {
    match mode {
        PgSslMode::Disable => "disable",
        PgSslMode::Allow => "allow",
        PgSslMode::Prefer => "prefer",
        PgSslMode::Require => "require",
        PgSslMode::VerifyCa => "verify-ca",
        PgSslMode::VerifyFull => "verify-full",
    }
}

/// Explain how `mode`, as sqlx implements it, can leave a session exposed.
///
/// `allow` and `prefer` fall back between TLS and plaintext as in libpq (see
/// [`tls_fallback_mode`]), and sqlx's `verify-ca` trusts the compiled-in root
/// store in addition to `sslrootcert`.
fn ssl_mode_warning(mode: PgSslMode) -> Option<String> {
    match mode {
        PgSslMode::Disable => {
            Some("sslmode=disable never uses TLS; the connection is unencrypted.".to_string())
        }
        PgSslMode::Allow => Some(
            "sslmode=allow connects unencrypted and uses TLS only if the server rejects \
             that; use require or stricter when encryption is mandatory."
                .to_string(),
        ),
        PgSslMode::Prefer => Some(
            "sslmode=prefer connects unencrypted when the server does not offer TLS or \
             rejects the TLS attempt; use require or stricter when encryption is mandatory."
                .to_string(),
        ),
        PgSslMode::VerifyCa => Some(format!(
            "sslmode=verify-ca does not check the hostname and also trusts every CA in \
             the {}; use verify-full to pin the server identity.",
            crate::tls::root_store()
        )),
        PgSslMode::Require | PgSslMode::VerifyFull => None,
    }
}

/// Explain how the root certificate changes what `mode` verifies, if it does.
///
/// sqlx adds `sslrootcert` to the compiled-in root store instead of trusting
/// it alone, which matters most under `verify-full`, where libpq would accept
/// only certificates from that CA. `verify-ca` already warns about this in
/// [`ssl_mode_warning`].
fn root_cert_warning(mode: PgSslMode, root_cert: &RootCert) -> Option<String> {
    match (mode, root_cert) {
        (PgSslMode::Prefer, RootCert::Available) => Some(
            "sslrootcert verifies the certificate chain when TLS is used, but a failed \
             check falls back to an unencrypted connection, as in libpq; use verify-full \
             to require a verified server."
                .to_string(),
        ),
        (PgSslMode::VerifyFull, RootCert::Available) => Some(format!(
            "sslrootcert is trusted in addition to the {}, not instead of it as in \
             libpq, so a certificate for this host from any of those CAs is also accepted.",
            crate::tls::root_store()
        )),
        (PgSslMode::Require, RootCert::Missing(path)) => Some(format!(
            "sslrootcert {path} does not exist, so the server certificate is not \
             verified (libpq does the same)."
        )),
        (PgSslMode::VerifyCa | PgSslMode::VerifyFull, RootCert::Missing(path)) => Some(format!(
            "sslrootcert {path} does not exist, so the connection will fail until it does."
        )),
        (PgSslMode::Prefer, RootCert::Unreadable(path)) => Some(format!(
            "sslrootcert {path} cannot be read, so pgmon connects without TLS, as libpq \
             does when it cannot set up TLS."
        )),
        (PgSslMode::Allow, RootCert::Unreadable(path)) => Some(format!(
            "sslrootcert {path} cannot be read, so the TLS retry after a rejected \
             unencrypted attempt will fail."
        )),
        (PgSslMode::VerifyCa | PgSslMode::VerifyFull, RootCert::Unreadable(path)) => Some(format!(
            "sslrootcert {path} cannot be read, so the connection will fail until it can."
        )),
        _ => None,
    }
}

pub(super) fn classify_connect_error(target_summary: &str, error: sqlx::Error) -> DbError {
    match error {
        sqlx::Error::Configuration(_) | sqlx::Error::Tls(_) => DbError::fatal(format!(
            "Failed to connect to Postgres using {target_summary}: {error}"
        )),
        sqlx::Error::Database(database_error)
            if is_fatal_connect_sqlstate(database_error.code().as_deref()) =>
        {
            DbError::fatal(format!(
                "Failed to connect to Postgres using {target_summary}: {database_error}"
            ))
        }
        other => DbError::transient(format!(
            "Failed to connect to Postgres using {target_summary}: {other}"
        )),
    }
}

pub(super) fn classify_query_error(error: sqlx::Error) -> DbError {
    match error {
        sqlx::Error::Configuration(_) | sqlx::Error::Tls(_) => {
            DbError::fatal(format!("PostgreSQL query failed: {error}"))
        }
        other => DbError::transient(format!("PostgreSQL query failed: {other}")),
    }
}

fn classify_connect_error_from_sqlx(error: sqlx::Error) -> DbError {
    match error {
        sqlx::Error::Configuration(_) | sqlx::Error::Tls(_) => DbError::fatal(format!(
            "Failed to inspect the PostgreSQL server version: {error}"
        )),
        other => DbError::transient(format!(
            "Failed to inspect the PostgreSQL server version: {other}"
        )),
    }
}

fn is_fatal_connect_sqlstate(code: Option<&str>) -> bool {
    code.is_some_and(|sqlstate| {
        sqlstate.starts_with("28")
            || sqlstate.starts_with("3D")
            || sqlstate.starts_with("3F")
            || sqlstate == "42501"
    })
}

fn build_pool_key(
    options: &PgConnectOptions,
    params: &BTreeMap<String, String>,
    database_override: Option<&str>,
    ssl_mode: PgSslMode,
    fallback_ssl_mode: Option<PgSslMode>,
) -> PoolKey {
    let socket = options
        .get_socket()
        .map(|path| path.to_string_lossy().into_owned());
    let host = socket
        .clone()
        .unwrap_or_else(|| options.get_host().to_string());
    let user = options.get_username().to_string();
    let database = database_override
        .map(str::to_string)
        .or_else(|| options.get_database().map(str::to_string))
        .unwrap_or_default();

    PoolKey {
        host,
        hostaddr: params.get("hostaddr").cloned(),
        port: options.get_port(),
        database,
        user,
        socket,
        ssl_mode: format!("{ssl_mode:?}"),
        ssl_attempts: format!("{:?} then {fallback_ssl_mode:?}", options.get_ssl_mode()),
        ssl_root_cert: params.get("sslrootcert").cloned(),
        ssl_cert: params.get("sslcert").cloned(),
        ssl_key: params.get("sslkey").cloned(),
        options: options.get_options().map(str::to_string),
        application_name: options.get_application_name().map(str::to_string),
        target_session_attrs: params.get("target_session_attrs").cloned(),
        password_fingerprint: params
            .get("password")
            .or_else(|| params.get("passfile"))
            .map(|value| hash_value(value)),
    }
}

fn looks_like_postgres_url(dsn: &str) -> bool {
    let trimmed = dsn.trim_start();
    trimmed.starts_with("postgres://") || trimmed.starts_with("postgresql://")
}

fn url_query_params(parsed: &Url) -> BTreeMap<String, String> {
    let mut params = parsed
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<BTreeMap<_, _>>();
    if !parsed.username().is_empty() {
        params
            .entry("user".to_string())
            .or_insert_with(|| parsed.username().to_string());
    }
    if let Some(password) = parsed.password() {
        params
            .entry("password".to_string())
            .or_insert_with(|| password.to_string());
    }
    if let Some(host) = parsed.host_str() {
        params
            .entry("host".to_string())
            .or_insert_with(|| host.to_string());
    }
    if let Some(port) = parsed.port() {
        params
            .entry("port".to_string())
            .or_insert_with(|| port.to_string());
    }
    let database = parsed.path().trim_start_matches('/');
    if !database.is_empty() {
        params
            .entry("dbname".to_string())
            .or_insert_with(|| database.to_string());
    }
    params
}

fn conninfo_to_url(dsn: &str) -> DbResult<String> {
    let params = parse_conninfo_params(dsn)?;
    let mut serializer = form_urlencoded::Serializer::new(String::new());

    for (key, value) in params {
        serializer.append_pair(&key, &value);
    }

    Ok(format!("postgresql://?{}", serializer.finish()))
}

fn parse_conninfo_params(dsn: &str) -> DbResult<BTreeMap<String, String>> {
    let mut chars = dsn.chars().peekable();
    let mut params = BTreeMap::new();

    while let Some(character) = chars.peek() {
        if character.is_whitespace() {
            chars.next();
            continue;
        }

        let mut key = String::new();
        while let Some(character) = chars.peek() {
            if *character == '=' || character.is_whitespace() {
                break;
            }
            key.push(*character);
            chars.next();
        }

        if key.is_empty() || chars.next() != Some('=') {
            return Err(DbError::fatal(format!(
                "Failed to parse Postgres connection settings for {}.",
                describe_connection_target(dsn)
            )));
        }

        let value = parse_conninfo_value(&mut chars).map_err(|error| {
            let mut message = String::new();
            let _ = write!(
                &mut message,
                "Failed to parse Postgres connection settings for {}: {error}",
                describe_connection_target(dsn)
            );
            DbError::fatal(message)
        })?;

        params.insert(key, value);
    }

    if params.is_empty() {
        return Err(DbError::fatal(format!(
            "Failed to parse Postgres connection settings for {}.",
            describe_connection_target(dsn)
        )));
    }

    Ok(params)
}

fn parse_conninfo_value<I>(chars: &mut std::iter::Peekable<I>) -> Result<String, &'static str>
where
    I: Iterator<Item = char>,
{
    let Some(first) = chars.peek().copied() else {
        return Ok(String::new());
    };

    let mut value = String::new();
    if first == '\'' {
        chars.next();
        while let Some(character) = chars.next() {
            match character {
                '\'' => return Ok(value),
                '\\' => {
                    if let Some(escaped) = chars.next() {
                        value.push(escaped);
                    }
                }
                other => value.push(other),
            }
        }
        return Err("unterminated quoted value");
    }

    while let Some(character) = chars.peek() {
        if character.is_whitespace() {
            break;
        }
        let character = chars.next().ok_or("unexpected end of value")?;
        if character == '\\' {
            if let Some(escaped) = chars.next() {
                value.push(escaped);
            }
        } else {
            value.push(character);
        }
    }

    Ok(value)
}

fn hash_value(value: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::{
        PoolKey, RootCert, RootCertSetting, classify_root_cert, conninfo_to_url,
        effective_ssl_mode, first_attempt_ssl_mode, parse_conninfo_params,
        prepare_connection_target, resolve_ssl_mode, retries_with_tls_fallback, root_cert_setting,
        ssl_mode_name, ssl_mode_warning, tls_fallback_detail, tls_fallback_mode,
    };
    use sqlx::postgres::PgSslMode;
    use std::{borrow::Cow, error::Error as StdError, fmt, io};
    use url::Url;

    /// A server error carrying only a SQLSTATE, for the retry decision tests.
    #[derive(Debug)]
    struct ServerError(&'static str);

    impl fmt::Display for ServerError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "server error {}", self.0)
        }
    }

    impl StdError for ServerError {}

    impl sqlx::error::DatabaseError for ServerError {
        fn message(&self) -> &'static str {
            "server error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.0))
        }

        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn server_error(code: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(ServerError(code)))
    }

    fn io_error(kind: io::ErrorKind) -> sqlx::Error {
        sqlx::Error::Io(io::Error::from(kind))
    }

    /// A root certificate path that exists whenever the tests run.
    const EXISTING_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    /// A root certificate path that never exists.
    const MISSING_FILE: &str = "/nonexistent/pgmon-test-ca.crt";
    /// A root certificate path that exists but is not a readable file, whatever
    /// user runs the tests.
    const EXISTING_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src");

    fn url(value: &str) -> Url {
        match Url::parse(value) {
            Ok(url) => url,
            Err(error) => panic!("test URL should parse: {error}"),
        }
    }

    #[test]
    fn test_parse_conninfo_params_handles_quotes_and_escapes() {
        let params = match parse_conninfo_params(
            "host='my host' dbname=metrics user=postgres password='pa\\'ss'",
        ) {
            Ok(params) => params,
            Err(error) => panic!("conninfo should parse: {error}"),
        };

        assert_eq!(params.get("host"), Some(&"my host".to_string()));
        assert_eq!(params.get("password"), Some(&"pa'ss".to_string()));
    }

    #[test]
    fn test_conninfo_to_url_uses_query_parameters() {
        let url = match conninfo_to_url("host=localhost dbname=postgres user=pgmon password=secret")
        {
            Ok(url) => url,
            Err(error) => panic!("conninfo should convert to a URL: {error}"),
        };

        assert_eq!(
            url,
            "postgresql://?dbname=postgres&host=localhost&password=secret&user=pgmon"
        );
    }

    #[test]
    fn test_prepare_connection_target_overrides_database_in_pool_key() {
        let target = match prepare_connection_target(
            "postgresql://pgmon@localhost/postgres",
            Some("analytics"),
        ) {
            Ok(target) => target,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(target.key.database, "analytics");
    }

    #[test]
    fn test_pool_key_distinguishes_password_fingerprint() {
        let first =
            match prepare_connection_target("postgresql://pgmon:secret@localhost/postgres", None) {
                Ok(target) => target,
                Err(error) => panic!("URL should parse: {error}"),
            };
        let second =
            match prepare_connection_target("postgresql://pgmon:secret-2@localhost/postgres", None)
            {
                Ok(target) => target,
                Err(error) => panic!("URL should parse: {error}"),
            };

        assert_ne!(first.key, second.key);
    }

    #[test]
    fn test_pool_key_captures_socket_and_ssl_mode() {
        let target = match prepare_connection_target(
            "postgresql:///?host=/var/run/postgresql&dbname=postgres&user=pgmon&sslmode=disable",
            None,
        ) {
            Ok(target) => target,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(
            target.key,
            PoolKey {
                host: "/var/run/postgresql".to_string(),
                hostaddr: None,
                port: 5432,
                database: "postgres".to_string(),
                user: "pgmon".to_string(),
                socket: Some("/var/run/postgresql".to_string()),
                ssl_mode: "Disable".to_string(),
                ssl_attempts: "Disable then None".to_string(),
                ssl_root_cert: None,
                ssl_cert: None,
                ssl_key: None,
                options: None,
                application_name: None,
                target_session_attrs: None,
                password_fingerprint: None,
            }
        );
    }

    #[test]
    fn test_effective_ssl_mode_upgrades_require_only_with_available_root_cert() {
        assert_eq!(
            ssl_mode_name(effective_ssl_mode(PgSslMode::Require, &RootCert::Available)),
            "verify-ca"
        );
        for root_cert in [RootCert::Unset, RootCert::Missing(MISSING_FILE.to_string())] {
            assert_eq!(
                ssl_mode_name(effective_ssl_mode(PgSslMode::Require, &root_cert)),
                "require"
            );
        }
    }

    #[test]
    fn test_effective_ssl_mode_keeps_other_modes_with_root_cert() {
        for mode in [
            PgSslMode::Disable,
            PgSslMode::Allow,
            PgSslMode::Prefer,
            PgSslMode::VerifyCa,
            PgSslMode::VerifyFull,
        ] {
            assert_eq!(
                ssl_mode_name(effective_ssl_mode(mode, &RootCert::Available)),
                ssl_mode_name(mode)
            );
        }
    }

    #[test]
    fn test_root_cert_setting_uses_last_dsn_value_like_sqlx() {
        let setting = root_cert_setting(
            &url("postgresql://localhost/postgres?sslrootcert=/first.crt&ssl-ca=/last.crt"),
            None,
        );
        assert_eq!(setting, Some(RootCertSetting::Dsn("/last.crt".to_string())));

        let setting = root_cert_setting(
            &url("postgresql://localhost/postgres?sslrootcert=/valid.crt&ssl-root-cert="),
            None,
        );
        assert_eq!(setting, Some(RootCertSetting::Dsn(String::new())));
    }

    #[test]
    fn test_root_cert_setting_prefers_dsn_over_environment() {
        let env = Some("/env.crt".to_string());

        let setting = root_cert_setting(&url("postgresql://localhost/postgres"), env.clone());
        assert_eq!(setting, Some(RootCertSetting::Env("/env.crt".to_string())));

        let setting = root_cert_setting(&url("postgresql://localhost/postgres?sslrootcert="), env);
        assert_eq!(setting, Some(RootCertSetting::Dsn(String::new())));
    }

    #[test]
    fn test_classify_root_cert_matches_libpq_file_rule() {
        let dsn = |value: &str| RootCertSetting::Dsn(value.to_string());

        assert_eq!(classify_root_cert(None), RootCert::Unset);
        assert_eq!(classify_root_cert(Some(&dsn(""))), RootCert::Unset);
        assert_eq!(classify_root_cert(Some(&dsn("system"))), RootCert::System);
        assert_eq!(
            classify_root_cert(Some(&RootCertSetting::Env("system".to_string()))),
            RootCert::System
        );
        assert_eq!(
            classify_root_cert(Some(&dsn(EXISTING_FILE))),
            RootCert::Available
        );
        assert_eq!(
            classify_root_cert(Some(&dsn(MISSING_FILE))),
            RootCert::Missing(MISSING_FILE.to_string())
        );
        assert_eq!(
            classify_root_cert(Some(&dsn(EXISTING_DIR))),
            RootCert::Unreadable(EXISTING_DIR.to_string())
        );
    }

    #[test]
    fn test_classify_root_cert_accepts_inline_pem_only_from_environment() {
        let pem = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";

        assert_eq!(
            classify_root_cert(Some(&RootCertSetting::Env(pem.to_string()))),
            RootCert::Available
        );
        assert_eq!(
            classify_root_cert(Some(&RootCertSetting::Dsn(pem.to_string()))),
            RootCert::Missing(pem.to_string())
        );
    }

    #[test]
    fn test_prepare_connection_target_verifies_require_with_existing_root_cert() {
        let dsn = format!(
            "postgresql://pgmon@localhost/postgres?sslmode=require&sslrootcert={EXISTING_FILE}"
        );
        let target = match prepare_connection_target(&dsn, None) {
            Ok(target) => target,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(target.key.ssl_mode, "VerifyCa");
        assert_eq!(ssl_mode_name(target.options.get_ssl_mode()), "verify-ca");
    }

    #[test]
    fn test_prepare_connection_target_keeps_require_with_missing_root_cert() {
        let dsn = format!(
            "postgresql://pgmon@localhost/postgres?sslmode=require&sslrootcert={MISSING_FILE}"
        );
        let target = match prepare_connection_target(&dsn, None) {
            Ok(target) => target,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(target.key.ssl_mode, "Require");
    }

    #[test]
    fn test_prepare_connection_target_rejects_system_root_cert() {
        let result = prepare_connection_target(
            "postgresql://pgmon@localhost/postgres?sslmode=verify-full&sslrootcert=system",
            None,
        );

        assert!(result.is_err_and(|error| error.to_string().contains("sslrootcert=system")));
    }

    #[test]
    fn test_resolve_ssl_mode_reports_require_upgrade_in_conninfo() {
        let dsn =
            format!("host=localhost dbname=postgres sslmode=require sslrootcert='{EXISTING_FILE}'");
        let resolution = match resolve_ssl_mode(&dsn) {
            Ok(resolution) => resolution,
            Err(error) => panic!("conninfo should parse: {error}"),
        };

        assert_eq!(resolution.requested, "require");
        assert_eq!(resolution.effective, "verify-ca");
        assert!(
            resolution
                .warnings
                .iter()
                .any(|warning| warning.contains("use verify-full"))
        );
    }

    #[test]
    fn test_resolve_ssl_mode_warns_about_missing_root_cert() {
        let dsn = format!(
            "postgresql://pgmon@localhost/postgres?sslmode=require&sslrootcert={MISSING_FILE}"
        );
        let resolution = match resolve_ssl_mode(&dsn) {
            Ok(resolution) => resolution,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(resolution.effective, "require");
        assert_eq!(
            resolution.warnings,
            vec![format!(
                "sslrootcert {MISSING_FILE} does not exist, so the server certificate is not \
                 verified (libpq does the same)."
            )]
        );
    }

    #[test]
    fn test_resolve_ssl_mode_warns_that_verify_full_root_cert_is_additive() {
        let dsn = format!(
            "postgresql://pgmon@localhost/postgres?sslmode=verify-full&sslrootcert={EXISTING_FILE}"
        );
        let resolution = match resolve_ssl_mode(&dsn) {
            Ok(resolution) => resolution,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(resolution.effective, "verify-full");
        assert!(matches!(
            resolution.warnings.as_slice(),
            [warning] if warning.contains("in addition to the")
        ));
    }

    #[test]
    fn test_resolve_ssl_mode_has_no_warning_for_verify_full_without_root_cert() {
        let resolution = match resolve_ssl_mode(
            "postgresql://pgmon@localhost/postgres?sslmode=verify-full&sslrootcert=",
        ) {
            Ok(resolution) => resolution,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(resolution.effective, "verify-full");
        assert!(resolution.warnings.is_empty());
    }

    #[test]
    fn test_ssl_mode_warning_flags_plaintext_capable_modes() {
        for mode in [PgSslMode::Disable, PgSslMode::Allow, PgSslMode::Prefer] {
            assert!(
                ssl_mode_warning(mode).is_some_and(|warning| warning.contains("unencrypted")),
                "{} should warn about plaintext",
                ssl_mode_name(mode)
            );
        }
        assert_eq!(ssl_mode_warning(PgSslMode::Require), None);
    }

    #[test]
    fn test_resolve_ssl_mode_rejects_invalid_sslmode() {
        assert!(resolve_ssl_mode("postgresql://pgmon@localhost/postgres?sslmode=bogus").is_err());
    }

    #[test]
    fn test_tls_fallback_mode_matches_libpq() {
        let name = |mode: Option<PgSslMode>| mode.map(ssl_mode_name);

        assert_eq!(
            name(tls_fallback_mode(PgSslMode::Prefer, &RootCert::Unset)),
            Some("disable")
        );
        assert_eq!(
            name(tls_fallback_mode(PgSslMode::Allow, &RootCert::Unset)),
            Some("require")
        );
        assert_eq!(
            name(tls_fallback_mode(PgSslMode::Allow, &RootCert::Available)),
            Some("verify-ca")
        );
        for mode in [
            PgSslMode::Disable,
            PgSslMode::Require,
            PgSslMode::VerifyCa,
            PgSslMode::VerifyFull,
        ] {
            assert_eq!(name(tls_fallback_mode(mode, &RootCert::Available)), None);
        }
    }

    #[test]
    fn test_prefer_retries_after_tls_and_authorization_failures() {
        for error in [
            sqlx::Error::Tls("server does not support TLS".into()),
            io_error(io::ErrorKind::InvalidData),
            server_error("28000"),
            server_error("28P01"),
        ] {
            assert!(
                retries_with_tls_fallback(PgSslMode::Prefer, &error),
                "prefer should retry after {error}"
            );
        }
    }

    #[test]
    fn test_prefer_does_not_retry_network_or_unrelated_errors() {
        for error in [
            io_error(io::ErrorKind::ConnectionRefused),
            io_error(io::ErrorKind::TimedOut),
            // Indistinguishable from a drop after the handshake, which libpq
            // does not retry.
            io_error(io::ErrorKind::UnexpectedEof),
            io_error(io::ErrorKind::ConnectionReset),
            io_error(io::ErrorKind::ConnectionAborted),
            io_error(io::ErrorKind::PermissionDenied),
            server_error("3D000"),
            server_error("53300"),
            sqlx::Error::PoolTimedOut,
        ] {
            assert!(
                !retries_with_tls_fallback(PgSslMode::Prefer, &error),
                "prefer should not retry after {error}"
            );
        }
    }

    #[test]
    fn test_allow_retries_only_after_authorization_failures() {
        assert!(retries_with_tls_fallback(
            PgSslMode::Allow,
            &server_error("28000")
        ));
        assert!(!retries_with_tls_fallback(
            PgSslMode::Allow,
            &io_error(io::ErrorKind::InvalidData)
        ));
        assert!(!retries_with_tls_fallback(
            PgSslMode::Allow,
            &server_error("3D000")
        ));
    }

    #[test]
    fn test_strict_modes_never_retry() {
        for mode in [
            PgSslMode::Disable,
            PgSslMode::Require,
            PgSslMode::VerifyCa,
            PgSslMode::VerifyFull,
        ] {
            assert!(!retries_with_tls_fallback(mode, &server_error("28000")));
            assert!(!retries_with_tls_fallback(
                mode,
                &io_error(io::ErrorKind::InvalidData)
            ));
        }
    }

    #[test]
    fn test_prepare_connection_target_builds_tls_fallback_options() {
        let fallback_mode = |dsn: &str| match prepare_connection_target(dsn, None) {
            Ok(target) => target
                .tls_fallback
                .map(|fallback| ssl_mode_name(fallback.options.get_ssl_mode())),
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(
            fallback_mode("postgresql://pgmon@localhost/postgres?sslmode=prefer"),
            Some("disable")
        );
        assert_eq!(
            fallback_mode(&format!(
                "postgresql://pgmon@localhost/postgres?sslmode=allow&sslrootcert={EXISTING_FILE}"
            )),
            Some("verify-ca")
        );
        assert_eq!(
            fallback_mode("postgresql://pgmon@localhost/postgres?sslmode=require"),
            None
        );
    }

    #[test]
    fn test_tls_fallback_detail_names_both_attempts() {
        let detail = tls_fallback_detail(
            PgSslMode::Prefer,
            PgSslMode::Disable,
            &server_error("28000"),
        );

        assert_eq!(
            detail,
            "retried with sslmode=disable after sslmode=prefer failed: error returned from \
             database: server error 28000"
        );
    }

    #[test]
    fn test_prefer_with_root_cert_verifies_first_and_falls_back_to_plaintext() {
        let dsn = format!(
            "postgresql://pgmon@localhost/postgres?sslmode=prefer&sslrootcert={EXISTING_FILE}"
        );
        let target = match prepare_connection_target(&dsn, None) {
            Ok(target) => target,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(ssl_mode_name(target.options.get_ssl_mode()), "verify-ca");
        assert_eq!(target.key.ssl_mode, "Prefer");
        let Some(fallback) = target.tls_fallback else {
            panic!("prefer should have a TLS fallback");
        };
        assert_eq!(ssl_mode_name(fallback.mode), "prefer");
        assert_eq!(ssl_mode_name(fallback.options.get_ssl_mode()), "disable");
    }

    #[test]
    fn test_pool_key_separates_prefer_from_verify_ca() {
        let key = |sslmode: &str| {
            let dsn = format!(
                "postgresql://pgmon@localhost/postgres?sslmode={sslmode}&sslrootcert={EXISTING_FILE}"
            );
            match prepare_connection_target(&dsn, None) {
                Ok(target) => target.key,
                Err(error) => panic!("URL should parse: {error}"),
            }
        };

        assert_ne!(key("prefer"), key("verify-ca"));
        assert_eq!(key("require"), key("verify-ca"));
    }

    #[test]
    fn test_resolve_ssl_mode_warns_that_prefer_verification_can_fall_back() {
        let dsn = format!(
            "postgresql://pgmon@localhost/postgres?sslmode=prefer&sslrootcert={EXISTING_FILE}"
        );
        let resolution = match resolve_ssl_mode(&dsn) {
            Ok(resolution) => resolution,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(resolution.effective, "prefer");
        assert!(
            resolution
                .warnings
                .iter()
                .any(|warning| warning.contains("a failed check falls back"))
        );
    }

    #[test]
    fn test_unreadable_root_cert_follows_libpq_failed_tls_setup() {
        let unreadable = RootCert::Unreadable(EXISTING_DIR.to_string());
        let name = |mode: Option<PgSslMode>| mode.map(ssl_mode_name);

        // prefer: TLS setup fails, so libpq ends up without TLS and no retry.
        assert_eq!(
            ssl_mode_name(first_attempt_ssl_mode(PgSslMode::Prefer, &unreadable)),
            "disable"
        );
        assert_eq!(
            name(tls_fallback_mode(PgSslMode::Prefer, &unreadable)),
            None
        );
        // require and allow's TLS retry still try to verify, and fail.
        assert_eq!(
            ssl_mode_name(effective_ssl_mode(PgSslMode::Require, &unreadable)),
            "verify-ca"
        );
        assert_eq!(
            name(tls_fallback_mode(PgSslMode::Allow, &unreadable)),
            Some("verify-ca")
        );
    }

    #[test]
    fn test_resolve_ssl_mode_warns_about_unreadable_root_cert() {
        let dsn = format!(
            "postgresql://pgmon@localhost/postgres?sslmode=prefer&sslrootcert={EXISTING_DIR}"
        );
        let resolution = match resolve_ssl_mode(&dsn) {
            Ok(resolution) => resolution,
            Err(error) => panic!("URL should parse: {error}"),
        };

        assert_eq!(resolution.effective, "prefer");
        assert!(
            resolution
                .warnings
                .iter()
                .any(|warning| warning.contains("cannot be read, so pgmon connects without TLS"))
        );
    }

    #[test]
    fn test_pool_key_changes_when_root_cert_appears() {
        let root_cert = std::env::temp_dir().join(format!(
            "pgmon-test-root-cert-appears-{}.crt",
            std::process::id()
        ));
        let dsn = format!(
            "postgresql://pgmon@localhost/postgres?sslmode=prefer&sslrootcert={}",
            root_cert.display()
        );
        let key = || match prepare_connection_target(&dsn, None) {
            Ok(target) => target.key,
            Err(error) => panic!("URL should parse: {error}"),
        };

        let _ = std::fs::remove_file(&root_cert);
        let before = key();
        if let Err(error) = std::fs::write(&root_cert, "not checked by this test") {
            panic!("test root certificate should be writable: {error}");
        }
        let after = key();
        let _ = std::fs::remove_file(&root_cert);

        assert_eq!(before.ssl_attempts, "Prefer then Some(Disable)");
        assert_eq!(after.ssl_attempts, "VerifyCa then Some(Disable)");
        assert_ne!(before, after);
    }
}
