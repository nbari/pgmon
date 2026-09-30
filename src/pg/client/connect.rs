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
    pub(super) target_summary: String,
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
    options = options.ssl_mode(ssl_mode);

    let key = build_pool_key(&options, &params, database_override);

    Ok(PreparedConnectionTarget {
        key,
        options,
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
        (PgSslMode::Require, RootCert::Available) => PgSslMode::VerifyCa,
        (other, _) => other,
    }
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
    let inline_pem =
        inline_pem_allowed && trimmed.starts_with("-----BEGIN") && trimmed.ends_with("-----");
    if inline_pem || Path::new(value).exists() {
        RootCert::Available
    } else {
        RootCert::Missing(value.to_string())
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
/// sqlx never attempts TLS under `allow`, unlike libpq, and its `verify-ca`
/// trusts the compiled-in root store in addition to `sslrootcert`.
fn ssl_mode_warning(mode: PgSslMode) -> Option<String> {
    match mode {
        PgSslMode::Disable | PgSslMode::Allow => Some(format!(
            "sslmode={} never uses TLS; the connection is unencrypted.",
            ssl_mode_name(mode)
        )),
        PgSslMode::Prefer => Some(
            "sslmode=prefer connects unencrypted when the server does not offer TLS; \
             use require or stricter when encryption is mandatory."
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
        ssl_mode: format!("{:?}", options.get_ssl_mode()),
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
        effective_ssl_mode, parse_conninfo_params, prepare_connection_target, resolve_ssl_mode,
        root_cert_setting, ssl_mode_name, ssl_mode_warning,
    };
    use sqlx::postgres::PgSslMode;
    use url::Url;

    /// A root certificate path that exists whenever the tests run.
    const EXISTING_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    /// A root certificate path that never exists.
    const MISSING_FILE: &str = "/nonexistent/pgmon-test-ca.crt";

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
}
