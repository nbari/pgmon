//! Connection helpers for `PgClient`.

use super::{DbError, DbResult, MIN_SUPPORTED_SERVER_VERSION_NUM, PgClient};
use crate::pg::conninfo::describe_connection_target;
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    hash::{Hash, Hasher},
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

impl PgClient {
    pub(super) fn from_pool(pool: PgPool, server_version_num: i32) -> Self {
        Self::new(pool, server_version_num)
    }

    pub(crate) async fn execute_admin_action(
        &self,
        connection: &mut super::PgClientConnection,
        query: &str,
    ) -> DbResult<()> {
        sqlx::raw_sql(query)
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
    let url = if looks_like_postgres_url(dsn) {
        dsn.to_string()
    } else {
        conninfo_to_url(dsn)?
    };

    let params = url_query_params(&url)?;
    let mut options = PgConnectOptions::from_str(&url).map_err(|error| {
        DbError::fatal(format!(
            "Failed to parse Postgres connection settings for {}: {error}",
            describe_connection_target(dsn)
        ))
    })?;
    if let Some(database) = database_override {
        options = options.database(database);
    }

    let key = build_pool_key(&options, &params, database_override);

    Ok(PreparedConnectionTarget {
        key,
        options,
        target_summary: describe_connection_target(dsn),
    })
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

fn url_query_params(url: &str) -> DbResult<BTreeMap<String, String>> {
    let parsed = Url::parse(url).map_err(|error| {
        DbError::fatal(format!(
            "Failed to parse PostgreSQL connection URL: {error}"
        ))
    })?;
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
    Ok(params)
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
    use super::{PoolKey, conninfo_to_url, parse_conninfo_params, prepare_connection_target};

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
}
