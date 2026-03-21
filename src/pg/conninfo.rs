//! `PostgreSQL` connection-string resolution helpers.
//!
//! When no explicit DSN is provided, this module can derive a conninfo string
//! from the first usable entry in `PGPASSFILE` or `~/.pgpass`.

use std::{env, fs, io, path::PathBuf};

use thiserror::Error;

/// Errors returned while resolving `PostgreSQL` connection settings.
#[derive(Debug, Error)]
pub enum ResolveDsnError {
    /// No explicit DSN or usable `.pgpass` entry was available.
    #[error(
        "No PostgreSQL connection settings found. Pass --dsn, set PGMON_DSN, or create a usable .pgpass file at {searched_path}"
    )]
    MissingConnectionSettings { searched_path: String },
    /// The `.pgpass` file could not be read.
    #[error("Failed to read .pgpass file at {path}: {source}")]
    ReadPgPass { path: PathBuf, source: io::Error },
    /// A `.pgpass` line was malformed.
    #[error("Invalid .pgpass entry on line {line}: expected 5 colon-separated fields")]
    InvalidPgPassEntry { line: usize },
    /// The `.pgpass` file existed but contained no usable entries.
    #[error("The .pgpass file at {path} does not contain any usable entries")]
    EmptyPgPass { path: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PgPassEntry {
    host: String,
    port: String,
    database: String,
    user: String,
    password: String,
}

impl PgPassEntry {
    fn to_conninfo(&self) -> String {
        let mut pairs = Vec::new();

        if is_specific_value(&self.host) {
            pairs.push(conninfo_pair("host", &self.host));
        }
        if is_specific_value(&self.port) {
            pairs.push(conninfo_pair("port", &self.port));
        }
        if is_specific_value(&self.database) {
            pairs.push(conninfo_pair("dbname", &self.database));
        }
        if is_specific_value(&self.user) {
            pairs.push(conninfo_pair("user", &self.user));
        }

        pairs.push(conninfo_pair("password", &self.password));
        pairs.join(" ")
    }
}

/// Resolve the DSN from the explicit CLI/env value or from `.pgpass`.
pub fn resolve_dsn(explicit_dsn: Option<&str>) -> Result<String, ResolveDsnError> {
    resolve_dsn_from_path(explicit_dsn, pgpass_path())
}

/// Build a safe, human-readable summary of a connection target without exposing passwords.
pub fn describe_connection_target(dsn: &str) -> String {
    if let Some(summary) = summarize_conninfo(dsn) {
        return summary;
    }

    if let Some(summary) = summarize_uri(dsn) {
        return summary;
    }

    "PostgreSQL".to_string()
}

/// Extract only the host information from a DSN for shorter display.
pub fn describe_host(dsn: &str) -> String {
    if let Some(host) = extract_host_from_conninfo(dsn) {
        return format!("host={host}");
    }
    if let Some(host) = extract_host_from_uri(dsn) {
        return format!("host={host}");
    }
    "PostgreSQL".to_string()
}

fn extract_host_from_conninfo(dsn: &str) -> Option<String> {
    let mut chars = dsn.chars().peekable();

    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }

        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' || c.is_whitespace() {
                break;
            }
            key.push(c);
            chars.next();
        }

        if chars.peek() == Some(&'=') {
            chars.next();
            let mut value = String::new();
            if chars.peek() == Some(&'\'') {
                chars.next();
                while let Some(c) = chars.next() {
                    if c == '\'' {
                        break;
                    } else if c == '\\' {
                        if let Some(escaped) = chars.next() {
                            value.push(escaped);
                        }
                    } else {
                        value.push(c);
                    }
                }
            } else {
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() {
                        break;
                    }
                    value.push(c);
                    chars.next();
                }
            }

            if key == "host" {
                return Some(value);
            }
        } else {
            chars.next();
        }
    }
    None
}

fn extract_host_from_uri(dsn: &str) -> Option<String> {
    let scheme_end = dsn.find("://")?;
    let remainder = &dsn[scheme_end + 3..];
    let (authority_and_path, _) = remainder.split_once(['?', '#']).unwrap_or((remainder, ""));
    let (authority, _) = authority_and_path
        .split_once('/')
        .unwrap_or((authority_and_path, ""));
    let (_, host_port) = authority
        .rsplit_once('@')
        .map_or((None, authority), |(auth, host)| (Some(auth), host));

    if host_port.is_empty() {
        None
    } else {
        Some(host_port.to_string())
    }
}

fn resolve_dsn_from_path(
    explicit_dsn: Option<&str>,
    pgpass_path: Option<PathBuf>,
) -> Result<String, ResolveDsnError> {
    if let Some(dsn) = explicit_dsn.filter(|value| !value.trim().is_empty()) {
        return Ok(dsn.to_owned());
    }

    let searched_path = pgpass_path
        .as_ref()
        .map_or_else(|| "~/.pgpass".to_owned(), |path| path.display().to_string());

    let Some(path) = pgpass_path else {
        return Err(ResolveDsnError::MissingConnectionSettings { searched_path });
    };

    if !path.exists() {
        return Err(ResolveDsnError::MissingConnectionSettings { searched_path });
    }

    let content = fs::read_to_string(&path).map_err(|source| ResolveDsnError::ReadPgPass {
        path: path.clone(),
        source,
    })?;
    let entries = parse_pgpass(&content)?;
    let entry = entries
        .into_iter()
        .next()
        .ok_or_else(|| ResolveDsnError::EmptyPgPass { path: path.clone() })?;

    Ok(entry.to_conninfo())
}

fn pgpass_path() -> Option<PathBuf> {
    if let Some(path) = env::var_os("PGPASSFILE").filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(path));
    }

    env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|path| path.join(".pgpass"))
}

fn parse_pgpass(content: &str) -> Result<Vec<PgPassEntry>, ResolveDsnError> {
    let mut entries = Vec::new();

    for (line_index, raw_line) in content.lines().enumerate() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let fields = split_pgpass_fields(raw_line);
        let Ok([host, port, database, user, password]) =
            <Vec<String> as TryInto<[String; 5]>>::try_into(fields)
        else {
            return Err(ResolveDsnError::InvalidPgPassEntry {
                line: line_index + 1,
            });
        };

        entries.push(PgPassEntry {
            host,
            port,
            database,
            user,
            password,
        });
    }

    Ok(entries)
}

/// Split a `.pgpass` line while honoring the file's backslash escaping rules.
fn split_pgpass_fields(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars();

    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                } else {
                    current.push('\\');
                }
            }
            ':' => {
                fields.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }

    fields.push(current);
    fields
}

fn is_specific_value(value: &str) -> bool {
    !value.is_empty() && value != "*"
}

fn conninfo_pair(key: &str, value: &str) -> String {
    format!("{key}={}", quote_conninfo_value(value))
}

fn quote_conninfo_value(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{escaped}'")
}

fn summarize_conninfo(dsn: &str) -> Option<String> {
    let mut parts = Vec::new();
    let mut chars = dsn.chars().peekable();

    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }

        // Parse key
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' || c.is_whitespace() {
                break;
            }
            key.push(c);
            chars.next();
        }

        if chars.peek() == Some(&'=') {
            chars.next(); // skip '='

            // Parse value
            let mut value = String::new();
            if chars.peek() == Some(&'\'') {
                chars.next(); // skip '\''
                while let Some(c) = chars.next() {
                    if c == '\'' {
                        break;
                    } else if c == '\\' {
                        if let Some(escaped) = chars.next() {
                            value.push(escaped);
                        }
                    } else {
                        value.push(c);
                    }
                }
            } else {
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() {
                        break;
                    }
                    value.push(c);
                    chars.next();
                }
            }

            match key.as_str() {
                "host" | "port" | "dbname" | "user" => {
                    parts.push(format!("{key}={value}"));
                }
                _ => {}
            }
        } else {
            chars.next();
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

fn summarize_uri(dsn: &str) -> Option<String> {
    let scheme_end = dsn.find("://")?;
    let remainder = &dsn[scheme_end + 3..];

    // Separate the query/fragment from authority/path
    let (authority_and_path, _) = remainder.split_once(['?', '#']).unwrap_or((remainder, ""));

    // Separate authority from path
    let (authority, path) = authority_and_path
        .split_once('/')
        .unwrap_or((authority_and_path, ""));

    // Separate credentials from host:port
    let (credentials, host_port) = authority
        .rsplit_once('@')
        .map_or((None, authority), |(auth, host)| (Some(auth), host));

    let user = credentials.and_then(|auth| {
        auth.split_once(':')
            .map_or(Some(auth), |(name, _)| Some(name))
    });

    let mut parts = Vec::new();
    if !host_port.is_empty() {
        parts.push(format!("host={host_port}"));
    }

    let database = path.trim_start_matches('/');
    if !database.is_empty() {
        parts.push(format!("dbname={database}"));
    }

    if let Some(user) = user.filter(|name| !name.is_empty()) {
        parts.push(format!("user={user}"));
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::{ResolveDsnError, parse_pgpass, resolve_dsn_from_path};
    use std::{
        error::Error,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn test_explicit_dsn_takes_precedence() -> Result<(), Box<dyn Error>> {
        let path = PathBuf::from("/tmp/does-not-matter.pgpass");
        let resolved = resolve_dsn_from_path(Some("postgresql://localhost/postgres"), Some(path))?;
        assert_eq!(resolved, "postgresql://localhost/postgres");
        Ok(())
    }

    #[test]
    fn test_resolve_dsn_from_pgpass_first_entry() -> Result<(), Box<dyn Error>> {
        let path = write_temp_pgpass(
            "db.example.com:5432:metrics:postgres:s3cret\nlocalhost:*:*:*:fallback\n",
        )?;

        let resolved = resolve_dsn_from_path(None, Some(path.clone()))?;

        assert_eq!(
            resolved,
            "host='db.example.com' port='5432' dbname='metrics' user='postgres' password='s3cret'"
        );

        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn test_resolve_dsn_uses_wildcards_as_defaults() -> Result<(), Box<dyn Error>> {
        let path = write_temp_pgpass("*:*:*:postgres:secret\n")?;

        let resolved = resolve_dsn_from_path(None, Some(path.clone()))?;

        assert_eq!(resolved, "user='postgres' password='secret'");

        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn test_parse_pgpass_supports_escaped_colons() -> Result<(), Box<dyn Error>> {
        let entries = parse_pgpass(r"localhost:5432:metrics:postgres:pa\:ss\\word")?;

        assert_eq!(entries.len(), 1);
        let entry = entries.first().ok_or("expected one entry")?;
        assert_eq!(entry.password, r"pa:ss\word");
        Ok(())
    }

    #[test]
    fn test_missing_dsn_and_pgpass_returns_error() {
        let result = resolve_dsn_from_path(None, Some(PathBuf::from("/tmp/pgmon-missing.pgpass")));
        assert!(matches!(
            result,
            Err(ResolveDsnError::MissingConnectionSettings { .. })
        ));
    }

    #[test]
    fn test_describe_connection_target() {
        use super::describe_connection_target;

        assert_eq!(
            describe_connection_target(
                "postgresql://user:pass@localhost:5432/mydb?sslmode=disable"
            ),
            "host=localhost:5432 dbname=mydb user=user"
        );
        assert_eq!(
            describe_connection_target(
                "host=localhost port=5432 dbname=mydb user=postgres password=secret"
            ),
            "host=localhost port=5432 dbname=mydb user=postgres"
        );
        assert_eq!(
            describe_connection_target("host='my host' user=postgres"),
            "host=my host user=postgres"
        );
        assert_eq!(
            describe_connection_target("postgresql://localhost?target_session_attrs=read-write"),
            "host=localhost"
        );
        assert_eq!(
            describe_connection_target("postgresql:///dbname?host=/var/run/postgresql"),
            "dbname=dbname"
        );
    }

    #[test]
    fn test_describe_host() {
        use super::describe_host;

        assert_eq!(
            describe_host("postgresql://user:pass@localhost:5432/mydb?sslmode=disable"),
            "host=localhost:5432"
        );
        assert_eq!(
            describe_host("host=localhost port=5432 dbname=mydb user=postgres password=secret"),
            "host=localhost"
        );
        assert_eq!(
            describe_host("host='my host' user=postgres"),
            "host=my host"
        );
        assert_eq!(
            describe_host("postgresql://localhost?target_session_attrs=read-write"),
            "host=localhost"
        );
    }

    fn write_temp_pgpass(content: &str) -> Result<PathBuf, Box<dyn Error>> {
        let filename = format!(
            "pgmon-test-{}.pgpass",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let path = std::env::temp_dir().join(filename);
        fs::write(&path, content)?;
        Ok(path)
    }
}
