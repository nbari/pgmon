//! Process logging.
//!
//! The TUI owns the terminal, so tracing output is never written there. Verbose
//! runs, `RUST_LOG`, and `PGMON_LOG` append to a file and the path is printed
//! when `pgmon` exits. Quiet TUI runs install no subscriber. Commands that do
//! not start the TUI still log to stderr.

use anyhow::{Context, Result};
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::sync::Mutex;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Where this process sends tracing output.
#[derive(Debug)]
pub struct Telemetry {
    log_path: Option<PathBuf>,
}

impl Telemetry {
    /// Log file used for this process, when tracing was redirected off the terminal.
    #[must_use]
    pub fn log_path(&self) -> Option<&std::path::Path> {
        self.log_path.as_deref()
    }
}

/// Install the process tracing subscriber.
///
/// `protect_terminal` is set for the TUI. In that mode the terminal is never
/// used as a log writer.
///
/// # Errors
///
/// Returns an error when a log file is required and cannot be created.
pub fn init(verbosity: u8, protect_terminal: bool) -> Result<Telemetry> {
    if protect_terminal && captures_logs(verbosity) {
        let path = resolve_log_path();
        install_file_subscriber(&path, verbosity)?;
        tracing::info!("pgmon log file opened");
        return Ok(Telemetry {
            log_path: Some(path),
        });
    }

    if !protect_terminal {
        install_stderr_subscriber(verbosity);
    }

    Ok(Telemetry { log_path: None })
}

fn captures_logs(verbosity: u8) -> bool {
    verbosity > 0 || env_is_set("RUST_LOG") || env_is_set("PGMON_LOG")
}

fn env_is_set(key: &str) -> bool {
    std::env::var_os(key).is_some_and(|value| !value.is_empty())
}

fn resolve_log_path() -> PathBuf {
    log_file_path(
        std::env::var_os("PGMON_LOG").map(PathBuf::from),
        dirs::state_dir().or_else(dirs::cache_dir),
    )
}

fn log_file_path(explicit: Option<PathBuf>, state_dir: Option<PathBuf>) -> PathBuf {
    if let Some(path) = explicit.filter(|path| !path.as_os_str().is_empty()) {
        return path;
    }
    let base = state_dir.unwrap_or_else(std::env::temp_dir);
    base.join("pgmon").join("pgmon.log")
}

fn install_file_subscriber(path: &std::path::Path, verbosity: u8) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create log directory: {}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open log file: {}", path.display()))?;
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(Mutex::new(file)).with_ansi(false))
        .with(filter(verbosity))
        .init();
    Ok(())
}

fn install_stderr_subscriber(verbosity: u8) {
    // `fmt::layer()` writes to stdout unless told otherwise, which would mix
    // tracing into `check-config`'s report.
    tracing_subscriber::registry()
        .with(fmt::layer().with_writer(std::io::stderr))
        .with(filter(verbosity))
        .init();
}

fn filter(verbosity: u8) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level(verbosity)))
}

fn level(verbosity: u8) -> &'static str {
    match verbosity {
        0 => "info",
        1 => "debug",
        _ => "trace",
    }
}

#[cfg(test)]
mod tests {
    use super::log_file_path;
    use std::fs::{self, OpenOptions};
    use std::path::PathBuf;
    use std::sync::Mutex;
    use tracing_subscriber::{fmt, prelude::*};

    #[test]
    fn test_log_file_path_uses_explicit_path() {
        let path = log_file_path(Some(PathBuf::from("/tmp/pgmon.log")), None);
        assert_eq!(path, PathBuf::from("/tmp/pgmon.log"));
    }

    #[test]
    fn test_log_file_path_ignores_empty_explicit_path() {
        let path = log_file_path(Some(PathBuf::new()), Some(PathBuf::from("/state")));
        assert_eq!(path, PathBuf::from("/state/pgmon/pgmon.log"));
    }

    #[test]
    fn test_log_file_path_falls_back_to_temp_dir() {
        let path = log_file_path(None, None);
        assert_eq!(path, std::env::temp_dir().join("pgmon").join("pgmon.log"));
    }

    #[test]
    fn test_file_layer_writes_plain_text() {
        let path = std::env::temp_dir().join(format!("pgmon-log-probe-{}", std::process::id()));
        let open_result = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path);
        assert!(open_result.is_ok());
        let Ok(file) = open_result else {
            return;
        };

        let subscriber = tracing_subscriber::registry()
            .with(fmt::layer().with_writer(Mutex::new(file)).with_ansi(false));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("pgmon log probe");
        });

        let contents = fs::read_to_string(&path);
        let _ = fs::remove_file(&path);
        assert!(contents.is_ok());
        let Ok(contents) = contents else {
            return;
        };
        assert!(contents.contains("pgmon log probe"));
        assert!(!contents.contains('\u{1b}'));
    }
}
