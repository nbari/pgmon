//! Build-time facts about the TLS backend compiled into this binary.
//!
//! Surfaced in `pgmon --version` and `pgmon check-config` so that "can this
//! build speak TLS at all?" is answerable without a PostgreSQL server to try it
//! against. Releases up to 0.7.1 shipped with no backend and only revealed it on
//! a failed connection (<https://github.com/nbari/pgmon/issues/6>).
//!
//! At least one backend feature must be enabled; the `compile_error!` below
//! enforces that. Enabling both is allowed so `--all-features` builds keep
//! working: sqlx then loads the host OS trust store, and [`root_store`] reports
//! the same precedence.

#[cfg(not(any(feature = "tls-rustls-ring", feature = "tls-rustls-ring-native-roots")))]
compile_error!(
    "no TLS backend selected: enable the `tls-rustls-ring` or \
     `tls-rustls-ring-native-roots` feature. Building without one produces a \
     pgmon that cannot connect to a PostgreSQL server requiring TLS \
     (https://github.com/nbari/pgmon/issues/6)."
);

/// Name of the active TLS backend.
#[must_use]
pub const fn backend() -> &'static str {
    "rustls (ring)"
}

/// Where the backend looks for trusted root certificates.
///
/// Mirrors sqlx, which prefers the host OS trust store when both TLS features
/// are enabled.
#[must_use]
pub const fn root_store() -> &'static str {
    if cfg!(feature = "tls-rustls-ring-native-roots") {
        "host OS trust store"
    } else {
        "bundled Mozilla roots (webpki-roots)"
    }
}

/// One-line summary for `--version`.
#[must_use]
pub fn summary() -> String {
    format!("{}, {}", backend(), root_store())
}

#[cfg(test)]
mod tests {
    use super::{root_store, summary};

    #[test]
    fn test_summary_names_backend_then_root_store() {
        assert_eq!(summary(), format!("rustls (ring), {}", root_store()));
    }

    #[test]
    #[cfg(not(feature = "tls-rustls-ring-native-roots"))]
    fn test_root_store_defaults_to_bundled_roots() {
        assert_eq!(root_store(), "bundled Mozilla roots (webpki-roots)");
    }

    #[test]
    #[cfg(feature = "tls-rustls-ring-native-roots")]
    fn test_root_store_prefers_host_store_like_sqlx() {
        assert_eq!(root_store(), "host OS trust store");
    }
}
