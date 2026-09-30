//! Build-time facts about the TLS backend compiled into this binary.
//!
//! Surfaced in `pgmon --version` and `pgmon check-config` so that "can this
//! build speak TLS at all?" is answerable without a PostgreSQL server to try it
//! against. Releases up to 0.7.1 shipped with no backend and only revealed it on
//! a failed connection (<https://github.com/nbari/pgmon/issues/6>).
//!
//! `build.rs` guarantees exactly one backend feature is enabled, so the
//! unsupported arms below are unreachable in any build that compiles.

/// Name of the active TLS backend.
#[must_use]
pub const fn backend() -> &'static str {
    if cfg!(any(
        feature = "tls-rustls-ring",
        feature = "tls-rustls-ring-native-roots"
    )) {
        "rustls (ring)"
    } else {
        "none (TLS unsupported)"
    }
}

/// Where the backend looks for trusted root certificates.
#[must_use]
pub const fn root_store() -> &'static str {
    if cfg!(feature = "tls-rustls-ring-native-roots") {
        "host OS trust store"
    } else if cfg!(feature = "tls-rustls-ring") {
        "bundled Mozilla roots (webpki-roots)"
    } else {
        "none"
    }
}

/// One-line summary for `--version`.
#[must_use]
pub fn summary() -> String {
    format!("{}, {}", backend(), root_store())
}

#[cfg(test)]
mod tests {
    use super::{backend, root_store, summary};

    #[test]
    fn test_build_has_a_tls_backend() {
        assert_ne!(
            backend(),
            "none (TLS unsupported)",
            "pgmon must be built with a TLS backend; see build.rs and issue #6"
        );
        assert_ne!(root_store(), "none");
    }

    #[test]
    fn test_summary_joins_backend_and_root_store() {
        assert_eq!(summary(), format!("{}, {}", backend(), root_store()));
    }
}
