fn main() {
    assert_tls_backend_selected();

    if let Err(e) = built::write_built_file() {
        eprintln!("Failed to acquire build-time information: {e}");
        std::process::exit(1);
    }
}

/// Fail the build unless exactly one TLS backend feature is enabled.
///
/// pgmon 0.7.1 and earlier shipped without any TLS backend because the `sqlx`
/// dependency set `default-features = false` and never re-enabled one, so every
/// released binary refused `sslmode=require` at runtime. Nothing in the test
/// suite can catch that, so it is pinned here instead.
fn assert_tls_backend_selected() {
    let webpki_roots = std::env::var_os("CARGO_FEATURE_TLS_RUSTLS_RING").is_some();
    let native_roots = std::env::var_os("CARGO_FEATURE_TLS_RUSTLS_RING_NATIVE_ROOTS").is_some();

    match (webpki_roots, native_roots) {
        (false, false) => println!(
            "cargo::error=no TLS backend selected: enable exactly one of the \
             `tls-rustls-ring` or `tls-rustls-ring-native-roots` features. \
             Building without one produces a pgmon that cannot connect to a \
             PostgreSQL server requiring TLS."
        ),
        (true, true) => println!(
            "cargo::error=both `tls-rustls-ring` and `tls-rustls-ring-native-roots` \
             are enabled; they select conflicting root certificate stores. \
             Enable exactly one."
        ),
        _ => {}
    }
}
