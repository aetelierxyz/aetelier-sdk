#[test]
fn rustls_resolves_one_provider_from_crate_features_alone() {
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    let _ = rustls::ClientConfig::builder();
    let resolved = rustls::crypto::CryptoProvider::get_default().expect("resolved");
    assert!(
        resolved.cipher_suites == rustls::crypto::ring::default_provider().cipher_suites
    );
}
