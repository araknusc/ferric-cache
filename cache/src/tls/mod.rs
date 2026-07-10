use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use rustls::{ServerConfig, pki_types::{CertificateDer, PrivateKeyDer}};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use tokio_rustls::TlsAcceptor;

use crate::config::TlsConfig;

pub fn create_tls_acceptor(config: &TlsConfig) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    if !config.enabled {
        return Err("TLS is not enabled in configuration".into());
    }

    let cert_file = config.cert_file.as_ref()
        .ok_or("TLS cert_file is required when TLS is enabled")?;
    let key_file = config.key_file.as_ref()
        .ok_or("TLS key_file is required when TLS is enabled")?;

    // Load certificates
    let certs = load_certs(cert_file)?;
    let key = load_private_key(key_file)?;

    // Build TLS configuration
    let tls_config = ServerConfig::builder();

    // Configure client certificate verification if required
    let tls_config = if config.require_client_cert {
        if let Some(ca_file) = &config.ca_file {
            let ca_certs = load_certs(ca_file)?;
            let mut root_cert_store = rustls::RootCertStore::empty();
            for cert in ca_certs {
                root_cert_store.add(cert)?;
            }

            tls_config
                .with_client_cert_verifier(rustls::server::WebPkiClientVerifier::builder(root_cert_store.into())
                    .build()?)
                .with_single_cert(certs, key)?
        } else {
            return Err("CA file required when client certificate verification is enabled".into());
        }
    } else {
        tls_config
            .with_no_client_auth()
            .with_single_cert(certs, key)?
    };

    Ok(TlsAcceptor::from(Arc::new(tls_config)))
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, Box<dyn std::error::Error>> {
    let certfile = File::open(path)?;
    let mut reader = BufReader::new(certfile);

    let certs = certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(certs)
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    let keyfile = File::open(path)?;
    let mut reader = BufReader::new(keyfile);

    // Try PKCS#8 format first
    let keys: Vec<_> = pkcs8_private_keys(&mut reader)
        .collect::<Result<Vec<_>, _>>()?;
    if !keys.is_empty() {
        return Ok(PrivateKeyDer::Pkcs8(keys[0].clone_key()));
    }

    // Try RSA format
    let keyfile = File::open(path)?;
    let mut reader = BufReader::new(keyfile);
    let keys: Vec<_> = rsa_private_keys(&mut reader)
        .collect::<Result<Vec<_>, _>>()?;
    if !keys.is_empty() {
        return Ok(PrivateKeyDer::Pkcs1(keys[0].clone_key()));
    }

    Err("Could not load private key from file".into())
}

// Helper function to generate self-signed certificates for testing
pub fn generate_self_signed_cert() -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
    // This would use a crate like rcgen to generate certificates
    // For now, return placeholder
    Err("Certificate generation not implemented - please provide certificate files".into())
}