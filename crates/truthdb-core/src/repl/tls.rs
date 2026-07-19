//! TLS for the replication transport. TLS is mandatory: it authenticates the
//! primary to the standby (the standby verifies the primary's certificate) and
//! encrypts the channel the shared-secret handshake and the WAL stream travel
//! over. This duplicates the small rustls `ServerConfig`/`ClientConfig` build
//! from `truthdb-tds` (which cannot be depended on here — it depends on this
//! crate); unlike the TDS path there is no PRELOGIN tunnelling, so the listener
//! uses a raw `tokio_rustls` accept.

use std::io;
use std::sync::Arc;

use rustls::pki_types::CertificateDer;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

/// Builds the primary's TLS `ServerConfig` from a certificate + private key PEM.
pub fn server_config_from_pem(cert_pem: &[u8], key_pem: &[u8]) -> io::Result<Arc<ServerConfig>> {
    let certs = load_certs(cert_pem)?;
    let key = rustls_pemfile::private_key(&mut &key_pem[..])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad private key: {e}")))?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "no private key in the TLS key PEM",
            )
        })?;
    let config =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| io::Error::other(format!("tls server config: {e}")))?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("tls server config: {e}"),
                )
            })?;
    Ok(Arc::new(config))
}

/// Builds a standby's TLS `ClientConfig` that trusts exactly the given
/// certificate(s) as roots — the primary's self-signed (or CA) cert. The standby
/// verifies the primary against this, so a wrong endpoint fails the handshake.
pub fn client_config_trusting(ca_pem: &[u8]) -> io::Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(ca_pem)? {
        roots
            .add(cert)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("add root: {e}")))?;
    }
    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| io::Error::other(format!("tls client config: {e}")))?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(Arc::new(config))
}

fn load_certs(pem: &[u8]) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs = rustls_pemfile::certs(&mut &pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad certificate: {e}")))?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no certificate in the TLS PEM",
        ));
    }
    Ok(certs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repl::framing::{read_repl_frame, write_repl_frame};
    use crate::repl::{Hello, REPL_PROTOCOL_VERSION, ReplFrame, ReplMsgType};

    /// Generates a self-signed `localhost` cert/key PEM for the integration test.
    fn self_signed() -> (String, String) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate cert");
        (cert.cert.pem(), cert.key_pair.serialize_pem())
    }

    /// A replication frame round-trips over a REAL localhost TCP + TLS connection:
    /// the standby trusts the primary's self-signed cert, completes the TLS
    /// handshake, and the frame survives the encrypted channel. This is the
    /// harness foundation the listener/sender/receiver slices reuse.
    #[tokio::test]
    async fn a_repl_frame_round_trips_over_real_tls() {
        let (cert_pem, key_pem) = self_signed();
        let server_config =
            server_config_from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).expect("server config");
        let client_config = client_config_trusting(cert_pem.as_bytes()).expect("client config");
        let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
        let connector = tokio_rustls::TlsConnector::from(client_config);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Primary side: accept one connection, TLS-wrap it, echo one frame.
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.expect("server tls handshake");
            let frame = read_repl_frame(&mut tls).await.unwrap();
            write_repl_frame(&mut tls, &frame).await.unwrap();
        });

        // Standby side: connect, verify the cert, TLS handshake, send + read back.
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let domain = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = connector
            .connect(domain, tcp)
            .await
            .expect("client tls handshake");

        let hello = Hello {
            protocol_version: REPL_PROTOCOL_VERSION,
            node_id: 1,
            cluster_uuid: [2u8; 16],
            epoch: 0,
            last_received_lsn: 4096,
            auth: vec![9, 9],
        };
        let sent = ReplFrame::encode(ReplMsgType::Hello, &hello).unwrap();
        write_repl_frame(&mut tls, &sent).await.unwrap();
        let got = read_repl_frame(&mut tls).await.unwrap();
        assert_eq!(got.msg_type, ReplMsgType::Hello);
        let decoded: Hello = got.decode().unwrap();
        assert_eq!(decoded.node_id, 1);
        assert_eq!(decoded.last_received_lsn, 4096);

        server.await.unwrap();
    }

    /// A standby that does NOT trust the primary's cert fails the TLS handshake —
    /// TLS is authenticating the primary, not just encrypting.
    #[tokio::test]
    async fn an_untrusted_cert_is_rejected() {
        let (cert_pem, key_pem) = self_signed();
        let (other_cert_pem, _) = self_signed(); // a different, untrusted cert
        let acceptor = tokio_rustls::TlsAcceptor::from(
            server_config_from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap(),
        );
        // The client trusts `other_cert`, not the server's actual cert.
        let connector = tokio_rustls::TlsConnector::from(
            client_config_trusting(other_cert_pem.as_bytes()).unwrap(),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((tcp, _)) = listener.accept().await {
                let _ = acceptor.accept(tcp).await; // handshake will fail
            }
        });

        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let domain = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        assert!(
            connector.connect(domain, tcp).await.is_err(),
            "an untrusted primary cert must fail the standby's TLS verification"
        );
    }
}
