//! Cert rotation via `Broker::update_tls`.
//!
//! Drives the same path qxd's SIGHUP handler will use: start a
//! broker with one cert, prove a client trusting that cert can
//! connect, swap the live `ServerConfig` for a *different* self-
//! signed cert, then prove:
//!
//!   1. A client trusting the *new* cert can complete a TLS
//!      handshake against the broker.
//!   2. A client trusting the *old* cert is rejected at handshake
//!      time (trust anchor mismatch), confirming the server is
//!      now presenting the new cert on fresh connections.

use std::time::Duration;

use qx_broker::{Broker, BrokerConfig, TlsConfig, bind_with_retry};
use tempfile::TempDir;

fn mk_cert() -> (Vec<u8>, Vec<u8>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    (
        cert.cert.pem().into_bytes(),
        cert.key_pair.serialize_pem().into_bytes(),
    )
}

#[tokio::test]
async fn update_tls_rotates_cert_for_new_handshakes() {
    let _tmp = TempDir::new().unwrap();

    let (old_cert_pem, old_key_pem) = mk_cert();
    let (new_cert_pem, new_key_pem) = mk_cert();

    let mut cfg = BrokerConfig::default();
    cfg.tls = Some(TlsConfig {
        cert_pem: old_cert_pem.clone(),
        key_pem: old_key_pem.clone(),
        client_ca_pem: None,
    });
    let broker = Broker::open_with_config(_tmp.path(), cfg);

    let listener = bind_with_retry("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("https://localhost:{}", addr.port());

    let serve_task = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.serve_with_listener(listener).await })
    };

    // Give the listener a moment to bind, then verify the broker
    // presents the OLD cert: a client trusting old_cert handshakes
    // cleanly.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut client_old = qx_broker::Client::connect_tls(
        url.clone(),
        old_cert_pem.clone(),
        "localhost".into(),
    )
    .await
    .expect("client trusting old cert must connect pre-rotation");
    client_old.open_topic("pre-rotation", 1).await.unwrap();
    drop(client_old);

    // Rotate.
    assert!(broker.live_tls_present_for_test());
    broker
        .update_tls(Some(&TlsConfig {
            cert_pem: new_cert_pem.clone(),
            key_pem: new_key_pem.clone(),
            client_ca_pem: None,
        }))
        .expect("rotation with valid PEM must succeed");
    assert!(broker.live_tls_present_for_test());

    // A client trusting NEW cert handshakes successfully.
    let mut client_new = qx_broker::Client::connect_tls(
        url.clone(),
        new_cert_pem.clone(),
        "localhost".into(),
    )
    .await
    .expect("client trusting new cert must connect post-rotation");
    client_new.open_topic("post-rotation", 1).await.unwrap();
    drop(client_new);

    // A client trusting only the OLD cert is rejected at handshake.
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        qx_broker::Client::connect_tls(url.clone(), old_cert_pem.clone(), "localhost".into()),
    )
    .await
    .expect("timeout waiting for handshake to fail");
    assert!(
        res.is_err(),
        "client trusting old cert must NOT connect after rotation"
    );

    // Cleanly shut the broker down so the spawned task returns.
    broker.shutdown().await;
    let _ = serve_task.await;
}

#[tokio::test]
async fn update_tls_rejects_malformed_pem_and_keeps_previous_cert() {
    // A malformed PEM rotation attempt must NOT clobber the live
    // config — qxd's SIGHUP loop relies on this to log the parse
    // failure and keep the listener up.
    let _tmp = TempDir::new().unwrap();

    let (cert_pem, key_pem) = mk_cert();
    let mut cfg = BrokerConfig::default();
    cfg.tls = Some(TlsConfig {
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
    });
    let broker = Broker::open_with_config(_tmp.path(), cfg);

    assert!(broker.live_tls_present_for_test());

    let err = broker
        .update_tls(Some(&TlsConfig {
            cert_pem: b"not a cert".to_vec(),
            key_pem: b"not a key".to_vec(),
            client_ca_pem: None,
        }))
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("Io"), "expected an Io error variant, got {msg}");

    // Still present — the rejected swap must leave the previous
    // config in place.
    assert!(broker.live_tls_present_for_test());
}
