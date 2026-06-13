//! End-to-end: keyx validator wired into the gRPC interceptor.
//!
//! Spins up a `wiremock` server pretending to be keyx, points the
//! broker at it via `BrokerConfig::keyx_validator`, and drives a
//! real Client over the wire. Three contracts pinned here:
//!
//!   1. Keyx accepts the bearer → the RPC reaches the handler.
//!   2. Keyx rejects (HTTP 401) → the client gets
//!      `Unauthenticated` with a `keyx: …` prefix.
//!   3. Keyx is transiently down (HTTP 503) AND a local
//!      `auth_token` is configured → the local token still works
//!      (fallback path). Without a local token, the RPC fails.

use std::sync::Arc;
use std::time::Duration;

use qx_broker::{Broker, BrokerConfig, Client, KeyxValidator};
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn ok_envelope() -> serde_json::Value {
    serde_json::json!({
        "status": "SUCCESS",
        "responseMessage": "OK",
        "data": { "valid": true, "customer_urn": "cus_abc" },
    })
}

async fn start_broker(
    keyx_validator: Option<Arc<KeyxValidator>>,
    local_auth: Option<String>,
) -> (TempDir, Arc<Broker>, String) {
    let tmp = TempDir::new().unwrap();
    let cfg = BrokerConfig {
        auth_token: local_auth,
        keyx_validator,
        ..Default::default()
    };
    let broker = Broker::open_with_config(tmp.path(), cfg);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = broker.clone();
    tokio::spawn(async move {
        b.serve_with_listener(listener).await.ok();
    });
    (tmp, broker, format!("http://{addr}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn keyx_verdict_yes_lets_the_rpc_through() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/keys/verify"))
        .and(header("authorization", "Bearer qx_live_good"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_envelope()))
        .mount(&server)
        .await;

    let validator = Arc::new(KeyxValidator::new(server.uri()).unwrap());
    let (_tmp, _broker, url) = start_broker(Some(validator), None).await;

    let mut client = Client::connect_with_token(url, "qx_live_good".into())
        .await
        .unwrap();
    let count = client.open_topic("t", 1).await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn keyx_4xx_rejects_with_unauthenticated_status() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "status": "FAILED",
            "responseMessage": "revoked",
            "data": null,
        })))
        .mount(&server)
        .await;

    let validator = Arc::new(KeyxValidator::new(server.uri()).unwrap());
    let (_tmp, _broker, url) = start_broker(Some(validator), None).await;

    let mut client = Client::connect_with_token(url, "qx_live_revoked".into())
        .await
        .unwrap();
    let err = client.open_topic("t", 1).await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("Unauthenticated") && msg.contains("keyx"),
        "expected keyx-prefixed unauthenticated error, got {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn keyx_5xx_falls_back_to_local_auth_token() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503).set_body_string("down"))
        .mount(&server)
        .await;

    // Use a SHORT negative-cache TTL so successive misses each
    // re-hit keyx and re-trigger the fallback path.
    let validator = Arc::new(
        KeyxValidator::with_options(
            server.uri(),
            Duration::from_secs(60),
            Duration::from_millis(0),
        )
        .unwrap(),
    );
    let local = "qx_local_emergency".to_string();
    let (_tmp, _broker, url) =
        start_broker(Some(validator), Some(local.clone())).await;

    // The local token works even though keyx is down — the broker
    // explicitly falls back when keyx returns 5xx / network error.
    let mut client = Client::connect_with_token(url.clone(), local).await.unwrap();
    client.open_topic("t", 1).await.unwrap();

    // A different bearer is rejected (local string-eq says no).
    let mut wrong = Client::connect_with_token(url, "qx_some_other_key".into())
        .await
        .unwrap();
    let err = wrong.open_topic("t2", 1).await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("Unauthenticated"));
}

#[tokio::test(flavor = "multi_thread")]
async fn keyx_5xx_without_local_token_refuses_rather_than_silently_allowing() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503).set_body_string("down"))
        .mount(&server)
        .await;

    let validator = Arc::new(KeyxValidator::new(server.uri()).unwrap());
    let (_tmp, _broker, url) = start_broker(Some(validator), None).await;

    let mut client = Client::connect_with_token(url, "qx_live_xxx".into())
        .await
        .unwrap();
    let err = client.open_topic("t", 1).await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("auth backend unavailable") || msg.contains("Unauthenticated"),
        "expected auth-unavailable refusal, got {msg}"
    );
}
