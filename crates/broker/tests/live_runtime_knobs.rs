//! Live mutability for the broker knobs qx's SIGHUP handler
//! rewires at runtime:
//!
//!   * `update_auth_token`        — bearer token rotation
//!   * `update_workflow_defaults` — broker-wide engine defaults
//!   * `warm_workflow`            — eager reopen after override evict
//!
//! These are unit-level checks. The end-to-end qx subprocess HUP
//! integration lives separately under `qx/tests`.

use std::collections::HashMap;

use qx_broker::{Broker, BrokerConfig};
use tempfile::TempDir;

#[test]
fn update_auth_token_rotates_then_disables() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::default();
    cfg.auth_token = Some("initial".into());
    let broker = Broker::open_with_config(tmp.path(), cfg);

    // Rotate: a different token replaces the initial one.
    broker.update_auth_token(Some("rotated".into()));
    assert_eq!(broker.live_auth_token_for_test(), Some("rotated".into()));

    // Disable: None turns auth off entirely.
    broker.update_auth_token(None);
    assert_eq!(broker.live_auth_token_for_test(), None);
}

#[test]
fn update_workflow_defaults_evicts_default_users_only() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::default();
    cfg.workflow_options = Some(qx_workflow::EngineOptions {
        encode_buf_capacity: 512,
        ..Default::default()
    });
    // "pinned" has a per-name override; "drifter" rides the defaults.
    cfg.workflow_overrides.insert(
        "pinned".into(),
        qx_workflow::EngineOptions {
            encode_buf_capacity: 8192,
            ..Default::default()
        },
    );
    let broker = Broker::open_with_config(tmp.path(), cfg);

    // Cache both workflows so the upcoming swap has something to act on.
    let pinned = broker.get_or_open_workflow_for_test("pinned").unwrap();
    let drifter = broker.get_or_open_workflow_for_test("drifter").unwrap();
    assert!(broker.workflow_is_cached_for_test("pinned"));
    assert!(broker.workflow_is_cached_for_test("drifter"));
    // Drop our local Arcs so warm-style reopens (and the contract
    // that the engine is reopen-able after eviction) aren't blocked
    // by redb's per-file lock.
    drop(pinned);
    drop(drifter);

    let mut evicted = broker.update_workflow_defaults(Some(qx_workflow::EngineOptions {
        encode_buf_capacity: 65_536,
        ..Default::default()
    }));
    evicted.sort();
    assert_eq!(
        evicted,
        vec!["drifter".to_string()],
        "only workflows opened under the default should be evicted"
    );
    assert!(
        broker.workflow_is_cached_for_test("pinned"),
        "override-pinned engine must survive a defaults change"
    );
    assert!(
        !broker.workflow_is_cached_for_test("drifter"),
        "default-using engine must be evicted by a defaults change"
    );

    // The evicted workflow re-opens cleanly under the new defaults.
    let _drifter_again = broker.get_or_open_workflow_for_test("drifter").unwrap();
    assert!(broker.workflow_is_cached_for_test("drifter"));
}

#[test]
fn update_workflow_defaults_returns_empty_when_nothing_cached() {
    // No engines cached → no evictions; the live default still gets
    // swapped so a *future* open uses the new value, but the return
    // value is empty (nothing to warm).
    let tmp = TempDir::new().unwrap();
    let broker = Broker::open_with_config(tmp.path(), BrokerConfig::default());
    let evicted = broker.update_workflow_defaults(Some(qx_workflow::EngineOptions {
        encode_buf_capacity: 4096,
        ..Default::default()
    }));
    assert!(evicted.is_empty());
}

#[test]
fn warm_workflow_reopens_after_override_evict() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::default();
    cfg.workflow_overrides.insert(
        "wf".into(),
        qx_workflow::EngineOptions {
            encode_buf_capacity: 256,
            ..Default::default()
        },
    );
    let broker = Broker::open_with_config(tmp.path(), cfg);

    // Make sure the engine is cached, then drop the local reference
    // so warm_workflow's reopen can take the redb file lock.
    let arc_before = broker.get_or_open_workflow_for_test("wf").unwrap();
    assert!(broker.workflow_is_cached_for_test("wf"));
    drop(arc_before);

    // Hot-swap with a different option — this evicts the cached
    // engine (the contract under test in workflow_overrides_hot_swap).
    let mut new_overrides = HashMap::new();
    new_overrides.insert(
        "wf".into(),
        qx_workflow::EngineOptions {
            encode_buf_capacity: 4096,
            ..Default::default()
        },
    );
    let changed = broker.update_workflow_overrides(new_overrides);
    assert_eq!(changed, vec!["wf".to_string()]);
    assert!(
        !broker.workflow_is_cached_for_test("wf"),
        "override change must evict the cached engine"
    );

    // warm_workflow rebuilds the cache so the next hot-path caller
    // doesn't pay the open cost.
    assert!(broker.warm_workflow("wf"), "warm should succeed when lock is free");
    assert!(
        broker.workflow_is_cached_for_test("wf"),
        "warm_workflow must repopulate the cache"
    );
}

#[test]
fn warm_workflow_is_idempotent() {
    // A second warm_workflow call should still succeed (no-op) when
    // the engine is already cached — qx may invoke warm_workflow
    // for a name that was already warm.
    let tmp = TempDir::new().unwrap();
    let broker = Broker::open_with_config(tmp.path(), BrokerConfig::default());

    assert!(broker.warm_workflow("idempotent"));
    assert!(broker.workflow_is_cached_for_test("idempotent"));
    assert!(broker.warm_workflow("idempotent"));
    assert!(broker.workflow_is_cached_for_test("idempotent"));
}
