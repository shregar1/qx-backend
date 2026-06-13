//! `Broker::update_workflow_overrides` — runtime swap test.
//!
//! qxd wires this method to SIGHUP so operators can re-tune
//! per-workflow `EngineOptions` without restarting the broker. The
//! contract is:
//!
//!   1. Workflows whose options changed have their cached `Engine`
//!      handle dropped — the next `get_or_open_workflow` reopens the
//!      on-disk engine with the new options.
//!   2. Workflows whose options didn't change are NOT disturbed.
//!   3. The return value lists exactly the workflows whose handles
//!      were dropped, so qxd can log a meaningful summary.

use std::collections::HashMap;

use qx_broker::{Broker, BrokerConfig};
use tempfile::TempDir;

#[test]
fn update_overrides_reports_changed_workflows() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::default();
    cfg.workflow_overrides.insert(
        "tiny".into(),
        qx_workflow::EngineOptions {
            encode_buf_capacity: 256,
            ..Default::default()
        },
    );
    cfg.workflow_overrides.insert(
        "big-blob".into(),
        qx_workflow::EngineOptions {
            encode_buf_capacity: 65536,
            ..Default::default()
        },
    );
    let broker = Broker::open_with_config(tmp.path(), cfg);

    // Mutating only "big-blob": expect exactly "big-blob" reported.
    let mut new_overrides = HashMap::new();
    new_overrides.insert(
        "tiny".into(),
        qx_workflow::EngineOptions {
            encode_buf_capacity: 256,
            ..Default::default()
        },
    );
    new_overrides.insert(
        "big-blob".into(),
        qx_workflow::EngineOptions {
            encode_buf_capacity: 131072,
            ..Default::default()
        },
    );
    let changed = broker.update_workflow_overrides(new_overrides);
    assert_eq!(changed, vec!["big-blob".to_string()]);
}

#[test]
fn update_overrides_reports_additions_and_removals() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::default();
    cfg.workflow_overrides.insert(
        "present-then-removed".into(),
        qx_workflow::EngineOptions::default(),
    );
    let broker = Broker::open_with_config(tmp.path(), cfg);

    // New map adds "new-arrival" and drops "present-then-removed".
    let mut new_overrides = HashMap::new();
    new_overrides.insert(
        "new-arrival".into(),
        qx_workflow::EngineOptions {
            encode_buf_capacity: 4096,
            ..Default::default()
        },
    );
    let mut changed = broker.update_workflow_overrides(new_overrides);
    changed.sort();
    assert_eq!(
        changed,
        vec!["new-arrival".to_string(), "present-then-removed".into()]
    );
}

#[test]
fn unchanged_overrides_return_empty() {
    let tmp = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::default();
    cfg.workflow_overrides.insert(
        "steady".into(),
        qx_workflow::EngineOptions {
            encode_buf_capacity: 2048,
            cancel_max_descendants: 1000,
            redb_cache_bytes: Some(1024 * 1024),
        },
    );
    let broker = Broker::open_with_config(tmp.path(), cfg.clone());

    let changed = broker.update_workflow_overrides(cfg.workflow_overrides.clone());
    assert!(changed.is_empty());
}

#[test]
fn changed_override_drops_cached_engine_handle() {
    // The whole point of the hot-swap is that a future
    // `get_or_open_workflow` opens the engine fresh with the new
    // options. We observe this via the broker's internal cache map —
    // after the swap, the workflow's engine is uncached, and only
    // reappears once a caller asks for it again.

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

    // Open the workflow once so the broker caches its Engine handle.
    let arc_before = broker.get_or_open_workflow_for_test("wf").unwrap();
    assert!(broker.workflow_is_cached_for_test("wf"));

    // Swap to a different EngineOptions for the same workflow name.
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
    // The cached handle was dropped from the broker's map even
    // though `arc_before` still keeps the underlying engine alive
    // for any in-flight caller that already holds it.
    assert!(
        !broker.workflow_is_cached_for_test("wf"),
        "changed override should evict the cached engine"
    );

    // Drop the in-flight Arc so the redb file lock is freed before
    // the broker tries to reopen against the same path.
    drop(arc_before);

    // Asking again repopulates the cache — this is the fresh open
    // that honours the new `EngineOptions`.
    let _arc_after = broker.get_or_open_workflow_for_test("wf").unwrap();
    assert!(broker.workflow_is_cached_for_test("wf"));
}
