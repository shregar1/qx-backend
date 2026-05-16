//! Black-box tests for the schema registry.

use qx_schema_registry::{Registry, RegistryError};
use tempfile::tempdir;

fn open() -> (tempfile::TempDir, Registry) {
    let tmp = tempdir().unwrap();
    let reg = Registry::open(tmp.path().join("reg.redb")).unwrap();
    (tmp, reg)
}

#[test]
fn register_returns_version_1_for_new_subject() {
    let (_tmp, reg) = open();
    let s = reg.register("orders.value", b"{\"name\":\"order_v1\"}").unwrap();
    assert_eq!(s.subject, "orders.value");
    assert_eq!(s.version, 1);
    assert_eq!(s.bytes, b"{\"name\":\"order_v1\"}");
}

#[test]
fn register_increments_version_per_subject() {
    let (_tmp, reg) = open();
    let v1 = reg.register("orders.value", b"v1").unwrap();
    let v2 = reg.register("orders.value", b"v2").unwrap();
    assert_eq!(v1.version, 1);
    assert_eq!(v2.version, 2);
    assert_ne!(v1.id, v2.id);
}

#[test]
fn schema_ids_are_globally_unique() {
    let (_tmp, reg) = open();
    let a = reg.register("alpha", b"x").unwrap();
    let b = reg.register("beta", b"y").unwrap();
    assert_ne!(a.id, b.id);
}

#[test]
fn get_by_id_returns_the_registered_schema() {
    let (_tmp, reg) = open();
    let s = reg.register("orders.value", b"payload").unwrap();
    let fetched = reg.get_by_id(s.id).unwrap();
    assert_eq!(fetched, s);
}

#[test]
fn get_version_returns_the_corresponding_schema() {
    let (_tmp, reg) = open();
    reg.register("orders.value", b"v1").unwrap();
    let v2 = reg.register("orders.value", b"v2").unwrap();
    let fetched = reg.get_version("orders.value", 2).unwrap();
    assert_eq!(fetched, v2);
}

#[test]
fn get_latest_returns_the_highest_version() {
    let (_tmp, reg) = open();
    reg.register("orders.value", b"v1").unwrap();
    reg.register("orders.value", b"v2").unwrap();
    let v3 = reg.register("orders.value", b"v3").unwrap();
    let latest = reg.get_latest("orders.value").unwrap();
    assert_eq!(latest, v3);
}

#[test]
fn list_versions_returns_all_versions_in_order() {
    let (_tmp, reg) = open();
    reg.register("s", b"a").unwrap();
    reg.register("s", b"b").unwrap();
    reg.register("s", b"c").unwrap();
    assert_eq!(reg.list_versions("s").unwrap(), vec![1, 2, 3]);
}

#[test]
fn unknown_subject_returns_subject_not_found() {
    let (_tmp, reg) = open();
    assert!(matches!(
        reg.get_latest("missing"),
        Err(RegistryError::SubjectNotFound(_))
    ));
    assert!(matches!(
        reg.get_version("missing", 1),
        Err(RegistryError::SubjectNotFound(_))
    ));
}

#[test]
fn missing_version_returns_version_not_found() {
    let (_tmp, reg) = open();
    reg.register("s", b"a").unwrap();
    assert!(matches!(
        reg.get_version("s", 99),
        Err(RegistryError::VersionNotFound { .. })
    ));
}

#[test]
fn list_subjects_returns_all_registered() {
    let (_tmp, reg) = open();
    reg.register("alpha", b"a").unwrap();
    reg.register("beta", b"b").unwrap();
    reg.register("alpha", b"a2").unwrap();
    let mut subjects = reg.list_subjects().unwrap();
    subjects.sort();
    assert_eq!(subjects, vec!["alpha".to_string(), "beta".to_string()]);
}

#[test]
fn reopen_preserves_schemas() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("reg.redb");
    let original_id = {
        let reg = Registry::open(&path).unwrap();
        reg.register("orders.value", b"v1").unwrap().id
    };
    let reg = Registry::open(&path).unwrap();
    let fetched = reg.get_by_id(original_id).unwrap();
    assert_eq!(fetched.bytes, b"v1");
    // Next register on same subject should be version 2
    let v2 = reg.register("orders.value", b"v2").unwrap();
    assert_eq!(v2.version, 2);
}
