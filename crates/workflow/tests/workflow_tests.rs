//! Black-box tests for `Engine`. Covers task creation, status transitions,
//! parent/child relationships, recursive cancellation, and recovery.

use qx_workflow::{Engine, TaskStatus, WorkflowError};
use tempfile::tempdir;

fn engine_in(tmp: &tempfile::TempDir) -> Engine {
    Engine::open(tmp.path().join("wf.redb")).unwrap()
}

#[test]
fn create_root_returns_task_with_no_parent() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let id = e.create_root(b"do thing").unwrap();
    let t = e.get(id).unwrap().unwrap();
    assert_eq!(t.id, id);
    assert!(t.parent_id.is_none());
    assert_eq!(t.status, TaskStatus::Pending);
    assert_eq!(&t.payload[..], b"do thing");
}

#[test]
fn create_root_idempotent_returns_same_id_for_same_key() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let id1 = e.create_root_idempotent(b"key-a", b"payload-1").unwrap();
    let id2 = e.create_root_idempotent(b"key-a", b"payload-2").unwrap();
    assert_eq!(id1, id2);
    // Second call must not have overwritten the first payload.
    let t = e.get(id1).unwrap().unwrap();
    assert_eq!(&t.payload[..], b"payload-1");
}

#[test]
fn create_root_idempotent_different_keys_get_different_ids() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let a = e.create_root_idempotent(b"a", b"x").unwrap();
    let b = e.create_root_idempotent(b"b", b"y").unwrap();
    assert_ne!(a, b);
}

#[test]
fn complete_with_emits_stages_outbox_entries() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let id = e.create_root(b"input").unwrap();
    let ids = e
        .complete_with_emits(
            id,
            b"done".to_vec(),
            &[
                ("topic-a", bytes::Bytes::from_static(b"emit-1")),
                ("topic-b", bytes::Bytes::from_static(b"emit-2")),
            ],
        )
        .unwrap();
    assert_eq!(ids.len(), 2);

    let pending = e.pending_emits().unwrap();
    assert_eq!(pending.len(), 2);
    let topics: Vec<&str> = pending.iter().map(|p| p.target_topic.as_str()).collect();
    assert!(topics.contains(&"topic-a"));
    assert!(topics.contains(&"topic-b"));

    // Marking one delivered shrinks the outbox.
    assert!(e.mark_emit_delivered(ids[0]).unwrap());
    let pending = e.pending_emits().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].emit_id, ids[1]);
}

#[test]
fn complete_with_emits_is_atomic() {
    // If we complete with emits and then reopen, both the completed state
    // and the outbox entry must be visible — neither can land without the
    // other.
    let tmp = tempdir().unwrap();
    let id = {
        let mut e = engine_in(&tmp);
        let id = e.create_root(b"in").unwrap();
        e.complete_with_emits(
            id,
            b"out".to_vec(),
            &[("t", bytes::Bytes::from_static(b"p"))],
        )
        .unwrap();
        id
    };
    let e = engine_in(&tmp);
    let t = e.get(id).unwrap().unwrap();
    assert_eq!(t.status, qx_workflow::TaskStatus::Done);
    assert_eq!(e.pending_emits().unwrap().len(), 1);
}

#[test]
fn create_root_idempotent_survives_reopen() {
    let tmp = tempdir().unwrap();
    let id_before = {
        let mut e = engine_in(&tmp);
        e.create_root_idempotent(b"key", b"once").unwrap()
    };
    let mut e = engine_in(&tmp);
    let id_after = e.create_root_idempotent(b"key", b"twice").unwrap();
    assert_eq!(id_before, id_after);
    assert_eq!(&e.get(id_after).unwrap().unwrap().payload[..], b"once");
}

#[test]
fn create_child_links_to_parent() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let root = e.create_root(b"root").unwrap();
    let child = e.create_child(root, b"child").unwrap();

    let kids = e.children(root).unwrap();
    assert_eq!(kids, vec![child]);

    let t = e.get(child).unwrap().unwrap();
    assert_eq!(t.parent_id, Some(root));
}

#[test]
fn task_ids_are_sequential_per_engine() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let a = e.create_root(b"a").unwrap();
    let b = e.create_root(b"b").unwrap();
    let c = e.create_child(a, b"c").unwrap();
    assert!(b > a);
    assert!(c > b);
}

#[test]
fn mark_running_transitions_status() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let id = e.create_root(b"work").unwrap();
    e.mark_running(id).unwrap();
    assert_eq!(e.get(id).unwrap().unwrap().status, TaskStatus::Running);
}

#[test]
fn complete_stores_result() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let id = e.create_root(b"work").unwrap();
    e.complete(id, b"answer".to_vec()).unwrap();
    let t = e.get(id).unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Done);
    assert_eq!(t.result.as_deref(), Some(b"answer".as_slice()));
}

#[test]
fn fail_stores_error_message() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let id = e.create_root(b"work").unwrap();
    e.fail(id, "timeout".into()).unwrap();
    let t = e.get(id).unwrap().unwrap();
    assert_eq!(t.status, TaskStatus::Failed);
    assert_eq!(t.error_str().and_then(|r| r.ok()), Some("timeout"));
}

#[test]
fn complete_on_terminal_task_returns_error() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let id = e.create_root(b"work").unwrap();
    e.complete(id, b"done".to_vec()).unwrap();
    let err = e.complete(id, b"redo".to_vec()).unwrap_err();
    assert!(matches!(err, WorkflowError::TerminalTask { .. }));
}

#[test]
fn cancel_single_task_no_children() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let id = e.create_root(b"work").unwrap();
    assert_eq!(e.cancel(id).unwrap(), 1);
    assert_eq!(e.get(id).unwrap().unwrap().status, TaskStatus::Cancelled);
}

#[test]
fn cancel_propagates_to_descendants_recursively() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let root = e.create_root(b"root").unwrap();
    let c1 = e.create_child(root, b"c1").unwrap();
    let c2 = e.create_child(root, b"c2").unwrap();
    let gc = e.create_child(c1, b"gc").unwrap();

    let cancelled = e.cancel(root).unwrap();
    assert_eq!(cancelled, 4);
    assert_eq!(e.get(root).unwrap().unwrap().status, TaskStatus::Cancelled);
    assert_eq!(e.get(c1).unwrap().unwrap().status, TaskStatus::Cancelled);
    assert_eq!(e.get(c2).unwrap().unwrap().status, TaskStatus::Cancelled);
    assert_eq!(e.get(gc).unwrap().unwrap().status, TaskStatus::Cancelled);
}

#[test]
fn cancel_skips_already_terminal_descendants() {
    let tmp = tempdir().unwrap();
    let mut e = engine_in(&tmp);
    let root = e.create_root(b"root").unwrap();
    let done_child = e.create_child(root, b"done").unwrap();
    let pending_child = e.create_child(root, b"pending").unwrap();

    e.complete(done_child, b"finished".to_vec()).unwrap();
    let cancelled = e.cancel(root).unwrap();
    // root + pending_child = 2; done_child preserved
    assert_eq!(cancelled, 2);
    assert_eq!(e.get(done_child).unwrap().unwrap().status, TaskStatus::Done);
    assert_eq!(
        e.get(pending_child).unwrap().unwrap().status,
        TaskStatus::Cancelled
    );
}

#[test]
fn reopen_preserves_task_state() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("wf.redb");
    let (root, child) = {
        let mut e = Engine::open(&path).unwrap();
        let root = e.create_root(b"root").unwrap();
        let child = e.create_child(root, b"child").unwrap();
        e.complete(child, b"done".to_vec()).unwrap();
        (root, child)
    };

    let e = Engine::open(&path).unwrap();
    assert_eq!(e.get(root).unwrap().unwrap().status, TaskStatus::Pending);
    assert_eq!(e.get(child).unwrap().unwrap().status, TaskStatus::Done);
    assert_eq!(e.children(root).unwrap(), vec![child]);
}
