//! Black-box tests for `IndexedSegment`. Verifies record-id addressing
//! (integer record numbers) layered over the byte-offset `Segment`.

use qx_storage::{IndexedSegment, StorageError};
use tempfile::tempdir;

#[test]
fn fresh_indexed_segment_starts_at_base() {
    let tmp = tempdir().unwrap();
    let s = IndexedSegment::create(tmp.path().join("0.log"), 0).unwrap();
    assert_eq!(s.base_record_id(), 0);
    assert_eq!(s.next_record_id(), 0);
    assert_eq!(s.len(), 0);
    assert!(s.is_empty());
}

#[test]
fn fresh_indexed_segment_with_nonzero_base() {
    let tmp = tempdir().unwrap();
    let s = IndexedSegment::create(tmp.path().join("100.log"), 100).unwrap();
    assert_eq!(s.base_record_id(), 100);
    assert_eq!(s.next_record_id(), 100);
}

#[test]
fn append_returns_sequential_record_ids_from_zero() {
    let tmp = tempdir().unwrap();
    let mut s = IndexedSegment::create(tmp.path().join("0.log"), 0).unwrap();
    assert_eq!(s.append(b"a").unwrap(), 0);
    assert_eq!(s.append(b"b").unwrap(), 1);
    assert_eq!(s.append(b"c").unwrap(), 2);
    assert_eq!(s.next_record_id(), 3);
    assert_eq!(s.len(), 3);
}

#[test]
fn append_returns_sequential_record_ids_from_nonzero_base() {
    let tmp = tempdir().unwrap();
    let mut s = IndexedSegment::create(tmp.path().join("50.log"), 50).unwrap();
    assert_eq!(s.append(b"a").unwrap(), 50);
    assert_eq!(s.append(b"b").unwrap(), 51);
    assert_eq!(s.next_record_id(), 52);
}

#[test]
fn read_by_record_id_round_trip() {
    let tmp = tempdir().unwrap();
    let mut s = IndexedSegment::create(tmp.path().join("0.log"), 0).unwrap();
    s.append(b"first").unwrap();
    s.append(b"second").unwrap();
    s.append(b"third").unwrap();
    assert_eq!(s.read(0).unwrap().payload(), b"first");
    assert_eq!(s.read(1).unwrap().payload(), b"second");
    assert_eq!(s.read(2).unwrap().payload(), b"third");
}

#[test]
fn read_unknown_record_id_returns_error() {
    let tmp = tempdir().unwrap();
    let mut s = IndexedSegment::create(tmp.path().join("0.log"), 0).unwrap();
    s.append(b"only").unwrap();
    assert!(matches!(s.read(99), Err(StorageError::RecordNotFound { id: 99 })));
}

#[test]
fn read_below_base_returns_error() {
    let tmp = tempdir().unwrap();
    let mut s = IndexedSegment::create(tmp.path().join("100.log"), 100).unwrap();
    s.append(b"x").unwrap();
    assert!(matches!(s.read(50), Err(StorageError::RecordNotFound { id: 50 })));
}

#[test]
fn reopen_rebuilds_index_from_log() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("0.log");
    {
        let mut s = IndexedSegment::create(&path, 0).unwrap();
        s.append(b"a").unwrap();
        s.append(b"b").unwrap();
        s.append(b"c").unwrap();
        s.sync().unwrap();
    }
    let s = IndexedSegment::open(&path, 0).unwrap();
    assert_eq!(s.len(), 3);
    assert_eq!(s.next_record_id(), 3);
    assert_eq!(s.read(0).unwrap().payload(), b"a");
    assert_eq!(s.read(1).unwrap().payload(), b"b");
    assert_eq!(s.read(2).unwrap().payload(), b"c");
}

#[test]
fn reopen_preserves_nonzero_base() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("100.log");
    {
        let mut s = IndexedSegment::create(&path, 100).unwrap();
        s.append(b"x").unwrap();
        s.append(b"y").unwrap();
        s.sync().unwrap();
    }
    let s = IndexedSegment::open(&path, 100).unwrap();
    assert_eq!(s.base_record_id(), 100);
    assert_eq!(s.next_record_id(), 102);
    assert_eq!(s.read(100).unwrap().payload(), b"x");
    assert_eq!(s.read(101).unwrap().payload(), b"y");
}

#[test]
fn append_after_reopen_continues_record_ids() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("0.log");
    {
        let mut s = IndexedSegment::create(&path, 0).unwrap();
        s.append(b"first").unwrap();
        s.sync().unwrap();
    }
    let mut s = IndexedSegment::open(&path, 0).unwrap();
    assert_eq!(s.append(b"second").unwrap(), 1);
    assert_eq!(s.read(0).unwrap().payload(), b"first");
    assert_eq!(s.read(1).unwrap().payload(), b"second");
}

#[test]
fn append_batch_returns_sequential_record_ids_from_base() {
    let tmp = tempdir().unwrap();
    let mut s = IndexedSegment::create(tmp.path().join("100.log"), 100).unwrap();
    let ids = s.append_batch(&[b"a", b"b", b"c"]).unwrap();
    assert_eq!(ids, vec![100, 101, 102]);
    assert_eq!(s.next_record_id(), 103);
    assert_eq!(s.read(101).unwrap().payload(), b"b");
}
