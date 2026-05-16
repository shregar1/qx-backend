//! Black-box tests for `Segment`. Exercises only the public API so
//! encapsulation is enforced — internal layout changes must not break tests.

use qx_storage::{Segment, StorageError};
use tempfile::tempdir;

#[test]
fn fresh_segment_has_no_records() {
    let tmp = tempdir().unwrap();
    let segment = Segment::create(tmp.path().join("0.log")).unwrap();
    assert_eq!(segment.len(), 0);
    assert!(segment.is_empty());
    assert_eq!(segment.size_bytes(), 0);
}

#[test]
fn create_fails_if_path_exists() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("0.log");
    let _ = Segment::create(&path).unwrap();
    assert!(Segment::create(&path).is_err());
}

#[test]
fn append_returns_monotonically_increasing_offsets() {
    let tmp = tempdir().unwrap();
    let mut segment = Segment::create(tmp.path().join("0.log")).unwrap();
    let off0 = segment.append(b"hello").unwrap();
    let off1 = segment.append(b"world").unwrap();
    let off2 = segment.append(b"!").unwrap();
    assert!(off1 > off0);
    assert!(off2 > off1);
}

#[test]
fn append_increments_record_count() {
    let tmp = tempdir().unwrap();
    let mut segment = Segment::create(tmp.path().join("0.log")).unwrap();
    assert_eq!(segment.len(), 0);
    segment.append(b"a").unwrap();
    assert_eq!(segment.len(), 1);
    segment.append(b"b").unwrap();
    assert_eq!(segment.len(), 2);
}

#[test]
fn read_returns_record_at_offset() {
    let tmp = tempdir().unwrap();
    let mut segment = Segment::create(tmp.path().join("0.log")).unwrap();
    let off = segment.append(b"hello").unwrap();
    let record = segment.read(off).unwrap();
    assert_eq!(record.payload(), b"hello");
}

#[test]
fn multiple_records_read_correctly_by_offset() {
    let tmp = tempdir().unwrap();
    let mut segment = Segment::create(tmp.path().join("0.log")).unwrap();
    let payloads: Vec<&[u8]> = vec![b"foo", b"barbaz", b"quux", b"a"];
    let offsets: Vec<u64> = payloads.iter().map(|p| segment.append(p).unwrap()).collect();
    for (off, expected) in offsets.iter().zip(payloads.iter()) {
        assert_eq!(segment.read(*off).unwrap().payload(), *expected);
    }
}

#[test]
fn empty_payload_roundtrips() {
    let tmp = tempdir().unwrap();
    let mut segment = Segment::create(tmp.path().join("0.log")).unwrap();
    let off = segment.append(b"").unwrap();
    assert_eq!(segment.read(off).unwrap().payload(), b"");
}

#[test]
fn data_persists_across_reopen() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("0.log");
    let off = {
        let mut s = Segment::create(&path).unwrap();
        let o = s.append(b"hello").unwrap();
        s.sync().unwrap();
        o
    };
    let s = Segment::open(&path).unwrap();
    assert_eq!(s.read(off).unwrap().payload(), b"hello");
}

#[test]
fn reopen_recovers_record_count_and_size() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("0.log");
    let expected_size;
    {
        let mut s = Segment::create(&path).unwrap();
        s.append(b"alpha").unwrap();
        s.append(b"beta").unwrap();
        s.append(b"gamma").unwrap();
        s.sync().unwrap();
        expected_size = s.size_bytes();
    }
    let s = Segment::open(&path).unwrap();
    assert_eq!(s.len(), 3);
    assert_eq!(s.size_bytes(), expected_size);
}

#[test]
fn append_after_reopen_continues_offsets() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("0.log");
    let first_off = {
        let mut s = Segment::create(&path).unwrap();
        let o = s.append(b"first").unwrap();
        s.sync().unwrap();
        o
    };
    let mut s = Segment::open(&path).unwrap();
    let second_off = s.append(b"second").unwrap();
    assert!(second_off > first_off);
    assert_eq!(s.read(first_off).unwrap().payload(), b"first");
    assert_eq!(s.read(second_off).unwrap().payload(), b"second");
}

#[test]
fn corrupted_payload_is_detected() {
    use std::io::{Seek, SeekFrom, Write};

    let tmp = tempdir().unwrap();
    let path = tmp.path().join("0.log");
    let off = {
        let mut s = Segment::create(&path).unwrap();
        let o = s.append(b"hello").unwrap();
        s.sync().unwrap();
        o
    };

    // Header is 8 bytes (length + crc), so payload starts at off+8.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(off + 8)).unwrap();
    file.write_all(b"X").unwrap();
    file.sync_data().unwrap();
    drop(file);

    let s = Segment::open(&path).unwrap();
    assert!(matches!(s.read(off), Err(StorageError::Corrupt { .. })));
}

#[test]
fn read_past_end_returns_error() {
    let tmp = tempdir().unwrap();
    let mut s = Segment::create(tmp.path().join("0.log")).unwrap();
    s.append(b"hi").unwrap();
    let bogus = s.size_bytes() + 100;
    assert!(s.read(bogus).is_err());
}

#[test]
fn append_batch_returns_offsets_for_each_payload() {
    let tmp = tempdir().unwrap();
    let mut s = Segment::create(tmp.path().join("0.log")).unwrap();
    let payloads: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
    let offsets = s.append_batch(&payloads).unwrap();
    assert_eq!(offsets.len(), 3);
    assert!(offsets[1] > offsets[0]);
    assert!(offsets[2] > offsets[1]);
    for (off, expected) in offsets.iter().zip(payloads.iter()) {
        assert_eq!(s.read(*off).unwrap().payload(), *expected);
    }
}

#[test]
fn append_batch_empty_is_noop() {
    let tmp = tempdir().unwrap();
    let mut s = Segment::create(tmp.path().join("0.log")).unwrap();
    let offsets = s.append_batch(&[]).unwrap();
    assert!(offsets.is_empty());
    assert_eq!(s.len(), 0);
    assert_eq!(s.size_bytes(), 0);
}

#[test]
fn append_batch_and_single_append_interoperate() {
    let tmp = tempdir().unwrap();
    let mut s = Segment::create(tmp.path().join("0.log")).unwrap();
    let off0 = s.append(b"first").unwrap();
    let batch_offs = s.append_batch(&[b"second", b"third"]).unwrap();
    let off3 = s.append(b"fourth").unwrap();
    assert_eq!(s.read(off0).unwrap().payload(), b"first");
    assert_eq!(s.read(batch_offs[0]).unwrap().payload(), b"second");
    assert_eq!(s.read(batch_offs[1]).unwrap().payload(), b"third");
    assert_eq!(s.read(off3).unwrap().payload(), b"fourth");
    assert_eq!(s.len(), 4);
}
