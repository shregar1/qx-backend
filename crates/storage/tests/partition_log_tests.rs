//! Black-box tests for `PartitionLog`. Verifies multi-segment Kafka-style log:
//! segment rolling, cross-segment reads, recovery, and continuity guarantees.

use qx_storage::{PartitionConfig, PartitionLog};
use tempfile::tempdir;

fn small_segments() -> PartitionConfig {
    PartitionConfig {
        max_segment_bytes: 32,
    }
}

#[test]
fn fresh_partition_is_empty() {
    let tmp = tempdir().unwrap();
    let log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    assert_eq!(log.len(), 0);
    assert!(log.is_empty());
    assert_eq!(log.next_record_id(), 0);
    assert_eq!(log.segment_count(), 0);
}

#[test]
fn append_starts_at_zero_and_increments() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    assert_eq!(log.append(b"a").unwrap(), 0);
    assert_eq!(log.append(b"b").unwrap(), 1);
    assert_eq!(log.append(b"c").unwrap(), 2);
    assert_eq!(log.next_record_id(), 3);
    assert_eq!(log.len(), 3);
}

#[test]
fn first_append_creates_segment() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    assert_eq!(log.segment_count(), 0);
    log.append(b"hello").unwrap();
    assert_eq!(log.segment_count(), 1);
}

#[test]
fn read_round_trip_single_segment() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    let id = log.append(b"hello").unwrap();
    assert_eq!(log.read(id).unwrap().payload(), b"hello");
}

#[test]
fn rolls_to_new_segment_when_active_full() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), small_segments()).unwrap();
    // Each frame = 8 header + 8 payload = 16 bytes. After 2 records (32 bytes)
    // the active segment is at max, so the 3rd append must roll.
    log.append(b"AAAAAAAA").unwrap();
    log.append(b"BBBBBBBB").unwrap();
    assert_eq!(log.segment_count(), 1);
    log.append(b"CCCCCCCC").unwrap();
    assert_eq!(log.segment_count(), 2);
}

#[test]
fn read_across_segment_boundaries() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), small_segments()).unwrap();
    let id0 = log.append(b"AAAAAAAA").unwrap();
    let id1 = log.append(b"BBBBBBBB").unwrap();
    let id2 = log.append(b"CCCCCCCC").unwrap();
    let id3 = log.append(b"DDDDDDDD").unwrap();
    assert!(log.segment_count() >= 2);
    assert_eq!(log.read(id0).unwrap().payload(), b"AAAAAAAA");
    assert_eq!(log.read(id1).unwrap().payload(), b"BBBBBBBB");
    assert_eq!(log.read(id2).unwrap().payload(), b"CCCCCCCC");
    assert_eq!(log.read(id3).unwrap().payload(), b"DDDDDDDD");
}

#[test]
fn reopen_recovers_state_across_segments() {
    let tmp = tempdir().unwrap();
    let cfg = small_segments();
    {
        let mut log = PartitionLog::open(tmp.path(), cfg.clone()).unwrap();
        for i in 0..10u32 {
            log.append(format!("rec-{i:03}").as_bytes()).unwrap();
        }
        log.sync().unwrap();
    }
    let log = PartitionLog::open(tmp.path(), cfg).unwrap();
    assert_eq!(log.len(), 10);
    assert_eq!(log.next_record_id(), 10);
    for i in 0..10u32 {
        let expected = format!("rec-{i:03}");
        assert_eq!(log.read(i as u64).unwrap().payload(), expected.as_bytes());
    }
}

#[test]
fn append_after_reopen_continues_record_ids() {
    let tmp = tempdir().unwrap();
    let cfg = PartitionConfig::default();
    {
        let mut log = PartitionLog::open(tmp.path(), cfg.clone()).unwrap();
        log.append(b"first").unwrap();
        log.sync().unwrap();
    }
    let mut log = PartitionLog::open(tmp.path(), cfg).unwrap();
    assert_eq!(log.append(b"second").unwrap(), 1);
    assert_eq!(log.read(0).unwrap().payload(), b"first");
    assert_eq!(log.read(1).unwrap().payload(), b"second");
}

#[test]
fn read_unknown_record_id_returns_error() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    log.append(b"x").unwrap();
    assert!(log.read(99).is_err());
}

#[test]
fn reopen_picks_up_existing_segment_files() {
    let tmp = tempdir().unwrap();
    let cfg = small_segments();
    {
        let mut log = PartitionLog::open(tmp.path(), cfg.clone()).unwrap();
        for i in 0..5u32 {
            log.append(format!("v{i}").as_bytes()).unwrap();
        }
        log.sync().unwrap();
    }
    let log = PartitionLog::open(tmp.path(), cfg).unwrap();
    assert!(log.segment_count() >= 2);
}

#[test]
fn append_batch_returns_contiguous_record_ids() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    let payloads: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
    let ids = log.append_batch(&payloads).unwrap();
    assert_eq!(ids, vec![0, 1, 2, 3]);
    for (id, expected) in ids.iter().zip(payloads.iter()) {
        assert_eq!(log.read(*id).unwrap().payload(), *expected);
    }
}

#[test]
fn truncate_before_on_empty_log_is_noop() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    assert_eq!(log.truncate_before(0).unwrap(), 0);
    assert_eq!(log.truncate_before(99).unwrap(), 0);
}

#[test]
fn truncate_before_drops_fully_sealed_segments() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), small_segments()).unwrap();
    // Each frame = 8 header + 8 payload = 16 bytes. With max=32, every two
    // records fills a segment, so after appending we expect multiple segments.
    for _ in 0..10 {
        log.append(b"AAAAAAAA").unwrap();
    }
    let segments_before = log.segment_count();
    assert!(segments_before >= 4);

    let removed = log.truncate_before(4).unwrap();
    assert!(removed > 0);
    assert!(log.segment_count() < segments_before);
    // Records past the truncation point still readable
    assert_eq!(log.read(9).unwrap().payload(), b"AAAAAAAA");
}

#[test]
fn truncate_after_within_active_segment_keeps_earlier_records() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    log.append(b"a").unwrap();
    log.append(b"b").unwrap();
    log.append(b"c").unwrap();
    log.append(b"d").unwrap();

    log.truncate_after(2).unwrap();
    assert_eq!(log.next_record_id(), 2);
    assert_eq!(log.read(0).unwrap().payload(), b"a");
    assert_eq!(log.read(1).unwrap().payload(), b"b");
    assert!(log.read(2).is_err());
    assert!(log.read(3).is_err());
}

#[test]
fn truncate_after_drops_segments_wholly_past_record_id() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), small_segments()).unwrap();
    for _ in 0..10 {
        log.append(b"AAAAAAAA").unwrap();
    }
    let segments_before = log.segment_count();
    assert!(segments_before >= 4);

    log.truncate_after(4).unwrap();
    assert_eq!(log.next_record_id(), 4);
    assert!(log.segment_count() <= segments_before);
    assert_eq!(log.read(3).unwrap().payload(), b"AAAAAAAA");
}

#[test]
fn truncate_after_then_append_resumes_at_truncation_point() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    log.append(b"keep").unwrap();
    log.append(b"drop-1").unwrap();
    log.append(b"drop-2").unwrap();

    log.truncate_after(1).unwrap();
    assert_eq!(log.next_record_id(), 1);

    let new_id = log.append(b"new").unwrap();
    assert_eq!(new_id, 1);
    assert_eq!(log.read(0).unwrap().payload(), b"keep");
    assert_eq!(log.read(1).unwrap().payload(), b"new");
}

#[test]
fn truncate_after_at_or_past_end_is_noop() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    log.append(b"a").unwrap();
    log.append(b"b").unwrap();
    log.truncate_after(log.next_record_id()).unwrap();
    log.truncate_after(999).unwrap();
    assert_eq!(log.next_record_id(), 2);
    assert_eq!(log.read(0).unwrap().payload(), b"a");
}

#[test]
fn truncate_before_keeps_active_segment_when_all_caught_up() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    log.append(b"only-record").unwrap();
    let removed = log.truncate_before(log.next_record_id()).unwrap();
    assert_eq!(removed, 0, "single active segment must never be deleted");
    assert_eq!(log.segment_count(), 1);
}

#[test]
fn append_batch_rolls_segment_when_active_full() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), small_segments()).unwrap();
    log.append_batch(&[b"AAAAAAAA", b"BBBBBBBB"]).unwrap();
    assert_eq!(log.segment_count(), 1);
    let more = log.append_batch(&[b"CCCCCCCC"]).unwrap();
    assert_eq!(more, vec![2]);
    assert_eq!(log.segment_count(), 2);
    assert_eq!(log.read(2).unwrap().payload(), b"CCCCCCCC");
}
