//! Black-box tests for `LogReader`. Verifies the consumer-side streaming
//! API: ordered reads, seek, batch fetch, cross-segment traversal.

use qx_storage::{LogReader, PartitionConfig, PartitionLog};
use tempfile::tempdir;

fn populate(log: &mut PartitionLog, n: u32) {
    for i in 0..n {
        log.append(format!("rec-{i:04}").as_bytes()).unwrap();
    }
}

#[test]
fn reader_on_empty_log_returns_none() {
    let tmp = tempdir().unwrap();
    let log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    let mut reader = LogReader::new(&log);
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn reader_returns_records_in_order() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    populate(&mut log, 5);

    let mut reader = LogReader::new(&log);
    for i in 0..5u32 {
        let (id, rec) = reader.read_next().unwrap().expect("record present");
        assert_eq!(id, i as u64);
        assert_eq!(rec.payload(), format!("rec-{i:04}").as_bytes());
    }
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn reader_position_tracks_cursor() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    populate(&mut log, 3);

    let mut reader = LogReader::new(&log);
    assert_eq!(reader.position(), 0);
    let _ = reader.read_next().unwrap();
    assert_eq!(reader.position(), 1);
    let _ = reader.read_next().unwrap();
    assert_eq!(reader.position(), 2);
}

#[test]
fn reader_starting_at_skips_earlier() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    populate(&mut log, 5);

    let mut reader = LogReader::starting_at(&log, 2);
    assert_eq!(reader.position(), 2);
    let (id, _) = reader.read_next().unwrap().expect("record present");
    assert_eq!(id, 2);
}

#[test]
fn reader_seek_changes_cursor() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    populate(&mut log, 5);

    let mut reader = LogReader::new(&log);
    reader.seek(4);
    let (id, rec) = reader.read_next().unwrap().expect("record present");
    assert_eq!(id, 4);
    assert_eq!(rec.payload(), b"rec-0004");
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn read_batch_returns_up_to_max() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    populate(&mut log, 10);

    let mut reader = LogReader::new(&log);
    let batch = reader.read_batch(3).unwrap();
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].0, 0);
    assert_eq!(batch[2].0, 2);
    assert_eq!(reader.position(), 3);
}

#[test]
fn read_batch_returns_remaining_when_fewer_available() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    populate(&mut log, 5);

    let mut reader = LogReader::starting_at(&log, 3);
    let batch = reader.read_batch(100).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(reader.position(), 5);
}

#[test]
fn read_batch_at_end_is_empty() {
    let tmp = tempdir().unwrap();
    let mut log = PartitionLog::open(tmp.path(), PartitionConfig::default()).unwrap();
    populate(&mut log, 3);

    let mut reader = LogReader::starting_at(&log, 10);
    let batch = reader.read_batch(5).unwrap();
    assert!(batch.is_empty());
    assert_eq!(reader.position(), 10);
}

#[test]
fn reader_traverses_segment_boundaries() {
    let tmp = tempdir().unwrap();
    let cfg = PartitionConfig {
        max_segment_bytes: 32,
    };
    let mut log = PartitionLog::open(tmp.path(), cfg).unwrap();
    let n: u32 = 20;
    for i in 0..n {
        log.append(format!("rec-{i:04}").as_bytes()).unwrap();
    }
    assert!(log.segment_count() > 1, "test setup requires multiple segments");

    let mut reader = LogReader::new(&log);
    for i in 0..n {
        let (id, rec) = reader.read_next().unwrap().expect("record present");
        assert_eq!(id, i as u64);
        assert_eq!(rec.payload(), format!("rec-{i:04}").as_bytes());
    }
    assert!(reader.read_next().unwrap().is_none());
}
