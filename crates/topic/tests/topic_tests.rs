//! Black-box tests for `Topic`. Covers hash-based routing, multi-partition
//! receive, receipt round-trip, batch send, and recovery.

use std::time::Duration;

use qx_queue::QueueConfig;
use qx_storage::PartitionConfig;
use qx_topic::{Topic, TopicConfig, TopicError};
use tempfile::tempdir;

fn config(partitions: u32) -> TopicConfig {
    TopicConfig {
        partition_count: partitions,
        queue: QueueConfig {
            partition: PartitionConfig::default(),
            visibility_timeout: Duration::from_secs(30),
            max_attempts: 5,
            durable_send: false,
        },
    }
}

#[test]
fn fresh_topic_has_zero_messages() {
    let tmp = tempdir().unwrap();
    let topic = Topic::open(tmp.path(), config(4)).unwrap();
    assert_eq!(topic.partition_count(), 4);
    let stats = topic.stats().unwrap();
    assert_eq!(stats.partition_count, 4);
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.inflight, 0);
}

#[test]
fn zero_partitions_rejected() {
    let tmp = tempdir().unwrap();
    let result = Topic::open(tmp.path(), config(0));
    assert!(matches!(result, Err(TopicError::ZeroPartitions)));
}

#[test]
fn send_with_key_routes_deterministically() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(8)).unwrap();
    let (p1, _) = topic.send(b"user-42", b"first").unwrap();
    let (p2, _) = topic.send(b"user-42", b"second").unwrap();
    assert_eq!(p1, p2, "same key must always map to same partition");
}

#[test]
fn partition_for_key_is_pure() {
    let tmp = tempdir().unwrap();
    let topic = Topic::open(tmp.path(), config(8)).unwrap();
    assert_eq!(topic.partition_for_key(b"x"), topic.partition_for_key(b"x"));
}

#[test]
fn distinct_keys_distribute_across_partitions() {
    let tmp = tempdir().unwrap();
    let topic = Topic::open(tmp.path(), config(4)).unwrap();
    let mut hit = std::collections::HashSet::new();
    for i in 0..200u32 {
        hit.insert(topic.partition_for_key(format!("key-{i}").as_bytes()));
    }
    assert!(
        hit.len() >= 3,
        "200 distinct keys across 4 partitions should hit at least 3, got {}",
        hit.len()
    );
}

#[test]
fn send_then_receive_round_trip() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    let (partition, id) = topic.send(b"key", b"hello").unwrap();
    let batch = topic.receive(10).unwrap();
    let msg = batch.iter().find(|m| m.payload == b"hello").expect("found");
    assert_eq!(msg.partition, partition);
    assert_eq!(msg.id, id);
}

#[test]
fn send_to_specific_partition_works() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    topic.send_to(2, b"forced").unwrap();
    let batch = topic.receive(10).unwrap();
    let msg = batch.iter().find(|m| m.payload == b"forced").unwrap();
    assert_eq!(msg.partition, 2);
}

#[test]
fn send_to_invalid_partition_returns_error() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    let err = topic.send_to(99, b"oob").unwrap_err();
    assert!(matches!(
        err,
        TopicError::InvalidPartition { partition: 99, count: 4 }
    ));
}

#[test]
fn receive_drains_from_multiple_partitions() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    for p in 0..4u32 {
        topic.send_to(p, format!("p{p}").as_bytes()).unwrap();
    }
    let batch = topic.receive(10).unwrap();
    assert_eq!(batch.len(), 4);
    let partitions: std::collections::HashSet<_> =
        batch.iter().map(|m| m.partition).collect();
    assert_eq!(partitions.len(), 4);
}

#[test]
fn receive_from_specific_partition() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    topic.send_to(0, b"a").unwrap();
    topic.send_to(1, b"b").unwrap();
    topic.send_to(2, b"c").unwrap();
    let batch = topic.receive_from(1, 10).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].payload, b"b");
    assert_eq!(batch[0].partition, 1);
}

#[test]
fn ack_via_topic_receipt() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    topic.send(b"key", b"once").unwrap();
    let mut batch = topic.receive(10).unwrap();
    let msg = batch.pop().unwrap();
    topic.ack(msg.receipt).unwrap();

    let stats = topic.stats().unwrap();
    assert_eq!(stats.inflight, 0);
    assert_eq!(stats.pending, 0);
}

#[test]
fn nack_via_topic_receipt_returns_message() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    topic.send(b"key", b"retry").unwrap();
    let mut batch = topic.receive(10).unwrap();
    let msg = batch.pop().unwrap();
    let partition = msg.partition;
    topic.nack(msg.receipt).unwrap();

    let batch = topic.receive_from(partition, 10).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].attempts, 2);
}

#[test]
fn send_batch_routes_each_item_by_key() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    let items: Vec<(&[u8], &[u8])> = vec![
        (b"k1", b"v1"),
        (b"k2", b"v2"),
        (b"k1", b"v3"),
    ];
    let results = topic.send_batch(&items).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].0, results[2].0,
        "same key in batch routes to same partition"
    );
}

#[test]
fn stats_aggregate_across_partitions() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    for p in 0..4u32 {
        topic.send_to(p, b"msg").unwrap();
    }
    let s = topic.stats().unwrap();
    assert_eq!(s.pending, 4);
}

#[test]
fn reopen_preserves_partition_count() {
    let tmp = tempdir().unwrap();
    let cfg = config(6);
    {
        let topic = Topic::open(tmp.path(), cfg.clone()).unwrap();
        assert_eq!(topic.partition_count(), 6);
    }
    let topic = Topic::open(tmp.path(), cfg).unwrap();
    assert_eq!(topic.partition_count(), 6);
}

#[test]
fn reopen_with_mismatched_partition_count_errors() {
    let tmp = tempdir().unwrap();
    {
        let _ = Topic::open(tmp.path(), config(4)).unwrap();
    }
    let result = Topic::open(tmp.path(), config(8));
    assert!(matches!(
        result,
        Err(TopicError::PartitionCountMismatch {
            configured: 8,
            on_disk: 4
        })
    ));
}

#[test]
fn stream_from_group_at_partition_returns_messages() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    topic.send_to(2, b"first").unwrap();
    topic.send_to(2, b"second").unwrap();

    let msgs = topic.stream("g", 2, 10).unwrap();
    assert_eq!(msgs.len(), 2);
    assert!(msgs.iter().all(|m| m.partition == 2));
    let payloads: Vec<_> = msgs.iter().map(|m| m.payload.clone()).collect();
    assert_eq!(payloads, vec![b"first".to_vec(), b"second".to_vec()]);
}

#[test]
fn commit_then_stream_resumes_at_offset_per_partition() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    for _ in 0..3 {
        topic.send_to(1, b"x").unwrap();
    }
    topic.commit_offset("g", 1, 2).unwrap();
    let msgs = topic.stream("g", 1, 10).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(topic.group_offset("g", 1).unwrap(), Some(2));
}

#[test]
fn send_keyed_then_compact_by_key_keeps_latest_per_key() {
    let tmp = tempdir().unwrap();
    let mut topic = Topic::open(tmp.path(), config(4)).unwrap();
    // Same key always goes to the same partition; multiple sends produce
    // multiple Pending records that compaction must collapse.
    topic.send_keyed(b"entity-1", b"v1").unwrap();
    topic.send_keyed(b"entity-1", b"v2").unwrap();
    topic.send_keyed(b"entity-1", b"v3").unwrap();
    topic.send_keyed(b"entity-2", b"only").unwrap();

    let dropped = topic.compact_by_key().unwrap();
    assert_eq!(dropped, 2);

    let batch = topic.receive(10).unwrap();
    let payloads: std::collections::HashSet<_> =
        batch.iter().map(|m| m.payload.clone()).collect();
    assert_eq!(batch.len(), 2);
    assert!(payloads.contains(b"v3".as_slice()));
    assert!(payloads.contains(b"only".as_slice()));
}

#[test]
fn reopen_preserves_messages_in_each_partition() {
    let tmp = tempdir().unwrap();
    let cfg = config(4);
    {
        let mut topic = Topic::open(tmp.path(), cfg.clone()).unwrap();
        topic.send_to(0, b"p0-msg").unwrap();
        topic.send_to(3, b"p3-msg").unwrap();
    }
    let mut topic = Topic::open(tmp.path(), cfg).unwrap();
    let batch = topic.receive(10).unwrap();
    let payloads: std::collections::HashSet<_> =
        batch.iter().map(|m| m.payload.clone()).collect();
    assert!(payloads.contains(b"p0-msg".as_slice()));
    assert!(payloads.contains(b"p3-msg".as_slice()));
}
