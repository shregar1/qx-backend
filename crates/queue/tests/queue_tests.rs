//! Black-box tests for `Queue`. Covers the full message lifecycle:
//! send → receive → ack/nack, visibility timeouts, DLQ, and recovery.

use std::thread::sleep;
use std::time::Duration;

use qx_queue::{QueueConfig, Queue, QueueError};
use qx_storage::PartitionConfig;
use tempfile::tempdir;

fn config(visibility: Duration, max_attempts: u32) -> QueueConfig {
    QueueConfig {
        partition: PartitionConfig::default(),
        visibility_timeout: visibility,
        max_attempts,
        durable_send: false, // tests run fast
        eventual_commits: false,
        track_message_state: true,
    }
}

#[test]
fn send_then_receive_returns_message() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let id = q.send(b"hello").unwrap();

    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].id, id);
    assert_eq!(&(batch[0].payload)[..], b"hello");
    assert_eq!(batch[0].attempts, 1);
}

#[test]
fn receive_on_empty_returns_empty() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let batch = q.receive(10).unwrap();
    assert!(batch.is_empty());
}

#[test]
fn ack_removes_from_queue() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_millis(10), 5)).unwrap();
    q.send(b"once").unwrap();
    let mut batch = q.receive(10).unwrap();
    let msg = batch.pop().unwrap();
    q.ack(msg.receipt).unwrap();

    sleep(Duration::from_millis(20));
    let batch = q.receive(10).unwrap();
    assert!(batch.is_empty(), "acked messages must not reappear");
}

#[test]
fn nack_returns_to_queue_with_attempts_incremented() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    q.send(b"retry").unwrap();
    let mut batch = q.receive(10).unwrap();
    let msg = batch.pop().unwrap();
    assert_eq!(msg.attempts, 1);
    q.nack(msg.receipt).unwrap();

    let mut batch = q.receive(10).unwrap();
    let again = batch.pop().unwrap();
    assert_eq!(again.attempts, 2);
}

#[test]
fn visibility_timeout_returns_message_to_pending() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_millis(20), 5)).unwrap();
    q.send(b"timeout").unwrap();

    let first = q.receive(10).unwrap();
    assert_eq!(first.len(), 1);

    sleep(Duration::from_millis(40));

    let second = q.receive(10).unwrap();
    assert_eq!(second.len(), 1, "timed-out message must reappear");
    assert_eq!(second[0].attempts, 2);
}

#[test]
fn extend_visibility_prevents_timeout() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_millis(20), 5)).unwrap();
    q.send(b"long-task").unwrap();

    let mut batch = q.receive(10).unwrap();
    let msg = batch.pop().unwrap();
    q.extend_visibility(msg.receipt, Duration::from_millis(200))
        .unwrap();

    sleep(Duration::from_millis(40));

    let second = q.receive(10).unwrap();
    assert!(
        second.is_empty(),
        "extended message must NOT reappear before its new deadline"
    );
}

#[test]
fn stale_receipt_on_ack_is_rejected() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_millis(10), 5)).unwrap();
    q.send(b"stale").unwrap();
    let mut batch = q.receive(10).unwrap();
    let stale = batch.pop().unwrap();

    // Let visibility timeout elapse and let the message be reclaimed
    sleep(Duration::from_millis(20));
    let mut batch = q.receive(10).unwrap();
    let _fresh = batch.pop().unwrap();

    let err = q.ack(stale.receipt).unwrap_err();
    assert!(matches!(err, QueueError::StaleReceipt));
}

#[test]
fn ack_unknown_receipt_returns_error() {
    use qx_queue::ReceiptHandle;

    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let bogus = ReceiptHandle {
        record_id: 99,
        attempt: 1,
    };
    assert!(matches!(
        q.ack(bogus),
        Err(QueueError::UnknownReceipt { .. })
    ));
}

#[test]
fn message_moves_to_dlq_after_max_attempts() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_millis(5), 2)).unwrap();
    q.send(b"poison").unwrap();

    for _ in 0..2 {
        let mut batch = q.receive(10).unwrap();
        let m = batch.pop().expect("message available");
        q.nack(m.receipt).unwrap();
    }

    sleep(Duration::from_millis(10));
    let batch = q.receive(10).unwrap();
    assert!(
        batch.is_empty(),
        "poison message must not be delivered after max_attempts"
    );

    let stats = q.stats().unwrap();
    assert_eq!(stats.dead, 1);
}

#[test]
fn receive_returns_up_to_max() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    for i in 0..5u32 {
        q.send(format!("m{i}").as_bytes()).unwrap();
    }
    let batch = q.receive(3).unwrap();
    assert_eq!(batch.len(), 3);
}

#[test]
fn receive_delivers_in_fifo_order() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let ids: Vec<_> = (0..5u32)
        .map(|i| q.send(format!("m{i}").as_bytes()).unwrap())
        .collect();
    let batch = q.receive(10).unwrap();
    let received_ids: Vec<_> = batch.iter().map(|m| m.id).collect();
    assert_eq!(received_ids, ids);
}

#[test]
fn stats_reflect_pending_and_inflight() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    for i in 0..3u32 {
        q.send(format!("m{i}").as_bytes()).unwrap();
    }
    let s = q.stats().unwrap();
    assert_eq!(s.pending, 3);
    assert_eq!(s.inflight, 0);

    let _ = q.receive(2).unwrap();
    let s = q.stats().unwrap();
    assert_eq!(s.pending, 1);
    assert_eq!(s.inflight, 2);
}

#[test]
fn reopen_preserves_state_across_restart() {
    let tmp = tempdir().unwrap();
    let cfg = config(Duration::from_secs(30), 5);
    let receipt = {
        let mut q = Queue::open(tmp.path(), cfg.clone()).unwrap();
        q.send(b"keep").unwrap();
        q.send(b"in-flight").unwrap();
        q.send(b"will-ack").unwrap();
        let mut batch = q.receive(2).unwrap();
        let to_ack = batch.pop().unwrap();
        q.ack(to_ack.receipt).unwrap();
        batch.pop().unwrap().receipt
    };

    let q = Queue::open(tmp.path(), cfg).unwrap();
    let stats = q.stats().unwrap();
    // 3 sent, 1 acked → 2 remain (1 inflight, 1 pending)
    assert_eq!(stats.pending + stats.inflight, 2);
    let _ = receipt;
}

#[test]
fn stream_on_empty_queue_returns_empty() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let msgs = q.stream("group-a", 10).unwrap();
    assert!(msgs.is_empty());
}

#[test]
fn stream_returns_messages_from_start_when_no_offset() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    for i in 0..3u32 {
        q.send(format!("m{i}").as_bytes()).unwrap();
    }
    let msgs = q.stream("g", 10).unwrap();
    let payloads: Vec<_> = msgs.iter().map(|m| m.payload.to_vec()).collect();
    assert_eq!(
        payloads,
        vec![b"m0".to_vec(), b"m1".to_vec(), b"m2".to_vec()]
    );
}

#[test]
fn stream_does_not_advance_offset_without_commit() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    q.send(b"a").unwrap();
    q.send(b"b").unwrap();
    let first = q.stream("g", 10).unwrap();
    let second = q.stream("g", 10).unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(second.len(), 2);
    assert_eq!(first[0].id, second[0].id);
}

#[test]
fn commit_offset_advances_stream_starting_point() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    for i in 0..5u32 {
        q.send(format!("m{i}").as_bytes()).unwrap();
    }
    let first = q.stream("g", 2).unwrap();
    let last_id = first.last().unwrap().id;
    q.commit_offset("g", last_id + 1).unwrap();

    let next = q.stream("g", 10).unwrap();
    let payloads: Vec<_> = next.iter().map(|m| m.payload.to_vec()).collect();
    assert_eq!(payloads, vec![b"m2".to_vec(), b"m3".to_vec(), b"m4".to_vec()]);
}

#[test]
fn multiple_groups_have_independent_offsets() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    for i in 0..3u32 {
        q.send(format!("m{i}").as_bytes()).unwrap();
    }
    q.commit_offset("group-a", 2).unwrap();
    let a = q.stream("group-a", 10).unwrap();
    let b = q.stream("group-b", 10).unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 3);
    assert_eq!(q.group_offset("group-a").unwrap(), Some(2));
    assert_eq!(q.group_offset("group-b").unwrap(), None);
}

#[test]
fn send_keyed_persists_compaction_key() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let id = q.send_keyed(b"entity-1", b"v1").unwrap();
    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].id, id);
    assert_eq!(&(batch[0].payload)[..], b"v1");
}

#[test]
fn compact_by_key_keeps_only_latest_per_key() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    q.send_keyed(b"entity-1", b"v1").unwrap();
    q.send_keyed(b"entity-1", b"v2").unwrap();
    let latest = q.send_keyed(b"entity-1", b"v3").unwrap();
    q.send_keyed(b"entity-2", b"only").unwrap();

    let dropped = q.compact_by_key().unwrap();
    assert_eq!(dropped, 2, "v1 and v2 for entity-1 should be dropped");

    let batch = q.receive(10).unwrap();
    let payloads: std::collections::HashSet<_> =
        batch.iter().map(|m| m.payload.to_vec()).collect();
    assert_eq!(batch.len(), 2);
    assert!(payloads.contains(b"v3".as_slice()));
    assert!(payloads.contains(b"only".as_slice()));
    assert!(batch.iter().any(|m| m.id == latest));
}

#[test]
fn compact_by_key_ignores_messages_without_key() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    q.send(b"keyless-a").unwrap();
    q.send(b"keyless-b").unwrap();
    q.send_keyed(b"k", b"v1").unwrap();
    q.send_keyed(b"k", b"v2").unwrap();

    let dropped = q.compact_by_key().unwrap();
    assert_eq!(dropped, 1);
    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 3);
}

#[test]
fn compact_by_key_preserves_inflight_messages() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    q.send_keyed(b"k", b"first").unwrap();
    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 1);
    // First is now InFlight; send a newer Pending one
    q.send_keyed(b"k", b"second").unwrap();

    let dropped = q.compact_by_key().unwrap();
    // Only Pending records are compacted; InFlight survives. Since there is
    // only one Pending for this key, nothing to drop.
    assert_eq!(dropped, 0);
}

#[test]
fn compact_on_empty_queue_returns_zero() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    assert_eq!(q.compact().unwrap(), 0);
}

#[test]
fn compact_after_acking_all_messages_drops_sealed_segments() {
    let tmp = tempdir().unwrap();
    let small = QueueConfig {
        partition: PartitionConfig {
            max_segment_bytes: 32,
        },
        visibility_timeout: Duration::from_secs(30),
        max_attempts: 5,
        durable_send: false,
        eventual_commits: false,
        track_message_state: true,
    };
    let mut q = Queue::open(tmp.path(), small).unwrap();
    for _ in 0..6 {
        q.send(b"AAAAAAAA").unwrap();
    }
    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 6);
    for msg in batch {
        q.ack(msg.receipt).unwrap();
    }

    let dropped = q.compact().unwrap();
    assert!(dropped > 0, "expected sealed segments to be reclaimed");
}

#[test]
fn compact_preserves_unacked_messages() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    for _ in 0..5 {
        q.send(b"keep").unwrap();
    }
    let _ = q.compact().unwrap();
    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 5);
}

#[test]
fn send_with_dedup_new_key_returns_fresh() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let (id, hit) = q.send_with_dedup(b"order-1", b"payload").unwrap();
    assert_eq!(id, 0);
    assert!(!hit);
}

#[test]
fn send_with_dedup_same_key_returns_existing_id() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let (id1, hit1) = q.send_with_dedup(b"order-1", b"first").unwrap();
    let (id2, hit2) = q.send_with_dedup(b"order-1", b"second").unwrap();
    assert_eq!(id1, id2);
    assert!(!hit1);
    assert!(hit2);

    // Only one message in the queue, not two
    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(&(batch[0].payload)[..], b"first");
}

#[test]
fn ack_clears_dedup_key_so_resend_succeeds() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let (id1, _) = q.send_with_dedup(b"order-1", b"first").unwrap();
    let batch = q.receive(10).unwrap();
    q.ack(batch.into_iter().next().unwrap().receipt).unwrap();

    let (id2, hit) = q.send_with_dedup(b"order-1", b"second").unwrap();
    assert_ne!(id1, id2);
    assert!(!hit);
}

#[test]
fn distinct_keys_are_independent() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let (a, _) = q.send_with_dedup(b"alpha", b"a").unwrap();
    let (b, _) = q.send_with_dedup(b"beta", b"b").unwrap();
    assert_ne!(a, b);
    assert_eq!(q.receive(10).unwrap().len(), 2);
}

#[test]
fn dedup_persists_across_reopen() {
    let tmp = tempdir().unwrap();
    let cfg = config(Duration::from_secs(30), 5);
    let initial_id = {
        let mut q = Queue::open(tmp.path(), cfg.clone()).unwrap();
        let (id, _) = q.send_with_dedup(b"order-1", b"first").unwrap();
        id
    };
    let mut q = Queue::open(tmp.path(), cfg).unwrap();
    let (id, hit) = q.send_with_dedup(b"order-1", b"different").unwrap();
    assert_eq!(id, initial_id);
    assert!(hit);
}

#[test]
fn send_batch_returns_ids_and_messages_are_receivable() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let payloads: [&[u8]; 3] = [b"one", b"two", b"three"];
    let ids = q.send_batch(&payloads).unwrap();
    assert_eq!(ids, vec![0, 1, 2]);

    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 3);
    let payloads_received: Vec<_> = batch.iter().map(|m| m.payload.as_ref()).collect();
    assert_eq!(payloads_received, vec![&b"one"[..], &b"two"[..], &b"three"[..]]);
}

#[test]
fn send_batch_with_cursor_blocks_duplicate_apply() {
    // The applier replicates a Raft entry by calling
    // `send_batch_with_cursor(payloads, raft_index)`. A second call
    // with the same `raft_index` (or any lower index) is what
    // openraft's crash-recovery path triggers — that call must
    // return `Ok(None)` and not touch on-disk state.
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();

    let first = q.send_batch_with_cursor(&[b"a", b"b", b"c"], 5).unwrap();
    assert_eq!(first, Some(vec![0u64, 1, 2]));

    // Same raft_index → duplicate, must be a no-op.
    let dup = q.send_batch_with_cursor(&[b"a", b"b", b"c"], 5).unwrap();
    assert_eq!(dup, None);

    // Lower raft_index than persisted cursor → also a no-op.
    let stale = q.send_batch_with_cursor(&[b"x"], 4).unwrap();
    assert_eq!(stale, None);

    // Strictly higher raft_index → applies and advances the cursor.
    let fresh = q.send_batch_with_cursor(&[b"d"], 6).unwrap();
    assert_eq!(fresh, Some(vec![3u64]));

    // Verify nothing slipped through: the queue should hold exactly
    // the four records we just appended.
    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 4);
    assert_eq!(&(batch[0].payload)[..], b"a");
    assert_eq!(&(batch[3].payload)[..], b"d");
}

#[test]
fn send_batch_with_cursor_persists_cursor_across_reopen() {
    // The cursor lives in redb alongside the range table. After
    // reopen, the cursor must still gate duplicate applies.
    let tmp = tempdir().unwrap();
    let cfg = config(Duration::from_secs(30), 5);
    {
        let mut q = Queue::open(tmp.path(), cfg.clone()).unwrap();
        let ids = q.send_batch_with_cursor(&[b"one"], 10).unwrap();
        assert_eq!(ids, Some(vec![0u64]));
    }
    let mut q = Queue::open(tmp.path(), cfg).unwrap();
    // Replay of raft_index 10 after reopen must still be a no-op.
    let dup = q.send_batch_with_cursor(&[b"one"], 10).unwrap();
    assert_eq!(dup, None);
    let fresh = q.send_batch_with_cursor(&[b"two"], 11).unwrap();
    assert_eq!(fresh, Some(vec![1u64]));
}

#[test]
fn ack_batch_drops_all_receipts_in_one_commit() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    let payloads: Vec<Vec<u8>> = (0..50).map(|i| format!("m{i}").into_bytes()).collect();
    let refs: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
    q.send_batch(&refs).unwrap();
    let batch = q.receive(50).unwrap();
    assert_eq!(batch.len(), 50);
    let receipts: Vec<_> = batch.into_iter().map(|m| m.receipt).collect();
    q.ack_batch(&receipts).unwrap();
    let stats = q.stats().unwrap();
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.inflight, 0);
}

#[test]
fn ack_batch_with_stale_receipt_aborts_whole_batch() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    q.send_batch(&[b"a", b"b"]).unwrap();
    let batch = q.receive(2).unwrap();
    let good = batch[0].receipt;
    let mut stale = batch[1].receipt;
    stale.attempt = 999; // fake a stale receipt
    let err = q.ack_batch(&[good, stale]).unwrap_err();
    assert!(matches!(err, qx_queue::QueueError::StaleReceipt));
    // Neither offset should be acked — the txn rolled back.
    let stats = q.stats().unwrap();
    assert_eq!(stats.inflight, 2);
}

#[test]
fn ack_in_middle_of_inflight_range_splits_correctly() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    q.send_batch(&[b"a", b"b", b"c", b"d", b"e"]).unwrap();
    let batch = q.receive(5).unwrap();
    // Ack the middle one — this exercises range-split on InFlight.
    let middle = batch[2].receipt;
    q.ack(middle).unwrap();
    // Remaining 4 are still InFlight.
    let stats = q.stats().unwrap();
    assert_eq!(stats.inflight, 4);
    // Acking the rest in scattered order still works.
    q.ack(batch[0].receipt).unwrap();
    q.ack(batch[4].receipt).unwrap();
    q.ack(batch[1].receipt).unwrap();
    q.ack(batch[3].receipt).unwrap();
    let stats = q.stats().unwrap();
    assert_eq!(stats.pending + stats.inflight + stats.dead, 0);
}

#[test]
fn many_single_sends_collapse_into_one_range_row() {
    // Sanity check the range-merge invariant: 1000 single sends should
    // collapse to one redb row internally. We can't assert the row
    // count directly from the public API, but receive() must still
    // deliver them in FIFO order and stats must show the correct
    // pending count.
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    for i in 0..1000u32 {
        q.send(format!("m{i}").as_bytes()).unwrap();
    }
    assert_eq!(q.stats().unwrap().pending, 1000);
    let batch = q.receive(1000).unwrap();
    assert_eq!(batch.len(), 1000);
    for (i, m) in batch.iter().enumerate() {
        assert_eq!(m.id, i as u64);
    }
}

#[test]
fn nack_middle_of_inflight_range_only_returns_that_offset() {
    let tmp = tempdir().unwrap();
    let mut q = Queue::open(tmp.path(), config(Duration::from_secs(30), 5)).unwrap();
    q.send_batch(&[b"a", b"b", b"c", b"d", b"e"]).unwrap();
    let batch = q.receive(5).unwrap();
    let middle = batch[2].receipt;
    q.nack(middle).unwrap();
    // The single nacked message should reappear; the other 4 stay InFlight.
    let again = q.receive(5).unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].id, batch[2].id);
    assert_eq!(again[0].attempts, 2);
}

#[test]
fn reopen_recovers_orphaned_log_records_as_pending() {
    // Crash window: a log record exists but state never got updated.
    // Recovery must surface it as Pending so it gets delivered.
    let tmp = tempdir().unwrap();
    let cfg = config(Duration::from_secs(30), 5);
    {
        let mut q = Queue::open(tmp.path(), cfg.clone()).unwrap();
        q.send(b"recoverable").unwrap();
    }

    // Append a "phantom" log record directly via the storage crate
    // to simulate a crash between log.append() and state.commit().
    {
        use qx_storage::PartitionLog;
        let mut log =
            PartitionLog::open(tmp.path().join("log"), PartitionConfig::default()).unwrap();
        log.append(b"phantom").unwrap();
        log.sync().unwrap();
    }

    let mut q = Queue::open(tmp.path(), cfg).unwrap();
    let batch = q.receive(10).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(&(batch[1].payload)[..], b"phantom");
}
