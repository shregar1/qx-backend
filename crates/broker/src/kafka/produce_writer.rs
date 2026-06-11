//! Per-partition group-commit writer for Produce.
//!
//! When many producers Produce concurrently to the same partition, the
//! Kafka handler used to lock the topic mutex per Produce and commit
//! one redb txn each. That fsync-per-Produce serialized producers
//! behind a ~4 ms ceiling regardless of how many were sending —
//! `kafka_concurrent/clients/16` plateaued at the same 21 MiB/s as
//! `clients/1`.
//!
//! This module spawns one tokio task per `(topic, partition)`. The task
//! drains its mpsc inbox eagerly: it awaits the first request, then
//! `try_recv`s as much as it can synchronously, concatenates all
//! payloads, and commits them in a single `Queue::send_batch`. Every
//! concurrent producer's payloads land in one fsync; each producer's
//! oneshot response is fanned out with its slice of the assigned
//! offsets.
//!
//! Latency for a single-producer workload is unchanged (no artificial
//! batching window — we drain only what's already queued). Throughput
//! for concurrent producers scales with how many requests land in the
//! drain window of one commit.

use std::sync::Arc;
use std::time::Duration;

use qx_storage::RecordId;
use qx_topic::Topic;
use tokio::sync::{mpsc, oneshot};

use crate::replication::{ProduceCommand, Replicator};

#[derive(Debug)]
pub struct WriteRequest {
    /// Reference-counted slices into the originating Produce frame.
    /// Cloning a `Bytes` is a refcount bump; passing the vec across
    /// the mpsc boundary moves ownership of the refcounts only.
    pub payloads: Vec<bytes::Bytes>,
    pub respond_to: oneshot::Sender<Result<Vec<RecordId>, String>>,
}

#[derive(Clone)]
pub struct WriterHandle {
    inbox: mpsc::UnboundedSender<WriteRequest>,
}

impl WriterHandle {
    pub fn send(&self, req: WriteRequest) {
        let _ = self.inbox.send(req);
    }
}

/// Spawn the writer task for `(topic, partition)`. Returns a handle the
/// broker can clone freely. Drops everything when the broker drops its
/// last reference to the sender (the recv loop returns `None`).
///
/// `batching_window` controls how long after the first request the
/// writer waits for more requests to arrive before committing. `0`
/// keeps the original behavior (commit as soon as the inbox drains).
/// Non-zero adds latency up to the window but amortizes the fsync
/// across multiple producers — the WAL-style group-commit knob.
///
/// `inline_safe` is true when the topic's apply path doesn't do any
/// b-tree work — kafka-only topics with eventual commits. In that
/// configuration `send_batch_to` reduces to a `log.append_batch`
/// (memcpy into an mmap), so the writer inlines it on the tokio
/// worker. Cuts a spawn_blocking round-trip per commit. In
/// queue mode (or sync durability) the writer falls back to
/// spawn_blocking to avoid holding the executor thread on b-tree
/// work.
pub fn spawn(
    topic_name: String,
    topic: Arc<Topic>,
    partition: u32,
    batching_window: Duration,
    inline_safe: bool,
    replicator: Arc<dyn Replicator>,
) -> WriterHandle {
    let (tx, rx) = mpsc::unbounded_channel::<WriteRequest>();
    tokio::spawn(run(
        topic_name,
        topic,
        partition,
        rx,
        batching_window,
        inline_safe,
        replicator,
    ));
    WriterHandle { inbox: tx }
}

async fn run(
    topic_name: String,
    topic: Arc<Topic>,
    partition: u32,
    mut rx: mpsc::UnboundedReceiver<WriteRequest>,
    window: Duration,
    eventual_apply: bool,
    replicator: Arc<dyn Replicator>,
) {
    // Reserved for future inline-on-tokio-worker experiments. An
    // earlier iteration of this writer inlined `topic.send_batch_to`
    // here when `eventual_apply && replicator.is_local_only()` was
    // true, expecting to save a `spawn_blocking` roundtrip. The
    // measurement said otherwise: holding the tokio worker through
    // the commit starved sibling producer tasks waiting on their
    // oneshot wakes, more than wiping out the 10–30 µs roundtrip
    // savings. spawn_blocking is the right answer here — leave the
    // commit off the tokio worker.
    let _ = eventual_apply;

    // Pipeline drain and commit. The writer task only drains the
    // mpsc; each round's commit runs in its own `tokio::spawn` which
    // chains after the previous round's JoinHandle. The chain
    // preserves per-partition offset ordering: round N+1's
    // `replicate_produce` only fires after round N's commit task
    // (and its fan-out) finished.
    //
    // I tried splitting commit-done from task-done (so the next
    // commit could overlap with round N's fan-out wakes), expecting
    // a small win from parallelising the N oneshot.sends with the
    // next commit's spawn_blocking dispatch. Under contention the
    // extra concurrent commit task multiplied tokio scheduler
    // pressure — worse, not better. Reverted to the simpler chain.
    let mut prev_commit: Option<tokio::task::JoinHandle<()>> = None;
    let mut buf: Vec<WriteRequest> = Vec::new();
    loop {
        // Block on the first request. If the sender side has closed
        // (broker shutdown), await the in-flight commit's full task
        // so its responders fire — dropping the writer task with a
        // commit still running would surface as "writer responder
        // dropped" to a producer that already got its data accepted.
        let Some(first) = rx.recv().await else {
            if let Some(h) = prev_commit.take() {
                let _ = h.await;
            }
            return;
        };
        buf.push(first);

        // Eagerly drain whatever else has queued up. No artificial wait
        // — under no contention this is a no-op and each producer pays
        // the same fsync as before. Under contention, every queued
        // request gets bundled into one commit.
        while let Ok(req) = rx.try_recv() {
            buf.push(req);
        }

        // Optional batching window: wait up to `window` for more
        // requests to arrive, but stop early if the window elapses.
        // Every late arrival joins the same commit, dropping the
        // effective fsync-per-record cost under concurrent load.
        if !window.is_zero() {
            let deadline = tokio::time::Instant::now() + window;
            loop {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break;
                }
                let remaining = deadline - now;
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Some(req)) => {
                        buf.push(req);
                        while let Ok(more) = rx.try_recv() {
                            buf.push(more);
                        }
                    }
                    Ok(None) => break, // channel closed; commit what we have
                    Err(_) => break,   // window elapsed
                }
            }
        }

        // Move owned payloads out of each request so we can ship them
        // through the replicator without borrow-lifetime issues. The
        // oneshot responders stay in `buf` so we can fan results back
        // after the commit. `Bytes` is refcount-owned, so this is a
        // pointer move per payload, not a memcpy.
        let mut counts: Vec<usize> = Vec::with_capacity(buf.len());
        let mut owned_payloads: Vec<bytes::Bytes> = Vec::new();
        for req in buf.iter_mut() {
            counts.push(req.payloads.len());
            owned_payloads.append(&mut req.payloads);
        }

        // Collect the responders out of `buf` so we can move them
        // (along with the payloads) into the spawned commit task.
        let responders: Vec<oneshot::Sender<Result<Vec<RecordId>, String>>> =
            buf.drain(..).map(|req| req.respond_to).collect();

        let cmd = ProduceCommand {
            topic: topic_name.clone(),
            partition,
            payloads: owned_payloads,
        };
        let topic_for_commit = topic.clone();
        let replicator_for_commit = replicator.clone();
        let prev = prev_commit.take();

        let handle = tokio::spawn(async move {
            if let Some(p) = prev {
                let _ = p.await;
            }
            let send_result =
                replicator_for_commit.replicate_produce(topic_for_commit, cmd).await;
            match send_result {
                Ok(all_ids) => {
                    let mut start = 0usize;
                    for (responder, count) in responders.into_iter().zip(counts) {
                        let ids = all_ids[start..start + count].to_vec();
                        start += count;
                        let _ = responder.send(Ok(ids));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    for responder in responders {
                        let _ = responder.send(Err(msg.clone()));
                    }
                }
            }
        });
        prev_commit = Some(handle);
    }
}
