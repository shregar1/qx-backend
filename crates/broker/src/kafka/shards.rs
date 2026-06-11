//! Per-core sharded execution for the Produce write path.
//!
//! In Hyper mode every partition writer is replaced by a dedicated OS
//! thread, pinned to one CPU core. The producer-side async task sends a
//! request through a lock-free `crossbeam_channel` and awaits a tokio
//! `oneshot::Receiver` resolved synchronously by the shard thread. No
//! `spawn_blocking` hop, no work-stealing scheduler, no thread-pool
//! dispatcher — once a partition is assigned to a shard, every Produce
//! for it lands on the same OS thread, keeping CPU caches hot.
//!
//! Each shard owns a slice of partitions (assigned by hash of
//! `(topic, partition_id)`). Shards are independent — they don't share
//! mutexes, queues, or memory beyond the partition `Arc<Topic>` they
//! commit through.
//!
//! Cross-shard cost: when a producer's request needs a partition owned
//! by another shard, the cost is one MPSC send + one oneshot await.
//! Crossbeam's MPSC is wait-free for the producer side, cache-friendly
//! for the consumer side; oneshot wakeups go through tokio's reactor
//! and add ~1 µs on the consumer side.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use qx_topic::Topic;
use tokio::sync::oneshot;

use super::produce_writer::WriteRequest;

/// One shard = one OS thread, one crossbeam channel, one slice of
/// partition writers.
pub struct ShardPool {
    shards: Vec<Sender<ShardMsg>>,
    /// Keep the JoinHandles around so the threads stay alive for the
    /// lifetime of the pool. Dropping the pool drops the senders;
    /// the shard threads see disconnect on `recv` and exit cleanly.
    _threads: Vec<std::thread::JoinHandle<()>>,
    /// Sticky round-robin partition→shard assignment. First submit for
    /// `(topic, partition)` claims the next slot; subsequent submits go
    /// to the same shard so a partition's writes always land on one
    /// thread (and stay cache-hot). Better load balancing than
    /// hash-mod-N: with 16 partitions and 8 shards, hash collisions
    /// concentrate hot partitions on the same shard, serializing
    /// them. Round-robin guarantees 2-per-shard exactly.
    assignments: Mutex<HashMap<(String, u32), usize>>,
    next_slot: AtomicUsize,
}

enum ShardMsg {
    Write {
        topic: Arc<Topic>,
        partition: u32,
        req: WriteRequest,
        /// Optional batching window. Non-zero lets the shard drain
        /// additional requests for the same partition before
        /// committing, just like the tokio writer did.
        window: Duration,
    },
}

impl ShardPool {
    /// Spawn `n` shard threads (defaults to `num_cpus::get()` when 0).
    /// Each thread is pinned to a distinct CPU core via
    /// `core_affinity` when the platform supports it; on
    /// unsupported platforms threads still run but aren't pinned —
    /// the architectural separation (no work-stealing) still applies.
    pub fn new(n: usize) -> Self {
        let n = if n == 0 { num_cpus::get().max(1) } else { n };
        let cores = core_affinity::get_core_ids().unwrap_or_default();
        let mut senders = Vec::with_capacity(n);
        let mut threads = Vec::with_capacity(n);
        for shard_id in 0..n {
            let (tx, rx) = crossbeam_channel::unbounded::<ShardMsg>();
            let core = cores.get(shard_id % cores.len().max(1)).copied();
            let handle = std::thread::Builder::new()
                .name(format!("qx-shard-{shard_id}"))
                .spawn(move || {
                    if let Some(c) = core {
                        // Best-effort pin. Returns false on unsupported
                        // platforms; we just keep going.
                        let _ = core_affinity::set_for_current(c);
                    }
                    shard_run(rx);
                })
                .expect("spawn shard thread");
            senders.push(tx);
            threads.push(handle);
        }
        Self {
            shards: senders,
            _threads: threads,
            assignments: Mutex::new(HashMap::new()),
            next_slot: AtomicUsize::new(0),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Pick (or remember) the shard that owns `(topic, partition)`. The
    /// first submit for a partition claims the next round-robin slot;
    /// every subsequent submit reuses that slot. This guarantees the
    /// most-even possible distribution: with 16 partitions and 8
    /// shards, exactly 2 partitions per shard.
    fn shard_for(&self, topic: &str, partition: u32) -> &Sender<ShardMsg> {
        let key = (topic.to_string(), partition);
        let mut map = self.assignments.lock().unwrap();
        let idx = *map.entry(key).or_insert_with(|| {
            let n = self.shards.len();
            self.next_slot.fetch_add(1, Ordering::Relaxed) % n
        });
        &self.shards[idx]
    }

    /// Submit a write to the shard owning this `(topic, partition)`.
    /// Returns the oneshot the producer task awaits for assigned ids.
    pub fn submit(
        &self,
        topic: Arc<Topic>,
        partition: u32,
        payloads: Vec<bytes::Bytes>,
        window: Duration,
    ) -> oneshot::Receiver<Result<Vec<u64>, String>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = WriteRequest {
            payloads,
            respond_to: resp_tx,
        };
        let sender = self.shard_for(&topic_name_for(&topic), partition);
        // unbounded send is wait-free under contention; failure means
        // the shard thread has exited (broker shutdown), in which case
        // the receiver side of resp_rx never resolves — the awaiting
        // task observes that as an error.
        let _ = sender.send(ShardMsg::Write {
            topic,
            partition,
            req,
            window,
        });
        resp_rx
    }
}

/// Synthesize a name for the topic so the shard hash is stable across
/// `Arc<Topic>` clones. `Topic` doesn't expose a name directly — the
/// broker keys it by name, so we approximate via the on-disk directory
/// path.
fn topic_name_for(topic: &Topic) -> String {
    topic
        .dir()
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn shard_run(rx: Receiver<ShardMsg>) {
    // The shard is a hot loop on the dedicated thread. Each iteration:
    //   1. block on `recv()` for the first request
    //   2. try_recv() everything in the queue, sorting into a bundle
    //      (same partition) or a deferred Vec (other partitions)
    //   3. optionally wait `window` for more same-partition requests
    //   4. commit the bundle as one batch
    //   5. process the deferred Vec sequentially (one commit each)
    //
    // We tried processing cross-partition requests inline during the
    // try_recv loop — that interrupted the drain and hurt batching.
    // We tried skipping cross-partition draining entirely and letting
    // the next recv() pick them up — that's correct but means the
    // channel stays nonempty between iterations, growing latency.
    // The deferred Vec is the empirically best of three.
    loop {
        let first = match rx.recv() {
            Ok(m) => m,
            Err(_) => return, // all senders dropped → shutdown
        };
        let ShardMsg::Write {
            topic,
            partition,
            req,
            window,
        } = first;

        let mut bundle: Vec<WriteRequest> = vec![req];
        let mut deferred: Vec<ShardMsg> = Vec::new();

        // Drain queued requests: same-partition → bundle, else → defer.
        while let Ok(msg) = rx.try_recv() {
            let ShardMsg::Write {
                topic: t,
                partition: p,
                req: r,
                window: w,
            } = msg;
            if Arc::ptr_eq(&topic, &t) && p == partition {
                bundle.push(r);
            } else {
                deferred.push(ShardMsg::Write {
                    topic: t,
                    partition: p,
                    req: r,
                    window: w,
                });
            }
        }

        // Optional batching window for the current partition.
        if !window.is_zero() {
            let deadline = std::time::Instant::now() + window;
            while std::time::Instant::now() < deadline {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                match rx.recv_timeout(remaining) {
                    Ok(ShardMsg::Write {
                        topic: t,
                        partition: p,
                        req: r,
                        window: w,
                    }) => {
                        if Arc::ptr_eq(&topic, &t) && p == partition {
                            bundle.push(r);
                        } else {
                            deferred.push(ShardMsg::Write {
                                topic: t,
                                partition: p,
                                req: r,
                                window: w,
                            });
                            // Don't keep waiting once a different-
                            // partition request lands — committing
                            // soon is fairer to it.
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        // Commit the same-partition bundle inline.
        let mut counts: Vec<usize> = Vec::with_capacity(bundle.len());
        let mut owned: Vec<bytes::Bytes> = Vec::new();
        for r in bundle.iter_mut() {
            counts.push(r.payloads.len());
            owned.append(&mut r.payloads);
        }
        let refs: Vec<&[u8]> = owned.iter().map(|b| b.as_ref()).collect();
        let send_result = topic.send_batch_to(partition, &refs);

        match send_result {
            Ok(all_ids) => {
                let mut start = 0usize;
                for (req, count) in bundle.into_iter().zip(counts) {
                    let ids = all_ids[start..start + count].to_vec();
                    start += count;
                    let _ = req.respond_to.send(Ok(ids));
                }
            }
            Err(e) => {
                let msg = e.to_string();
                for req in bundle {
                    let _ = req.respond_to.send(Err(msg.clone()));
                }
            }
        }

        // Process the deferred messages: one commit per other partition.
        // We can't peek-and-batch them because crossbeam doesn't
        // support peek, and re-queueing has the same issue (no self
        // sender). Inline-sequential keeps fairness.
        for msg in deferred {
            let ShardMsg::Write {
                topic: t,
                partition: p,
                req: r,
                window: _,
            } = msg;
            let payloads = r.payloads;
            let refs: Vec<&[u8]> = payloads.iter().map(|b| b.as_ref()).collect();
            match t.send_batch_to(p, &refs) {
                Ok(ids) => {
                    let _ = r.respond_to.send(Ok(ids));
                }
                Err(e) => {
                    let _ = r.respond_to.send(Err(e.to_string()));
                }
            }
        }
    }
}
