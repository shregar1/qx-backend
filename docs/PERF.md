# qx-broker — Performance

End-to-end benchmarks against a single broker over the Kafka wire
protocol on localhost TCP. Each iteration hand-assembles a request
frame, sends it, reads the response, and measures wall-clock latency.
No client library — the bench encodes its own frames so the numbers
reflect what the broker actually does, not a client's batching layer.

Run with:

```bash
cargo bench -p qx-broker --bench kafka_bench
```

Numbers below are from `criterion` (100 samples, 1–3 s warmup) on
Apple Silicon, macOS, APFS. Times are median; throughput is reported
as median bytes/sec.

## Headline measurements

### Sustained throughput, log-only storage

`bench_sustained_async`: N concurrent clients each Produce 100
records of P bytes against a different partition of one topic.
Broker configured with `eventual_commits = true` and
`log_only_storage = true`.

| Clients × payload | Throughput | Latency (median) |
|---|---|---|
| 8  × 1 KiB | 1.48 GiB/s | 515 µs |
| 16 × 1 KiB | 1.25 GiB/s | 1.22 ms |
| 16 × 4 KiB | 944 MiB/s  | 6.6 ms |
| 32 × 4 KiB | 419 MiB/s  | 30 ms (scheduler-bound, see below) |

### Single-Produce latency

| Records × payload | Sync durability | Async durability |
|---|---|---|
| 1 × 64 B   | 4.4 ms  / 14 KiB/s     | ~3 ms / ~20 KiB/s |
| 1 × 1 KiB  | 4.4 ms  / 226 KiB/s    | ~3 ms / ~300 KiB/s |
| 100 × 1 KiB | 5.1 ms / 19 MiB/s     | <1 ms / 100+ MiB/s |

### Fetch throughput

| Records served | Bytes | Latency | Throughput |
|---|---|---|---|
| 100 × 64 B   | 6.4 KiB | 170 µs  | 35 MiB/s |
| 100 × 1 KiB  | 100 KiB | 268 µs  | 364 MiB/s |
| 1000 × 64 B  | 64 KiB  | 1.27 ms | 48 MiB/s |
| 1000 × 1 KiB | 1 MiB   | 2.51 ms | 390 MiB/s |

Fetch is a pure log read — no fsync, no b-tree work. The
small-payload ceiling (~35 MiB/s at 64 B) is bounded by fixed
per-record encoding cost (~1.7 µs of CRC + varint per record).

## Broker modes

The broker ships in one binary with two operating profiles selected
via `BrokerConfig::mode`:

### `BrokerMode::Standard` (default)

The portable, safe profile. tokio multi-threaded work-stealing
scheduler, epoll/kqueue I/O, full message-state tracking with fsync
on every commit. Every flag retains its conservative default; queue
semantics and offset semantics both work.

**Pros**
- Runs everywhere tokio runs (Linux, macOS, BSD)
- Strongest durability: power-failure-safe out of the box
- SQS-style `receive`/`ack`/`nack` available alongside Kafka wire
- Predictable, no surprises

**Cons**
- ~4 ms single-record Produce latency (one fsync)
- Sustained throughput tops out around 100–200 MiB/s without flags
  flipped, ~1.5 GiB/s with `eventual_commits` + `log_only_storage`

### `BrokerMode::Hyper`

Targets Redpanda-class single-node throughput. Auto-applies
aggressive defaults (`eventual_commits = true`, `log_only_storage = true`)
unless the operator overrides them. Future pushes will gate
additional optimizations here (per-core sharded execution, io_uring
on Linux, zero-allocation decode) — the mode flag is the seam.

**Pros**
- Highest sustained throughput on the same hardware
- Same wire protocol, same client compatibility, same admin APIs
- One flag flips the entire profile — no need to know which dials

**Cons**
- Loses SQS-style queue semantics: `receive`/`ack`/`nack` return empty
  (offset-based reads still work)
- Async durability: power loss can lose recent records (bounded by
  OS flush cadence, typically <30 s)
- Today the difference is the auto-applied flags only; the per-core
  and io_uring work that would close the rest of the Redpanda gap is
  roadmap

### When to pick which

| Workload | Pick |
|---|---|
| SQS-style work queue with visibility timeouts | Standard |
| Kafka-compatible streaming, zero-data-loss required | Standard (with sync durability) |
| Kafka-compatible streaming, max throughput | Hyper |
| Development / CI / local dev | Standard |
| Production stream processing at scale | Hyper (once roadmap lands) |

## Storage flags (apply in both modes)

For finer control, three flags can be set independently of `mode`:

**Queue mode** (default — `track_message_state: true`). Every
Produce records per-message state in a redb b-tree alongside the
append-only log. Enables SQS-style operations: `receive`/`ack`/
`nack`/`extend_visibility`, visibility-timeout-based retries,
dead-letter routing. Roughly 5 ms per Produce floor on Apple
Silicon APFS due to fsync.

**Log-only mode** (`log_only_storage: true` on `BrokerConfig`).
Skips the b-tree entirely — Produce writes only to the append-only
log. SQS-style operations return empty for these topics; offset-
based reads (`Fetch`, `stream`, consumer-group commits) work
normally. This is the mode benchmarked above.

**Async durability** (`eventual_commits: true`). Configures redb's
internal write transactions to use `Durability::Eventual` — the
commit returns once the page cache has the write, no fsync. Power
failure can lose recent records bounded by OS flush cadence
(typically <30 s); a process crash is still safe because the OS
page cache survives.

The 1.48 GiB/s headline above is log-only + async durability.
Standalone, each flag accounts for roughly:

- async durability: ~5–7× win on single-Produce latency (4.4 ms → ~700 µs)
- log-only storage: ~10× win on sustained throughput at low–medium concurrency

## Optimization history

| Push | What changed | Headline effect (8×1KiB sustained) |
|---|---|---|
| Initial | Sync durability, message-state on every Produce | 43 MiB/s |
| Per-record clone removed | `into_iter()` instead of `value.clone()` on the decoded batch | 136 MiB/s (3.2×) |
| Log-only storage mode | Skip the b-tree commit when SQS semantics aren't needed | 1.48 GiB/s (10.9×) |

Other landed improvements that don't show up in the headline number
but matter for other workloads:

- **Per-partition mutex.** `Topic` was `Mutex<Vec<Queue>>` — every
  producer to any partition serialized at that one mutex. Refactored
  to `Vec<Mutex<Queue>>`. 16-partition Produce went 153 ms → 37 ms
  (4.2×).
- **Group-commit writer.** One mpsc-fed writer task per partition.
  Concurrent producers' Produces get drained into a single commit.
  16-concurrent producers went 72 ms → 15 ms (4.7×).
- **`join_all` in the Produce handler.** Replaced sequential
  partition awaits with parallel ones. Mostly visible on multi-
  partition Produce, where it lets per-partition fsyncs overlap.

## Known bottlenecks

At the current numbers, what's still slow and why:

1. **Single-record sync-durability latency stays at ~4 ms.** That's
   the cost of one redb fsync on macOS APFS. Group commit amortizes
   it across producers; a lone synchronous producer still pays it.
   Async-durability mode skips fsync entirely (with the durability
   trade-off noted above). A WAL with periodic batched fsync is the
   way to push past this without giving up the durability story —
   not yet implemented.

2. **Scheduler contention at 32+ concurrent producers.** The 32×4KiB
   sustained-throughput case drops to ~419 MiB/s vs 944 MiB/s at
   16 producers. Profiling points to the tokio work-stealing
   scheduler bouncing tasks between cores and the blocking pool
   dispatcher serializing some commits. Per-core sharded execution
   (one OS thread per partition writer, pinned to a core, communicating
   via `crossbeam_channel`) would address this. Tried inlining the
   commit on the tokio worker — helped this case by 40%, regressed
   everything else by 35–65%, reverted.

3. **Fetch small-payload ceiling.** ~35 MiB/s at 64 B records. CRC
   + varint per record is ~1.7 µs of CPU. Vectorizing the encoder
   would help, but the workload is rare in practice.

4. **No io_uring on Linux.** tokio uses epoll for socket I/O — one
   syscall per ready event. io_uring batches them. Estimated 1.5–2×
   on socket-heavy workloads. Linux-only, would need `tokio-uring`
   or a dedicated runtime.

## What this document does not measure

- Multi-broker / replicated topics (data-plane replication is
  scaffolded but not on the bench path)
- Fetch with multiple concurrent consumers
- Long-running sustained stress beyond criterion's sampling window
- Memory pressure with large topic catalogs (1000+ topics)
- Recovery time on broker restart with large persistent state
- Behavior under TLS — TLS handshake is excluded from these numbers
- Compressed produce batches — bench uses uncompressed
