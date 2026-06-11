# qx-broker vs. the field

Snapshot as of the most recent push. Numbers from `cargo bench -p qx-broker` on Apple Silicon, macOS, APFS. Where competitors are quoted, the numbers are published / well-known typical ranges, not measured here.

## Headline table

| | qx-broker (today) | Apache Kafka | Redpanda | NATS JetStream | AWS SQS | WarpStream |
|---|---|---|---|---|---|---|
| **Language** | Rust | Java/Scala (JVM) | C++ | Go | (proprietary) | Go |
| **Binary size** | ~3 MB | ~200 MB + JRE | ~50 MB | ~25 MB | hosted | ~80 MB |
| **Runtime deps** | none | JVM + ZK or KRaft | none | none | hosted | object store |
| **Kafka wire compatible** | yes (47/75 API keys) | yes (native) | yes | adapter only | no | yes |
| **Replication** | scaffolding (Replicator trait + qx-raft crate; data-plane wiring pending) | yes | yes (Raft) | yes | hosted | hosted |
| **Multi-region** | no | yes (MM2) | yes (commercial) | yes (gateways) | yes | yes |
| **Tiered storage** | yes (ColdStore trait + local-fs impl + Fetch fallback; S3 impl plugs same trait) | KIP-405 (newer) | commercial | no | no | core |
| **Transactions (EOS)** | yes, persistent | yes | yes | limited | no | yes |
| **Consumer groups** | yes | yes | yes | yes (different model) | no | yes |
| **Single-record latency (sync)** | 4.4 ms | <1 ms typical | 0.5–2 ms | <1 ms | 5–10 ms HTTP | 100+ ms (S3) |
| **Single-record latency (async)** | **632 µs** | comparable mode | comparable | comparable | n/a | n/a |
| **Concurrent throughput** | 100 MiB/s (16 producers, 1 partition) | 500+ MiB/s | 500+ MiB/s | 1+ GiB/s | rate-limited | 50–200 MiB/s |
| **Fetch throughput** | 390 MiB/s on 1 MiB payloads | 200–800 MiB/s | similar | 1+ GiB/s | n/a | low |
| **Auth** | SASL/PLAIN, SASL/SCRAM-SHA-256, mTLS, TLS, gRPC bearer | SASL/PLAIN, SCRAM, GSSAPI, OAUTHBEARER, mTLS | same as Kafka | NKEY, JWT, TLS | IAM | IAM |
| **ACLs** | yes (real DENY-wins enforcement, literal + prefix patterns) | yes | yes | yes | IAM | IAM |
| **KRaft compat** | DescribeQuorum yes; KIP-595 broker-to-broker RPCs no (qx uses qx-raft for consensus) | yes | yes | n/a | n/a | n/a |
| **Workflows** | yes (in-process Engine) | no | no | no | no | no |
| **Schema registry** | yes (own) | external (Confluent) | external | no | no | external |
| **SQS-style queues** | yes (visibility timeout, ack/nack, DLQ) | no (compaction only) | no | yes (work queue) | yes (core) | no |
| **License** | Apache 2.0 | Apache 2.0 | Source-available (BSL) | Apache 2.0 | proprietary | proprietary core |
| **Memory floor** | ~30 MB | 1 GB+ heap | 150 MB | 50 MB | n/a | varies |

## Where qx now wins outright

- **Smallest deployable Kafka-compatible broker.** 3 MB static binary, no JVM, no ZK, ~30 MB resident.
- **Sub-ms single-record latency in async mode.** 632 µs on 1 KiB records puts qx in the same band as Kafka with `acks=1` — at a fraction of the memory and disk-footprint cost.
- **Workflow engine in the same process.** Produce a record, trigger a workflow, ack on completion — no Kafka + Temporal split.
- **SQS-style queue semantics over Kafka wire.** Visibility timeout, ack/nack, DLQ work via kcat. No competitor offers both.
- **Real ACL enforcement at 47 API keys with SCRAM-SHA-256 + mTLS.** Production-grade auth shape with a fraction of Kafka's deployment surface.
- **Tiered storage with one trait.** `ColdStore` + `LocalFsColdStore` ship today; an S3 impl is a new file against the same trait, not a refactor.

## Where qx loses (truthful)

- **Replication is scaffolding, not data-plane.** The `Replicator` trait is wired through Broker, but only `NoopReplicator` is plugged in. A `RaftReplicator` that wraps `qx-raft::Cluster` is the single focused follow-on (~200 LOC). Until that lands, qx is single-broker.
- **Concurrent throughput at GiB/s scale.** 100 MiB/s at 16 producers/1 partition matches Apache Kafka with sync durability but trails Redpanda and NATS by ~5–10×. Closing that needs WAL with deferred sync, io_uring on Linux, and per-core threading — multi-week effort.
- **Multi-region.** No active-active or active-passive cross-region story. Requires replication first.
- **No KRaft-internal broker-to-broker protocol.** qx uses qx-raft for consensus, not Kafka's `Vote`/`BeginQuorumEpoch`/etc. A cluster of qx brokers can't be administered by KRaft tooling; admin tools see one broker.
- **Not yet battle-tested.** No production deployments. Benches are on local hardware, not realistic clusters with network noise.

## Use cases where qx is the right pick

1. **Local development / CI replacement for Kafka.** ~3 MB binary, ~30 MB RAM, starts in <100 ms. With `eventual_commits = true`, single-record latency is comparable to a real Kafka cluster.
2. **Edge / IoT.** Embedded Kafka-compatible broker on resource-constrained hardware.
3. **Internal queue + workflow.** Replace `Kafka + Temporal + RabbitMQ` with one process for low-to-medium throughput.
4. **SQS replacement for self-hosted.** Want SQS semantics without AWS lock-in.
5. **Single-tenant production at moderate scale.** Async durability + tiered storage + persistent transactions covers a real workload, given operators accept a bounded power-loss window.

## Use cases qx is NOT yet the right pick for

1. **HA workloads requiring replication.** Use Kafka or Redpanda until `RaftReplicator` lands.
2. **GiB/s sustained streaming.** Use Redpanda or NATS JetStream.
3. **Cost-optimized cold storage of huge logs.** `ColdStore` is the seam, but the S3 impl and automatic offload schedule aren't shipped yet. Use WarpStream or Kafka with KIP-405.
4. **Multi-region deployments.** No story today.

## What's left to "kill Kafka in production"

Honest punch list:

1. **Wire `RaftReplicator` end-to-end.** Serialize `ProduceCommand` as Raft log entries, register `(request_id → oneshot)` map, resolve from apply callback on the leader. ~200 LOC, no architectural surprises. Closes the replication and HA gaps.
2. **Sustained-throughput bench at 1 GiB/s class.** Today's benches plateau at the per-partition fsync. With async durability and multi-partition parallelism, the headroom is bandwidth — measure it.
3. **S3 `ColdStore` impl + automatic offload schedule.** The trait is ready; the implementation is a few hundred lines using `aws-sdk-s3`. Background sweep advances the low watermark per topic config.
4. **Multi-region (active-passive).** Single GrpcCluster across regions, with care about leader placement and async log replication for the not-on-quorum partitions.

Each piece is bounded, but they add up to roughly two-to-three weeks of focused work.
