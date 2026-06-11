#!/bin/bash
# Runs the storage / queue benchmarks inside the rust:1.95-slim Linux
# container so we can compare:
#   - sync I/O on Linux vs macOS (host-level effect)
#   - io_uring vs sync on Linux (the slice 3 question)
#   - the range-encoding queue bench on Linux (slice 1 portability)
#   - the kill -9 crash harness on Linux (slice 4 portability)
#
# Expected to be invoked from the workspace root via:
#
#   docker run --rm \
#     -v "$(pwd):/work" \
#     -v qx-cargo-cache:/usr/local/cargo/registry \
#     -v qx-target-linux:/work/target \
#     -w /work rust:1.95-slim \
#     bash docker/bench.sh [bench|crash|all]
#
# The named docker volumes cache the cargo registry and a Linux-flavor
# target/ across runs so the second invocation skips the cold-build
# cost.

set -e

if ! command -v gcc >/dev/null 2>&1; then
    apt-get update -qq
    apt-get install -qq -y build-essential pkg-config >/dev/null
fi

mode="${1:-bench}"

if [[ "$mode" == "bench" || "$mode" == "all" ]]; then
    echo "=== building examples / benches ==="
    cargo build --release -p qx-storage --example io_backends >/dev/null
    cargo build --release -p qx-queue --example crash_producer >/dev/null
    cargo build --release --tests -p qx-queue >/dev/null

    echo "=== io_backends (sync + io_uring) ==="
    cargo run --release -p qx-storage --example io_backends

    echo "=== qx-queue range-encoding bench ==="
    cargo test -p qx-queue --release --test bench_redb -- --ignored --nocapture
fi

if [[ "$mode" == "crash" || "$mode" == "all" ]]; then
    echo "=== kill -9 crash harness (20 iters, min 1k acked) ==="
    CRASH_ITERS=20 MIN_ACKED=1000 \
        cargo test -p qx-queue --release --test crash_safety -- --ignored --nocapture
fi
