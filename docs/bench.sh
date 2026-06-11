#!/usr/bin/env bash
# Stable bench harness.
#
# Runs a criterion benchmark N times in a row and reports min/median/
# p95/max throughput from the criterion-reported medians. Because the
# bench environment is noisy on commodity laptops (background work,
# CPU governor swings, thermal throttling), single criterion runs
# can vary 4-5× even on identical code. Running N times and reading
# the *distribution* across runs is the only honest way to claim a
# stable number.
#
# Usage:
#   bash docs/bench.sh <criterion-target-substring> [N] [measurement_secs]
#
# Examples:
#   bash docs/bench.sh kafka_sustained_async/clients-x-payload/16x1024 10 10
#   bash docs/bench.sh kafka_sustained_async                            5  5
#
# Before running:
#   - Close other heavy processes (browser, IDEs with active builds).
#   - Wait for any in-flight `cargo test`/`cargo build` to finish.
#   - On macOS, disable any test-runner watchers (rust-analyzer reload).
#   - On Linux, consider `cpupower frequency-set -g performance` + `taskset -c 0-7`.
#
# What the script writes to stdout:
#   - one line per run: `run N: <thrpt-median> MiB/s`
#   - a summary block at the end with min/median/p95/max + the spread
#     ratio (max / min). Any spread >1.5× means the environment is too
#     noisy to trust optimization deltas smaller than the spread.

set -euo pipefail

TARGET="${1:?Usage: bash docs/bench.sh <target> [N=5] [measurement=10]}"
RUNS="${2:-5}"
MEAS="${3:-10}"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

echo "qx bench harness — target=$TARGET runs=$RUNS measurement=${MEAS}s"
echo "repo: $repo_root"
echo

# Refuse to run if anything else is using cargo. Concurrent cargo runs
# blow up the bench environment.
if pgrep -lf '/cargo|/rustc' | grep -v "bench" >/dev/null 2>&1; then
    echo "warning: cargo/rustc processes are running. Bench numbers will be unreliable." >&2
fi

cd "$repo_root"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

medians=()
for i in $(seq 1 "$RUNS"); do
    out="$tmpdir/run-$i.txt"
    cargo bench -p qx-broker --bench kafka_bench -- \
        "$TARGET" --warm-up-time 3 --measurement-time "$MEAS" \
        > "$out" 2>&1
    # criterion output format:
    #   thrpt:  [low  median  high]  e.g. "thrpt:  [223.75 MiB/s 227.87 MiB/s 233.75 MiB/s]"
    # we want the median (second of three). The FIRST `thrpt:` line is
    # the absolute number; subsequent `thrpt:` lines are the change-
    # over-previous-run deltas like `+74.972%`. Grab the first.
    median_line="$(grep -E 'thrpt:' "$out" | head -1)"
    median="$(echo "$median_line" | awk -F'[][]' '{print $2}' | awk '{print $3}')"
    unit="$(echo "$median_line" | awk -F'[][]' '{print $2}' | awk '{print $4}')"
    # Normalize every measurement to MiB/s so the summary math
    # compares apples-to-apples. Criterion picks the unit per run
    # based on magnitude; without normalization 779 MiB/s and
    # 1.05 GiB/s would sort numerically and the smaller-number
    # "GiB/s" would look like the loser.
    case "$unit" in
        GiB/s) median_mib=$(awk "BEGIN {printf \"%.2f\", $median * 1024}") ;;
        MiB/s) median_mib="$median" ;;
        KiB/s) median_mib=$(awk "BEGIN {printf \"%.2f\", $median / 1024}") ;;
        *)     median_mib="$median" ;;
    esac
    medians+=("$median_mib")
    echo "run $i: $median $unit  ($median_mib MiB/s)"
done

# Stats across runs. All numeric, no unit conversion — we'll trust
# the unit's consistent within a benchmark target (criterion picks one).
echo
echo "summary across $RUNS runs:"
printf '%s\n' "${medians[@]}" | sort -n | awk '
{
    n++
    vals[n] = $1
    sum += $1
    if (n == 1 || $1 < min) min = $1
    if ($1 > max) max = $1
}
END {
    median = (n % 2 == 0) ? (vals[n/2] + vals[n/2 + 1]) / 2 : vals[(n+1)/2]
    p95_idx = int(n * 0.95 + 0.5)
    if (p95_idx < 1) p95_idx = 1
    if (p95_idx > n) p95_idx = n
    p95 = vals[p95_idx]
    spread = max / min
    printf "  min:    %.2f\n", min
    printf "  median: %.2f\n", median
    printf "  p95:    %.2f\n", p95
    printf "  max:    %.2f\n", max
    printf "  spread: %.2fx (max/min)\n", spread
    if (spread > 1.5) {
        printf "\nspread > 1.5x — environment is too noisy to trust optimization deltas smaller than %.2fx\n", spread
    }
}'
