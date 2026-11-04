#!/usr/bin/env bash
# Compare a lazy replica with a fully local replica on the same index.
#
# Creates the index on the primary, loads it, computes exact neighbors, waits for both replicas
# to catch up, then runs the same queries against each replica: first with an empty block cache
# (lazy replica restarted), then warm. Output is saved under $BENCH_HOME/results/.
#
# Start the cluster first (cluster.sh start). Settings, all optional:
#   COUNT=100000 DIM=768 CLUSTERS=1000   generated dataset
#   FVECS=path/base.fvecs QUERY_FVECS=path/query.fvecs   use a standard dataset instead
#   QUERIES=1000 FILTERED_PCT=20 TOPK=10 QPS=0 CONCURRENCY=8 DURATION=60 WARMUP=10
#   SKIP_LOAD=1   reuse the index and query set from an earlier run
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
INSTALL_BIN="${INSTALL_BIN:-$REPO_DIR/build/install/nrtsearch/bin}"
BENCH="$INSTALL_BIN/vector_bench"
BENCH_HOME="${BENCH_HOME:-$HOME/vector_bench}"

PRIMARY_PORT="${PRIMARY_PORT:-50051}"
LAZY_PORT="${LAZY_PORT:-50061}"
LOCAL_PORT="${LOCAL_PORT:-50071}"
COUNT="${COUNT:-100000}"
DIM="${DIM:-768}"
CLUSTERS="${CLUSTERS:-1000}"
QUERIES="${QUERIES:-1000}"
FILTERED_PCT="${FILTERED_PCT:-20}"
TOPK="${TOPK:-10}"
QPS="${QPS:-0}"
CONCURRENCY="${CONCURRENCY:-8}"
DURATION="${DURATION:-60}"
WARMUP="${WARMUP:-10}"

REGISTRY="$BENCH_HOME/registry.json"
QUERY_SET="$BENCH_HOME/queries_${COUNT}.bin"
RESULTS="$BENCH_HOME/results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS"

dataset=(--count "$COUNT")
if [[ -n "${FVECS:-}" ]]; then
  dataset+=(--fvecs "$FVECS")
else
  dataset+=(--dim "$DIM" --clusters "$CLUSTERS")
fi

run() {
  echo "+ vector_bench $*" | tee -a "$RESULTS/commands.txt"
  "$BENCH" "$@"
}

{
  echo "Settings: COUNT=$COUNT DIM=$DIM FVECS=${FVECS:-} QUERIES=$QUERIES TOPK=$TOPK QPS=$QPS"
  echo "          CONCURRENCY=$CONCURRENCY DURATION=$DURATION WARMUP=$WARMUP"
  echo "          CACHE_TYPE=${CACHE_TYPE:-MEMORY} CACHE_SIZE_MB=${CACHE_SIZE_MB:-1024} BLOCK_SIZE_KB=${BLOCK_SIZE_KB:-64}"
} | tee "$RESULTS/settings.txt"

if [[ -z "${SKIP_LOAD:-}" ]]; then
  run setup --port "$PRIMARY_PORT" --registryFile "$REGISTRY" "${dataset[@]}" | tee "$RESULTS/setup.txt"
  # replicas only start indexes created after they started when told to reload
  "$SCRIPT_DIR/cluster.sh" reload
  run load --port "$PRIMARY_PORT" --registryFile "$REGISTRY" "${dataset[@]}" | tee "$RESULTS/load.txt"
  # load adds a field per metadata key on the primary; replicas only load field definitions at
  # startup and on reloadState, so reload them again to see the new fields
  "$SCRIPT_DIR/cluster.sh" reload
fi

if [[ ! -f "$QUERY_SET" ]]; then
  gt=(groundtruth "${dataset[@]}" --out "$QUERY_SET" --queries "$QUERIES" --k 100
    --filteredPct "$FILTERED_PCT")
  [[ -n "${QUERY_FVECS:-}" ]] && gt+=(--queryFvecs "$QUERY_FVECS")
  run "${gt[@]}" | tee "$RESULTS/groundtruth.txt"
fi

# replicas see the data once the primary has refreshed and uploaded, and they have polled
echo "Waiting for replicas to have all $COUNT vectors"
for port in "$LAZY_PORT" "$LOCAL_PORT"; do
  for _ in $(seq 1 600); do
    line="$("$BENCH" info --port "$port" --registryFile "$REGISTRY" 2>/dev/null | head -1 || true)"
    n="$(sed -n 's/^[^ ]* [^ ]*: \([0-9,]*\) vectors.*/\1/p' <<< "$line" | tr -d ,)"
    if [[ -n "$n" && "$n" -ge "$COUNT" ]]; then
      echo "  port $port: $line"
      break
    fi
    sleep 2
  done
  [[ -n "${n:-}" && "$n" -ge "$COUNT" ]] || echo "  warning: port $port has ${n:-no} vectors after 20 minutes" >&2
done
run info --port "$PRIMARY_PORT" --registryFile "$REGISTRY" | tee "$RESULTS/info_primary.txt"

query=(query --registryFile "$REGISTRY" --queries "$QUERY_SET" --topK "$TOPK" --qps "$QPS"
  --concurrency "$CONCURRENCY" --duration "$DURATION")

echo; echo "=== local replica (all files on disk) ==="
run "${query[@]}" --port "$LOCAL_PORT" --warmup "$WARMUP" | tee "$RESULTS/query_local.txt"

echo; echo "=== lazy replica, cold block cache ==="
"$SCRIPT_DIR/cluster.sh" restart lazy
run "${query[@]}" --port "$LAZY_PORT" --warmup 0 | tee "$RESULTS/query_lazy_cold.txt"

echo; echo "=== lazy replica, warm block cache ==="
run "${query[@]}" --port "$LAZY_PORT" --warmup "$WARMUP" | tee "$RESULTS/query_lazy_warm.txt"

run info --port "$LAZY_PORT" --registryFile "$REGISTRY" | tee "$RESULTS/info_lazy.txt"
run info --port "$LOCAL_PORT" --registryFile "$REGISTRY" | tee "$RESULTS/info_local.txt"

echo; echo "=== summary ==="
for f in query_local query_lazy_cold query_lazy_warm; do
  echo "$f:"
  grep -E '^(Throughput|Latency|Recall|S3 cache|Warning|Queued)' "$RESULTS/$f.txt" | sed 's/^/  /'
done | tee "$RESULTS/summary.txt"
# Real bytes on local disk, by file type. The index size from info counts lazy files at their
# full size, though they stay in S3.
disk_usage() {
  local dir="$1"
  [[ -d "$dir" ]] || { echo "  (no index data at $dir)"; return; }
  find "$dir" -type f -name '*.*' | awk -F. '{print $NF}' | sort -u | while read -r ext; do
    bytes="$(find "$dir" -type f -name "*.$ext" -exec du -k {} + | awk '{s += $1} END {print s * 1024}')"
    printf '  %-8s %10.1f MB\n' ".$ext" "$(echo "$bytes / 1048576" | bc -l)"
  done
  printf '  %-8s %10.1f MB\n' total "$(du -sk "$dir" | awk '{print $1 / 1024}')"
}
{
  echo "Local disk, lazy replica ($BENCH_HOME/replica_lazy/index):"
  disk_usage "$BENCH_HOME/replica_lazy/index"
  echo "Local disk, local replica ($BENCH_HOME/replica_local/index):"
  disk_usage "$BENCH_HOME/replica_local/index"
} | tee -a "$RESULTS/summary.txt"
echo "Results in $RESULTS"
