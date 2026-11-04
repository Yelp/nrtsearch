#!/usr/bin/env bash
# Run a local vector test cluster as plain processes: a primary, an isolated replica with lazy
# remote data, and an isolated replica that downloads everything (the baseline).
#
#   cluster.sh start [primary|lazy|local|all]   start nodes (default: all)
#   cluster.sh stop [primary|lazy|local|all]    stop nodes
#   cluster.sh restart <node>                   stop then start, e.g. to empty a replica's cache
#   cluster.sh reload                           make replicas load indexes created since they
#                                               started (after vector_bench setup)
#   cluster.sh status                           show which nodes are running
#   cluster.sh logs <node>                      follow a node's log
#   cluster.sh clean                            stop everything and delete local data
#
# Settings come from the environment; BENCH_BUCKET is required:
#   BENCH_BUCKET     S3 bucket for index data (required)
#   AWS_PROFILE      AWS credentials profile; any default credentials chain source works
#   BENCH_SERVICE    service name, the S3 prefix for this cluster (default: vector-bench)
#   BENCH_HOME       local data, logs and configs (default: ~/vector_bench)
#   CACHE_TYPE       lazy replica block cache: MEMORY, OFF_HEAP or DISK (default: MEMORY)
#   CACHE_SIZE_MB    lazy replica block cache size (default: 1024)
#   BLOCK_SIZE_KB    lazy replica block size (default: 64)
#   POLL_SECONDS     replica polling interval for new index versions (default: 5)
#   PRIMARY_HEAP, REPLICA_HEAP   JVM heap sizes (default: 4g, 2g)
#   PRIMARY_PORT, LAZY_PORT, LOCAL_PORT   gRPC ports (default: 50051, 50061, 50071)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
INSTALL_BIN="${INSTALL_BIN:-$REPO_DIR/build/install/nrtsearch/bin}"

export BENCH_SERVICE="${BENCH_SERVICE:-vector-bench}"
export BENCH_HOME="${BENCH_HOME:-$HOME/vector_bench}"
export CACHE_TYPE="${CACHE_TYPE:-MEMORY}"
export CACHE_SIZE_MB="${CACHE_SIZE_MB:-1024}"
export BLOCK_SIZE_KB="${BLOCK_SIZE_KB:-64}"
export POLL_SECONDS="${POLL_SECONDS:-5}"
export PRIMARY_PORT="${PRIMARY_PORT:-50051}"
export PRIMARY_REPLICATION_PORT="${PRIMARY_REPLICATION_PORT:-50052}"
export LAZY_PORT="${LAZY_PORT:-50061}"
export LAZY_REPLICATION_PORT="${LAZY_REPLICATION_PORT:-50062}"
export LOCAL_PORT="${LOCAL_PORT:-50071}"
export LOCAL_REPLICATION_PORT="${LOCAL_REPLICATION_PORT:-50072}"
PRIMARY_HEAP="${PRIMARY_HEAP:-4g}"
REPLICA_HEAP="${REPLICA_HEAP:-2g}"

ALL_NODES=(primary lazy local)

die() { echo "error: $*" >&2; exit 1; }

node_config() {
  case "$1" in
    primary) echo primary ;;
    lazy) echo replica_lazy ;;
    local) echo replica_local ;;
    *) die "unknown node '$1', expected primary, lazy or local" ;;
  esac
}

node_port() {
  case "$1" in
    primary) echo "$PRIMARY_PORT" ;;
    lazy) echo "$LAZY_PORT" ;;
    local) echo "$LOCAL_PORT" ;;
  esac
}

node_heap() {
  if [[ "$1" == primary ]]; then echo "$PRIMARY_HEAP"; else echo "$REPLICA_HEAP"; fi
}

pid_file() { echo "$BENCH_HOME/run/$1.pid"; }
log_file() { echo "$BENCH_HOME/logs/$1.log"; }

is_running() {
  local pid_file
  pid_file="$(pid_file "$1")"
  [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

# Fill in ${VAR} references in a config template from the environment.
render_config() {
  local template="$1" out="$2"
  local content
  content="$(cat "$template")"
  local var
  for var in $(grep -o '\${[A-Z_]*}' "$template" | sort -u | tr -d '${}'); do
    [[ -n "${!var:-}" ]] || die "$var is not set (needed by $(basename "$template"))"
    content="${content//\$\{$var\}/${!var}}"
  done
  printf '%s\n' "$content" > "$out"
}

# Explain why a node can't use S3, from its log. When the bucket lookup fails, nrtsearch keeps
# running with a dummy S3 client, so the failure only shows later as confusing upload or restore
# errors. Returns 1 if the log shows the failure.
check_s3_access() {
  local node="$1" log
  log="$(log_file "$node")"
  if grep -q "failed to get the location of S3 bucket" "$log" 2>/dev/null; then
    {
      echo "  $node could not reach s3://$BENCH_BUCKET, and is running with a dummy S3 client."
      echo "  Usual causes: no AWS credentials in the environment it was started from (set"
      echo "  AWS_PROFILE, or export keys: eval \"\$(aws configure export-credentials --profile"
      echo "  <name> --format env)\"), or a wrong bucket name. The AWS error:"
      grep -A 3 "failed to get the location of S3 bucket" "$log" |
        grep -m 2 -E "Exception|Caused by" | sed 's/^/    /' || true
    } >&2
    return 1
  fi
  return 0
}

wait_ready() {
  local node="$1" port log
  port="$(node_port "$node")"
  log="$(log_file "$node")"
  for _ in $(seq 1 120); do
    if ! check_s3_access "$node"; then
      stop_node "$node" > /dev/null
      return 1
    fi
    if ! is_running "$node"; then
      echo "  $node exited during startup; last log lines:" >&2
      tail -n 30 "$log" >&2
      return 1
    fi
    if grep -q "Server started, listening on $port" "$log" 2>/dev/null; then
      if ! grep -q "S3 region: " "$log"; then
        echo "  warning: $node started but its log doesn't show the S3 bucket region" >&2
      fi
      echo "  $node ready on port $port"
      return 0
    fi
    sleep 1
  done
  echo "  $node did not report ready within 120s; see $log" >&2
  return 1
}

# Isolated replicas read the cluster state the primary writes to S3, and can't create it. Check it
# exists before starting one, with the AWS CLI if installed, else from the local primary's log.
check_global_state() {
  local node="$1" key="$BENCH_SERVICE/global_state/_current" primary_log err
  if command -v aws > /dev/null; then
    if err="$(aws s3api head-object --bucket "$BENCH_BUCKET" --key "$key" 2>&1 > /dev/null)"; then
      return 0
    fi
    if grep -qE "Not Found|404" <<< "$err"; then
      {
        echo "error: can't start $node: there is no cluster state at s3://$BENCH_BUCKET/$key"
        echo "  The primary writes it on its first start with S3 access. Start the primary first"
        echo "  (cluster.sh start primary), and check BENCH_BUCKET ($BENCH_BUCKET) and"
        echo "  BENCH_SERVICE ($BENCH_SERVICE) match the ones the primary was started with."
      } >&2
    else
      echo "error: can't start $node: checking s3://$BENCH_BUCKET/$key failed:" >&2
      sed 's/^/  /' <<< "$err" >&2
    fi
    return 1
  fi
  # no AWS CLI: rely on the local primary, which logs loading or creating the state
  primary_log="$(log_file primary)"
  if ! is_running primary; then
    echo "  note: can't check the cluster state in S3 (no aws CLI) and the primary isn't" \
      "running here; starting $node anyway" >&2
    return 0
  fi
  if ! check_s3_access primary; then
    echo "error: can't start $node until the primary can reach S3; restart it with" \
      "credentials: cluster.sh restart primary" >&2
    return 1
  fi
  if ! grep -qE "Loaded remote state|Committed state" "$primary_log"; then
    echo "error: can't start $node: the primary's log doesn't show it loaded or wrote the" \
      "cluster state in S3; see $primary_log" >&2
    return 1
  fi
  return 0
}

start_node() {
  local node="$1" config
  config="$(node_config "$node")"
  [[ -n "${BENCH_BUCKET:-}" ]] || die "set BENCH_BUCKET to the S3 bucket to use"
  if is_running "$node"; then
    echo "$node already running (pid $(cat "$(pid_file "$node")"))"
    # a node started earlier, perhaps without credentials, may be running without S3
    if ! check_s3_access "$node"; then
      echo "  restart it from a shell with AWS credentials: cluster.sh restart $node" >&2
      return 1
    fi
    return 0
  fi
  [[ -x "$INSTALL_BIN/nrtsearch_server" ]] ||
    die "$INSTALL_BIN/nrtsearch_server not found; run ./gradlew installDist first"
  if [[ "$node" != primary ]]; then
    check_global_state "$node" || return 1
  fi
  mkdir -p "$BENCH_HOME"/{run,logs,config} "$BENCH_HOME/$config"
  render_config "$SCRIPT_DIR/config/$config.yaml" "$BENCH_HOME/config/$config.yaml"
  echo "Starting $node with $BENCH_HOME/config/$config.yaml"
  JAVA_OPTS="${JAVA_OPTS:-} -Xms$(node_heap "$node") -Xmx$(node_heap "$node")" \
    nohup "$INSTALL_BIN/nrtsearch_server" "$BENCH_HOME/config/$config.yaml" \
    > "$(log_file "$node")" 2>&1 &
  echo $! > "$(pid_file "$node")"
  # tracked by pid file; disown so the shell doesn't report the job when it's stopped
  disown
  wait_ready "$node"
}

stop_node() {
  local node="$1" pid
  if ! is_running "$node"; then
    rm -f "$(pid_file "$node")"
    return 0
  fi
  pid="$(cat "$(pid_file "$node")")"
  echo "Stopping $node (pid $pid)"
  kill "$pid"
  for _ in $(seq 1 30); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "  $node did not stop after 30s, killing"
    kill -9 "$pid"
  fi
  rm -f "$(pid_file "$node")"
}

nodes_arg() {
  if [[ -z "${1:-}" || "$1" == all ]]; then
    echo "${ALL_NODES[@]}"
  else
    node_config "$1" > /dev/null
    echo "$1"
  fi
}

grpc_reload() {
  # reloadState makes a node read global state again and start indexes it doesn't have
  "$INSTALL_BIN/nrtsearch_client" -p "$1" reloadState > /dev/null
}

case "${1:-}" in
  start)
    for node in $(nodes_arg "${2:-}"); do start_node "$node"; done
    ;;
  stop)
    nodes=($(nodes_arg "${2:-}"))
    # stop replicas before the primary
    for ((i = ${#nodes[@]} - 1; i >= 0; i--)); do stop_node "${nodes[i]}"; done
    ;;
  restart)
    [[ -n "${2:-}" ]] || die "restart needs a node: primary, lazy or local"
    stop_node "$2"
    start_node "$2"
    ;;
  reload)
    for node in lazy local; do
      if is_running "$node"; then
        echo "Reloading state on $node"
        grpc_reload "$(node_port "$node")"
      fi
    done
    ;;
  status)
    for node in "${ALL_NODES[@]}"; do
      if is_running "$node"; then
        echo "$node: running (pid $(cat "$(pid_file "$node")"), port $(node_port "$node"))"
      else
        echo "$node: stopped"
      fi
    done
    ;;
  logs)
    [[ -n "${2:-}" ]] || die "logs needs a node: primary, lazy or local"
    tail -f "$(log_file "$2")"
    ;;
  clean)
    "$0" stop all
    echo "Deleting $BENCH_HOME (index data in s3://${BENCH_BUCKET:-<bucket>}/$BENCH_SERVICE is kept)"
    rm -rf "$BENCH_HOME"
    ;;
  *)
    sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
