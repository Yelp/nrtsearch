#!/usr/bin/env bash
#
# Run a local multi-primary nrtsearch cluster (N primaries with s3_lease election, M replicas
# following the leader) against a real S3 bucket, for failover testing.
#
# Usage: ha_local.sh <command> [args]   (see "ha_local.sh help")
#
# Can also be sourced (by ha_failover_test.sh) to use its functions.
#
# Written for macOS bash 3.2 as well as newer bash: no associative arrays or mapfile.

set -euo pipefail

HA_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HA_REPO_DIR="$(cd "$HA_SCRIPT_DIR/../.." && pwd)"
HA_BIN_DIR="${HA_BIN_DIR:-$HA_REPO_DIR/build/install/nrtsearch/bin}"
HA_DIR="${HA_DIR:-/tmp/nrtsearch-ha-local}"
HA_ENV_FILE="$HA_DIR/cluster.env"
# fast startup for the many short client JVMs
HA_CLIENT_JAVA_OPTS="${HA_CLIENT_JAVA_OPTS:--XX:TieredStopAtLevel=1 -XX:+UseSerialGC -Xshare:auto}"
HA_SERVER_JAVA_OPTS="${HA_SERVER_JAVA_OPTS:--Xms256m -Xmx1g}"

# ---------------------------------------------------------------------------------------------
# output helpers

if [ -t 1 ]; then
  C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_BLUE=$'\033[34m'; C_OFF=$'\033[0m'
else
  C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_OFF=""
fi

ts() { date +%H:%M:%S; }
log() { echo "${C_BLUE}[$(ts)]${C_OFF} $*" >&2; }
warn() { echo "${C_YELLOW}[$(ts)] WARN${C_OFF} $*" >&2; }
die() { echo "${C_RED}[$(ts)] ERROR${C_OFF} $*" >&2; exit 1; }

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

# ---------------------------------------------------------------------------------------------
# cluster settings

load_env() {
  [ -f "$HA_ENV_FILE" ] || die "No cluster at $HA_DIR; run: $0 init --profile <aws profile> --bucket <bucket>"
  # shellcheck disable=SC1090
  . "$HA_ENV_FILE"
  export AWS_PROFILE="$HA_AWS_PROFILE"
  if [ -n "${HA_REGION:-}" ]; then
    export AWS_REGION="$HA_REGION"
  fi
  if [ "${HA_EXPORT_CREDENTIALS:-0}" = "1" ]; then
    export_credentials
  fi
}

# For profiles the Java SDK can not use directly (e.g. AWS SSO, whose SDK module is not in the
# nrtsearch distribution): resolve temporary credentials with the AWS CLI and pass them to the JVMs
# as environment variables. They expire (typically after an hour); rerun to refresh. Servers keep
# the credentials they started with.
export_credentials() {
  command -v aws >/dev/null || die "--export-credentials needs the aws CLI"
  local creds
  creds="$(aws configure export-credentials --profile "$HA_AWS_PROFILE" --format env)" \
    || die "Could not export credentials for profile $HA_AWS_PROFILE (try: aws sso login --profile $HA_AWS_PROFILE)"
  eval "$creds"
  unset AWS_PROFILE
}

primaries() { local i; for i in $(seq 1 "$HA_PRIMARIES"); do echo "p$i"; done; }
replicas() { local i; [ "$HA_REPLICAS" -gt 0 ] || return 0; for i in $(seq 1 "$HA_REPLICAS"); do echo "r$i"; done; }
all_nodes() { primaries; replicas; }

check_node() {
  case "$1" in
    p[0-9]*) [ "${1#p}" -ge 1 ] && [ "${1#p}" -le "$HA_PRIMARIES" ] || die "No primary $1 (have $HA_PRIMARIES)";;
    r[0-9]*) [ "${1#r}" -ge 1 ] && [ "${1#r}" -le "$HA_REPLICAS" ] || die "No replica $1 (have $HA_REPLICAS)";;
    *) die "Unknown node '$1': use p1..p$HA_PRIMARIES or r1..r$HA_REPLICAS";;
  esac
}

# Expand node arguments: "all", "primaries", "replicas", or node names.
expand_nodes() {
  local a
  for a in "$@"; do
    case "$a" in
      all) all_nodes;;
      primaries) primaries;;
      replicas) replicas;;
      *) check_node "$a"; echo "$a";;
    esac
  done
}

node_port() {
  case "$1" in
    p*) echo $((HA_BASE_PORT + ${1#p} * 10));;
    r*) echo $((HA_BASE_PORT + 100 + ${1#r} * 10));;
  esac
}
node_repl_port() { echo $(( $(node_port "$1") + 1 )); }
node_dir() { echo "$HA_DIR/nodes/$1"; }
node_log() { echo "$(node_dir "$1")/server.log"; }
node_pid() { cat "$(node_dir "$1")/pid" 2>/dev/null || true; }

is_running() {
  local pid; pid="$(node_pid "$1")"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

is_paused() {
  local pid; pid="$(node_pid "$1")"
  [ -n "$pid" ] && ps -o stat= -p "$pid" 2>/dev/null | grep -q '^T'
}

node_state() {
  if ! is_running "$1"; then echo "down"
  elif is_paused "$1"; then echo "paused"
  else echo "running"; fi
}

# ---------------------------------------------------------------------------------------------
# tools

client() {
  # client <node> <nrtsearch_client args...>
  local node="$1"; shift
  JAVA_OPTS="$HA_CLIENT_JAVA_OPTS" "$HA_BIN_DIR/nrtsearch_client" -h localhost -p "$(node_port "$node")" "$@"
}

# The last JSON line printed by a command (log lines go to stdout too).
json_line() { grep '^{' | tail -1; }

resolve_leader_json() {
  # prints the resolveLeader response or {"error":...}; never fails
  local out
  out="$(client "$1" resolveLeader --timeoutMs "${2:-2000}" 2>/dev/null | json_line || true)"
  if [ -z "$out" ]; then out='{"error":"NO_RESPONSE","description":"client failed"}'; fi
  echo "$out"
}

doc_count_json() {
  local out
  out="$(client "$1" docCount -i "$HA_INDEX" --timeoutMs "${2:-5000}" 2>/dev/null | json_line || true)"
  if [ -z "$out" ]; then out='{"error":"NO_RESPONSE","description":"client failed"}'; fi
  echo "$out"
}

# Doc count of a node, or empty if it can not be counted.
doc_count() { doc_count_json "$1" | jq -r '.totalHits // empty'; }

nrt_utils() {
  JAVA_OPTS="$HA_CLIENT_JAVA_OPTS" "$HA_BIN_DIR/nrt_utils" "$@"
}

fence_state() {
  local region_args=()
  if [ -n "${HA_REGION:-}" ]; then region_args=(--region "$HA_REGION"); fi
  nrt_utils getFenceState -s "$HA_SERVICE" -b "$HA_BUCKET" "${region_args[@]+"${region_args[@]}"}" 2>/dev/null | grep '^{'
}

# Current leader as "<node> <epoch>", from the primary that confirms it is the leader. Empty if no
# running primary claims leadership. Fails if more than one primary claims it at once.
current_leader() {
  local n out claims=""
  for n in $(primaries); do
    [ "$(node_state "$n")" = "running" ] || continue
    out="$(resolve_leader_json "$n" 1500)"
    if [ "$(echo "$out" | jq -r '.isLeader // false')" = "true" ]; then
      claims="$claims$n $(echo "$out" | jq -r '.epoch')"$'\n'
    fi
  done
  claims="$(printf '%s' "$claims" | sed '/^$/d')"
  local count; count="$(printf '%s' "$claims" | grep -c . || true)"
  if [ "$count" -gt 1 ]; then
    warn "More than one primary claims leadership: $(echo "$claims" | tr '\n' ' ')"
    printf '%s\n' "$claims" | sort -k2 -n | tail -1
    return 2
  fi
  printf '%s' "$claims"
}

leader_node() { current_leader | awk '{print $1}'; }

# ---------------------------------------------------------------------------------------------
# config generation

write_primary_config() {
  local n="$1" d; d="$(node_dir "$n")"
  mkdir -p "$d/state" "$d/index"
  cat > "$d/config.yaml" <<EOF
nodeName: $n
hostName: localhost
port: $(node_port "$n")
replicationPort: $(node_repl_port "$n")
serviceName: $HA_SERVICE
bucketName: $HA_BUCKET
stateDir: $d/state
indexDir: $d/index
stateConfig:
  backendType: REMOTE
  remote:
    readOnly: false
indexStartConfig:
  autoStart: true
  mode: PRIMARY
  dataLocationType: REMOTE
s3RefreshUpload: true
s3RefreshNotifyAfterUpload: true
syncInitialNrtPoint: true
leaderElection:
  type: s3_lease
  # all nodes run on this machine
  allowLoopbackHost: true
  refreshIntervalMs: $HA_REFRESH_MS
  params:
    ttlMs: $HA_TTL_MS
EOF
}

write_replica_config() {
  local n="$1" d; d="$(node_dir "$n")"
  mkdir -p "$d/state" "$d/index"
  cat > "$d/config.yaml" <<EOF
nodeName: $n
hostName: localhost
port: $(node_port "$n")
replicationPort: $(node_repl_port "$n")
serviceName: $HA_SERVICE
bucketName: $HA_BUCKET
stateDir: $d/state
indexDir: $d/index
stateConfig:
  backendType: REMOTE
  remote:
    readOnly: true
indexStartConfig:
  autoStart: true
  mode: REPLICA
  dataLocationType: REMOTE
  primaryDiscovery:
    resolver: LEADER_RPC
    file: $HA_DIR/primaries.json
    leaderResolvePollMs: 1000
syncInitialNrtPoint: true
EOF
}

write_configs() {
  local n first=1
  {
    echo "["
    for n in $(primaries); do
      [ $first -eq 1 ] || echo ","
      first=0
      printf '  {"host": "localhost", "port": %d}' "$(node_repl_port "$n")"
    done
    echo
    echo "]"
  } > "$HA_DIR/primaries.json"
  for n in $(primaries); do write_primary_config "$n"; done
  for n in $(replicas); do write_replica_config "$n"; done
}

# ---------------------------------------------------------------------------------------------
# commands

cmd_init() {
  local profile="" bucket="" service="" region="" primaries_n=3 replicas_n=1 ttl=10000 base_port=8100
  local index="ha_test" refresh_ms=5000 export_creds=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --profile) profile="$2"; shift 2;;
      --bucket) bucket="$2"; shift 2;;
      --service) service="$2"; shift 2;;
      --region) region="$2"; shift 2;;
      --primaries) primaries_n="$2"; shift 2;;
      --replicas) replicas_n="$2"; shift 2;;
      --ttl-ms) ttl="$2"; shift 2;;
      --base-port) base_port="$2"; shift 2;;
      --index) index="$2"; shift 2;;
      --refresh-ms) refresh_ms="$2"; shift 2;;
      --export-credentials) export_creds=1; shift;;
      *) die "Unknown init option: $1";;
    esac
  done
  [ -n "$profile" ] || die "init needs --profile <aws profile name>"
  [ -n "$bucket" ] || die "init needs --bucket <bucket>"
  [ "$primaries_n" -ge 1 ] || die "--primaries must be >= 1"
  if [ -f "$HA_ENV_FILE" ]; then
    load_env
    for n in $(all_nodes); do
      if is_running "$n"; then die "Cluster at $HA_DIR is running; run '$0 destroy' first"; fi
    done
  fi
  # a fresh service name per init: no state is shared with earlier runs, nothing has to be deleted
  if [ -z "$service" ]; then service="ha-local-${USER:-dev}-$(date +%Y%m%d-%H%M%S)"; fi
  rm -rf "$HA_DIR/nodes"
  mkdir -p "$HA_DIR"
  cat > "$HA_ENV_FILE" <<EOF
HA_AWS_PROFILE='$profile'
HA_BUCKET='$bucket'
HA_SERVICE='$service'
HA_REGION='$region'
HA_PRIMARIES=$primaries_n
HA_REPLICAS=$replicas_n
HA_TTL_MS=$ttl
HA_BASE_PORT=$base_port
HA_INDEX='$index'
HA_REFRESH_MS=$refresh_ms
HA_EXPORT_CREDENTIALS=$export_creds
EOF
  load_env
  write_configs
  log "Cluster dir: $HA_DIR"
  log "Service:     $HA_SERVICE  (s3://$HA_BUCKET/$HA_SERVICE/)"
  log "Nodes:       $(all_nodes | tr '\n' ' ')"
  for n in $(all_nodes); do log "  $n client port $(node_port "$n"), replication port $(node_repl_port "$n")"; done
  cmd_preflight
}

cmd_preflight() {
  load_env
  [ -x "$HA_BIN_DIR/nrtsearch_server" ] || die "No build at $HA_BIN_DIR; run: $0 build"
  command -v jq >/dev/null || die "jq is required"
  log "Checking S3 access to bucket $HA_BUCKET with AWS profile $HA_AWS_PROFILE ..."
  local out region_args=()
  if [ -n "${HA_REGION:-}" ]; then region_args=(--region "$HA_REGION"); fi
  if ! out="$(nrt_utils getFenceState -s "$HA_SERVICE" -b "$HA_BUCKET" "${region_args[@]+"${region_args[@]}"}" 2>&1)"; then
    echo "$out" | grep -i 'exception\|error\|unable\|sso' | grep -v '^\s*at ' | head -8 >&2
    if echo "$out" | grep -qi 'sso'; then
      warn "Profile $HA_AWS_PROFILE looks like an AWS SSO profile; rerun init with --export-credentials"
    fi
    die "Could not read s3://$HA_BUCKET with profile $HA_AWS_PROFILE (expired session? missing region: pass --region)"
  fi
  out="$(echo "$out" | grep '^{')"
  if echo "$out" | jq -e 'select(.type != "LEASE" and .exists == true)' >/dev/null 2>&1; then
    warn "Service $HA_SERVICE already has state in S3 (continuing with it)"
  fi
  log "S3 access OK"
}

cmd_build() {
  (cd "$HA_REPO_DIR" && ./gradlew installDist -q)
  log "Built $HA_BIN_DIR"
}

start_node() {
  local n="$1"
  if is_running "$n"; then log "$n already running (pid $(node_pid "$n"))"; return 0; fi
  local d; d="$(node_dir "$n")"
  [ -f "$d/config.yaml" ] || die "No config for $n; run init"
  echo "===== start $(date) =====" >> "$(node_log "$n")"
  NRTSEARCH_SERVER_OPTS="$HA_SERVER_JAVA_OPTS" nohup "$HA_BIN_DIR/nrtsearch_server" "$d/config.yaml" \
    >> "$(node_log "$n")" 2>&1 &
  echo $! > "$d/pid"
  log "Started $n (pid $!, port $(node_port "$n"), log $(node_log "$n"))"
}

# Wait until a node logs that its server is listening, since its last start.
wait_started() {
  local n="$1" timeout="${2:-180}" start; start=$(date +%s)
  while true; do
    if ! is_running "$n"; then
      tail -30 "$(node_log "$n")" >&2
      die "$n exited during startup, see $(node_log "$n")"
    fi
    if awk '/===== start/{found=0} /Server started, listening on/{found=1} END{exit !found}' "$(node_log "$n")"; then
      if awk '/===== start/{found=0} /return a dummy S3Client/{found=1} END{exit !found}' "$(node_log "$n")"; then
        die "$n could not reach S3 (it fell back to a dummy client); check AWS profile/region"
      fi
      return 0
    fi
    [ $(( $(date +%s) - start )) -lt "$timeout" ] || die "$n not started after ${timeout}s, see $(node_log "$n")"
    sleep 1
  done
}

cmd_start() {
  load_env
  [ $# -gt 0 ] || set -- all
  local nodes n; nodes="$(expand_nodes "$@")"
  for n in $nodes; do start_node "$n"; done
  for n in $nodes; do wait_started "$n"; log "$n is up"; done
}

stop_node() {
  local n="$1" pid; pid="$(node_pid "$n")"
  if ! is_running "$n"; then log "$n is not running"; return 0; fi
  if is_paused "$n"; then kill -CONT "$pid"; fi
  kill -TERM "$pid"
  local i
  for i in $(seq 1 90); do
    is_running "$n" || { log "Stopped $n"; return 0; }
    sleep 1
  done
  warn "$n did not stop in 90s, killing"
  kill -KILL "$pid" 2>/dev/null || true
}

cmd_stop() { load_env; [ $# -gt 0 ] || set -- all; local n; for n in $(expand_nodes "$@"); do stop_node "$n"; done; }

cmd_kill() {
  load_env
  local n
  for n in $(expand_nodes "$@"); do
    if is_running "$n"; then kill -KILL "$(node_pid "$n")"; log "Killed $n (SIGKILL)"; else log "$n is not running"; fi
  done
}

cmd_pause() {
  load_env
  local n
  for n in $(expand_nodes "$@"); do
    is_running "$n" || die "$n is not running"
    kill -STOP "$(node_pid "$n")"; log "Paused $n (SIGSTOP), simulating a long GC pause"
  done
}

cmd_resume() {
  load_env
  local n
  for n in $(expand_nodes "$@"); do
    is_running "$n" || die "$n is not running"
    kill -CONT "$(node_pid "$n")"; log "Resumed $n (SIGCONT)"
  done
}

cmd_status() {
  load_env
  printf '%-4s %-8s %-7s %-6s %-9s %-16s %-10s %s\n' NODE STATE PID PORT ROLE EPOCH LEADER DOCS
  local n state out role epoch leader docs pid
  for n in $(all_nodes); do
    state="$(node_state "$n")"; role="-"; epoch="-"; leader="-"; docs="-"
    if [ "$state" = "running" ]; then
      case "$n" in
        p*)
          out="$(resolve_leader_json "$n" 1500)"
          if echo "$out" | jq -e '.error' >/dev/null; then
            role="$(echo "$out" | jq -r '.error')"
          else
            if [ "$(echo "$out" | jq -r '.isLeader')" = "true" ]; then role="ACTIVE"; else role="standby"; fi
            epoch="$(echo "$out" | jq -r '.epoch')"
            leader="$(echo "$out" | jq -r '.nodeName')"
          fi;;
        r*) role="replica";;
      esac
      docs="$(doc_count "$n")"; docs="${docs:--}"
    fi
    pid="$(node_pid "$n")"
    printf '%-4s %-8s %-7s %-6s %-9s %-16s %-10s %s\n' "$n" "$state" "${pid:--}" \
      "$(node_port "$n")" "$role" "$epoch" "$leader" "$docs"
  done
  printf '\nexpected committed docs: %s (next doc id %s)\n' "$(committed_docs)" "$(next_doc_id)"
}

cmd_leader() {
  load_env
  local l; l="$(current_leader)" || true
  [ -n "$l" ] || die "No running primary claims leadership"
  echo "$l"
}

cmd_fence() {
  load_env
  fence_state | jq -c '
    if .type == "LEASE" then
      {type, exists, holder: .lease.holder.nodeName?, epoch: .lease.holder.epoch?, released: .lease.released?, renewal: .lease.renewal?, etag, lastModified}
    else {type, index, epoch, tombstone, value, etag} end'
}

# ---------------------------------------------------------------------------------------------
# test data
#
# Documents have sequential doc_id values. The harness tracks the number of committed documents
# (acknowledged by a successful commit) to verify none are lost on failover.

state_file() { echo "$HA_DIR/$1"; }
committed_docs() { cat "$(state_file committed)" 2>/dev/null || echo 0; }
next_doc_id() { cat "$(state_file next_id)" 2>/dev/null || echo 0; }

cmd_setup_index() {
  load_env
  local leader; leader="$(leader_node)"
  [ -n "$leader" ] || die "No leader; start primaries first"
  local fields="$HA_DIR/fields.json"
  cat > "$fields" <<EOF
{
  "indexName": "$HA_INDEX",
  "field": [
    {"name": "doc_id", "type": "ATOM", "search": true, "storeDocValues": true},
    {"name": "value", "type": "INT", "storeDocValues": true}
  ]
}
EOF
  log "Creating and starting index $HA_INDEX on leader $leader"
  client "$leader" createIndex -i "$HA_INDEX" --fields="@$fields" --start > "$HA_DIR/setup_index.log" 2>&1 \
    || { cat "$HA_DIR/setup_index.log" >&2; die "createIndex failed"; }
  echo 0 > "$(state_file committed)"
  echo 0 > "$(state_file next_id)"
  log "Index $HA_INDEX created"
}

# Add documents to a node, without committing. Prints the number added.
add_docs() {
  local node="$1" count="$2" start; start="$(next_doc_id)"
  local csv="$HA_DIR/docs_${start}.csv" i
  {
    echo "doc_id,value"
    for i in $(seq "$start" $((start + count - 1))); do echo "$i,$i"; done
  } > "$csv"
  # ids are consumed even if adding fails, so a retry never reuses one
  echo $((start + count)) > "$(state_file next_id)"
  client "$node" addDocuments -i "$HA_INDEX" -t csv -f "$csv" > "$HA_DIR/last_add.log" 2>&1
}

commit_node() { client "$1" commit -i "$HA_INDEX" > "$HA_DIR/last_commit.log" 2>&1; }

# Add documents and commit on the leader (or a given node); counts them as committed on success.
cmd_index() {
  load_env
  local count="${1:-100}" node="${2:-}"
  [ -n "$node" ] || node="$(leader_node)"
  [ -n "$node" ] || die "No leader"
  # with a single writer, everything added since the last successful commit is part of this one
  local before; before="$(next_doc_id)"
  add_docs "$node" "$count" || { tail -5 "$HA_DIR/last_add.log" >&2; die "addDocuments to $node failed"; }
  commit_node "$node" || { tail -5 "$HA_DIR/last_commit.log" >&2; die "commit on $node failed"; }
  echo $(( $(committed_docs) + count )) > "$(state_file committed)"
  log "Indexed and committed $count docs on $node (ids $before..$((before + count - 1))); committed total $(committed_docs)"
}

cmd_count() { load_env; local n; for n in $(expand_nodes "${@:-all}"); do echo "$n $(doc_count_json "$n")"; done; }

cmd_logs() {
  load_env
  check_node "$1"
  tail -n "${2:-100}" -f "$(node_log "$1")"
}

cmd_client() { load_env; local n="$1"; shift; check_node "$n"; client "$n" "$@"; }

cmd_destroy() {
  load_env
  local n
  for n in $(all_nodes); do
    if is_running "$n"; then
      if is_paused "$n"; then kill -CONT "$(node_pid "$n")" || true; fi
      kill -KILL "$(node_pid "$n")" || true
    fi
  done
  rm -rf "$HA_DIR"
  log "Removed $HA_DIR. S3 state is kept; remove it with:"
  log "  aws s3 rm --recursive s3://$HA_BUCKET/$HA_SERVICE/ --profile $HA_AWS_PROFILE"
}

usage() {
  cat <<EOF
Local multi-primary nrtsearch cluster against real S3.

  $(basename "$0") build                       build the distribution (gradle installDist)
  $(basename "$0") init --profile P --bucket B [--region R] [--primaries 3] [--replicas 1]
                    [--ttl-ms 10000] [--base-port 8100] [--service NAME] [--index ha_test]
                    [--export-credentials]  (for SSO profiles; needs the aws CLI)
                                               write configs for a new cluster (fresh service name)
  $(basename "$0") start [nodes]               start nodes (default all) and wait for them
  $(basename "$0") stop [nodes]                graceful stop (SIGTERM)
  $(basename "$0") kill <nodes>                hard kill (SIGKILL)
  $(basename "$0") pause <nodes>               SIGSTOP, e.g. to simulate a GC pause
  $(basename "$0") resume <nodes>              SIGCONT
  $(basename "$0") status                      role, epoch, leader seen, doc count per node
  $(basename "$0") leader                      "<node> <epoch>" of the confirmed leader
  $(basename "$0") fence                       lease and _current pointer epochs in S3
  $(basename "$0") setup-index                 create and start the test index on the leader
  $(basename "$0") index [count] [node]        add docs and commit (default: on the leader)
  $(basename "$0") count [nodes]               exact doc count per node
  $(basename "$0") logs <node> [lines]         tail a node log
  $(basename "$0") client <node> <args...>     run nrtsearch_client against a node
  $(basename "$0") destroy                     kill all nodes and remove local data

Nodes: p1..pN (primaries), r1..rM (replicas), or: all, primaries, replicas.
Cluster dir: \$HA_DIR (default /tmp/nrtsearch-ha-local).
EOF
}

main() {
  local cmd="${1:-help}"; [ $# -gt 0 ] && shift
  case "$cmd" in
    build) cmd_build "$@";;
    init) cmd_init "$@";;
    preflight) cmd_preflight "$@";;
    start) cmd_start "$@";;
    stop) cmd_stop "$@";;
    kill) cmd_kill "$@";;
    pause) cmd_pause "$@";;
    resume) cmd_resume "$@";;
    status) cmd_status "$@";;
    leader) cmd_leader "$@";;
    fence) cmd_fence "$@";;
    setup-index) cmd_setup_index "$@";;
    index) cmd_index "$@";;
    count) cmd_count "$@";;
    logs) cmd_logs "$@";;
    client) cmd_client "$@";;
    destroy) cmd_destroy "$@";;
    help|-h|--help) usage;;
    *) usage; die "Unknown command: $cmd";;
  esac
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
