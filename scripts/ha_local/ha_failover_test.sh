#!/usr/bin/env bash
#
# Failover scenarios for a local multi-primary cluster against real S3. Each scenario injects a
# failure, then verifies:
#   - exactly one primary becomes leader, with a higher epoch
#   - no committed (acknowledged) document is lost, on the leader, replicas, and standbys
#   - replicas follow the new leader (new writes reach them)
#   - the old leader can not write after it lost leadership (fencing)
#   - every claimed _current pointer and the lease in S3 carry the new leader's epoch
#
# Usage:
#   ha_failover_test.sh --profile <aws profile> --bucket <bucket> [--region R] [--export-credentials]
#                       [--primaries 3] [--replicas 1] [--ttl-ms 10000] [--keep] [scenario...]
#
# Scenarios (default: all, in this order):
#   kill_leader       SIGKILL the leader
#   stop_leader       graceful stop of the leader (lease released, fast handover)
#   pause_leader      SIGSTOP the leader past the lease ttl (GC pause / partition), then resume it
#   uncommitted       kill the leader right after adding docs without a commit
#   standby_write     writes to a standby are rejected
#   full_restart      stop and restart all primaries
#
# A fresh cluster (new service name) is created for every run. Afterwards all nodes are killed but
# logs and data are kept; with --keep the cluster is left running for inspection (ha_local.sh
# status / fence / logs). Remove it with ha_local.sh destroy.

HA_TEST_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=ha_local.sh
. "$HA_TEST_SCRIPT_DIR/ha_local.sh"
set +e  # failures are checked explicitly
set -u

PASSES=0
FAILS=0
RESULTS=""
TIMINGS=""

pass() { PASSES=$((PASSES + 1)); RESULTS="$RESULTS${C_GREEN}PASS${C_OFF} [$SCENARIO] $*"$'\n'; echo "${C_GREEN}  PASS${C_OFF} $*" >&2; }
fail() { FAILS=$((FAILS + 1)); RESULTS="$RESULTS${C_RED}FAIL${C_OFF} [$SCENARIO] $*"$'\n'; echo "${C_RED}  FAIL${C_OFF} $*" >&2; }
timing() { TIMINGS="$TIMINGS  [$SCENARIO] $*"$'\n'; log "timing: $*"; }
check() { local desc="$1"; shift; if "$@"; then pass "$desc"; else fail "$desc"; fi; }

# A failed required step leaves the cluster in an unknown state: stop the run.
require() {
  local desc="$1"; shift
  if "$@"; then pass "$desc"; else fail "$desc"; summary; exit 1; fi
}

SCENARIO="setup"

# wait_until <timeout s> <command...>: retry every second until the command succeeds
wait_until() {
  local timeout="$1"; shift
  local start; start=$(date +%s)
  while true; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    [ $(( $(date +%s) - start )) -lt "$timeout" ] || return 1
    sleep 1
  done
}

leader_epoch() { current_leader 2>/dev/null | awk '{print $2}'; }

# Wait for a confirmed leader with an epoch above <old epoch>; sets NEW_LEADER and NEW_EPOCH.
wait_new_leader() {
  local old="$1" timeout="$2" start l
  start=$(date +%s)
  NEW_LEADER=""; NEW_EPOCH=""
  while true; do
    l="$(current_leader 2>/dev/null)" || true
    if [ -n "$l" ] && [ "$(echo "$l" | awk '{print $2}')" -gt "$old" ]; then
      NEW_LEADER="$(echo "$l" | awk '{print $1}')"
      NEW_EPOCH="$(echo "$l" | awk '{print $2}')"
      return 0
    fi
    [ $(( $(date +%s) - start )) -lt "$timeout" ] || return 1
    sleep 1
  done
}

count_is() { [ "$(doc_count "$1")" = "$2" ]; }
count_at_least() { local c; c="$(doc_count "$1")"; [ -n "$c" ] && [ "$c" -ge "$2" ]; }

# Wait for a node's count to reach the expected value; reports the last seen count on failure.
check_count() {
  local node="$1" expected="$2" timeout="${3:-90}"
  if wait_until "$timeout" count_is "$node" "$expected"; then
    pass "$node has all $expected committed docs"
  else
    fail "$node doc count $(doc_count "$node" | sed 's/^$/unavailable/'), expected $expected"
  fi
}

check_replicas() {
  local r
  for r in $(replicas); do check_count "$r" "$(committed_docs)" "${1:-90}"; done
}

# A standby reports itself not leader and knows the current leader.
is_standby_of() {
  local out; out="$(resolve_leader_json "$1")"
  [ "$(echo "$out" | jq -r '.isLeader')" = "false" ] && [ "$(echo "$out" | jq -r '.nodeName')" = "$2" ]
}

# S3 state matches the leader: lease held by it at its epoch, every claimed pointer (global state,
# index state, point state) at its epoch, and no pointer from a newer epoch.
check_fence() {
  local leader="$1" epoch="$2" fs
  fs="$(fence_state)" || { fail "could not read fence state from S3"; return; }
  echo "$fs" > "$HA_DIR/fence_${SCENARIO}.jsonl"
  local holder lease_epoch
  holder="$(echo "$fs" | jq -r 'select(.type == "LEASE") | .lease.holder.nodeName')"
  lease_epoch="$(echo "$fs" | jq -r 'select(.type == "LEASE") | .lease.holder.epoch')"
  if [ "$holder" = "$leader" ] && [ "$lease_epoch" = "$epoch" ]; then
    pass "S3 lease held by $leader at epoch $epoch"
  else
    fail "S3 lease held by $holder at epoch $lease_epoch, expected $leader at $epoch"
  fi
  local bad
  bad="$(echo "$fs" | jq -c --argjson e "$epoch" \
    'select(.type == "GLOBAL_STATE" or .type == "INDEX_STATE" or .type == "POINT_STATE")
     | select(.exists == true and .epoch != $e) | {type, index, epoch}')"
  if [ -z "$bad" ]; then
    pass "all claimed _current pointers are at epoch $epoch"
  else
    fail "pointers not at epoch $epoch: $(echo "$bad" | tr '\n' ' ')"
  fi
  bad="$(echo "$fs" | jq -c --argjson e "$epoch" 'select(.epoch? != null and .epoch > $e) | {type, index, epoch}')"
  [ -z "$bad" ] || fail "pointers from an epoch newer than the leader: $bad"
}

# Index a batch on the leader and verify it reaches every replica (they follow the new leader).
check_writes_follow() {
  local leader="$1"
  if (cmd_index 50 "$leader"); then
    pass "indexed and committed 50 docs on $leader"
  else
    fail "indexing on $leader failed"
    return
  fi
  check_count "$leader" "$(committed_docs)" 30
  check_replicas 90
}

# Restart a node that was a leader and check it rejoins as a standby with all committed docs.
check_rejoins_as_standby() {
  local node="$1" leader="$2"
  start_node "$node"
  wait_started "$node"
  check "$node rejoined as standby of $leader" wait_until 60 is_standby_of "$node" "$leader"
  # standbys poll remote state (every 10s by default)
  check_count "$node" "$(committed_docs)" 90
}

failover_timeout() { echo $(( HA_TTL_MS * 3 / 1000 + 60 )); }

# ---------------------------------------------------------------------------------------------

scenario_kill_leader() {
  local old epoch t0 t1
  old="$(leader_node)"; epoch="$(leader_epoch)"
  log "Leader is $old at epoch $epoch; killing it"
  t0=$(now_ms)
  cmd_kill "$old"
  require "new leader elected after SIGKILL of $old" wait_new_leader "$epoch" "$(failover_timeout)"
  t1=$(now_ms)
  timing "SIGKILL failover $old -> $NEW_LEADER: $(( t1 - t0 )) ms (lease ttl $HA_TTL_MS ms)"
  check "new leader $NEW_LEADER epoch $NEW_EPOCH > $epoch" [ "$NEW_EPOCH" -gt "$epoch" ]
  check_count "$NEW_LEADER" "$(committed_docs)" 10
  check_fence "$NEW_LEADER" "$NEW_EPOCH"
  check_replicas 90
  check_writes_follow "$NEW_LEADER"
  check_rejoins_as_standby "$old" "$NEW_LEADER"
}

scenario_stop_leader() {
  local old epoch t0 t1
  old="$(leader_node)"; epoch="$(leader_epoch)"
  log "Leader is $old at epoch $epoch; stopping it gracefully"
  t0=$(now_ms)
  stop_node "$old"
  require "new leader elected after graceful stop of $old" wait_new_leader "$epoch" "$(failover_timeout)"
  t1=$(now_ms)
  timing "graceful failover $old -> $NEW_LEADER: $(( t1 - t0 )) ms, including shutdown"
  check_count "$NEW_LEADER" "$(committed_docs)" 10
  check_fence "$NEW_LEADER" "$NEW_EPOCH"
  check_writes_follow "$NEW_LEADER"
  check_rejoins_as_standby "$old" "$NEW_LEADER"
}

scenario_pause_leader() {
  local old epoch t0 t1
  old="$(leader_node)"; epoch="$(leader_epoch)"
  log "Leader is $old at epoch $epoch; pausing it (SIGSTOP) past the lease ttl"
  t0=$(now_ms)
  cmd_pause "$old"
  require "new leader elected while $old is paused" wait_new_leader "$epoch" "$(failover_timeout)"
  t1=$(now_ms)
  timing "pause failover $old -> $NEW_LEADER: $(( t1 - t0 )) ms"
  local new="$NEW_LEADER" new_epoch="$NEW_EPOCH"
  # writes on the new leader while the old one is still paused
  check_writes_follow "$new"
  local committed_before; committed_before="$(committed_docs)"

  log "Resuming $old; it must not be able to write"
  cmd_resume "$old"
  # immediately try to write to the stale leader: add without commit, then commit
  local start; start="$(next_doc_id)"
  add_docs "$old" 25
  local add_rc=$?
  commit_node "$old"
  local commit_rc=$?
  if [ $commit_rc -ne 0 ]; then
    pass "commit on resumed stale leader $old rejected (add rc=$add_rc)"
    grep -o 'FAILED_PRECONDITION[^"]*' "$HA_DIR/last_commit.log" | head -1 | sed 's/^/    /' >&2
  else
    fail "commit on resumed stale leader $old succeeded"
  fi
  check "$old demoted to standby of $new" wait_until 60 is_standby_of "$old" "$new"
  check "only $new claims leadership" [ "$(leader_node)" = "$new" ]
  # docs ${start}.. sent to the stale leader must not appear anywhere
  check_count "$new" "$committed_before" 10
  check_fence "$new" "$new_epoch"
  check_replicas 90
  check_count "$old" "$committed_before" 90
  if grep -q -i 'fenced\|Demoting to standby' "$(node_log "$old")"; then
    log "  $old log: $(grep -i 'fenced\|Demoting to standby' "$(node_log "$old")" | tail -2 | cut -c1-200 | tr '\n' ' ')"
  fi
}

scenario_uncommitted() {
  local old epoch replica_before r
  old="$(leader_node)"; epoch="$(leader_epoch)"
  local committed; committed="$(committed_docs)"
  replica_before=""
  for r in $(replicas); do replica_before="$replica_before $r=$(doc_count "$r")"; done
  log "Adding 50 docs to $old without commit, then killing it"
  add_docs "$old" 50 || fail "add to $old failed"
  cmd_kill "$old"
  require "new leader elected" wait_new_leader "$epoch" "$(failover_timeout)"
  local c; c="$(doc_count "$NEW_LEADER")"
  # the new leader starts from the last version in S3: the last commit, or a later refresh upload
  if [ -n "$c" ] && [ "$c" -ge "$committed" ] && [ "$c" -le $((committed + 50)) ]; then
    pass "$NEW_LEADER has $c docs: all $committed committed, $((c - committed)) of 50 uncommitted survived via refresh upload"
  else
    fail "$NEW_LEADER has ${c:-no} docs, expected $committed..$((committed + 50))"
  fi
  # replicas only advance to durable versions, so they never go back
  for r in $(replicas); do
    local before; before="$(echo "$replica_before" | tr ' ' '\n' | grep "^$r=" | cut -d= -f2)"
    check "$r did not go back from $before docs" wait_until 30 count_at_least "$r" "${before:-0}"
  done
  # commit what survived so later scenarios have an exact expected count
  if [ -n "$c" ] && commit_node "$NEW_LEADER"; then echo "$c" > "$(state_file committed)"; fi
  check_replicas 90
  check_rejoins_as_standby "$old" "$NEW_LEADER"
}

scenario_standby_write() {
  local leader standby n
  leader="$(leader_node)"
  for n in $(primaries); do [ "$n" != "$leader" ] && [ "$(node_state "$n")" = "running" ] && { standby="$n"; break; }; done
  [ -n "${standby:-}" ] || { fail "no running standby"; return; }
  if commit_node "$standby"; then
    fail "commit on standby $standby succeeded"
  elif grep -q FAILED_PRECONDITION "$HA_DIR/last_commit.log"; then
    pass "commit on standby $standby rejected with FAILED_PRECONDITION"
  else
    fail "commit on standby $standby failed, but not with FAILED_PRECONDITION: $(grep -m1 -i 'exception' "$HA_DIR/last_commit.log")"
  fi
  if client "$standby" createIndex -i "ha_rejected_$(date +%s)" > "$HA_DIR/last_create.log" 2>&1; then
    fail "createIndex on standby $standby succeeded"
  else
    pass "createIndex on standby $standby rejected"
  fi
}

scenario_full_restart() {
  local epoch; epoch="$(leader_epoch)"
  log "Stopping all primaries (leader at epoch $epoch)"
  cmd_stop primaries
  cmd_start primaries
  require "leader elected after full restart" wait_new_leader "$epoch" "$(failover_timeout)"
  check_count "$NEW_LEADER" "$(committed_docs)" 30
  check_fence "$NEW_LEADER" "$NEW_EPOCH"
  local n
  for n in $(primaries); do
    [ "$n" = "$NEW_LEADER" ] && continue
    check "$n is standby of $NEW_LEADER" wait_until 60 is_standby_of "$n" "$NEW_LEADER"
  done
  check_writes_follow "$NEW_LEADER"
}

# ---------------------------------------------------------------------------------------------

summary() {
  echo >&2
  echo "==================== summary ====================" >&2
  printf '%s' "$RESULTS" >&2
  if [ -n "$TIMINGS" ]; then echo "timings:" >&2; printf '%s' "$TIMINGS" >&2; fi
  echo "passed: $PASSES, failed: $FAILS" >&2
  echo "cluster: $HA_DIR, service s3://${HA_BUCKET:-?}/${HA_SERVICE:-?}/" >&2
}

setup_cluster() {
  SCENARIO="setup"
  cmd_init "$@" || exit 1
  cmd_start primaries
  require "a leader is elected" wait_until "$(failover_timeout)" leader_node_nonempty
  local leader; leader="$(leader_node)"
  log "Leader: $(current_leader)"
  cmd_setup_index
  require "initial docs indexed" cmd_index 200 "$leader"
  local n
  for n in $(primaries); do
    [ "$n" = "$leader" ] && continue
    check_count "$n" "$(committed_docs)" 90
  done
  if [ "$HA_REPLICAS" -gt 0 ]; then
    cmd_start replicas
    check_replicas 120
  fi
  check_fence "$leader" "$(leader_epoch)"
}

leader_node_nonempty() { [ -n "$(leader_node)" ]; }

main_test() {
  local init_args=() scenarios=() keep=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --profile|--bucket|--region|--primaries|--replicas|--ttl-ms|--base-port|--service|--index)
        init_args+=("$1" "$2"); shift 2;;
      --export-credentials) init_args+=("$1"); shift;;
      --keep) keep=1; shift;;
      -h|--help) sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
      -*) die "Unknown option $1";;
      *) scenarios+=("$1"); shift;;
    esac
  done
  if [ ${#scenarios[@]} -eq 0 ]; then
    scenarios=(kill_leader stop_leader pause_leader uncommitted standby_write full_restart)
  fi
  local s
  for s in "${scenarios[@]}"; do
    declare -F "scenario_$s" >/dev/null || die "Unknown scenario: $s"
  done
  [ ${#init_args[@]} -gt 0 ] || die "Pass --profile <aws profile> --bucket <bucket>"

  if [ $keep -eq 0 ]; then
    # nodes are killed, logs and local data are kept for inspection (ha_local.sh destroy removes them)
    trap 'summary; log "Killing all nodes (use --keep to leave them running); logs: $HA_DIR/nodes/*/server.log"; cmd_kill all >/dev/null 2>&1' EXIT
  else
    trap 'summary; log "Cluster left running: $HA_SCRIPT_DIR/ha_local.sh status"' EXIT
  fi
  setup_cluster "${init_args[@]}"
  for s in "${scenarios[@]}"; do
    SCENARIO="$s"
    echo >&2
    log "==================== scenario: $s ===================="
    "scenario_$s"
  done
  [ $FAILS -eq 0 ]
}

main_test "$@"
