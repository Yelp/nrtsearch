# Local multi-primary failover testing

Scripts to run a multi-primary nrtsearch cluster on one machine against a **real S3 bucket**, and
to verify failover. Real S3 matters here: lease renewal and pointer fencing depend on S3's
conditional writes (`If-Match` / `If-None-Match`) and content derived ETags, which the in-memory
test stores only approximate.

- `ha_local.sh`: start, stop, kill, pause and inspect the nodes of a local cluster.
- `ha_failover_test.sh`: automated failover scenarios with pass/fail checks.

## Prerequisites

- Java 21, `jq`, `python3` (only for millisecond timestamps).
- An AWS profile that can read and write the bucket. Every run uses a new prefix
  (`ha-local-<user>-<timestamp>/`) in the bucket; nothing outside it is touched.
- A build: `scripts/ha_local/ha_local.sh build` (runs `./gradlew installDist`).

Credentials are taken from the profile via `AWS_PROFILE`, so static keys, `role_arn` +
`source_profile` and `credential_process` profiles work. The nrtsearch distribution does not
include the AWS SDK SSO module, so for **AWS SSO profiles** pass `--export-credentials` (needs the
`aws` CLI and a current `aws sso login`): temporary keys are exported to the JVMs instead. They
usually last an hour; a full test run takes about 10-15 minutes.

If the profile has no region, pass `--region <bucket region>`.

## Automated failover test

```bash
scripts/ha_local/ha_failover_test.sh --profile my-profile --bucket my-test-bucket
# a subset, more nodes, keep the cluster running afterwards:
scripts/ha_local/ha_failover_test.sh --profile my-profile --bucket my-test-bucket \
    --primaries 3 --replicas 2 --keep kill_leader pause_leader
```

Setup starts the primaries, waits for one leader, creates the `ha_test` index on it, indexes and
commits 200 documents, waits until the standbys and then the replicas have them, and checks the S3
fence state. Then each scenario runs against the same cluster:

| scenario | failure | verifies |
|---|---|---|
| `kill_leader` | `SIGKILL` the leader | new leader with a higher epoch, failover time, no committed doc lost, S3 lease and pointers at the new epoch, replicas follow (new writes reach them), the old node rejoins as a standby and catches up |
| `stop_leader` | graceful stop (lease released) | as above, with a faster handover |
| `pause_leader` | `SIGSTOP` past the lease ttl (GC pause, partition), new writes on the new leader, then `SIGCONT` | the resumed stale leader's commit is rejected, it demotes to a standby of the new leader, the docs sent to it appear nowhere, S3 pointers stay at the new epoch |
| `uncommitted` | kill the leader right after adding 50 docs without a commit | all committed docs survive (some uncommitted docs may survive through refresh uploads), replicas never go back in version |
| `standby_write` | commit and createIndex sent to a standby | rejected with `FAILED_PRECONDITION` |
| `full_restart` | stop and restart all primaries | a leader with a higher epoch, all committed docs, the others standby |

The tracked "committed" count only grows on a successful commit, so a doc count below it on any
node means an acknowledged commit was lost.

At the end a summary of all checks and the measured failover times is printed. All nodes are then
killed, but logs and data stay in `$HA_DIR` (default `/tmp/nrtsearch-ha-local`); `--keep` leaves
the cluster running. The fence state seen by each scenario is saved as
`$HA_DIR/fence_<scenario>.jsonl`.

Failover time is measured from the failure until a primary confirms leadership, polling about once
per second, so it is approximate. With the default `--ttl-ms 10000`, expect roughly 10-20s after a
kill or pause (takeover needs the lease unchanged for a ttl plus the poll interval), and a few
seconds after a graceful stop.

## Manual testing

```bash
H=scripts/ha_local/ha_local.sh
$H init --profile my-profile --bucket my-test-bucket   # --primaries 3 --replicas 1 --ttl-ms 10000
$H start primaries
$H status          # role, epoch, leader each node sees, doc count
$H setup-index     # create + start ha_test on the leader
$H index 500       # add 500 docs to the leader and commit
$H start replicas  # replicas start the indices in global state, so start them after setup-index
$H fence           # lease holder/epoch, and the epoch of every _current pointer in S3

$H kill p1         # or: stop p1 / pause p1 / resume p1
$H leader          # "<node> <epoch>"
$H count           # exact doc counts on all nodes
$H logs p2
$H client p2 search -f /path/to/search.json   # any nrtsearch_client command

$H destroy         # kill everything, delete local data (S3 data is kept; the command to remove it is printed)
```

Nodes are `p1..pN` (primaries) and `r1..rM` (replicas); commands also accept `all`,
`primaries` and `replicas`. Ports: primary `pK` uses client port `8100 + 10K` and replication port
`+1`; replica `rK` uses `8200 + 10K` (change with `--base-port`).

## Configuration used

Every primary runs with the same multi-primary settings (see `docs/multi_primary.rst`): remote
state, remote data, `s3RefreshUpload` and `s3RefreshNotifyAfterUpload`, and
`leaderElection: {type: s3_lease, allowLoopbackHost: true, refreshIntervalMs: 5000, params: {ttlMs}}`
(`allowLoopbackHost`, because all nodes advertise `localhost`). Replicas use
`primaryDiscovery.resolver: LEADER_RPC` with a discovery file listing every primary's replication
port (`$HA_DIR/primaries.json`), since the primaries' ports differ. Generated configs are in
`$HA_DIR/nodes/<node>/config.yaml`.

## Tools added for this

- `nrtsearch_client resolveLeader [--timeoutMs N]`: the leader a node knows, as one JSON line.
- `nrtsearch_client docCount -i <index>`: exact document count and searcher version, as JSON.
- `nrt_utils getFenceState -s <service> -b <bucket>`: the lease object and every `_current`
  pointer of the indices in global state, with the resource name, writer epoch, tombstone flag and
  ETag, one JSON object per line.

They print `{"error": "<grpc status>", ...}` and exit with code 2 when a call fails. The commands
also log to stdout; the scripts take the last line starting with `{`.
