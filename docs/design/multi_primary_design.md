# Multi-Primary nrtsearch: Design (as implemented)

Status: implemented on branch `ha_indexing` (phases 1–3). Phase 4 (ZooKeeper elector plugin) not started.
This document describes the code as it is, not the original proposal; section 17 lists where it diverged.

User facing configuration docs: `docs/multi_primary.rst`, `docs/server_configuration.rst` (`leaderElection`, `primaryDiscovery`).

---

## 1. Summary

A cluster (all nodes sharing a `serviceName`) can run N primary capable nodes. Exactly one is the **active
primary** (the writer for every index); the others are **standbys** that keep their local index data close to
the latest version in S3, so one of them can take over quickly.

Three mechanisms make this work:

1. **Election (liveness)**: a pluggable `PrimaryElector` picks the active primary and hands out strictly
   increasing **epochs**. The built-in elector is an S3 lease (`s3_lease`).
2. **Fencing (safety)**: every remote `_current` pointer (global state, index state, point state, warming
   queries) records the epoch of its writer and is only updated with S3 compare-and-set. A new leader **claims**
   every pointer before writing, after which any write by an older leader fails, however long it was paused.
   Correctness does not depend on the elector.
3. **Discovery**: every node answers `resolveLeader`. Replicas (`primaryDiscovery.resolver: LEADER_RPC`) and
   clients (`NrtSearchClient.Builder(Leader)`) poll seed nodes and follow the confirmed leader.

Durability on failover is **last durable version in S3** (last commit, or a later refresh upload). Acknowledged
commits are never lost; uncommitted documents may be.

With `leaderElection.type: none` (the default) behavior is unchanged from before this work.

## 2. Goals and non-goals

Goals

- N primaries per cluster, one active, automatic failover.
- No split brain: two nodes that both believe they lead can never both make durable progress.
- Pluggable election; S3 lease built in, ZooKeeper as an external plugin (keeps the dependency out of core).
- Replicas and ingestion clients follow failovers without config changes.
- Fast promotion: standbys are warm, promotion downloads only changed files.
- Zero change when election is disabled.

Non-goals

- Per index leadership (leadership is per cluster).
- Zero data loss for uncommitted documents.
- Remote stores other than AWS S3 (conditional `If-Match`/`If-None-Match` writes are required).
- Mixed version clusters with election enabled (old servers cannot parse fenced pointers).

## 3. Background: single primary nrtsearch

- Remote state lives in S3 under `<serviceName>/`: `global_state/`, and per index `<name>-<id>/{state,point_state,warming,data}/`.
  Each resource directory has a `_current` object naming the current version file.
- Before this work, `_current` writes were unconditional puts, `primaryGen` was the primary's start time in ms,
  and nothing compared generations across nodes (Lucene only asserts ordering).
- Replicas found the primary from a static host or a discovery file and registered with `addReplicas`.

## 4. Architecture overview

```
                 ┌──────────────── S3 bucket (serviceName/) ───────────────┐
                 │ leader/lease          lease JSON (holder, epoch, renewal)│
                 │ global_state/_current "<file>\n#nrt-fence epoch=E nonce" │
                 │ <idx-id>/state/_current, point_state/_current, warming/… │
                 │ <idx-id>/data/…       index files (keyed by ephemeral id)│
                 └───────▲───────────────▲─────────────────▲────────────────┘
          lease CAS,     │   claim + CAS │  poll lease,    │ poll point state,
          pointer CAS    │   writes      │  read state     │ download files
              ┌──────────┴───┐     ┌─────┴─────────┐ ┌─────┴─────────┐
              │ primary A    │     │ primary B     │ │ primary C     │
              │ ACTIVE (E)   │     │ STANDBY       │ │ STANDBY       │
              └──▲────────▲──┘     └──▲────────────┘ └──▲────────────┘
     newNRTPoint │        │ writes    │ resolveLeader   │
     copy files  │        │           │ (seeds)         │
              ┌──┴─────┐ ┌┴──────────────────────────────┴─┐
              │replicas│ │ ingestion clients (Leader builder)│
              └────────┘ └──────────────────────────────────┘
```

Main components (server, package `com.yelp.nrtsearch.server.leader` unless noted):

| Component | Role |
|---|---|
| `PrimaryElector`, `PrimaryElectorProvider`, `PrimaryElectorCreator`, `plugins.PrimaryElectorPlugin` | Election API and registry (built-in `s3_lease` + plugin types) |
| `S3LeaseElector` | Built-in lease elector |
| `ConditionalObjectStore`, `S3ConditionalObjectStore`, `InMemoryConditionalObjectStore` | Compare-and-set object store (S3 `If-Match`/`If-None-Match`) |
| `EpochPointerFence` | Claim and fenced write of `_current` pointers |
| `LeaderElection` | Holds the store and fence; created by `modules.LeaderModule` when enabled |
| `LeaderManager` | Role state machine (STANDBY/PROMOTING/ACTIVE/DEMOTING), transitions, state refresh |
| `state.BackendGlobalState` (`RoleController`) | Executes role transitions and refreshes on indices |
| `remote.s3.S3Backend` | Writes pointers through the fence; reads fenced pointer bodies |
| `handler.Handler.verifyActivePrimary`, `index.ShardState.verifyWritable` | Write gating |
| `handler.ResolveLeaderHandler` | `resolveLeader` on both gRPC services |
| `nrt.NRTReplicaNode`, `nrt.NrtDataManager` | Stale generation rejection, standby following, delta restore |
| `grpc.SharedLeaderClients`, `grpc.discovery.LeaderRpcNameResolverProvider` | Leader following replica channels |
| clientlib `leader.*`, `util.CoalescingRunner`, `grpc.NrtSearchClient.Builder(Leader)` | Leader resolution for replicas and clients |

## 5. Configuration and validation

```yaml
serviceName: my_cluster
hostName: primary-a.example.com     # must be reachable by other nodes
stateConfig:
  backendType: REMOTE
  remote: {readOnly: false}
indexStartConfig:
  autoStart: true
  mode: PRIMARY
  dataLocationType: REMOTE
s3RefreshUpload: true
s3RefreshNotifyAfterUpload: true
leaderElection:
  type: s3_lease          # none (default) | s3_lease | <plugin type>
  claimConcurrency: 32    # parallel index claims/restarts on promotion
  refreshIntervalMs: 30000  # standby global state refresh period, 0 = only on request
  reloadWaitMs: 10000     # max wait of a ReloadState request for its refresh
  allowLoopbackHost: false  # allow localhost hostName (single machine setups only)
  params:                 # elector specific
    ttlMs: 15000          # s3_lease: lease ttl
    safetyMarginMs: 2000  #           must be < ttlMs/2
    pollIntervalMs: 5000  #           default ttlMs/3, must be < ttlMs - safetyMarginMs
    leaseKey: ...         #           default <serviceName>/leader/lease
```

Startup validation (`BackendGlobalState.validateLeaderElectionConfig`), all errors reported together:

- `indexStartConfig.mode: PRIMARY`, `dataLocationType: REMOTE`, `stateConfig.backendType: REMOTE`.
- `s3RefreshUpload` and `s3RefreshNotifyAfterUpload` true (see 11.2: replicas must never be ahead of S3).
- `enforcePrimaryGenOrdering` true. It defaults to true when election is enabled, or when
  `primaryDiscovery.resolver: LEADER_RPC`; otherwise false (wall clock generations are not comparable across hosts).
- `hostName` not loopback (`localhost`, `127.*`, `::1`, `0.0.0.0`, empty) unless `allowLoopbackHost`.
- Unknown elector type fails before ports are bound (`PrimaryElectorCreator.validateType`).

Replica side (`IndexStartConfig`):

```yaml
indexStartConfig:
  mode: REPLICA
  primaryDiscovery:
    resolver: LEADER_RPC        # FILE (default, unchanged) | LEADER_RPC
    file: /path/primaries.json  # [{"host":..,"port":<replication port>}, ...]  or
    host: a,b,c                 # comma separated, with shared `port`
    port: 0                     # replication port (overrides file ports if > 0)
    leaderResolvePollMs: 2000
    allowSingleLeaderSeed: false
```

A single seed host is rejected (if it is the failed primary the replica could never find the new leader) unless
`allowSingleLeaderSeed` (e.g. a DNS or load balanced name covering all primaries). The check runs at startup for
auto started replicas, and for `primaryAddress` in `startIndex` requests (`INVALID_ARGUMENT`). An auto started
LEADER_RPC replica with neither host nor file only logs a warning (addresses may come from `startIndex` requests).

## 6. Election

### 6.1 API

```java
interface PrimaryElector extends Closeable {
  void start(LeaderListener listener);   // onElected(LeaderInfo), onLost(LeaderInfo)
  Optional<LeaderInfo> currentLeader();  // last known leader, may be this node
  boolean isLeader();                    // conservative: false as soon as leadership may be lost
  void resign(long epoch);               // atomically give up only if still held at epoch
  void close();                          // give up, never acquire again
}
record LeaderInfo(String leaderId /*ephemeral id*/, String nodeName, String host,
                  int port, int replicationPort, long epoch) {}
```

Contract: epochs strictly increase across all leadership changes; `isLeader` is conservative; `resign(epoch)`
is epoch scoped so a stale failure never releases newer leadership; no acquisition after `close`.
A faulty elector can stall failover but cannot corrupt data, because of fencing (section 7).

Plugins implement `PrimaryElectorPlugin.getPrimaryElectors() -> Map<String, PrimaryElectorProvider>`.
`PrimaryElectorCreator` registers `s3_lease` plus plugin types (duplicate names and `none` rejected).
Providers receive a `PrimaryElectorContext(config, localInfo, conditionalStore, prometheusRegistry)` and the
`leaderElection.params` map.

The elector is created lazily in `LeaderManager.start()`, called from `GlobalState.serverStarted(port)` after
both gRPC servers are bound, so `localInfo` advertises the real ports (including ephemeral port 0 configs).

### 6.2 S3 lease elector (`s3_lease`)

Lease object `<serviceName>/leader/lease`:

```json
{"holder": {"leaderId":..,"nodeName":..,"host":..,"port":..,"replicationPort":..,"epoch":E},
 "ttlMs": 15000, "released": false, "renewal": 42}
```

One scheduler thread per node runs `tick()` every `pollIntervalMs`:

- **Holder**: renew with `putIfMatch(heldVersion)`, incrementing `renewal`. S3's ETag is the MD5 of the
  content, so the counter is what makes each renewal a new version that contenders can observe.
  Validity: `renewSendTime + ttl - safetyMargin` on the holder's **monotonic** clock (send time, not response
  time). Precondition failure → lost. IO error → retry, lost once validity expires. A tick that starts after
  validity expired (e.g. after a long pause) declares loss without writing.
- **Contender**: `get` the lease. A new version (ETag) resets `observedAt`. Take over with
  `putIfMatch(observedVersion)` and `epoch + 1` once the same version has been seen unchanged for
  `ttl + safetyMargin` on its own monotonic clock, or immediately if `released`. If no lease exists,
  `putIfAbsent` with epoch `max(lastSeenEpoch + 1, wallClockMs)`.
- Wall clock is only used for the very first epoch, so it is larger than any pre-election `primaryGen`
  (which was a start time in ms). After that, epochs are `previous + 1` across all nodes.
- Expiry never compares clocks across hosts. Between the holder giving up and a takeover there are at least
  `2 * safetyMargin`, assuming bounded clock **rate** drift; correctness does not rely on this.
- `resign(epoch)` / `close()` write `released: true` (keeping the epoch so the next leader increments it) and
  block re-acquisition locally for one ttl, giving other nodes the first chance.
- Listener callbacks run on a separate single thread, in order; `isLeader()` and `currentLeader()` are lock free.

Failover time ≈ `ttl + safetyMargin + pollInterval` + promotion. Measured locally against S3 (ttl 10s):
SIGKILL ≈ 21s, SIGSTOP ≈ 16–20s, graceful stop ≈ 8s (including shutdown), as seen by a 1–3s polling harness.

## 7. Fencing (`EpochPointerFence`)

### 7.1 Problem

An "If-Match on the ETag I last wrote" rule is not enough: until the new leader writes a pointer, the old
leader's ETag still matches. An old leader resuming from a GC pause could move `_current` after the new leader
restored an older point, forking history.

### 7.2 Rule

The elector provides liveness only. Safety comes from compare-and-set plus the epoch stored **in the pointer**:

- Every fenced pointer body is `<value>\n#nrt-fence epoch=<E> nonce=<uuid>` (also `x-amz-meta-nrt-epoch`).
  The nonce makes every write unique content, so every write produces a new ETag (S3 ETag = content MD5).
- Readers use `EpochPointerFence.decodeValue` (everything before `"\n#nrt-fence "`); unfenced pointers
  (only the value) still parse. A marker without a valid epoch is an error (promotion fails rather than
  guessing an order).
- **Claim** (`claim(key)`): read the pointer; if its epoch > E, fail (`FencedException`, triggers resign);
  otherwise rewrite the same value with epoch E using `putIfMatch(readVersion)`; retry on precondition
  failure (up to 10 times). A missing pointer is created as a **tombstone** (empty value) with
  `putIfAbsent`, so an older leader cannot create it later. Readers (`S3Backend.exists`, `getCurrentResourceName`)
  treat a tombstone as absent.
- **Write** (`write(key, value)`): if the key was claimed in the current epoch, `putIfMatch(ownedVersion)`;
  a precondition failure means another node claimed it → `FencedException`. A key not yet claimed (e.g. an
  index created by this leader) is claimed by the write itself in one conditional write.
- Owned versions are only valid for the epoch they were written in. If the fence moves (deactivated or a new
  epoch) while a write is in flight, the write is reported as fenced even if it landed, so it is never
  acknowledged; this is safe because a newer leader claims (and so supersedes or rejects) everything.
- Inactive fence (standby, or after loss): every write is rejected.
- A fenced failure in the **current** epoch invokes the registered listener (`LeaderManager.onFenced`), which
  resigns that epoch and demotes. Failures from older epochs (e.g. this node's own newer claim) are ignored.

### 7.3 What is fenced

`S3Backend` routes all server side pointer writes through `setCurrentResourceInternal` → `fence.write` when a
fence is configured: global state, index state, point state, warming queries. The public
`S3Backend.setCurrentResource` stays unfenced for operator tooling (`nrt_utils`); an operator write moves the
ETag, so the active leader's next write is fenced and it resigns and is re-elected through a fresh claim.
Index data files are keyed by ephemeral id and never collide, so they need no fencing; files uploaded by a
fenced leader are orphans.

### 7.4 Why it is safe

- After a new leader with epoch E' claims a pointer, every older leader's owned version is stale, so all its
  writes to that pointer fail, no matter how long it was paused.
- A write that an old leader completed before the claim is read by the claim and becomes the base the new
  leader restores from; a commit acknowledged in that window is therefore not lost.
- Two simultaneous leaders (lease expiry during a partition, a buggy elector): the higher epoch wins each claim,
  the lower sees a newer epoch and backs off. A crash mid promotion leaves some pointers at E; the next leader
  (E' > E) re-claims all of them.

## 8. Node lifecycle (`LeaderManager`)

### 8.1 Roles

```
            onElected(E)                    all claimed, indices primary
 STANDBY ───────────────▶ PROMOTING ───────────────────────────────▶ ACTIVE
    ▲                        │ failure / leadership changed              │ onLost, fenced write,
    │                        ▼                                           │ or close
    └──────────────────── DEMOTING ◀─────────────────────────────────────┘
          indices restarted as standby followers
```

- All transitions run in order on one `LeaderManager-transition` thread. Each queued transition carries the
  `leadershipChangeCount` at queue time and is skipped if stale.
- `onLost` deactivates the fence **synchronously** in the callback, before the slower demotion is queued, so no
  remote state write can happen after loss is known. One demotion per loss (`lossHandled`), however many
  sources report it (elector, fenced write, failed promotion).
- `isActive()` = role ACTIVE and `elector.isLeader()`.

### 8.2 Promotion (epoch E)

1. role = PROMOTING; `fence.activate(E)`.
2. `claimAndReloadGlobalState`: claim `global_state/_current`, reload global state (replacing index state
   managers whose index id changed).
3. `stopIndicesNotStarted`: stop local standby followers of indices the previous leader stopped.
4. For every started index, in parallel (`claimConcurrency`): claim `state`, `point_state` and `warming`
   pointers. All claims finish before any index restarts.
5. Check still leader (same change count, `elector.isLeader()`).
6. For every started index, in parallel: restart as primary with `primaryGen = E` (fresh `IndexStateManager`,
   `StartOptions(keepMatchingLocalFiles=true)` → delta restore from the claimed point state).
7. Check still leader; role = ACTIVE.

Any failure → `resignAndDemote(E)`: demote now and `elector.resign(E)` (scoped to E).

### 8.3 Demotion

Fence already inactive. If any index may have been restarted as primary, restart all started indices as standby
followers; then role = STANDBY and request a state refresh. Uncommitted data on the old primary is discarded.

### 8.4 Startup and shutdown

- Boot: replication server starts → `replicationStarted` starts all started indices as **standby followers** →
  client server starts → `serverStarted(port)` → `LeaderManager.start()` creates and starts the elector (and the
  periodic refresh). Every node boots as a standby.
- A standby never creates missing global state; it uses an empty state until the leader creates it.
- Shutdown (`NrtsearchServer.stop`, `BackendGlobalState.close`): `releaseLeadership()` first → `LeaderManager.close()`
  deactivates the fence, closes the elector (lease written `released`, so a standby takes over immediately),
  completes refresh waiters with `ClosedException`, waits up to 60s for a running transition, then indices close.

## 9. Standby state following

### 9.1 Index data

Standby indices are started as `REPLICA` with an isolated replica override (`StartOptions(false,
IsolatedReplicaConfig(enabled, pollingIntervalSeconds, 0, 0))`): they poll the S3 point state (every 10s, or
`isolatedReplicaConfig.pollingIntervalSeconds` if configured), download new files, and serve searches. They do
not register with the active primary. On promotion, local files matching the point state (same length and
Lucene footer checksum) are kept and only the rest is downloaded (`NrtDataManager.prepareDeltaRestore`).

### 9.2 Global and index state: leader owned refresh

The active primary is the source of global state. Standbys follow it with `LeaderManager.requestRefresh()`:

- Triggered periodically (`refreshIntervalMs`), after demotion, and by `ReloadState` requests.
- Requests are merged: at most one refresh queued; each request gets a future completed by a refresh that
  **started after** it. Refreshes run on the transition thread, so never concurrently with a role change.
- `RoleController.refreshStandby(abort)` loads global state and every index state **without** the global
  lock, checking `abort` (pending role change, closed, leadership change) between indices; only the final swap
  takes the lock. If state changed underneath, or a role change is pending, it returns false and is retried
  (up to 3 immediate retries, then the next periodic/requested refresh). It then makes local started indices
  match remote state: stops followers of stopped/deleted indices, starts followers of newly started ones.
- On the active primary a refresh is a no op.
- `ReloadState` with election: waits up to `reloadWaitMs`; `DEADLINE_EXCEEDED` if not done (it continues in the
  background), `UNAVAILABLE` only for shutdown, other failures reported with their cause.

Worst case, a promotion waits for one standby index load in progress.

## 10. Index start rules and write gating

### 10.1 Starting indices with election

`startIndexFromConfig(..., LeaderStartRole)`:

| Role | Used by | Start as |
|---|---|---|
| `PRIMARY` | promotion | `PRIMARY`, `primaryGen = fence epoch` (must be > 0), delta restore |
| `STANDBY` | boot, refresh, demotion | `REPLICA` standby follower, full restore |
| `FROM_ROLE` | client requests | primary if `isActive()`, else standby follower |

Client requests: `startIndex` is only accepted as `{indexName, mode: PRIMARY}` (restore, primaryGen, primary
address/file and port are managed by the leader → `INVALID_ARGUMENT`; use `startIndexV2`). `startIndex`,
`startIndexV2`, `stopIndex` require the active primary.

`ShardState.startPrimary` also enforces `primaryGen > restored point state primaryGen` when
`enforcePrimaryGenOrdering` (a newer primary already wrote this index → refuse to become a writer).

### 10.2 Gated operations

`Handler.verifyActivePrimary(op)` (no-op without election) on: `createIndex`, `deleteIndex`, `registerFields`,
`updateFields`, `settings`, `settingsV2`, `commit` (inside the commit task), `writeNRTPoint`, and the primary
side replication RPCs `addReplicas`, `recvCopyState`. `ShardState.verifyWritable(op)` checks leadership first,
then that the shard is a primary with an open writer: add/update documents, delete documents, delete by query,
delete all, force merge, force merge deletes.

Errors: `NotActivePrimaryException` → `FAILED_PRECONDITION` with
`"<op> requires the active primary, this node is <ROLE>, current leader: <node> (<host>:<port>, epoch <E>)"`.
`Handler.findStatusException` unwraps these and `FencedException` from executor wrappers so clients see
`FAILED_PRECONDITION` (redirect) rather than `UNKNOWN`/`INTERNAL` (retry). A node that just lost leadership and
whose elector still names itself reports the leader as `unknown` (`Handler.knownLeader`).

## 11. Replication

### 11.1 Stale primary rejection

`NRTReplicaNode.maybeNewPrimary` rejects a `primaryGen` lower than the last one seen (when
`enforcePrimaryGenOrdering`) with `StalePrimaryGenException` → `FAILED_PRECONDITION` from `newNRTPoint` and
`copyFiles`; the existing primary logic drops replicas that return it. Metric:
`nrt_stale_primary_gen_rejected_count{index}`. A deposed primary also cannot serve `recvCopyState` or accept
`addReplicas` (gated, 10.2).

### 11.2 Replicas never ahead of S3

With `s3RefreshNotifyAfterUpload`, replicas are only notified of points already uploaded, and notification
happens only after a successful (fenced) point state upload. A stale primary whose upload fails CAS never
notifies. So replicas are always at or behind the claimed point V, and the new primary's next version is
V+1; this avoids the version regression / segment name reuse problem on failover. Cost: replica refresh latency
includes the upload.

### 11.3 Leader following replicas (`LEADER_RPC`)

- `StartIndexProcessor` acquires a client from `SharedLeaderClients` keyed by seed source (`hosts:...` or
  `file:...`) and poll interval. All replica indices of a server with the same seeds share one reference counted
  `LeaderTracker` (one set of `resolveLeader` probes per poll), but each index gets its own channel (a large
  merge copy on one index does not delay another). Channels target `nrtleader:///leader` with a
  `LeaderNameResolver` subscribed to the shared tracker. Close is bounded (`shutdownNow` + 2s wait).
- `ShardState.KeepAlive` pings every `replicaReplicationPortPingInterval` (10s): if not known to the current
  primary, `tryAddReplicas`; on successful registration it requests a catch-up sync
  (`syncFromCurrentPrimary`, up to 5s primary wait) through a `CoalescingRunner` on a dedicated thread, so a
  re-registration during a running sync queues exactly one more sync.
- Control calls from replica to primary (`getConnectedNodes`, `addReplicas`, `recvCopyState`) have a 10s
  deadline. Without it, a call to a primary that hangs without closing its connection (GC pause, partition)
  stays on the old connection after the channel moves to the new leader, and the replica never registers with
  it (found by the local `pause_leader` test). File streams and `copyFiles` are not given short deadlines.
- `syncFromCurrentPrimary` stops on close/interrupt and returns false; an interrupted initial sync fails replica
  start. `KeepAlive.close` interrupts and waits up to 5s for the sync thread before the replica node closes.

## 12. Leader discovery

### 12.1 `resolveLeader`

```proto
rpc resolveLeader(ResolveLeaderRequest) returns (ResolveLeaderResponse); // LuceneServer (GET /v1/resolve_leader) and ReplicationServer
message ResolveLeaderResponse { bool isLeader; string leaderId; string nodeName; string host;
                                int32 port; int32 replicationPort; int64 epoch; }
```

- With election: the elector's last known leader, `isLeader` = this node is active and is that leader.
  `UNAVAILABLE` if no leader is known, or if the only known leader is this node and it is STANDBY/DEMOTING.
- Without election: a PRIMARY reports itself (epoch 0; loopback hostName rejected unless allowed);
  other modes `UNAVAILABLE`.

### 12.2 `LeaderResolver` (clientlib)

Each resolve: probe all seeds in parallel (2s deadline each); take the answer with the highest epoch (preferring
the leader's own answer at equal epoch); accept it if it came from the leader itself with `isLeader`, otherwise
ask the candidate to confirm `isLeader` at that epoch. No confirmation → no result. A node that missed a failover
reports an older epoch and loses; a deposed leader no longer confirms. Probe channels (`ProbeChannels`) are
reused and pruned to seeds plus candidate.

### 12.3 `LeaderTracker` and `LeaderNameResolver`

- Polls every `pollIntervalMs` on one thread; `refresh()` (called by gRPC on connection failure) is merged by a
  `CoalescingRunner`, which re-submits follow up runs instead of looping (no starvation) and survives task errors.
- Only moves to the same or a newer epoch; keeps the last confirmed leader when none is confirmed.
- Every publish has a sequence number; each subscriber only accepts a higher sequence, so deliveries are ordered
  even though a new subscriber gets the known leader immediately on the subscribing thread. A subscriber added
  after a poll found no leader gets `onNoLeader` at once (calls fail fast); before the first poll completes it
  waits.
- `LeaderNameResolver` publishes the confirmed address; before any confirmation it reports `UNAVAILABLE`; after
  that it never points a channel at an unconfirmed node.

### 12.4 Ingestion clients

```java
new NrtSearchClient.Builder(new NrtSearchClient.Builder.Leader(
    List.of(new Node("primary-a", 8000), new Node("primary-b", 8000)))).build();
```

Uses the client service port; default poll 2s; `Leader(Supplier<Collection<Node>>, pollMs)` accepts any seed
source (`SeedSources.fixed`, `SeedSources.file`, re-read every poll).

## 13. Guarantees

| Property | Mechanism |
|---|---|
| At most one node makes durable progress per pointer | epoch claim + CAS (7) |
| Acknowledged commits survive failover | commit returns only after the fenced point state upload; a fenced upload fails the commit (`FAILED_PRECONDITION`) |
| New leader starts from latest durable version | claims read the latest pointers; restore from the claimed point state |
| Replicas never regress | notify-after-upload (11.2) + stale gen rejection (11.1) |
| Deposed leader cannot feed replicas | `recvCopyState`/`addReplicas` gated; replicas reject lower gen |
| Clients are redirected, not retried | `FAILED_PRECONDITION` with leader hint; `resolveLeader` |
| Uncommitted documents | may be lost (some survive if a refresh upload happened) |

## 14. Failure scenarios

| Scenario | Behavior |
|---|---|
| Active crashes (SIGKILL) | Lease unrenewed; a standby takes over after ttl+margin, claims, promotes with delta restore. Replicas re-resolve and re-register. |
| Graceful stop | Lease released on close; takeover on the next standby poll. |
| Long GC pause / partition of active | `isLeader()` expires on its monotonic clock; standby takes over and claims. On resume the old node's renew fails, it demotes; any write fails CAS (fenced) or is rejected by the gate. Verified with SIGSTOP against S3. |
| Pause during a point state upload | If before the claim: claim sees and keeps that write. If after: CAS fails, commit not acknowledged, no replica notified. |
| Two nodes think they lead | Higher epoch wins every claim; lower resigns on fenced write. |
| Crash during promotion | Partially claimed at E; next leader re-claims at E' > E. |
| Standby lags state (index created/stopped) | Periodic/requested refresh; promotion stops followers of stopped indices. |
| Replica pointed at hung primary | Control call deadlines; next ping goes to the new leader. |
| S3 unavailable | Lease cannot renew → leader steps down at validity expiry; no node can claim; cluster read only until S3 returns. |
| Elector bug (duplicate/regressing epochs) | Liveness only; claims reject epochs ≤ pointer epoch. |
| Operator rewrites a pointer with `nrt_utils` | Leader's next CAS fails, it resigns and a fresh claim takes over. |

## 15. Costs (S3 requests)

- Lease: holder 1 conditional PUT per `pollIntervalMs` (default ttl/3 = 5s); each standby 1 GET per poll.
- Promotion: global state claim (GET + PUT) plus 3 pointers × (GET + PUT) per started index, at
  `claimConcurrency` parallelism, plus delta file downloads. 1,000 indices ≈ 6,000 small requests; distinct
  prefixes, so per prefix limits do not apply.
- Standby refresh: global state + every index state GET each `refreshIntervalMs`, on every standby (see 18).
- Standby data following: point state GET per index per polling interval, plus new files.

## 16. Operations

- Metrics: `nrt_stale_primary_gen_rejected_count`; leader transitions are logged
  (`Acquired leader lease`, `Promoting`, `Active primary at epoch`, `Demoting to standby`, `Remote state write fenced`).
- Tools: `nrtsearch_client resolveLeader` and `docCount -i <index>` (single line JSON), `nrt_utils getFenceState
  -s <service> -b <bucket>` (lease and per pointer value, epoch, tombstone, ETag).
- Local failover harness: `scripts/ha_local/` (`ha_local.sh` cluster control against real S3, `ha_failover_test.sh`
  scenarios: kill, graceful stop, pause/resume, uncommitted data, standby writes, full restart). 56/56 checks pass.
- Upgrade: all nodes must run this version before enabling election (old servers cannot read fenced pointers).
  Enabling election on an existing cluster works: the first epoch (wall clock ms) exceeds existing primaryGens.

## 17. Changes from the original proposal

| Original | Implemented | Why |
|---|---|---|
| If-Match on last seen ETag | Claim on promote + epoch per pointer + CAS | Old ETag still matches until the new leader writes (GC pause fork) |
| Epoch in S3 user metadata, same body | Epoch and nonce in the body | S3 ETag is content MD5; identical bodies keep the ETag, breaking renewal and claims |
| Lease renew rewrites same content | `renewal` counter | Same as above |
| Warming pointer claimed lazily | Claimed with the other index pointers before any restart | Simpler invariant: all existing pointers claimed first |
| Writable/read only toggle on the state backend | Fence inactive on standbys; leader owns refresh | Fewer modes; one place enforces writes |
| Standby reloads state itself | Leader manager owned, merged, abortable refresh on the transition thread | Repeated races between reloads and role changes in review |
| Upload thread checks `isLeader()` | Fence on every pointer write; notify only after successful upload | Check-then-act is racy |
| Fixed epochs from 1 | First epoch = wall clock ms | Must exceed pre-election primaryGens (start times) |
| One resolver per replica index | One shared tracker per seed source, channel per index | Probe load and consistent failover across indices |
| Replica gen check only | Plus `enforcePrimaryGenOrdering` primary start check, gated replication RPCs | Defense in depth |
| No call deadlines | Control call deadlines replica → primary | Found by pause test on real S3 |
| ZooKeeper elector | Plugin API ready; plugin not written | Phase 4 |

## 18. Known limitations and follow-ups

- **Standby refresh cost**: every refresh reads all index states. Follow-up: compare each index's `_current`
  pointer and only reload changed ones.
- **Primary → hung replica calls** (`newNRTPoint`, `copyFiles`) are not yet checked for deadlines; add a
  paused replica scenario to the harness.
- **ZooKeeper elector plugin** (`../nrtsearch-leader-plugins/zookeeper-plugin`, Curator `LeaderLatch`, CAS epoch
  znode) not started.
- AWS S3 only; S3 compatible stores must honor `If-Match`/`If-None-Match` on PUT (the findify mock does not;
  tests use a write-through CAS store).
- Uncommitted documents are lost on failover; clients must re-send after `FAILED_PRECONDITION`.
- Failover time is bounded below by lease ttl + safety margin + poll; lowering ttl increases lease traffic.
- Orphaned data files from fenced uploads are not garbage collected automatically (existing cleanup tools apply).
- SSO AWS profiles need the SDK `sso` module, not in the distribution (local harness exports credentials instead).

## 19. Testing

- Unit: `S3LeaseElectorTest` (acquire, renew, takeover, release, expiry, safety margin, content hash versions),
  `EpochPointerFenceTest` (claim, tombstone, CAS, fenced listener, epoch moves), `LeaderManagerTest`
  (transitions, stale events, refresh merging/abort/retry, close), `PrimaryElectorCreatorTest`,
  `VerifyActivePrimaryTest`, `PrimaryGenFencingTest`, `NrtDataManagerDeltaRestoreTest`,
  `LeaderElectionValidationTest`, `ReplicationClientHungPrimaryTest`, clientlib `LeaderResolverTest`,
  `CoalescingRunnerTest`, CLI/tool tests.
- Integration: `MultiPrimaryLifecycleTest` (34 tests: failover, fail back, stale leader fenced after takeover,
  standby errors, refresh, LEADER_RPC replicas, resolveLeader) using `ManualElectorPlugin` (deterministic
  elect/revoke/pause) and `WriteThroughConditionalStore` (content hash CAS over the S3 mock).
- Real S3: `scripts/ha_local/ha_failover_test.sh`, three clean runs (after two fixes it found).
