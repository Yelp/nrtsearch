/*
 * Copyright 2026 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.leader.LeaderManager;
import com.yelp.nrtsearch.test_utils.ManualElectorPlugin;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MultiPrimaryLifecycleTest {
  private static final String INDEX = "test_index";
  private static final String LEADER_CONFIG =
      String.join(
          "\n",
          "s3RefreshUpload: true",
          "s3RefreshNotifyAfterUpload: true",
          "leaderElection:",
          "  type: " + ManualElectorPlugin.TYPE,
          "  allowLoopbackHost: true");

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private ManualElectorPlugin.Cluster cluster;

  @Before
  public void setUp() {
    cluster = new ManualElectorPlugin.Cluster();
  }

  @After
  public void cleanup() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  private TestServer primaryServer() throws IOException {
    return TestServer.builder(folder)
        .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
        .withRemoteStateBackend(false)
        .withAdditionalConfig(LEADER_CONFIG)
        .withPlugins(List.of(new ManualElectorPlugin(cluster)))
        .build();
  }

  private static LeaderManager leaderManager(TestServer server) {
    return server.getGlobalState().getLeaderManager().orElseThrow();
  }

  private static ShardState shard(TestServer server) throws IOException {
    return server.getGlobalState().getIndexOrThrow(INDEX).getShard(0);
  }

  private static String nodeName(TestServer server) {
    return server.getGlobalState().getNodeName();
  }

  /** Request a state refresh on a leader election node and wait for it to finish. */
  private static void refreshAndWait(TestServer server) throws Exception {
    server.reloadState();
    leaderManager(server).awaitTransitions();
  }

  private long elect(TestServer server) throws Exception {
    long epoch = cluster.elect(nodeName(server));
    leaderManager(server).awaitTransitions();
    return epoch;
  }

  /** Bootstrap: first leader creates and starts the index. */
  private long bootstrap(TestServer a) throws Exception {
    long epoch = elect(a);
    assertTrue(leaderManager(a).isActive());
    a.createSimpleIndex(INDEX);
    a.startIndexV2(StartIndexV2Request.newBuilder().setIndexName(INDEX).build());
    assertTrue(shard(a).isPrimary());
    return epoch;
  }

  @Test
  public void testStandbyRejectsStateChanges() throws Exception {
    TestServer a = primaryServer();
    assertEquals(LeaderManager.Role.STANDBY, leaderManager(a).getRole());
    assertFalse(leaderManager(a).isActive());
    StatusRuntimeException e =
        assertThrows(StatusRuntimeException.class, () -> a.createSimpleIndex(INDEX));
    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    assertTrue(e.getMessage(), e.getMessage().contains("createIndex requires the active primary"));
  }

  @Test
  public void testStandbyReportsLeaderInErrors() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> b.commit(INDEX));
    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    assertTrue(e.getMessage(), e.getMessage().contains("current leader: " + nodeName(a)));
  }

  /** A paused old leader must not serve copy state to replicas once it is no longer leader. */
  @Test
  public void testStaleLeaderStopsServingReplication() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    String indexId = a.getGlobalState().getIndexStateManagerOrThrow(INDEX).getIndexId();
    a.getReplicationClient().recvCopyState(INDEX, indexId, 0);

    // revoke a's leadership; its elector reports not leader before the demotion completes
    cluster.revokeCurrent();
    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () -> a.getReplicationClient().recvCopyState(INDEX, indexId, 0));
    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
  }

  @Test
  public void testFailoverToStandby() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    long epochA = bootstrap(a);
    a.addSimpleDocs(INDEX, 1, 2, 3);
    a.commit(INDEX);

    // b picks up the index from global state and follows as standby
    refreshAndWait(b);
    assertTrue(b.isStarted(INDEX));
    assertTrue(shard(b).isReplica());
    assertTrue(shard(b).isStandbyFollower());

    long epochB = elect(b);
    assertTrue(epochB > epochA);
    assertTrue(leaderManager(b).isActive());
    assertEquals(LeaderManager.Role.STANDBY, leaderManager(a).getRole());

    // b is primary with the new epoch, restored from a's last commit
    assertTrue(shard(b).isPrimary());
    assertEquals(epochB, shard(b).nrtPrimaryNode.getPrimaryGen());
    b.verifySimpleDocIds(INDEX, 1, 2, 3);

    // a was demoted to a standby follower and rejects writes
    assertTrue(shard(a).isStandbyFollower());
    RuntimeException writeError =
        assertThrows(RuntimeException.class, () -> a.addSimpleDocs(INDEX, 99));
    assertTrue(
        writeError.getMessage(),
        writeError.getMessage().contains("FAILED_PRECONDITION")
            && writeError.getMessage().contains("current leader: " + nodeName(b)));

    b.addSimpleDocs(INDEX, 4);
    b.commit(INDEX);
    b.verifySimpleDocIds(INDEX, 1, 2, 3, 4);

    // fail back to a
    long epochA2 = elect(a);
    assertTrue(epochA2 > epochB);
    assertTrue(shard(a).isPrimary());
    a.verifySimpleDocIds(INDEX, 1, 2, 3, 4);
  }

  /**
   * The old leader is paused and never told it lost leadership. Once the new leader claims the
   * remote state, the old leader's commit must fail and it must demote itself.
   */
  @Test
  public void testStaleLeaderCommitFencedAfterTakeover() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1, 2);
    a.commit(INDEX);
    refreshAndWait(b);

    // b is elected while a still believes it is leader
    cluster.electWithoutRevoking(nodeName(b));
    leaderManager(b).awaitTransitions();
    assertTrue(leaderManager(b).isActive());
    assertTrue(shard(a).isPrimary());

    // a resumes and accepts a write, but cannot make it durable
    a.addSimpleDocs(INDEX, 3);
    StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> a.commit(INDEX));
    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    assertTrue(e.getMessage(), e.getMessage().contains("no longer leader"));

    leaderManager(a).awaitTransitions();
    leaderManager(a).awaitTransitions();
    assertEquals(LeaderManager.Role.STANDBY, leaderManager(a).getRole());
    assertTrue(shard(a).isStandbyFollower());

    // b's lineage does not contain the doc a failed to commit
    b.verifySimpleDocIds(INDEX, 1, 2);
    b.addSimpleDocs(INDEX, 4);
    b.commit(INDEX);
    b.verifySimpleDocIds(INDEX, 1, 2, 4);
  }

  /** Once leadership is revoked, writes are rejected even before the index is restarted. */
  @Test
  public void testWritesRejectedAfterLeadershipLost() throws Exception {
    TestServer a = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    cluster.revokeCurrent();
    // rejected by the leadership check, or by the closed writer if demotion already restarted it
    RuntimeException e = assertThrows(RuntimeException.class, () -> a.addSimpleDocs(INDEX, 2));
    assertTrue(
        e.getMessage(),
        e.getMessage().contains("requires the active primary")
            || e.getMessage().contains("open writer")
            || e.getMessage().contains("on a replica node is not supported"));
  }

  /** Stopping the active primary releases leadership, so a standby can be elected right away. */
  @Test
  public void testShutdownReleasesLeadership() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    a.stop();
    assertFalse(cluster.hasLeader());
    elect(b);
    b.verifySimpleDocIds(INDEX, 1);
  }

  /** Adding documents on a standby returns FAILED_PRECONDITION, not INTERNAL. */
  @Test
  public void testAddDocumentsOnStandbyIsFailedPrecondition() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    elect(b);
    leaderManager(a).awaitTransitions();
    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () -> a.addDocs(java.util.stream.Stream.of(a.getSimpleDocRequest(INDEX, 2))));
    // the test helper wraps the client side status exception
    assertTrue(e.getCause() instanceof StatusRuntimeException);
    assertEquals(
        io.grpc.Status.Code.FAILED_PRECONDITION,
        ((StatusRuntimeException) e.getCause()).getStatus().getCode());
  }

  /** A state reload while indices change role must not start a replaced (closed) index manager. */
  @Test
  public void testReloadDuringRoleChange() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    java.util.concurrent.atomic.AtomicBoolean stop =
        new java.util.concurrent.atomic.AtomicBoolean();
    java.util.List<Throwable> errors = new java.util.concurrent.CopyOnWriteArrayList<>();
    Thread reloader =
        new Thread(
            () -> {
              while (!stop.get()) {
                try {
                  b.getGlobalState().reloadStateFromBackend();
                } catch (Throwable t) {
                  errors.add(t);
                }
              }
            });
    reloader.setDaemon(true);
    reloader.start();
    try {
      for (int i = 0; i < 3; i++) {
        elect(b);
        elect(a);
      }
    } finally {
      stop.set(true);
      reloader.join(10000);
    }
    assertEquals(List.of(), errors);
    elect(b);
    assertTrue(shard(b).isPrimary());
    b.verifySimpleDocIds(INDEX, 1);
    b.addSimpleDocs(INDEX, 2);
    b.commit(INDEX);
    b.verifySimpleDocIds(INDEX, 1, 2);
  }

  /** Once an active node starts shutting down, it no longer publishes remote state. */
  @Test
  public void testStoppingLeaderStopsPublishing() throws Exception {
    TestServer a = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    a.getGlobalState().releaseLeadership();
    assertFalse(leaderManager(a).getFence().isActive());
    assertFalse(leaderManager(a).isActive());
    assertEquals(LeaderManager.Role.DEMOTING, leaderManager(a).getRole());
    assertThrows(RuntimeException.class, () -> a.addSimpleDocs(INDEX, 2));
    assertThrows(StatusRuntimeException.class, () -> a.commit(INDEX));
    // the index upload path is fenced, so no new points are published for replicas to follow
    String uniqueName = a.getGlobalState().getDataResourceForIndex(INDEX);
    assertThrows(
        com.yelp.nrtsearch.server.leader.FencedException.class,
        () ->
            a.getRemoteBackend()
                .uploadWarmingQueries(a.getServiceName(), uniqueName, "x".getBytes()));
  }

  /** A fenced commit is reported as FAILED_PRECONDITION, so clients redirect to the leader. */
  @Test
  public void testFencedCommitIsFailedPrecondition() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    cluster.electWithoutRevoking(nodeName(b));
    leaderManager(b).awaitTransitions();
    a.addSimpleDocs(INDEX, 2);
    StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> a.commit(INDEX));
    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
  }

  /** A reload during promotion starts indices only as standby; only promotion starts primaries. */
  @Test
  public void testReloadNeverStartsPrimary() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    // b has not loaded the index yet; promote b while reloading concurrently
    java.util.concurrent.atomic.AtomicBoolean stop =
        new java.util.concurrent.atomic.AtomicBoolean();
    Thread reloader =
        new Thread(
            () -> {
              while (!stop.get()) {
                try {
                  b.getGlobalState().reloadStateFromBackend();
                } catch (Throwable ignored) {
                }
              }
            });
    reloader.setDaemon(true);
    reloader.start();
    try {
      elect(b);
    } finally {
      stop.set(true);
      reloader.join(10000);
    }
    leaderManager(b).awaitTransitions();
    assertTrue(leaderManager(b).isActive());
    b.addSimpleDocs(INDEX, 2);
    b.commit(INDEX);
    b.verifySimpleDocIds(INDEX, 1, 2);
    assertTrue(leaderManager(b).isActive());
  }

  /** A standby can not start, or stop, indices; indices become primary only by promotion. */
  @Test
  public void testStartAndStopRejectedOnStandby() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    assertTrue(shard(b).isStandbyFollower());
    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                b.startIndex(
                    StartIndexRequest.newBuilder()
                        .setIndexName(INDEX)
                        .setMode(Mode.PRIMARY)
                        .build()));
    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    e =
        assertThrows(
            StatusRuntimeException.class,
            () -> b.startIndexV2(StartIndexV2Request.newBuilder().setIndexName(INDEX).build()));
    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    e = assertThrows(StatusRuntimeException.class, () -> b.stopIndex(INDEX));
    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    // replica mode start requests are not supported with leader election
    e =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                a.startIndex(
                    StartIndexRequest.newBuilder()
                        .setIndexName(INDEX)
                        .setMode(Mode.REPLICA)
                        .build()));
    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    assertTrue(shard(b).isStandbyFollower());
  }

  /** On the active primary, ReloadState returns without waiting behind role changes. */
  @Test
  public void testReloadStateDoesNotBlock() throws Exception {
    TestServer a = primaryServer();
    bootstrap(a);
    java.util.concurrent.CountDownLatch block = new java.util.concurrent.CountDownLatch(1);
    // occupy the transition thread
    java.lang.reflect.Field f = LeaderManager.class.getDeclaredField("transitionExecutor");
    f.setAccessible(true);
    ((java.util.concurrent.ExecutorService) f.get(leaderManager(a)))
        .execute(
            () -> {
              try {
                block.await();
              } catch (InterruptedException ignored) {
              }
            });
    long start = System.nanoTime();
    for (int i = 0; i < 20; i++) {
      a.reloadState();
    }
    assertTrue(System.nanoTime() - start < java.util.concurrent.TimeUnit.SECONDS.toNanos(5));
    block.countDown();
    leaderManager(a).awaitTransitions();
  }

  /** A refresh on the active leader does not reload or restart its primary indices. */
  @Test
  public void testRefreshOnLeaderLeavesPrimariesAlone() throws Exception {
    TestServer a = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    ShardState before = shard(a);
    refreshAndWait(a);
    assertSame(before, shard(a));
    assertTrue(shard(a).isPrimary());
    // uncommitted docs in the primary writer are still there
    a.addSimpleDocs(INDEX, 2);
    a.commit(INDEX);
    a.verifySimpleDocIds(INDEX, 1, 2);
  }

  /** A standby picks up an index created on the leader with a refresh. */
  @Test
  public void testStandbyRefreshPicksUpNewIndex() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.createSimpleIndex("other_index");
    a.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("other_index").build());
    a.addSimpleDocs("other_index", 7);
    a.commit("other_index");
    refreshAndWait(b);
    ShardState other = b.getGlobalState().getIndexOrThrow("other_index").getShard(0);
    assertTrue(other.isStandbyFollower());
    elect(b);
    b.verifySimpleDocIds("other_index", 7);
    assertTrue(b.getGlobalState().getIndexOrThrow("other_index").getShard(0).isPrimary());
  }

  /** ReloadState waits a bounded time for its refresh, then returns DEADLINE_EXCEEDED. */
  @Test
  public void testReloadStateBoundedWait() throws Exception {
    TestServer a = primaryServer();
    TestServer b =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .withAdditionalConfig(LEADER_CONFIG + "\n  reloadWaitMs: 200")
            .withPlugins(List.of(new ManualElectorPlugin(cluster)))
            .build();
    bootstrap(a);
    java.util.concurrent.CountDownLatch blocker = new java.util.concurrent.CountDownLatch(1);
    java.lang.reflect.Field f = LeaderManager.class.getDeclaredField("transitionExecutor");
    f.setAccessible(true);
    ((java.util.concurrent.ExecutorService) f.get(leaderManager(b)))
        .execute(
            () -> {
              try {
                blocker.await();
              } catch (InterruptedException ignored) {
              }
            });
    try {
      StatusRuntimeException e = assertThrows(StatusRuntimeException.class, b::reloadState);
      assertEquals(io.grpc.Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
    } finally {
      blocker.countDown();
    }
    // the refresh still runs in the background
    leaderManager(b).awaitTransitions();
    assertTrue(shard(b).isStandbyFollower());
  }

  /**
   * A standby stops its follower of an index stopped on the leader, and stays stopped on takeover.
   */
  @Test
  public void testStandbyStopsIndexStoppedOnLeader() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    assertTrue(b.isStarted(INDEX));
    a.stopIndex(INDEX);
    refreshAndWait(b);
    assertFalse(b.isStarted(INDEX));
    elect(b);
    assertFalse(b.isStarted(INDEX));
    // the new leader can start it again
    b.startIndexV2(StartIndexV2Request.newBuilder().setIndexName(INDEX).build());
    assertTrue(shard(b).isPrimary());
    b.verifySimpleDocIds(INDEX, 1);
  }

  /** Promotion stops a follower of an index the previous leader stopped, even without a refresh. */
  @Test
  public void testPromotionStopsFollowerOfStoppedIndex() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    assertTrue(shard(b).isStandbyFollower());
    a.stopIndex(INDEX);
    // b is elected before refreshing
    elect(b);
    assertFalse(b.isStarted(INDEX));
  }

  /** ReloadState during a role change waits for the refresh after it, instead of skipping. */
  @Test
  public void testReloadDuringRoleChangeWaits() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.createSimpleIndex("other_index");
    a.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("other_index").build());
    a.commit("other_index");
    // a role change on b: lose leadership that b never had is a no-op, so elect and revoke a
    elect(b);
    elect(a);
    // b is demoting or standby; reload must pick up other_index as a standby follower
    b.reloadState();
    leaderManager(b).awaitTransitions();
    assertTrue(b.getGlobalState().getIndexOrThrow("other_index").getShard(0).isStandbyFollower());
  }

  /**
   * A deterministic refresh failure is not reported as UNAVAILABLE "shutting down", which clients
   * would retry. Here global state lists an index whose index state does not exist, so loading it
   * throws IllegalStateException.
   */
  @Test
  public void testRefreshFailureNotReportedAsShutdown() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    refreshAndWait(b);
    // the active leader writes global state referencing a missing index state
    GlobalStateInfo current = a.getGlobalState().getStateInfo();
    GlobalStateInfo broken =
        current.toBuilder()
            .putIndices(
                "missing_index",
                IndexGlobalState.newBuilder().setId("no_such_id").setStarted(true).build())
            .setGen(current.getGen() + 1)
            .build();
    a.getRemoteBackend()
        .uploadGlobalState(
            a.getServiceName(),
            com.yelp.nrtsearch.server.state.StateUtils.globalStateToUTF8(broken));
    StatusRuntimeException e = assertThrows(StatusRuntimeException.class, b::reloadState);
    assertFalse(e.getMessage(), e.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE);
    assertFalse(e.getMessage(), e.getMessage().contains("shutting down"));
    assertTrue(e.getMessage(), e.getMessage().contains("No committed state for index"));
  }

  private static ResolveLeaderResponse resolveClient(TestServer server) {
    return server
        .getClient()
        .getBlockingStub()
        .resolveLeader(ResolveLeaderRequest.newBuilder().build());
  }

  private static ResolveLeaderResponse resolveReplication(TestServer server) {
    return server
        .getReplicationClient()
        .getBlockingStub()
        .resolveLeader(ResolveLeaderRequest.newBuilder().build());
  }

  /** resolveLeader reports the active primary, with its bound ports, from every node. */
  @Test
  public void testResolveLeader() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    // no leader yet
    StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> resolveClient(a));
    assertEquals(io.grpc.Status.Code.UNAVAILABLE, e.getStatus().getCode());

    long epochA = elect(a);
    for (TestServer server : List.of(a, b)) {
      for (ResolveLeaderResponse r : List.of(resolveClient(server), resolveReplication(server))) {
        assertEquals(nodeName(a), r.getNodeName());
        assertEquals(a.getPort(), r.getPort());
        assertEquals(a.getReplicationPort(), r.getReplicationPort());
        assertEquals(epochA, r.getEpoch());
        assertEquals(server == a, r.getIsLeader());
      }
    }

    long epochB = elect(b);
    ResolveLeaderResponse fromA = resolveClient(a);
    assertEquals(nodeName(b), fromA.getNodeName());
    assertEquals(b.getPort(), fromA.getPort());
    assertEquals(epochB, fromA.getEpoch());
    assertFalse(fromA.getIsLeader());
    assertTrue(resolveClient(b).getIsLeader());
  }

  /**
   * A replica with primaryDiscovery.resolver LEADER_RPC follows the active primary, given all
   * primaries as seeds, and follows a failover without any discovery change.
   */
  @Test
  public void testReplicaFollowsLeaderAcrossFailover() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1, 2);
    a.commit(INDEX);
    refreshAndWait(b);

    // seed file lists both primaries' replication ports
    java.nio.file.Path seeds = folder.getRoot().toPath().resolve(TestServer.DISCOVERY_FILE);
    java.nio.file.Files.writeString(
        seeds,
        "[{\"host\":\"localhost\",\"port\":"
            + a.getReplicationPort()
            + "},{\"host\":\"localhost\",\"port\":"
            + b.getReplicationPort()
            + "}]");
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, 0, IndexDataLocationType.REMOTE)
            .withPrimaryResolver("LEADER_RPC")
            .withSyncInitialNrtPoint(true)
            .withAdditionalConfig(
                "s3RefreshUpload: true\n"
                    + "s3RefreshNotifyAfterUpload: true\n"
                    + "replicaReplicationPortPingInterval: 300")
            .build();
    assertTrue(replica.getConfiguration().getEnforcePrimaryGenOrdering());
    replica.verifySimpleDocIds(INDEX, 1, 2);

    a.addSimpleDocs(INDEX, 3);
    a.commit(INDEX);
    replica.waitForReplication(INDEX, a, 20000);
    replica.verifySimpleDocIds(INDEX, 1, 2, 3);

    // fail over to b; the replica's seeds do not change
    elect(b);
    b.addSimpleDocs(INDEX, 4);
    b.commit(INDEX);
    replica.waitForReplication(INDEX, b, 30000);
    replica.verifySimpleDocIds(INDEX, 1, 2, 3, 4);
  }

  /**
   * An ingestion client built from all primaries as seeds sends writes to the active primary, and
   * follows a failover without reconfiguration.
   */
  @Test
  public void testIngestionClientFollowsLeader() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    refreshAndWait(b);
    com.yelp.nrtsearch.clientlib.grpc.NrtSearchClient client =
        new com.yelp.nrtsearch.clientlib.grpc.NrtSearchClient.Builder(
                new com.yelp.nrtsearch.clientlib.grpc.NrtSearchClient.Builder.Leader(
                    com.yelp.nrtsearch.clientlib.leader.SeedSources.fixed(
                        List.of(
                            new com.yelp.nrtsearch.clientlib.Node("localhost", a.getPort()),
                            new com.yelp.nrtsearch.clientlib.Node("localhost", b.getPort()))),
                    100))
            .build();
    try {
      com.yelp.nrtsearch.test_utils.TestDocumentHelper.addDocuments(
          client.getAsyncStub(), java.util.stream.Stream.of(a.getSimpleDocRequest(INDEX, 1)));
      client.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(INDEX).build());
      a.verifySimpleDocIds(INDEX, 1);

      elect(b);
      // the client resolves the new leader within a few polls
      long deadline = System.currentTimeMillis() + 10000;
      while (true) {
        try {
          com.yelp.nrtsearch.test_utils.TestDocumentHelper.addDocuments(
              client.getAsyncStub(), java.util.stream.Stream.of(a.getSimpleDocRequest(INDEX, 2)));
          client.getBlockingStub().commit(CommitRequest.newBuilder().setIndexName(INDEX).build());
          break;
        } catch (RuntimeException e) {
          if (System.currentTimeMillis() > deadline) {
            throw e;
          }
          Thread.sleep(100);
        }
      }
      b.verifySimpleDocIds(INDEX, 1, 2);
    } finally {
      client.shutdown();
    }
  }

  /**
   * A replica with a static host list (all primaries share the replication port in production; in
   * this single machine test both hosts reach the same port) resolves the leader through it, and
   * all its indices share one leader resolver, released when the replica stops.
   */
  @Test
  public void testReplicaHostListSharesResolver() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.createSimpleIndex("other_index");
    a.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("other_index").build());
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    a.commit("other_index");
    refreshAndWait(b);
    int sharedBefore = sharedClients();
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, a.getReplicationPort(), IndexDataLocationType.REMOTE)
            .withPrimaryResolver("LEADER_RPC")
            .withPrimaryHost("localhost,127.0.0.1")
            .withSyncInitialNrtPoint(true)
            .withAdditionalConfig(
                "s3RefreshUpload: true\n"
                    + "s3RefreshNotifyAfterUpload: true\n"
                    + "replicaReplicationPortPingInterval: 300")
            .build();
    // two indices, one shared leader client
    assertEquals(sharedBefore + 1, sharedClients());
    replica.verifySimpleDocIds(INDEX, 1);
    replica.stop();
    assertEquals(sharedBefore, sharedClients());
  }

  /**
   * A replica seeded with all primaries keeps following after the original leader is stopped
   * entirely: the remaining seed reports the new leader.
   */
  @Test
  public void testReplicaFollowsAfterLeaderStopped() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    java.nio.file.Path seeds = folder.getRoot().toPath().resolve(TestServer.DISCOVERY_FILE);
    java.nio.file.Files.writeString(
        seeds,
        "[{\"host\":\"localhost\",\"port\":"
            + a.getReplicationPort()
            + "},{\"host\":\"localhost\",\"port\":"
            + b.getReplicationPort()
            + "}]");
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, 0, IndexDataLocationType.REMOTE)
            .withPrimaryResolver("LEADER_RPC")
            .withSyncInitialNrtPoint(true)
            .withAdditionalConfig(
                "s3RefreshUpload: true\n"
                    + "s3RefreshNotifyAfterUpload: true\n"
                    + "replicaReplicationPortPingInterval: 300")
            .build();
    replica.verifySimpleDocIds(INDEX, 1);
    // the leader goes away completely
    a.stop();
    elect(b);
    b.addSimpleDocs(INDEX, 2);
    b.commit(INDEX);
    replica.waitForReplication(INDEX, b, 30000);
    replica.verifySimpleDocIds(INDEX, 1, 2);
  }

  private TestServer leaderRpcReplicaWithoutAutoStart(boolean allowSingleLeaderSeed)
      throws IOException {
    return TestServer.builder(folder)
        .withAutoStartConfig(false, Mode.REPLICA, 0, IndexDataLocationType.REMOTE)
        .withPrimaryResolver("LEADER_RPC")
        .withPrimaryHost("")
        .withAllowSingleLeaderSeed(allowSingleLeaderSeed)
        .withAdditionalConfig("s3RefreshUpload: true\ns3RefreshNotifyAfterUpload: true")
        .build();
  }

  /** A StartIndex request with a single primary address is rejected on a LEADER_RPC replica. */
  @Test
  public void testStartIndexSingleLeaderSeedRejected() throws Exception {
    TestServer a = primaryServer();
    bootstrap(a);
    a.commit(INDEX);
    TestServer replica = leaderRpcReplicaWithoutAutoStart(false);
    StatusRuntimeException e =
        assertThrows(
            StatusRuntimeException.class,
            () -> replica.startReplicaIndex(INDEX, -1, a.getReplicationPort(), null));
    assertTrue(e.getMessage(), e.getMessage().contains("single host"));
    // a host list is accepted
    replica.startIndex(
        StartIndexRequest.newBuilder()
            .setIndexName(INDEX)
            .setMode(Mode.REPLICA)
            .setPrimaryAddress("localhost,127.0.0.1")
            .setPort(a.getReplicationPort())
            .build());
    assertTrue(replica.isStarted(INDEX));
  }

  @Test
  public void testStartIndexSingleLeaderSeedAllowed() throws Exception {
    TestServer a = primaryServer();
    bootstrap(a);
    a.commit(INDEX);
    TestServer replica = leaderRpcReplicaWithoutAutoStart(true);
    replica.startReplicaIndex(INDEX, -1, a.getReplicationPort(), null);
    assertTrue(replica.isStarted(INDEX));
  }

  /** A replica start that fails releases its shared leader client. */
  @Test
  public void testFailedReplicaStartReleasesSharedClient() throws Exception {
    TestServer a = primaryServer();
    bootstrap(a);
    a.commit(INDEX);
    TestServer replica = leaderRpcReplicaWithoutAutoStart(false);
    int before = sharedClients();
    // a point state that can not be parsed makes the restore, after the client is acquired, fail
    String uniqueName = a.getGlobalState().getDataResourceForIndex(INDEX);
    com.yelp.nrtsearch.server.remote.s3.S3Backend s3 =
        (com.yelp.nrtsearch.server.remote.s3.S3Backend) a.getRemoteBackend();
    String pointPrefix =
        com.yelp.nrtsearch.server.remote.s3.S3Backend.getIndexResourcePrefix(
            a.getServiceName(),
            uniqueName,
            com.yelp.nrtsearch.server.remote.RemoteBackend.IndexResourceType.POINT_STATE);
    String pointFile = s3.getCurrentResourceName(pointPrefix);
    s3.getS3()
        .putObject(
            r -> r.bucket(s3.getServiceBucket()).key(pointPrefix + pointFile),
            software.amazon.awssdk.core.sync.RequestBody.fromString("not a point state"));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            replica.startIndex(
                StartIndexRequest.newBuilder()
                    .setIndexName(INDEX)
                    .setMode(Mode.REPLICA)
                    .setPrimaryAddress("localhost,127.0.0.1")
                    .setPort(a.getReplicationPort())
                    .setRestore(
                        RestoreIndex.newBuilder()
                            .setServiceName(a.getServiceName())
                            .setResourceName(INDEX)
                            .setDeleteExistingData(true))
                    .build()));
    assertFalse(replica.isStarted(INDEX));
    assertEquals(before, sharedClients());
  }

  private static int sharedClients() throws Exception {
    java.lang.reflect.Method m = SharedLeaderClients.class.getDeclaredMethod("size");
    m.setAccessible(true);
    return (int) m.invoke(null);
  }

  /** StartIndex v1 rejects fields the leader would otherwise silently ignore. */
  @Test
  public void testStartIndexV1RejectsIgnoredFields() throws Exception {
    TestServer a = primaryServer();
    bootstrap(a);
    for (StartIndexRequest request :
        List.of(
            StartIndexRequest.newBuilder()
                .setIndexName(INDEX)
                .setMode(Mode.PRIMARY)
                .setRestore(RestoreIndex.newBuilder().setServiceName("s").setResourceName("r"))
                .build(),
            StartIndexRequest.newBuilder()
                .setIndexName(INDEX)
                .setMode(Mode.PRIMARY)
                .setPrimaryGen(5)
                .build())) {
      StatusRuntimeException e =
          assertThrows(StatusRuntimeException.class, () -> a.startIndex(request));
      assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }
  }

  /** A new leader can back up warming queries that an earlier leader wrote. */
  @Test
  public void testWarmingQueriesWritableAfterFailover() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    String uniqueName = a.getGlobalState().getDataResourceForIndex(INDEX);
    a.getRemoteBackend().uploadWarmingQueries(a.getServiceName(), uniqueName, "a".getBytes());
    refreshAndWait(b);

    elect(b);
    b.getRemoteBackend().uploadWarmingQueries(b.getServiceName(), uniqueName, "b".getBytes());
    assertTrue(leaderManager(b).isActive());
    assertEquals(
        "b",
        new String(
            b.getRemoteBackend()
                .downloadWarmingQueries(b.getServiceName(), uniqueName)
                .readAllBytes()));
  }

  /** A commit by the old leader that completes before the new leader claims is not lost. */
  @Test
  public void testCommitBeforeClaimIsRestored() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    refreshAndWait(b);
    a.addSimpleDocs(INDEX, 2);
    a.commit(INDEX);

    cluster.electWithoutRevoking(nodeName(b));
    leaderManager(b).awaitTransitions();
    b.verifySimpleDocIds(INDEX, 1, 2);
  }
}
