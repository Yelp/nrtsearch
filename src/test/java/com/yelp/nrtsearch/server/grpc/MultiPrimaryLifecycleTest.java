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
          "  type: " + ManualElectorPlugin.TYPE);

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
    assertTrue(e.getMessage(), e.getMessage().contains("not the active primary"));
  }

  @Test
  public void testFailoverToStandby() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    long epochA = bootstrap(a);
    a.addSimpleDocs(INDEX, 1, 2, 3);
    a.commit(INDEX);

    // b picks up the index from global state and follows as standby
    b.reloadState();
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
        writeError.getMessage().contains("on a replica node is not supported"));

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
    b.reloadState();

    // b is elected while a still believes it is leader
    cluster.electWithoutRevoking(nodeName(b));
    leaderManager(b).awaitTransitions();
    assertTrue(leaderManager(b).isActive());
    assertTrue(shard(a).isPrimary());

    // a resumes and accepts a write, but cannot make it durable
    a.addSimpleDocs(INDEX, 3);
    StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> a.commit(INDEX));
    assertTrue(e.getMessage(), e.getMessage().contains("Error waiting for commit refresh"));

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

  /** A commit by the old leader that completes before the new leader claims is not lost. */
  @Test
  public void testCommitBeforeClaimIsRestored() throws Exception {
    TestServer a = primaryServer();
    TestServer b = primaryServer();
    bootstrap(a);
    a.addSimpleDocs(INDEX, 1);
    a.commit(INDEX);
    b.reloadState();
    a.addSimpleDocs(INDEX, 2);
    a.commit(INDEX);

    cluster.electWithoutRevoking(nodeName(b));
    leaderManager(b).awaitTransitions();
    b.verifySimpleDocIds(INDEX, 1, 2);
  }
}
