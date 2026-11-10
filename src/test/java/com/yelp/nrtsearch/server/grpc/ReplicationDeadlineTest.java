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
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.nrt.NRTPrimaryNode;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that a replica which is connected but never responds (e.g. hung after OOM) cannot block
 * primary refreshes, since the primary applies a deadline to newNRTPoint and drops the replica.
 */
public class ReplicationDeadlineTest {
  private static final String TEST_INDEX = "test_index";
  private static final String HUNG_REPLICA_NAME = "hung_replica";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private Server hungReplicaServer;

  @After
  public void cleanup() {
    if (hungReplicaServer != null) {
      hungReplicaServer.shutdownNow();
    }
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  /** Replication server that accepts newNRTPoint calls but never responds. */
  private static class HungReplica extends ReplicationServerGrpc.ReplicationServerImplBase {
    final CountDownLatch newNRTPointReceived = new CountDownLatch(1);

    @Override
    public void newNRTPoint(NewNRTPoint request, StreamObserver<TransferStatus> responseObserver) {
      newNRTPointReceived.countDown();
    }
  }

  private TestServer startPrimary(String additionalConfig) throws IOException {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withAdditionalConfig(additionalConfig)
            .build();
    primaryServer.createSimpleIndex(TEST_INDEX);
    primaryServer.startPrimaryIndex(TEST_INDEX, -1, null);
    return primaryServer;
  }

  private void registerReplica(TestServer primaryServer, int replicaPort) throws IOException {
    String indexId =
        primaryServer.getGlobalState().getIndexStateManagerOrThrow(TEST_INDEX).getIndexId();
    primaryServer
        .getReplicationClient()
        .addReplicas(TEST_INDEX, indexId, HUNG_REPLICA_NAME, "localhost", replicaPort);
  }

  private NRTPrimaryNode getPrimaryNode(TestServer primaryServer) throws IOException {
    return primaryServer.getGlobalState().getIndexOrThrow(TEST_INDEX).getShard(0).nrtPrimaryNode;
  }

  @Test
  public void testHungReplicaRemovedOnRefresh() throws Exception {
    TestServer primaryServer = startPrimary("newNRTPointDeadlineMs: 500");
    HungReplica hungReplica = new HungReplica();
    hungReplicaServer = ServerBuilder.forPort(0).addService(hungReplica).build().start();
    registerReplica(primaryServer, hungReplicaServer.getPort());

    NRTPrimaryNode primaryNode = getPrimaryNode(primaryServer);
    assertEquals(1, primaryNode.getNodesInfo().size());
    assertEquals(
        500,
        primaryNode.getNodesInfo().iterator().next().getReplicationServerClient().getDeadlineMs());

    primaryServer.addSimpleDocs(TEST_INDEX, 1, 2);
    long startMs = System.currentTimeMillis();
    // refresh sends the NRT point to replicas, this would block forever without a deadline
    primaryServer.refresh(TEST_INDEX);
    long elapsedMs = System.currentTimeMillis() - startMs;

    assertTrue(hungReplica.newNRTPointReceived.await(10, TimeUnit.SECONDS));
    assertTrue("refresh took " + elapsedMs + "ms", elapsedMs < 10000);
    // hung replica is dropped, so later refreshes do not wait on it again
    assertEquals(0, primaryNode.getNodesInfo().size());

    primaryServer.addSimpleDocs(TEST_INDEX, 3, 4);
    primaryServer.refresh(TEST_INDEX);
    primaryServer.verifySimpleDocs(TEST_INDEX, 4);
  }

  @Test
  public void testHungReplicaRemovedOnWriteNRTPoint() throws Exception {
    TestServer primaryServer = startPrimary("newNRTPointDeadlineMs: 500");
    HungReplica hungReplica = new HungReplica();
    hungReplicaServer = ServerBuilder.forPort(0).addService(hungReplica).build().start();
    registerReplica(primaryServer, hungReplicaServer.getPort());

    NRTPrimaryNode primaryNode = getPrimaryNode(primaryServer);
    assertEquals(1, primaryNode.getNodesInfo().size());

    primaryServer.addSimpleDocs(TEST_INDEX, 1, 2);
    SearcherVersion searcherVersion =
        primaryServer.getReplicationClient().writeNRTPoint(TEST_INDEX);

    assertTrue(searcherVersion.getDidRefresh());
    assertTrue(hungReplica.newNRTPointReceived.await(10, TimeUnit.SECONDS));
    assertEquals(0, primaryNode.getNodesInfo().size());
  }
}
