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
package com.yelp.nrtsearch.server.nrt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.grpc.TestServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NRTPrimaryNodeReplicaRemovalTest {
  private static final String TEST_INDEX = "test_index";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupServers();
  }

  @AfterClass
  public static void cleanupClass() {
    TestServer.cleanupAll();
  }

  private NRTPrimaryNode startPrimary() throws Exception {
    TestServer primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    primaryServer.createSimpleIndex(TEST_INDEX);
    primaryServer.startPrimaryIndex(TEST_INDEX, -1, null);
    return primaryServer.getGlobalState().getIndexOrThrow(TEST_INDEX).getShard(0).nrtPrimaryNode;
  }

  private ReplicationServerClient mockClient() {
    ReplicationServerClient client = mock(ReplicationServerClient.class);
    when(client.getHost()).thenReturn("localhost");
    when(client.getPort()).thenReturn(1234);
    return client;
  }

  /**
   * A broadcast that timed out on a stale entry must not remove an equal (same name and address)
   * entry that registered while the broadcast was running.
   */
  @Test
  public void testRemoveReplicaByIdentity() throws Exception {
    NRTPrimaryNode primaryNode = startPrimary();
    ReplicationServerClient staleClient = mockClient();
    ReplicationServerClient newClient = mockClient();
    NRTPrimaryNode.ReplicaDetails stale = new NRTPrimaryNode.ReplicaDetails("replica", staleClient);
    NRTPrimaryNode.ReplicaDetails current = new NRTPrimaryNode.ReplicaDetails("replica", newClient);
    assertEquals(stale, current);

    // stale entry already removed, replica re-registered with an equal entry
    primaryNode.replicasInfos.add(current);
    primaryNode.removeReplica(stale);

    assertEquals(1, primaryNode.replicasInfos.size());
    assertSame(current, primaryNode.replicasInfos.peek());
    verify(newClient, never()).close();
    // stale entry was not in the queue, its client is closed by whoever removed it
    verify(staleClient, never()).close();
  }

  @Test
  public void testRemoveReplicaClosesClient() throws Exception {
    NRTPrimaryNode primaryNode = startPrimary();
    ReplicationServerClient client = mockClient();
    NRTPrimaryNode.ReplicaDetails replica = new NRTPrimaryNode.ReplicaDetails("replica", client);
    primaryNode.replicasInfos.add(replica);

    primaryNode.removeReplica(replica);

    assertEquals(0, primaryNode.replicasInfos.size());
    verify(client).close();
  }
}
