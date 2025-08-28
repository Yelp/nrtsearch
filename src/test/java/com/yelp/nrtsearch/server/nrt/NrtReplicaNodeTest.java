/*
 * Copyright 2024 Yelp Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.grpc.ReplicationServerClient;
import com.yelp.nrtsearch.server.utils.HostPort;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.store.Directory;
import org.junit.Test;

public class NrtReplicaNodeTest {

  @Test
  public void testDefaultCopyThread() {
    NrtCopyThread ct = NRTReplicaNode.getNrtCopyThread(null, 0);
    assertTrue(ct instanceof DefaultCopyThread);
  }

  @Test
  public void testProportionalCopyThread() {
    NrtCopyThread ct = NRTReplicaNode.getNrtCopyThread(null, 15);
    assertTrue(ct instanceof ProportionalCopyThread);
  }

  private NRTReplicaNode getNrtReplicaNodeWithoutPrimary() throws IOException {
    return new NRTReplicaNode(
        "test_index",
        "id",
        null,
        new HostPort("host", 0),
        "testNode",
        mock(Directory.class),
        null,
        null,
        false,
        false,
        true,
        0);
  }

  @Test
  public void testHasPrimaryConnection_true() throws IOException {
    NRTReplicaNode replicaNode =
        new NRTReplicaNode(
            "test_index",
            "id",
            mock(ReplicationServerClient.class),
            new HostPort("host", 0),
            "testNode",
            mock(Directory.class),
            null,
            null,
            false,
            false,
            true,
            0);
    assertTrue(replicaNode.hasPrimaryConnection());
  }

  @Test
  public void testHasPrimaryConnection_false() throws IOException {
    NRTReplicaNode replicaNode = getNrtReplicaNodeWithoutPrimary();
    assertFalse(replicaNode.hasPrimaryConnection());
  }

  @Test
  public void testNoPrimaryConnection_newCopyJob() throws IOException {
    NRTReplicaNode replicaNode = getNrtReplicaNodeWithoutPrimary();
    try {
      replicaNode.newCopyJob("reason", Map.of(), Map.of(), false, null);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Cannot create new copy job, primary connection not available", e.getMessage());
    }
  }

  @Test
  public void testNoPrimaryConnection_isKnownToPrimary() throws IOException {
    NRTReplicaNode replicaNode = getNrtReplicaNodeWithoutPrimary();
    assertFalse(replicaNode.isKnownToPrimary());
  }

  @Test
  public void testNoPrimaryConnection_syncFromCurrentPrimaryNoop() throws IOException {
    NRTReplicaNode replicaNode = getNrtReplicaNodeWithoutPrimary();
    replicaNode.syncFromCurrentPrimary(0, 0);
  }
}
