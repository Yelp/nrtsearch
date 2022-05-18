/*
 * Copyright 2022 Yelp Inc.
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
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import java.io.IOException;
import java.util.Collections;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IndexStartTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void testNoIndices() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.STANDALONE, 0, IndexDataLocationType.LOCAL)
            .build();
    assertEquals(Collections.emptySet(), server.indices());
    assertTrue(server.isReady());

    server.restart();
    assertEquals(Collections.emptySet(), server.indices());
    assertTrue(server.isReady());
  }

  @Test
  public void testIndexNotStarted_local() throws IOException {
    indexNotStarted(IndexDataLocationType.LOCAL, Mode.PRIMARY);
  }

  @Test
  public void testIndexNotStarted_remote() throws IOException {
    indexNotStarted(IndexDataLocationType.REMOTE, Mode.PRIMARY);
  }

  @Test
  public void testIndexNotStarted_standalone() throws IOException {
    indexNotStarted(IndexDataLocationType.LOCAL, Mode.STANDALONE);
  }

  private void indexNotStarted(IndexDataLocationType locationType, Mode mode) throws IOException {
    TestServer server =
        TestServer.builder(folder).withAutoStartConfig(true, mode, 0, locationType).build();
    server.createIndex("test_index");
    assertTrue(server.isReady());
    assertFalse(server.isStarted("test_index"));

    server.restart();
    assertTrue(server.isReady());
    assertFalse(server.isStarted("test_index"));
  }

  @Test
  public void testIndexNotStartedReplica_local() throws IOException {
    indexNotStartedReplica(IndexDataLocationType.LOCAL);
  }

  @Test
  public void testIndexNotStartedReplica_remote() throws IOException {
    indexNotStartedReplica(IndexDataLocationType.REMOTE);
  }

  private void indexNotStartedReplica(IndexDataLocationType locationType) throws IOException {
    TestServer server =
        TestServer.builder(folder).withAutoStartConfig(true, Mode.PRIMARY, 0, locationType).build();
    server.createIndex("test_index");
    assertTrue(server.isReady());
    assertFalse(server.isStarted("test_index"));

    server.restart();
    assertTrue(server.isReady());
    assertFalse(server.isStarted("test_index"));
  }

  @Test
  public void testIndexAutoStarts_local() throws IOException {
    indexAutoStarts(IndexDataLocationType.LOCAL);
  }

  @Test
  public void testIndexAutoStarts_remote() throws IOException {
    indexAutoStarts(IndexDataLocationType.REMOTE);
  }

  @Test
  public void testIndexAutoStarts_standalone() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    server.createSimpleIndex("test_index");
    server.startStandaloneIndex("test_index", null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    server.restart();
    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));
    server.verifySimpleDocs("test_index", 3);
  }

  private void indexAutoStarts(IndexDataLocationType locationType) throws IOException {
    TestServer server =
        TestServer.builder(folder).withAutoStartConfig(true, Mode.PRIMARY, 0, locationType).build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    server.restart();
    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));
    server.verifySimpleDocs("test_index", 3);
  }

  @Test
  public void testIndexAutoStartsReplica_local() throws IOException {
    indexAutoStartsReplica(IndexDataLocationType.LOCAL);
  }

  @Test
  public void testIndexAutoStartsReplica_remote() throws IOException {
    indexAutoStartsReplica(IndexDataLocationType.REMOTE);
  }

  private void indexAutoStartsReplica(IndexDataLocationType locationType) throws IOException {
    TestServer server =
        TestServer.builder(folder).withAutoStartConfig(true, Mode.PRIMARY, 0, locationType).build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, server.getReplicationPort(), locationType)
            .build();
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    replicaServer.verifySimpleDocs("test_index", 3);
  }

  @Test
  public void testStopIndex_local() throws IOException {
    stopIndex(IndexDataLocationType.LOCAL);
  }

  @Test
  public void testStopIndex_remote() throws IOException {
    stopIndex(IndexDataLocationType.REMOTE);
  }

  private void stopIndex(IndexDataLocationType locationType) throws IOException {
    TestServer server =
        TestServer.builder(folder).withAutoStartConfig(true, Mode.PRIMARY, 0, locationType).build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    server.stopIndex("test_index");
    assertTrue(server.isReady());
    assertFalse(server.isStarted("test_index"));

    server.restart();
    assertTrue(server.isReady());
    assertFalse(server.isStarted("test_index"));
  }

  @Test
  public void testStopIndexReplica_local() throws IOException {
    stopIndexReplica(IndexDataLocationType.LOCAL);
  }

  @Test
  public void testStopIndexReplica_remote() throws IOException {
    stopIndexReplica(IndexDataLocationType.REMOTE);
  }

  private void stopIndexReplica(IndexDataLocationType locationType) throws IOException {
    TestServer server =
        TestServer.builder(folder).withAutoStartConfig(true, Mode.PRIMARY, 0, locationType).build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    server.stopIndex("test_index");
    assertTrue(server.isReady());
    assertFalse(server.isStarted("test_index"));

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, server.getReplicationPort(), locationType)
            .build();
    assertTrue(replicaServer.isReady());
    assertFalse(replicaServer.isStarted("test_index"));
  }

  @Test
  public void testStartIndexReplica() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    server.stopIndex("test_index");
    assertTrue(server.isReady());
    assertFalse(server.isStarted("test_index"));

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.REMOTE)
            .withSyncInitialNrtPoint(false)
            .build();
    assertTrue(replicaServer.isReady());
    assertFalse(replicaServer.isStarted("test_index"));

    replicaServer.startReplicaIndex(
        "test_index",
        -1,
        server.getReplicationPort(),
        RestoreIndex.newBuilder()
            .setServiceName(replicaServer.getServiceName())
            .setResourceName("test_index")
            .setDeleteExistingData(true)
            .build());
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    replicaServer.verifySimpleDocs("test_index", 3);
  }

  @Test
  public void testStopIndexReplica() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.REMOTE)
            .build();
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    replicaServer.verifySimpleDocs("test_index", 3);

    replicaServer.stopIndex("test_index");
    assertFalse(replicaServer.isReady());
    assertFalse(replicaServer.isStarted("test_index"));
  }

  @Test
  public void testMultipleIndices() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");

    server.createSimpleIndex("test_index_2");
    server.startPrimaryIndex("test_index_2", -1, null);
    server.addSimpleDocs("test_index_2", 3, 4, 5);
    server.refresh("test_index_2");
    server.commit("test_index_2");

    server.createSimpleIndex("test_index_3");
    server.startPrimaryIndex("test_index_3", -1, null);
    server.addSimpleDocs("test_index_3", 6, 7);
    server.refresh("test_index_3");
    server.commit("test_index_3");

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));
    assertTrue(server.isStarted("test_index_2"));
    assertTrue(server.isStarted("test_index_3"));

    server.stopIndex("test_index_2");

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));
    assertFalse(server.isStarted("test_index_2"));
    assertTrue(server.isStarted("test_index_3"));

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.REMOTE)
            .withSyncInitialNrtPoint(false)
            .build();
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    assertFalse(replicaServer.isStarted("test_index_2"));
    assertTrue(replicaServer.isStarted("test_index_3"));
    replicaServer.verifySimpleDocs("test_index", 3);
    replicaServer.verifySimpleDocs("test_index_3", 2);
  }

  @Test
  public void testLocalPrimaryRemoteReplica() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.REMOTE)
            .build();
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    replicaServer.verifySimpleDocs("test_index", 3);
  }

  @Test
  public void testRemotePrimaryLocalReplica() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .build();
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    replicaServer.verifySimpleDocs("test_index", 3);
  }

  @Test
  public void testRemoteDataClearLocal() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    server.restart(true);
    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));
    server.verifySimpleDocs("test_index", 3);
  }

  @Test
  public void testRemoteDataClearLocalReplica() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.REMOTE)
            .withSyncInitialNrtPoint(false)
            .build();
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    replicaServer.verifySimpleDocs("test_index", 3);

    replicaServer.restart(true);
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    replicaServer.verifySimpleDocs("test_index", 3);
  }
}
