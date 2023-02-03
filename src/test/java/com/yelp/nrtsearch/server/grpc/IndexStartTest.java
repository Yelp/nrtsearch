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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.luceneserver.NRTReplicaNode;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import org.apache.lucene.replicator.nrt.ReplicaDeleterManager;
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

  @Test
  public void testIndexAutoStarts_failStandaloneRemote() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    server.createSimpleIndex("test_index");
    try {
      server.startStandaloneIndex("test_index", null);
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage()
              .contains("STANDALONE index mode cannot be used with REMOTE data location type"));
    }
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
            .withDecInitialCommit(true)
            .build();
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    replicaServer.verifySimpleDocs("test_index", 3);
    ReplicaDeleterManager rdm =
        replicaServer
            .getGlobalState()
            .getIndex("test_index")
            .getShard(0)
            .nrtReplicaNode
            .getReplicaDeleterManager();
    assertFalse(rdm == null);
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
  public void testStartIndexReplicaCleansUpArchiverData() throws IOException {
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

    Path archiverRoot =
        Path.of(replicaServer.getGlobalState().getConfiguration().getArchiveDirectory());
    assertEquals(0, archiverRoot.toFile().list().length);
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

  @Test
  public void testRecreateDeletedIndex() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 3);

    String indexId =
        server.getGlobalState().getDataResourceForIndex("test_index").split("test_index-")[1];

    server.deleteIndex("test_index");
    assertTrue(server.isReady());
    assertFalse(server.indices().contains("test_index"));

    server.createIndex(
        CreateIndexRequest.newBuilder()
            .setIndexName("test_index")
            .setExistsWithId(indexId)
            .build());
    server.startPrimaryIndex(
        "test_index",
        -1,
        RestoreIndex.newBuilder()
            .setServiceName(TestServer.SERVICE_NAME)
            .setResourceName("test_index")
            .build());
    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));
    server.verifySimpleDocs("test_index", 3);

    server.restart();
    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));
    server.verifySimpleDocs("test_index", 3);
  }

  @Test
  public void testDiscoveryFileUpdateInterval() throws IOException {
    TestServer primary =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .withWriteDiscoveryFile(true)
            .build();
    primary.createSimpleIndex("test_index");
    primary.startPrimaryIndex("test_index", -1, null);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withSyncInitialNrtPoint(false)
            .build();
    ReplicationServerClient replicationClient =
        replicaServer
            .getGlobalState()
            .getIndex("test_index")
            .getShard(0)
            .nrtReplicaNode
            .getPrimaryAddress();
    assertEquals(
        ReplicationServerClient.FILE_UPDATE_INTERVAL_MS,
        replicationClient.getDiscoveryFileUpdateIntervalMs());

    assertNotEquals(100, ReplicationServerClient.FILE_UPDATE_INTERVAL_MS);
    TestServer replicaServer2 =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.REPLICA, -1, IndexDataLocationType.REMOTE)
            .withSyncInitialNrtPoint(false)
            .withAdditionalConfig("discoveryFileUpdateIntervalMs: 100")
            .build();
    replicationClient =
        replicaServer2
            .getGlobalState()
            .getIndex("test_index")
            .getShard(0)
            .nrtReplicaNode
            .getPrimaryAddress();
    assertEquals(100, replicationClient.getDiscoveryFileUpdateIntervalMs());
  }

  public void testCreateWithProperties() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withLocalStateBackend()
            .build();

    CreateIndexRequest createRequest = getCreateWithPropertiesRequest();
    server.createIndex(createRequest);

    assertTrue(server.indices().contains("test_index"));
    assertFalse(server.isStarted("test_index"));

    verifyCreateProperties(server);
  }

  @Test
  public void testCreateAndStart() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withLocalStateBackend()
            .build();

    CreateIndexRequest createRequest =
        getCreateWithPropertiesRequest().toBuilder().setStart(true).build();
    server.createIndex(createRequest);

    assertTrue(server.indices().contains("test_index"));
    assertTrue(server.isStarted("test_index"));

    verifyCreateProperties(server);
  }

  @Test
  public void testCreateAndStartRestart() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withLocalStateBackend()
            .build();

    CreateIndexRequest createRequest =
        getCreateWithPropertiesRequest().toBuilder().setStart(true).build();
    server.createIndex(createRequest);

    assertTrue(server.indices().contains("test_index"));
    assertTrue(server.isStarted("test_index"));

    server.restart();
    assertTrue(server.indices().contains("test_index"));
    assertTrue(server.isStarted("test_index"));

    verifyCreateProperties(server);
  }

  @Test
  public void testCreateAndStartV2() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withLocalStateBackend()
            .build();

    CreateIndexRequest createRequest = getCreateWithPropertiesRequest();
    server.createIndex(createRequest);

    assertTrue(server.indices().contains("test_index"));
    assertFalse(server.isStarted("test_index"));

    server.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());

    assertTrue(server.indices().contains("test_index"));
    assertTrue(server.isStarted("test_index"));
    assertTrue(server.getGlobalState().getIndex("test_index").getShard(0).isPrimary());

    verifyCreateProperties(server);
  }

  @Test
  public void testCreateAndStartV2Restart() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withLocalStateBackend()
            .build();

    CreateIndexRequest createRequest = getCreateWithPropertiesRequest();
    server.createIndex(createRequest);

    assertTrue(server.indices().contains("test_index"));
    assertFalse(server.isStarted("test_index"));

    server.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());

    assertTrue(server.indices().contains("test_index"));
    assertTrue(server.isStarted("test_index"));
    assertTrue(server.getGlobalState().getIndex("test_index").getShard(0).isPrimary());

    server.restart();
    assertTrue(server.indices().contains("test_index"));
    assertTrue(server.isStarted("test_index"));
    assertTrue(server.getGlobalState().getIndex("test_index").getShard(0).isPrimary());

    verifyCreateProperties(server);
  }

  @Test
  public void testStartV2Replica() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withLocalStateBackend()
            .build();
    server.createSimpleIndex("test_index");
    server.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());

    assertTrue(server.indices().contains("test_index"));
    assertTrue(server.isStarted("test_index"));
    server.stopIndex("test_index");

    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.LOCAL)
            .withLocalStateBackend()
            .withSyncInitialNrtPoint(false)
            .build();

    assertTrue(replica.indices().contains("test_index"));
    assertFalse(replica.isStarted("test_index"));
    replica.startIndexV2(StartIndexV2Request.newBuilder().setIndexName("test_index").build());

    assertTrue(replica.indices().contains("test_index"));
    assertTrue(replica.isStarted("test_index"));
    assertTrue(replica.getGlobalState().getIndex("test_index").getShard(0).isReplica());
  }

  private CreateIndexRequest getCreateWithPropertiesRequest() {
    IndexSettings initialSettings =
        IndexSettings.newBuilder()
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .build();
    IndexLiveSettings initialLiveSettings =
        IndexLiveSettings.newBuilder()
            .setAddDocumentsMaxBufferLen(Int32Value.newBuilder().setValue(1000).build())
            .build();
    return CreateIndexRequest.newBuilder()
        .setIndexName("test_index")
        .setSettings(initialSettings)
        .setLiveSettings(initialLiveSettings)
        .addAllFields(TestServer.simpleFields)
        .build();
  }

  private void verifyCreateProperties(TestServer server) throws IOException {
    assertTrue(
        server
            .getGlobalState()
            .getIndexStateManager("test_index")
            .getSettings()
            .getIndexMergeSchedulerAutoThrottle()
            .getValue());
    assertEquals(
        1000,
        server
            .getGlobalState()
            .getIndexStateManager("test_index")
            .getLiveSettings()
            .getAddDocumentsMaxBufferLen()
            .getValue());
    assertEquals(
        new HashSet<>(TestServer.simpleFieldNames),
        server.getGlobalState().getIndex("test_index").getAllFields().keySet());
  }

  @Test
  public void testReplicaDecInitialCommit() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);

    assertTrue(server.isReady());
    assertTrue(server.isStarted("test_index"));

    server.addSimpleDocs("test_index", 1, 2, 3);
    server.commit("test_index");
    server.refresh("test_index");
    server.verifySimpleDocs("test_index", 3);

    TestServer replicaServer =
        TestServer.builder(folder)
            .withDecInitialCommit(true)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.REMOTE)
            .build();
    assertTrue(replicaServer.isReady());
    assertTrue(replicaServer.isStarted("test_index"));
    replicaServer.verifySimpleDocs("test_index", 3);
    NRTReplicaNode nrtReplicaNode =
        replicaServer.getGlobalState().getIndex("test_index").getShard(0).nrtReplicaNode;
    ReplicaDeleterManager rdm = nrtReplicaNode.getReplicaDeleterManager();

    assertFalse(rdm == null);

    server.deleteAllDocuments("test_index");
    server.refresh("test_index");
    server.commit("test_index");
    server.verifySimpleDocs("test_index", 0);

    nrtReplicaNode.syncFromCurrentPrimary(120000, 300000);

    replicaServer.verifySimpleDocs("test_index", 0);
    String[] replicaFiles = nrtReplicaNode.getDirectory().listAll();
    assertEquals(1, replicaFiles.length);
    assertEquals("write.lock", replicaFiles[0]);
  }
}
