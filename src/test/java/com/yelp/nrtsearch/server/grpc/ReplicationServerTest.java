/*
 * Copyright 2020 Yelp Inc.
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

import static com.yelp.nrtsearch.server.grpc.ReplicationServerClient.BINARY_MAGIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.config.IndexStartConfig;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReplicationServerTest {
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private TestServer primaryServer;
  private TestServer replicaServer;

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  @Test
  public void recvCopyState() throws IOException, InterruptedException {
    initDefaultServer();

    primaryServer.addSimpleDocs("test_index", 1, 2);
    primaryServer.refresh("test_index");

    CopyStateRequest copyStateRequest =
        CopyStateRequest.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName("test_index")
            .setIndexId(
                primaryServer
                    .getGlobalState()
                    .getIndexStateManagerOrThrow("test_index")
                    .getIndexId())
            .setReplicaId(0)
            .build();
    CopyState copyState =
        primaryServer.getReplicationClient().getBlockingStub().recvCopyState(copyStateRequest);
    assertEquals(1, copyState.getGen());
    FilesMetadata filesMetadata = copyState.getFilesMetadata();
    assertEquals(3, filesMetadata.getNumFiles());
  }

  @Test
  public void copyFiles() throws IOException, InterruptedException {
    initServerSyncInitialNrtPointFalse();

    // Stop replica so it does not get nrt point from indexing
    replicaServer.stopIndex("test_index");

    // index 2 documents to primary
    primaryServer.addSimpleDocs("test_index", 1, 2);

    // This causes the copyState on primary to be refreshed
    primaryServer.refresh("test_index");

    // capture the copy state on primary (client node in this test case)
    CopyStateRequest copyStateRequest =
        CopyStateRequest.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName("test_index")
            .setIndexId(
                primaryServer
                    .getGlobalState()
                    .getIndexStateManagerOrThrow("test_index")
                    .getIndexId())
            .setReplicaId(0)
            .build();
    CopyState copyState =
        primaryServer.getReplicationClient().getBlockingStub().recvCopyState(copyStateRequest);
    assertEquals(1, copyState.getGen());
    FilesMetadata filesMetadata = copyState.getFilesMetadata();
    assertEquals(3, filesMetadata.getNumFiles());

    // send the file metadata info to replica
    replicaServer.startReplicaIndex("test_index", -1, primaryServer.getReplicationPort(), null);
    CopyFiles.Builder requestBuilder =
        CopyFiles.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName("test_index")
            .setIndexId(
                primaryServer
                    .getGlobalState()
                    .getIndexStateManagerOrThrow("test_index")
                    .getIndexId())
            .setPrimaryGen(primaryServer.getGlobalState().getGeneration());
    requestBuilder.setFilesMetadata(filesMetadata);

    Iterator<TransferStatus> transferStatusIterator =
        replicaServer.getReplicationClient().getBlockingStub().copyFiles(requestBuilder.build());
    int done = 0;
    int failed = 0;
    int ongoing = 0;
    while (transferStatusIterator.hasNext()) {
      TransferStatus transferStatus = transferStatusIterator.next();
      if (transferStatus.getCode().equals(TransferStatusCode.Done)) {
        done++;
      } else if (transferStatus.getCode().equals(TransferStatusCode.Failed)) {
        failed++;
      } else if (transferStatus.getCode().equals(TransferStatusCode.Ongoing)) {
        ongoing++;
      }
    }
    assertEquals(1, done);
    assertTrue(0 <= ongoing);
    assertEquals(0, failed);
  }

  @Test
  public void basicReplication() throws IOException, InterruptedException {
    initDefaultServer();

    // index 2 documents to primary
    primaryServer.addSimpleDocs("test_index", 1, 2);
    // refresh (also sends NRTPoint to replicas, but none started at this point)
    primaryServer.refresh("test_index");
    // add 2 more docs to primary
    primaryServer.addSimpleDocs("test_index", 3, 4);

    // publish new NRT point (retrieve the current searcher version on primary)
    primaryServer.refresh("test_index");

    // primary should show 4 hits now
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 4);

    // replica should too!
    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4);
  }

  @Test
  public void getConnectedNodes() throws IOException, InterruptedException {
    initDefaultServer();

    // primary should have registered replica in its connected nodes list
    GetNodesResponse getNodesResponse =
        primaryServer
            .getReplicationClient()
            .getBlockingStub()
            .getConnectedNodes(GetNodesRequest.newBuilder().setIndexName("test_index").build());
    assertEquals(1, getNodesResponse.getNodesCount());
    assertEquals("localhost", getNodesResponse.getNodesList().getFirst().getHostname());
    assertEquals(
        replicaServer.getReplicationPort(), getNodesResponse.getNodesList().getFirst().getPort());
  }

  @Test
  public void replicaConnectivity() throws IOException, InterruptedException {
    initServerSyncInitialNrtPointFalse();

    // search on replica: no documents!
    replicaServer.verifySimpleDocIds("test_index");

    // index 4 documents to primary
    primaryServer.addSimpleDocs("test_index", 1, 2, 3, 4);
    // publish new NRT point (retrieve the current searcher version on primary)
    primaryServer.refresh("test_index");

    // search on replica: 4 documents!
    replicaServer.waitForReplication("test_index");
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4);
  }

  @Test
  public void testSyncOnIndexStart() throws IOException, InterruptedException {
    initServerSyncInitialNrtPointFalse();

    // Stop replica so it does not get nrt point from indexing
    replicaServer.stopIndex("test_index");

    // index 4 documents to primary
    primaryServer.addSimpleDocs("test_index", 1, 2, 3, 4);
    // publish new NRT point (retrieve the current searcher version on primary)
    primaryServer.refresh("test_index");

    // startIndex replica
    replicaServer.startReplicaIndex("test_index", -1, primaryServer.getReplicationPort(), null);
    // search on replica: no documents!
    replicaServer.verifySimpleDocIds("test_index");

    replicaServer
        .getGlobalState()
        .getIndexOrThrow("test_index")
        .getShard(0)
        .nrtReplicaNode
        .syncFromCurrentPrimary(120000, 300000);

    // search on replica: 4 documents!
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4);
  }

  @Test
  public void testInitialSyncMaxTime() throws IOException, InterruptedException {
    initServerSyncInitialNrtPointFalse();

    // Stop replica so it does not get nrt point from indexing
    replicaServer.stopIndex("test_index");

    // index 4 documents to primary
    primaryServer.addSimpleDocs("test_index", 1, 2, 3, 4);
    // publish new NRT point (retrieve the current searcher version on primary)
    primaryServer.refresh("test_index");

    // startIndex replica
    replicaServer.startReplicaIndex("test_index", -1, primaryServer.getReplicationPort(), null);
    // search on replica: no documents!
    replicaServer.verifySimpleDocIds("test_index");

    replicaServer
        .getGlobalState()
        .getIndexOrThrow("test_index")
        .getShard(0)
        .nrtReplicaNode
        .syncFromCurrentPrimary(120000, 0);

    // search on replica: still no documents
    replicaServer.verifySimpleDocIds("test_index");
  }

  @Test
  public void testInitialSyncTimeout() throws IOException {
    initServers("initialSyncPrimaryWaitMs: 1000");

    primaryServer.stopIndex("test_index");

    // search on replica: no documents!
    replicaServer.verifySimpleDocIds("test_index");

    long startTime = System.currentTimeMillis();
    replicaServer
        .getGlobalState()
        .getIndexOrThrow("test_index")
        .getShard(0)
        .nrtReplicaNode
        .syncFromCurrentPrimary(2000, 30000);
    long endTime = System.currentTimeMillis();
    assertTrue((endTime - startTime) > 1000);
  }

  @Test
  public void testInitialSyncWithCurrentVersion() throws IOException, InterruptedException {
    initServerSyncInitialNrtPointFalse();

    // Stop replica so it does not get nrt point from indexing
    replicaServer.stopIndex("test_index");

    // index 4 documents to primary
    primaryServer.addSimpleDocs("test_index", 1, 2, 3, 4);
    // publish new NRT point (retrieve the current searcher version on primary)
    primaryServer.refresh("test_index");

    // startIndex replica
    replicaServer.startReplicaIndex("test_index", -1, primaryServer.getReplicationPort(), null);
    // search on replica: no documents!
    replicaServer.verifySimpleDocIds("test_index");

    replicaServer
        .getGlobalState()
        .getIndexOrThrow("test_index")
        .getShard(0)
        .nrtReplicaNode
        .syncFromCurrentPrimary(120000, 300000);

    // sync again after we already have the current version
    replicaServer
        .getGlobalState()
        .getIndexOrThrow("test_index")
        .getShard(0)
        .nrtReplicaNode
        .syncFromCurrentPrimary(120000, 300000);

    // search on replica: 4 documents!
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4);
  }

  @Test
  public void testAddDocumentsOnReplicaFailure() throws IOException, InterruptedException {
    initDefaultServer();

    try {
      replicaServer.addSimpleDocs("test_index", 1, 2);
      fail();
    } catch (RuntimeException e) {
      assertTrue(
          e.getMessage()
              .contains("Adding documents to an index on a replica node is not supported"));
    }
  }

  @Test
  public void testRemoteCommitAndRestore() throws IOException, InterruptedException {
    // Start primary with remote commit enabled
    primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.REMOTE)
            .withAdditionalConfig("remoteCommit: true")
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    // Index 4 documents to primary
    primaryServer.addSimpleDocs("test_index", 1, 2, 3, 4);

    // Refresh and commit to S3
    primaryServer.refresh("test_index");
    primaryServer.commit("test_index");

    // Wait for S3 upload to complete
    Thread.sleep(2000);

    // Verify primary can search the documents
    primaryServer.verifySimpleDocIds("test_index", 1, 2, 3, 4);

    // Start replica in remote-only mode (reads from S3 without downloading files)
    replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true,
                Mode.REPLICA,
                primaryServer.getReplicationPort(),
                IndexStartConfig.IndexDataLocationType.REMOTE)
            .withAdditionalConfig("remoteOnlyIndex: true\nsyncInitialNrtPoint: false")
            .build();

    // Register replica with primary
    replicaServer.registerWithPrimary("test_index");

    // Give replica time to start up and restore from S3
    Thread.sleep(1000);

    // Refresh replica to ensure searcher sees the data
    replicaServer.refresh("test_index");

    // Verify replica can search documents restored from S3
    replicaServer.verifySimpleDocIds("test_index", 1, 2, 3, 4);

    // Verify that data files are NOT downloaded locally in remote-only mode
    // The replica should only have the segments_* file locally, all other files should be in S3
    String configuredIndexDir = replicaServer.getGlobalState().getConfiguration().getIndexDir();

    // Try to find the actual index directory - it may have a timestamp suffix
    Path indexBaseDir = Paths.get(configuredIndexDir);
    Path replicaIndexDir = null;

    if (Files.exists(indexBaseDir)) {
      List<Path> testIndexDirs =
          Files.list(indexBaseDir)
              .filter(p -> p.getFileName().toString().startsWith("test_index"))
              .toList();

      if (!testIndexDirs.isEmpty()) {
        replicaIndexDir = testIndexDirs.getFirst().resolve("shard0").resolve("index");
      }
    }

    if (replicaIndexDir != null && Files.exists(replicaIndexDir)) {
      try (Stream<Path> stream = Files.list(replicaIndexDir)) {
        List<Path> dataFiles =
            stream
                .filter(
                    p -> {
                      String name = p.getFileName().toString();
                      // Segments file and write.lock are OK, but .cfs, .cfe, .si should NOT exist
                      return !name.startsWith("segments_")
                          && !name.equals("write.lock")
                          && (name.endsWith(".cfs")
                              || name.endsWith(".cfe")
                              || name.endsWith(".si")
                              || name.endsWith(".fdt")
                              || name.endsWith(".fdx")
                              || name.endsWith(".fnm"));
                    })
                .toList();

        assertEquals("Replica should not have data files in remote-only mode", 0, dataFiles.size());
      }
    } else {
      fail("Could not find replica index directory to verify remote-only mode");
    }
  }

  private void initDefaultServer() throws IOException {
    initServers("");
  }

  private void initServerSyncInitialNrtPointFalse() throws IOException {
    initServers("syncInitialNrtPoint: false");
  }

  private void initServers(String extraConfig) throws IOException {
    primaryServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.PRIMARY, 0, IndexStartConfig.IndexDataLocationType.REMOTE)
            .withAdditionalConfig(extraConfig)
            .build();
    primaryServer.createSimpleIndex("test_index");
    primaryServer.startPrimaryIndex("test_index", -1, null);

    replicaServer =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true,
                Mode.REPLICA,
                primaryServer.getReplicationPort(),
                IndexStartConfig.IndexDataLocationType.REMOTE)
            .withAdditionalConfig(extraConfig)
            .build();
    replicaServer.registerWithPrimary("test_index");
  }
}
