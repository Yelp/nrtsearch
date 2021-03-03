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

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static com.yelp.nrtsearch.server.grpc.ReplicationServerClient.BINARY_MAGIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.ArchiverImpl;
import com.yelp.nrtsearch.server.utils.Tar;
import com.yelp.nrtsearch.server.utils.TarImpl;
import io.findify.s3mock.S3Mock;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
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

  private GrpcServer luceneServerPrimary;
  private GrpcServer replicationServerPrimary;

  private GrpcServer luceneServerSecondary;
  private GrpcServer replicationServerSecondary;

  private final String BUCKET_NAME = "archiver-unittest";
  private Archiver archiver;
  private S3Mock api;
  private AmazonS3 s3;
  private Path s3Directory;
  private Path archiverDirectory;

  @After
  public void tearDown() throws IOException {
    api.shutdown();
    luceneServerPrimary.getGlobalState().close();
    luceneServerSecondary.getGlobalState().close();
    rmDir(Paths.get(luceneServerPrimary.getIndexDir()).getParent());
    rmDir(Paths.get(luceneServerSecondary.getIndexDir()).getParent());
  }

  @Before
  public void setUp() throws IOException {
    // setup S3 for backup/restore
    s3Directory = folder.newFolder("s3").toPath();
    archiverDirectory = folder.newFolder("archiver").toPath();
    api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
    api.start();
    s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint("http://127.0.0.1:8011");
    s3.createBucket(BUCKET_NAME);
    archiver =
        new ArchiverImpl(s3, BUCKET_NAME, archiverDirectory, new TarImpl(Tar.CompressionMode.LZ4));

    // set up primary servers
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerPrimaryConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.PRIMARY, folder.getRoot());
    GlobalState globalStatePrimary = new GlobalState(luceneServerPrimaryConfiguration);
    luceneServerPrimary =
        new GrpcServer(
            grpcCleanup,
            luceneServerPrimaryConfiguration,
            folder,
            false,
            globalStatePrimary,
            luceneServerPrimaryConfiguration.getIndexDir(),
            testIndex,
            globalStatePrimary.getPort(),
            archiver);
    replicationServerPrimary =
        new GrpcServer(
            grpcCleanup,
            luceneServerPrimaryConfiguration,
            folder,
            true,
            globalStatePrimary,
            luceneServerPrimaryConfiguration.getIndexDir(),
            testIndex,
            luceneServerPrimaryConfiguration.getReplicationPort(),
            archiver);
    // set up secondary servers
    LuceneServerConfiguration luceneServerSecondaryConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.REPLICA, folder.getRoot());
    GlobalState globalStateSecondary = new GlobalState(luceneServerSecondaryConfiguration);

    luceneServerSecondary =
        new GrpcServer(
            grpcCleanup,
            luceneServerSecondaryConfiguration,
            folder,
            false,
            globalStateSecondary,
            luceneServerSecondaryConfiguration.getIndexDir(),
            testIndex,
            globalStateSecondary.getPort(),
            archiver);
    replicationServerSecondary =
        new GrpcServer(
            grpcCleanup,
            luceneServerSecondaryConfiguration,
            folder,
            true,
            globalStateSecondary,
            luceneServerSecondaryConfiguration.getIndexDir(),
            testIndex,
            globalStateSecondary.getReplicationPort(),
            archiver);
  }

  @Test
  public void recvCopyState() throws IOException, InterruptedException {
    GrpcServer.TestServer testServer =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    testServer.addDocuments();
    assertEquals(false, testServer.error);
    assertEquals(true, testServer.completed);

    // This causes the copyState on primary to be refreshed
    luceneServerPrimary
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());

    CopyStateRequest copyStateRequest =
        CopyStateRequest.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName(replicationServerPrimary.getTestIndex())
            .setReplicaId(0)
            .build();
    CopyState copyState =
        replicationServerPrimary.getReplicationServerBlockingStub().recvCopyState(copyStateRequest);
    assertEquals(0, copyState.getGen());
    FilesMetadata filesMetadata = copyState.getFilesMetadata();
    assertEquals(3, filesMetadata.getNumFiles());
  }

  @Test
  public void copyFiles() throws IOException, InterruptedException {
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    testServerPrimary.addDocuments();

    // This causes the copyState on primary to be refreshed
    luceneServerPrimary
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());

    // capture the copy state on primary (client node in this test case)
    CopyStateRequest copyStateRequest =
        CopyStateRequest.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName(replicationServerPrimary.getTestIndex())
            .setReplicaId(0)
            .build();
    CopyState copyState =
        replicationServerPrimary.getReplicationServerBlockingStub().recvCopyState(copyStateRequest);
    assertEquals(0, copyState.getGen());
    FilesMetadata filesMetadata = copyState.getFilesMetadata();
    assertEquals(3, filesMetadata.getNumFiles());

    // send the file metadata info to replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
    CopyFiles.Builder requestBuilder =
        CopyFiles.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName("test_index")
            .setPrimaryGen(0);
    requestBuilder.setFilesMetadata(filesMetadata);

    Iterator<TransferStatus> transferStatusIterator =
        replicationServerSecondary
            .getReplicationServerBlockingStub()
            .copyFiles(requestBuilder.build());
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
    // index 2 documents to primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    testServerPrimary.addDocuments();
    // refresh (also sends NRTPoint to replicas, but none started at this point)
    luceneServerPrimary
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());
    // startIndex replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
    // add 2 more docs to primary
    testServerPrimary.addDocuments();

    // publish new NRT point (retrieve the current searcher version on primary)
    SearcherVersion searcherVersionPrimary =
        replicationServerPrimary
            .getReplicationServerBlockingStub()
            .writeNRTPoint(IndexName.newBuilder().setIndexName("test_index").build());

    // primary should show 4 hits now
    SearchResponse searchResponsePrimary =
        luceneServerPrimary
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(luceneServerPrimary.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .setVersion(searcherVersionPrimary.getVersion())
                    .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
                    .build());

    // replica should too!
    SearchResponse searchResponseSecondary =
        luceneServerSecondary
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(luceneServerSecondary.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .setVersion(searcherVersionPrimary.getVersion())
                    .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
                    .build());

    validateSearchResults(searchResponsePrimary);
    validateSearchResults(searchResponseSecondary);
  }

  @Test
  public void getConnectedNodes() throws IOException, InterruptedException {
    // startIndex primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    // startIndex replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
    // primary should have registered replica in its connected nodes list
    GetNodesResponse getNodesResponse =
        replicationServerPrimary
            .getReplicationServerBlockingStub()
            .getConnectedNodes(GetNodesRequest.newBuilder().setIndexName("test_index").build());
    assertEquals(1, getNodesResponse.getNodesCount());
    assertEquals("localhost", getNodesResponse.getNodesList().get(0).getHostname());
    assertEquals(9003, getNodesResponse.getNodesList().get(0).getPort());
  }

  @Test
  public void replicaConnectivity() throws IOException, InterruptedException {
    // set ping interval to 10 ms
    luceneServerSecondary.getGlobalState().setReplicaReplicationPortPingInterval(10);
    // startIndex replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
    // search on replica: no documents!
    SearchResponse searchResponseSecondary =
        luceneServerSecondary
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(luceneServerSecondary.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
                    .build());
    assertEquals(0, searchResponseSecondary.getHitsCount());

    // index 4 documents to primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    testServerPrimary.addDocuments();
    testServerPrimary.addDocuments();
    // publish new NRT point (retrieve the current searcher version on primary)
    SearcherVersion searcherVersionPrimary =
        replicationServerPrimary
            .getReplicationServerBlockingStub()
            .writeNRTPoint(IndexName.newBuilder().setIndexName("test_index").build());

    // search on replica: 4 documents!
    searchResponseSecondary =
        luceneServerSecondary
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(luceneServerSecondary.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .setVersion(searcherVersionPrimary.getVersion())
                    .addAllRetrieveFields(LuceneServerTest.RETRIEVED_VALUES)
                    .build());
    validateSearchResults(searchResponseSecondary);
  }

  public static void validateSearchResults(SearchResponse searchResponse) {
    assertEquals(4, searchResponse.getTotalHits().getValue());
    assertEquals(4, searchResponse.getHitsList().size());
    SearchResponse.Hit firstHit = searchResponse.getHits(0);
    LuceneServerTest.checkHits(firstHit);
    SearchResponse.Hit secondHit = searchResponse.getHits(1);
    LuceneServerTest.checkHits(secondHit);
  }
}
