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
import static com.yelp.nrtsearch.server.grpc.LuceneServerTest.RETRIEVED_VALUES;
import static com.yelp.nrtsearch.server.grpc.LuceneServerTest.checkHits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.amazonaws.services.s3.AmazonS3;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.ArchiverImpl;
import com.yelp.nrtsearch.server.backup.Tar;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReplicationTestFailureScenarios {
  public static final String TEST_INDEX = "test_index";
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains stateDir are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(BUCKET_NAME);

  private GrpcServer luceneServerPrimary;
  private GrpcServer replicationServerPrimary;

  private GrpcServer luceneServerSecondary;
  private GrpcServer replicationServerSecondary;

  private static final String BUCKET_NAME = "archiver-unittest";
  private Archiver archiver;
  private AmazonS3 s3;
  private Path archiverDirectory;

  @After
  public void tearDown() throws IOException {
    luceneServerPrimary.getGlobalState().close();
    luceneServerSecondary.getGlobalState().close();
    rmDir(Paths.get(luceneServerPrimary.getIndexDir()).getParent());
    rmDir(Paths.get(luceneServerSecondary.getIndexDir()).getParent());
  }

  @Before
  public void setUp() throws IOException {
    // setup S3 for backup/restore
    archiverDirectory = folder.newFolder("archiver").toPath();
    s3 = s3Provider.getAmazonS3();
    archiver =
        new ArchiverImpl(s3, BUCKET_NAME, archiverDirectory, new TarImpl(Tar.CompressionMode.LZ4));

    startPrimaryServer();
    startSecondaryServer();
  }

  public void startPrimaryServer() throws IOException {
    LuceneServerConfiguration luceneServerPrimaryConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.PRIMARY, folder.getRoot());
    luceneServerPrimary =
        new GrpcServer(
            grpcCleanup,
            luceneServerPrimaryConfiguration,
            folder,
            null,
            luceneServerPrimaryConfiguration.getIndexDir(),
            TEST_INDEX,
            luceneServerPrimaryConfiguration.getPort(),
            archiver);
    replicationServerPrimary =
        new GrpcServer(
            grpcCleanup,
            luceneServerPrimaryConfiguration,
            folder,
            luceneServerPrimary.getGlobalState(),
            luceneServerPrimaryConfiguration.getIndexDir(),
            TEST_INDEX,
            9001,
            archiver);
    luceneServerPrimary.getGlobalState().replicationStarted(9001);
  }

  public void startSecondaryServer() throws IOException {
    LuceneServerConfiguration luceneSecondaryConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.REPLICA, folder.getRoot());

    luceneServerSecondary =
        new GrpcServer(
            grpcCleanup,
            luceneSecondaryConfiguration,
            folder,
            null,
            luceneSecondaryConfiguration.getIndexDir(),
            TEST_INDEX,
            luceneSecondaryConfiguration.getPort(),
            archiver);
    replicationServerSecondary =
        new GrpcServer(
            grpcCleanup,
            luceneSecondaryConfiguration,
            folder,
            luceneServerSecondary.getGlobalState(),
            luceneSecondaryConfiguration.getIndexDir(),
            TEST_INDEX,
            luceneSecondaryConfiguration.getReplicationPort(),
            archiver);
    luceneServerSecondary
        .getGlobalState()
        .replicationStarted(luceneSecondaryConfiguration.getReplicationPort());
  }

  public void shutdownPrimaryServer() throws IOException {
    luceneServerPrimary.getGlobalState().close();
    rmDir(Paths.get(luceneServerPrimary.getIndexDir()).getParent());
    luceneServerPrimary.shutdown();
    replicationServerPrimary.shutdown();
  }

  public void shutdownSecondaryServer() throws IOException {
    luceneServerSecondary.getGlobalState().close();
    rmDir(Paths.get(luceneServerSecondary.getIndexDir()).getParent());
    luceneServerSecondary.shutdown();
    replicationServerSecondary.shutdown();
  }

  @Test
  public void replicaDownedWhenPrimaryIndexing() throws IOException, InterruptedException {
    // startIndex Primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    // startIndex replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);

    // add 2 docs to primary
    testServerPrimary.addDocuments();

    // backup index
    backupIndex();

    // refresh (also sends NRTPoint to replicas)
    luceneServerPrimary
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    // stop replica instance
    shutdownSecondaryServer();

    // add 2 docs to primary
    testServerPrimary.addDocuments();

    // re-start replica instance from a fresh index state i.e. empty index dir
    startSecondaryServer();
    // startIndex replica
    testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA, 0, true);

    // add 2 more docs (6 total now), annoying, sendNRTPoint gets called from primary only upon a
    // flush i.e. an index operation
    testServerPrimary.addDocuments();

    // publish new NRT point (retrieve the current searcher version on primary)
    publishNRTAndValidateSearchResults(6);
  }

  @Test
  public void primaryStoppedAndRestartedWithNoPreviousIndex()
      throws IOException, InterruptedException {
    // startIndex Primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    // startIndex replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
    // add 2 docs to primary
    testServerPrimary.addDocuments();
    // add 2 docs to primary
    testServerPrimary.addDocuments();
    // both primary and replica should have 4 docs
    publishNRTAndValidateSearchResults(4);
    // backup primary
    backupIndex();
    // commit secondary state
    luceneServerSecondary
        .getBlockingStub()
        .commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());

    // non-graceful primary shutdown (i.e. blow away index directory)
    shutdownPrimaryServer();
    // start primary again with new empty index directory
    startPrimaryServer();
    // startIndex Primary with primaryGen = 1
    testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1);
    // stop and restart secondary to connect to "new" primary
    gracefullRestartSecondary();
    // add 2 docs to primary
    testServerPrimary.addDocuments();
    // both primary and secondary have only 2 docs now (replica has fewer docs than before as
    // expected)
    publishNRTAndValidateSearchResults(2);
  }

  @Test
  public void primaryStoppedAndRestartedWithPreviousLocalIndex()
      throws IOException, InterruptedException {
    // startIndex primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    // startIndex replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
    // add 2 docs to primary
    testServerPrimary.addDocuments();
    // both primary and replica should have 2 docs
    publishNRTAndValidateSearchResults(2);
    // commit primary
    luceneServerPrimary
        .getBlockingStub()
        .commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());
    // commit metadata state for secondary
    luceneServerSecondary
        .getBlockingStub()
        .commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());

    // gracefully stop and restart primary
    gracefullRestartPrimary(1);
    // stop and restart secondary to connect to "new" primary
    gracefullRestartSecondary();
    // add 2 docs to primary
    testServerPrimary.addDocuments();
    publishNRTAndValidateSearchResults(4);
  }

  @Test
  public void primaryDurabilityBasic() throws IOException, InterruptedException {
    // startIndex primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    // add 2 docs to primary
    testServerPrimary.addDocuments();
    // backup index
    backupIndex();
    // non-graceful primary shutdown (i.e. blow away index and state directory)
    shutdownPrimaryServer();
    // start primary again with latest commit point
    startPrimaryServer();
    // startIndex Primary with primaryGen = 1
    testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1, true);
    // add 2 docs to primary
    testServerPrimary.addDocuments();

    // publish new NRT point (retrieve the current searcher version on primary)
    SearcherVersion searcherVersionPrimary =
        replicationServerPrimary
            .getReplicationServerBlockingStub()
            .writeNRTPoint(IndexName.newBuilder().setIndexName(TEST_INDEX).build());

    // primary should show numDocs hits now
    SearchResponse searchResponsePrimary =
        luceneServerPrimary
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(luceneServerPrimary.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .setVersion(searcherVersionPrimary.getVersion())
                    .addAllRetrieveFields(RETRIEVED_VALUES)
                    .build());

    validateSearchResults(4, searchResponsePrimary);
  }

  @Test
  public void primaryDurabilitySyncWithReplica() throws IOException, InterruptedException {
    // startIndex primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    // startIndex replica
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(luceneServerSecondary, true, Mode.REPLICA);
    // add 2 docs to primary
    testServerPrimary.addDocuments();
    // both primary and replica should have 2 docs
    publishNRTAndValidateSearchResults(2);

    // backupIndex (with 2 docs)
    backupIndex();
    // commit metadata state for secondary, since we wil reuse this upon startIndex
    luceneServerSecondary
        .getBlockingStub()
        .commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());

    // add 6 more docs to primary but dont commit, NRT is at 8 docs but commit point at 2 docs
    testServerPrimary.addDocuments();
    testServerPrimary.addDocuments();
    testServerPrimary.addDocuments();
    publishNRTAndValidateSearchResults(8);

    // non-graceful primary shutdown (i.e. blow away index directory and stateDir)
    shutdownPrimaryServer();

    // start primary again, download data from latest commit point
    startPrimaryServer();
    // startIndex Primary with primaryGen = 1
    testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1, true);
    // stop and restart secondary to connect to "new" primary,
    gracefullRestartSecondary();
    // add 2 more docs to primary
    testServerPrimary.addDocuments();
    publishNRTAndValidateSearchResults(4);
  }

  @Test
  public void primaryDurabilityWithMultipleCommits() throws IOException, InterruptedException {
    // startIndex primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);
    for (int i = 0; i < 2; i++) {
      // add 4 docs to primary
      testServerPrimary.addDocuments();
      testServerPrimary.addDocuments();
      backupIndex();
    }

    // non-graceful primary shutdown (i.e. blow away index directory and stateDir)
    shutdownPrimaryServer();

    // start primary again, download data from latest commit point
    startPrimaryServer();
    // startIndex Primary with primaryGen = 1
    testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1, true);

    // add 2 docs to primary
    testServerPrimary.addDocuments();

    // publish new NRT point (retrieve the current searcher version on primary)
    SearcherVersion searcherVersionPrimary =
        replicationServerPrimary
            .getReplicationServerBlockingStub()
            .writeNRTPoint(IndexName.newBuilder().setIndexName(TEST_INDEX).build());

    // primary should show numDocs hits now
    SearchResponse searchResponsePrimary =
        luceneServerPrimary
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(luceneServerPrimary.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .setVersion(searcherVersionPrimary.getVersion())
                    .addAllRetrieveFields(RETRIEVED_VALUES)
                    .build());

    validateSearchResults(10, searchResponsePrimary);
  }

  private void gracefullRestartSecondary() {
    luceneServerSecondary
        .getBlockingStub()
        .stopIndex(StopIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    luceneServerSecondary
        .getBlockingStub()
        .startIndex(
            StartIndexRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setMode(Mode.REPLICA)
                .setPrimaryAddress("localhost")
                .setPort(9001) // primary port for replication server
                .build());
  }

  @Test
  public void testPrimaryEphemeralIdChanges() throws IOException, InterruptedException {
    // startIndex primary
    GrpcServer.TestServer testServerPrimary =
        new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY);

    // add docs to primary and check ephemeral id
    AddDocumentResponse response = testServerPrimary.addDocuments();
    String firstId = response.getPrimaryId();
    response = testServerPrimary.addDocuments();
    assertEquals(firstId, response.getPrimaryId());

    // backup index
    backupIndex();
    // non-graceful primary shutdown (i.e. blow away index and state directory)
    shutdownPrimaryServer();
    // start primary again with latest commit point
    startPrimaryServer();
    // startIndex Primary with primaryGen = 1
    testServerPrimary = new GrpcServer.TestServer(luceneServerPrimary, true, Mode.PRIMARY, 1, true);

    // add docs to primary and check ephemeral id
    response = testServerPrimary.addDocuments();
    String secondId = response.getPrimaryId();
    assertNotEquals(firstId, secondId);
    response = testServerPrimary.addDocuments();
    assertEquals(secondId, response.getPrimaryId());
  }

  private void gracefullRestartPrimary(int primaryGen) {
    luceneServerPrimary
        .getBlockingStub()
        .stopIndex(StopIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    luceneServerPrimary
        .getBlockingStub()
        .startIndex(
            StartIndexRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .setMode(Mode.PRIMARY)
                .setPrimaryGen(primaryGen)
                .build());
  }

  private void publishNRTAndValidateSearchResults(int numDocs) {
    // publish new NRT point (retrieve the current searcher version on primary)
    SearcherVersion searcherVersionPrimary =
        replicationServerPrimary
            .getReplicationServerBlockingStub()
            .writeNRTPoint(IndexName.newBuilder().setIndexName(TEST_INDEX).build());
    // primary should show numDocs hits now
    SearchResponse searchResponsePrimary =
        luceneServerPrimary
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(luceneServerPrimary.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .setVersion(searcherVersionPrimary.getVersion())
                    .addAllRetrieveFields(RETRIEVED_VALUES)
                    .build());
    // replica should also have numDocs hits
    SearchResponse searchResponseSecondary =
        luceneServerSecondary
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(luceneServerSecondary.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .setVersion(searcherVersionPrimary.getVersion())
                    .addAllRetrieveFields(RETRIEVED_VALUES)
                    .build());

    validateSearchResults(numDocs, searchResponsePrimary);
    validateSearchResults(numDocs, searchResponseSecondary);
  }

  public static void validateSearchResults(int numHitsExpected, SearchResponse searchResponse) {
    assertEquals(numHitsExpected, searchResponse.getTotalHits().getValue());
    assertEquals(numHitsExpected, searchResponse.getHitsList().size());
    SearchResponse.Hit firstHit = searchResponse.getHits(0);
    checkHits(firstHit);
    SearchResponse.Hit secondHit = searchResponse.getHits(1);
    checkHits(secondHit);
  }

  private void backupIndex() {
    luceneServerPrimary
        .getBlockingStub()
        .backupIndex(
            BackupIndexRequest.newBuilder()
                .setIndexName("test_index")
                .setServiceName("testservice")
                .setResourceName("testresource")
                .build());
  }
}
