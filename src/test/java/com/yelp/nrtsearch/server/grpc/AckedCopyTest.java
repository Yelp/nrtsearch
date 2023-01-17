/*
 * Copyright 2021 Yelp Inc.
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
import static com.yelp.nrtsearch.server.grpc.ReplicationServerTest.validateSearchResults;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.ArchiverImpl;
import com.yelp.nrtsearch.server.backup.Tar;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import io.findify.s3mock.S3Mock;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AckedCopyTest {
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

  public void setUp(int chunkSize, int ackEvery, int maxInFlight) throws IOException {
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

    String extraConfig =
        String.join(
            "\n",
            "FileCopyConfig:",
            "  ackedCopy: true",
            "  chunkSize: " + chunkSize,
            "  ackEvery: " + ackEvery,
            "  maxInFlight: " + maxInFlight);

    // set up primary servers
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerPrimaryConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.PRIMARY, folder.getRoot(), extraConfig);
    luceneServerPrimary =
        new GrpcServer(
            grpcCleanup,
            luceneServerPrimaryConfiguration,
            folder,
            null,
            luceneServerPrimaryConfiguration.getIndexDir(),
            testIndex,
            luceneServerPrimaryConfiguration.getPort(),
            archiver);
    replicationServerPrimary =
        new GrpcServer(
            grpcCleanup,
            luceneServerPrimaryConfiguration,
            folder,
            luceneServerPrimary.getGlobalState(),
            luceneServerPrimaryConfiguration.getIndexDir(),
            testIndex,
            luceneServerPrimaryConfiguration.getReplicationPort(),
            archiver);
    luceneServerPrimary
        .getGlobalState()
        .replicationStarted(luceneServerPrimaryConfiguration.getReplicationPort());
    // set up secondary servers
    LuceneServerConfiguration luceneServerSecondaryConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.REPLICA, folder.getRoot(), extraConfig);

    luceneServerSecondary =
        new GrpcServer(
            grpcCleanup,
            luceneServerSecondaryConfiguration,
            folder,
            null,
            luceneServerSecondaryConfiguration.getIndexDir(),
            testIndex,
            luceneServerSecondaryConfiguration.getPort(),
            archiver);
    replicationServerSecondary =
        new GrpcServer(
            grpcCleanup,
            luceneServerSecondaryConfiguration,
            folder,
            luceneServerSecondary.getGlobalState(),
            luceneServerSecondaryConfiguration.getIndexDir(),
            testIndex,
            luceneServerSecondaryConfiguration.getReplicationPort(),
            archiver);
    luceneServerSecondary
        .getGlobalState()
        .replicationStarted(luceneServerSecondaryConfiguration.getReplicationPort());
  }

  @Test
  public void ackAllLimit1() throws IOException, InterruptedException {
    setUp(2, 1, 1);
    testReplication();
  }

  @Test
  public void ack2Limit2() throws IOException, InterruptedException {
    setUp(2, 2, 2);
    testReplication();
  }

  @Test
  public void ack2Limit4() throws IOException, InterruptedException {
    setUp(2, 2, 4);
    testReplication();
  }

  @Test
  public void ack2Limit2LargeChunk() throws IOException, InterruptedException {
    setUp(1024, 2, 2);
    testReplication();
  }

  private void testReplication() throws IOException, InterruptedException {
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
}
