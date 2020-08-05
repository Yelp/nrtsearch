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

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.collect.ImmutableList;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.ArchiverImpl;
import com.yelp.nrtsearch.server.utils.Tar;
import com.yelp.nrtsearch.server.utils.TarImpl;
import io.findify.s3mock.S3Mock;
import io.grpc.testing.GrpcCleanupRule;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.iq80.leveldb.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BackupRestoreIndexRequestHandlerTest {
  private final String BUCKET_NAME = "archiver-unittest";
  private Archiver archiver;
  private S3Mock api;
  private AmazonS3 s3;
  private Path s3Directory;
  private Path archiverDirectory;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private GrpcServer grpcServer;

  @Before
  public void setup() throws IOException {
    s3Directory = folder.newFolder("s3").toPath();
    archiverDirectory = folder.newFolder("archiver").toPath();
    api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
    api.start();
    s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint("http://127.0.0.1:8011");
    s3.createBucket(BUCKET_NAME);
    archiver =
        new ArchiverImpl(s3, BUCKET_NAME, archiverDirectory, new TarImpl(Tar.CompressionMode.LZ4));
    grpcServer = setUpGrpcServer();
  }

  private GrpcServer setUpGrpcServer() throws IOException {
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    GlobalState globalState = new GlobalState(luceneServerConfiguration);
    return new GrpcServer(
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        false,
        globalState,
        luceneServerConfiguration.getIndexDir(),
        "test_index",
        globalState.getPort(),
        archiver);
  }

  @After
  public void teardown() throws IOException {
    api.shutdown();
    tearDownGrpcServer();
  }

  private void tearDownGrpcServer() throws IOException {
    grpcServer.getGlobalState().close();
    grpcServer.shutdown();
    rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
  }

  @Test
  public void testBackupRequestHandlerUpload() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex();
    Path downloadPath = archiver.download("testservice", "testresource_data");
    List<String> actual = getFiles(downloadPath);
    List<String> expected = getFiles(Paths.get(grpcServer.getIndexDir()));
    assertEquals(expected, actual);
  }

  @Test
  public void testRestoreHandler() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex();
    testAddDocs.addDocuments();

    grpcServer
        .getBlockingStub()
        .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());

    deleteIndexAndMetadata();

    grpcServer
        .getBlockingStub()
        .startIndex(
            StartIndexRequest.newBuilder()
                .setIndexName("test_index")
                .setMode(Mode.STANDALONE)
                .setRestore(
                    RestoreIndex.newBuilder()
                        .setServiceName("testservice")
                        .setResourceName("testresource")
                        .build())
                .build());

    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(RETRIEVED_VALUES)
                    .build());

    assertEquals(2, searchResponse.getTotalHits().getValue());
    assertEquals(2, searchResponse.getHitsList().size());
    SearchResponse.Hit firstHit = searchResponse.getHits(0);
    checkHits(firstHit);
    SearchResponse.Hit secondHit = searchResponse.getHits(1);
    checkHits(secondHit);
  }

  @Test
  public void testSnapshotRestore() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // Backup Index
    backupIndex();
    // stop server and remove data and state files.
    tearDownGrpcServer();
    // restart server and Index
    grpcServer = setUpGrpcServer();
    new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE, 0, true);
    // search
    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(RETRIEVED_VALUES)
                    .build());
    assertEquals(2, searchResponse.getTotalHits().getValue());
    assertEquals(2, searchResponse.getHitsList().size());
    SearchResponse.Hit firstHit = searchResponse.getHits(0);
    checkHits(firstHit);
    SearchResponse.Hit secondHit = searchResponse.getHits(1);
    checkHits(secondHit);
  }

  private void deleteIndexAndMetadata() throws IOException {
    for (Path path :
        Arrays.asList(
            grpcServer.getGlobalState().getStateDir(), Paths.get(grpcServer.getIndexDir()))) {
      rmDir(path);
    }
  }

  public List<String> getFiles(Path basePath) {
    List<String> result = new ArrayList<>();
    ImmutableList<File> childFiles = FileUtils.listFiles(basePath.toFile());
    for (File childFile : childFiles) {
      if (Files.isDirectory(childFile.toPath())) {
        result.addAll(getFiles(childFile.toPath()));
      } else if (Files.isRegularFile(childFile.toPath())) {
        result.add(childFile.getName());
      }
    }
    return result.stream()
        // the versions are bumped post a ReleaseSnapshot hence wont be same as snapshotted backed
        // up version
        // which will be on version less
        .filter(x -> !x.startsWith("snapshots") && !x.startsWith("stateRefCounts"))
        .collect(Collectors.toList());
  }

  private void backupIndex() {
    grpcServer
        .getBlockingStub()
        .backupIndex(
            BackupIndexRequest.newBuilder()
                .setIndexName("test_index")
                .setServiceName("testservice")
                .setResourceName("testresource")
                .build());
  }
}
