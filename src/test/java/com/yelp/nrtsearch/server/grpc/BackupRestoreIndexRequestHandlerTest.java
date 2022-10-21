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
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.collect.ImmutableList;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.ArchiverImpl;
import com.yelp.nrtsearch.server.backup.Tar;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import io.findify.s3mock.S3Mock;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
    return new GrpcServer(
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        null,
        luceneServerConfiguration.getIndexDir(),
        "test_index",
        luceneServerConfiguration.getPort(),
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
  public void testBackupRequestHandlerUpload_completeDirectoryBackup()
      throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex(true);
    Path downloadPath = archiver.download("testservice", "testresource_data");
    List<String> actual = getFiles(downloadPath);
    List<String> expected = getFiles(Paths.get(grpcServer.getIndexDir()));
    assertEquals(expected, actual);
  }

  @Test
  public void testBackupRequestHandlerUpload() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex(false);
    Path downloadPath = archiver.download("testservice", "testresource_data");
    List<String> actual = getFiles(downloadPath);
    List<String> expected = getFiles(Paths.get(grpcServer.getIndexDir()));
    // There are two write.lock files, one under ../test_index/shard0/taxonomy and another one under
    // ../test_index/shard0/index
    expected.remove("write.lock");
    expected.remove("write.lock");
    // The empty index is committed on first start, resulting in segments file that is not in the
    // latest index version
    expected.remove("segments_1");

    assertEquals(expected, actual);
  }

  @Test
  public void testRestoreHandler_completeDirectoryBackup()
      throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex(true);
    testAddDocs.addDocuments();

    restartIndexWithRestoreAndVerify(true, false);
  }

  @Test
  public void testRestoreHandler_deleteExistingDataAndRestoreIndexWithoutDeleteExistingDataOption()
      throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex(false);
    testAddDocs.addDocuments();

    restartIndexWithRestoreAndVerify(true, false);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testRestoreHandler_retainExistingDataAndRestoreIndex()
      throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex(false);
    testAddDocs.addDocuments();

    try {
      restartIndexWithRestoreAndVerify(false, false);
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Cannot restore, directory has index data file:"));
      throw e;
    }
  }

  @Test
  public void testRestoreHandler_retainExistingDataAndRestoreIndexWithDeleteExistingDataOption()
      throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex(false);
    testAddDocs.addDocuments();

    restartIndexWithRestoreAndVerify(false, true);
  }

  @Test
  public void testRestoreHandler_deleteExistingDataAndRestoreIndexWithDeleteExistingDataOption()
      throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex(false);
    testAddDocs.addDocuments();

    restartIndexWithRestoreAndVerify(true, true);
  }

  /**
   * When a backup is downloaded we just point the index directory the downloaded files. This test
   * verifies that if deleteExistingData is set during restore it deletes the directories from the
   * backup as well.
   */
  @Test
  public void
      testRestoreHandler_indexInitiallyStartedFromBackup_deleteExistingDataAndRestoreIndexWithDeleteExistingDataOption()
          throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex(false);
    testAddDocs.addDocuments();

    restartIndexWithRestoreAndVerify(true, true);

    restartIndexWithRestoreAndVerify(true, true);
  }

  @Test(expected = StatusRuntimeException.class)
  public void testRestoreHandler_throwErrorIfIndexStarted()
      throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    testAddDocs.addDocuments();

    backupIndex(false);
    testAddDocs.addDocuments();

    try {
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
                          .setDeleteExistingData(true)
                          .build())
                  .build());
    } catch (StatusRuntimeException e) {
      assertEquals(
          "INVALID_ARGUMENT: error while trying to start index: test_index\nIndex test_index is already started",
          e.getMessage());
      throw e;
    }
  }

  private void restartIndexWithRestoreAndVerify(
      boolean deleteIndexDataBeforeRestore, boolean setDeleteExistingDataInRestoreRequest)
      throws IOException {
    grpcServer
        .getBlockingStub()
        .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());

    if (deleteIndexDataBeforeRestore) {
      deleteIndexAndMetadata();
    }

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
                        .setDeleteExistingData(setDeleteExistingDataInRestoreRequest)
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

  /**
   * As compared to testRestoreHandler, this test does not add more documents after a backup and
   * restarts index in a different way.
   */
  @Test
  public void testSnapshotRestore() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // Backup Index
    backupIndex(false);
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
    try (DirectoryStream<Path> stream =
        Files.newDirectoryStream(Path.of(grpcServer.getIndexDir()))) {
      for (Path path : stream) {
        try (DirectoryStream<Path> indexStream = Files.newDirectoryStream(path)) {
          for (Path indexPath : indexStream) {
            if (Files.isDirectory(indexPath)) {
              rmDir(indexPath);
            } else {
              Files.delete(indexPath);
            }
          }
        }
      }
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

  private void backupIndex(boolean completeDirectory) {
    grpcServer
        .getBlockingStub()
        .backupIndex(
            BackupIndexRequest.newBuilder()
                .setIndexName("test_index")
                .setServiceName("testservice")
                .setResourceName("testresource")
                .setCompleteDirectory(completeDirectory)
                .build());
  }
}
