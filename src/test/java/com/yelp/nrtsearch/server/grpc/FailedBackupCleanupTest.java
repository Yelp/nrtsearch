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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.google.gson.Gson;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.ArchiverImpl;
import com.yelp.nrtsearch.server.backup.Tar;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FailedBackupCleanupTest {
  private final String BUCKET_NAME = "archiver-unittest";
  private Archiver archiver;
  private AmazonS3 s3;
  private Path archiverDirectory;
  private CountDownLatch s3TransferStartedLatch;
  private LuceneServerConfiguration luceneServerConfiguration;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private GrpcServer grpcServer;

  @Before
  public void setup() throws IOException {
    archiverDirectory = Paths.get(folder.getRoot().toString(), "archiver");
    s3TransferStartedLatch = new CountDownLatch(1);
    s3 = new DoNothingAndWaitAmazonS3(s3TransferStartedLatch);
    archiver =
        new ArchiverImpl(s3, BUCKET_NAME, archiverDirectory, new TarImpl(Tar.CompressionMode.LZ4));
    luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(
            Mode.PRIMARY, folder.getRoot(), archiverDirectory);
    grpcServer = setUpGrpcServer();
  }

  private GrpcServer setUpGrpcServer() throws IOException {
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
    grpcServer.getGlobalState().close();
    grpcServer.forceShutdown();
    rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
  }

  @Test(expected = StatusRuntimeException.class)
  public void testBackupWhileAnotherBackupRunning() throws IOException, InterruptedException {
    // Create index and add 2 documents
    GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.PRIMARY);
    testAddDocs.addDocuments();

    attemptBackupAsync();

    // This will wait until the server tries to upload the backup to S3, so that
    // a snapshot and the backup indicator file have been created
    s3TransferStartedLatch.await(10, TimeUnit.SECONDS);

    BackupIndexRequest request =
        BackupIndexRequest.newBuilder()
            .setIndexName(grpcServer.getTestIndex())
            .setServiceName("testservice")
            .setResourceName("testresource")
            .build();

    try {
      grpcServer.getBlockingStub().backupIndex(request);
    } catch (StatusRuntimeException e) {
      assertEquals(
          "UNKNOWN: error while trying to backupIndex for index test_index for service: testservice, resource: testresource\n"
              + "A backup is ongoing for index test_index, please try again after the current backup is finished",
          e.getMessage());
      throw e;
    }
  }

  @Test
  public void testFailedBackupCleanup() throws IOException, InterruptedException {
    // Create index and add 2 documents
    GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.PRIMARY);
    testAddDocs.addDocuments();

    // Verify that 2 documents were added
    SearchResponse searchResponse = search();
    assertEquals(2, searchResponse.getHitsCount());

    // Verify that no snapshots or tmp files are present
    List<Long> allSnapshotIndexGen = getSnapshotIndexGensList();
    assertTrue(allSnapshotIndexGen.isEmpty());

    attemptBackupAsync();

    // This will wait until the server tries to upload the backup to S3, so that
    // a snapshot and the backup indicator file have been created
    s3TransferStartedLatch.await(10, TimeUnit.SECONDS);

    // Verify that snapshot was created during backup
    allSnapshotIndexGen = getSnapshotIndexGensList();
    assertEquals(List.of(2L), allSnapshotIndexGen);

    forceShutdownServer();

    Path backupIndicatorFilePath = Paths.get(archiverDirectory.toString(), "backup.txt");

    // Verify that the backup indicator file was created
    assertTrue(Files.exists(backupIndicatorFilePath));

    String backupIndicatorFileContents = Files.readString(backupIndicatorFilePath);

    // Hacky way to assert exact file contents instead of asserting parsed json
    TestBackupIndicatorDetails expectedBackupDetails =
        new TestBackupIndicatorDetails(grpcServer.getTestIndex(), 2, -1, 0);
    TestBackupIndicatorDetails actualBackupDetails =
        new Gson().fromJson(backupIndicatorFileContents, TestBackupIndicatorDetails.class);
    assertEquals(expectedBackupDetails, actualBackupDetails);

    // Verify that the backup created a temporary file which wasn't cleaned up
    List<Path> tmpFilesInArchiverDirectory = getTmpFilesInArchiverDirectory();
    assertEquals(1, tmpFilesInArchiverDirectory.size());

    grpcServer = setUpGrpcServer();
    grpcServer
        .getBlockingStub()
        .startIndex(
            StartIndexRequest.newBuilder()
                .setIndexName(grpcServer.getTestIndex())
                .setMode(Mode.PRIMARY)
                .build());

    // Confirm that the previous index with the data was started
    searchResponse = search();
    assertEquals(2, searchResponse.getHitsCount());

    // Verify that backup indicator file was deleted
    assertFalse(Files.exists(backupIndicatorFilePath));

    // Verify that the snapshot taken during backup was released
    allSnapshotIndexGen = getSnapshotIndexGensList();
    assertTrue(allSnapshotIndexGen.isEmpty());

    // Verify that temporary file created during backup was cleaned up
    tmpFilesInArchiverDirectory = getTmpFilesInArchiverDirectory();
    assertEquals(0, tmpFilesInArchiverDirectory.size());
  }

  private List<Long> getSnapshotIndexGensList() {
    return grpcServer
        .getBlockingStub()
        .getAllSnapshotIndexGen(
            GetAllSnapshotGenRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build())
        .getIndexGensList();
  }

  private void forceShutdownServer() throws IOException {
    grpcServer.getGlobalState().close();
    grpcServer.forceShutdown();
  }

  private SearchResponse search() {
    return grpcServer
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(grpcServer.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .build());
  }

  private void attemptBackupAsync() {
    BackupIndexRequest request =
        BackupIndexRequest.newBuilder()
            .setIndexName(grpcServer.getTestIndex())
            .setServiceName("testservice")
            .setResourceName("testresource")
            .build();

    StreamObserver<BackupIndexResponse> responseObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(BackupIndexResponse value) {}

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        };
    grpcServer.getStub().backupIndex(request, responseObserver);
  }

  private List<Path> getTmpFilesInArchiverDirectory() throws IOException {
    return Files.walk(archiverDirectory)
        .filter(path -> path.toString().endsWith(".tmp"))
        .collect(Collectors.toList());
  }

  /**
   * An S3 client implementation that will do nothing and simply wait when calling putObject. This
   * lets us easily interrupt the backup process after the backup indicator file is created,
   * snapshot is taken and also the temporary file for backup has been created.
   */
  private static class DoNothingAndWaitAmazonS3 extends AmazonS3Client {

    private final CountDownLatch countDownLatch;

    public DoNothingAndWaitAmazonS3(CountDownLatch countDownLatch) {
      this.countDownLatch = countDownLatch;
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest) {
      countDownLatch.countDown();
      sleepForALongTime();
      return null;
    }

    private void sleepForALongTime() {
      try {
        Thread.sleep(100000);
      } catch (InterruptedException ignored) {
      }
    }
  }

  private static class TestBackupIndicatorDetails {
    String indexName;
    long indexGen;
    long stateGen;
    long taxonomyGen;

    public TestBackupIndicatorDetails(
        String indexName, long indexGen, long stateGen, long taxonomyGen) {
      this.indexName = indexName;
      this.indexGen = indexGen;
      this.stateGen = stateGen;
      this.taxonomyGen = taxonomyGen;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestBackupIndicatorDetails that = (TestBackupIndicatorDetails) o;
      return indexGen == that.indexGen
          && stateGen == that.stateGen
          && taxonomyGen == that.taxonomyGen
          && Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(indexName, indexGen, stateGen, taxonomyGen);
    }
  }
}
