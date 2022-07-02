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
package com.yelp.nrtsearch.tools.nrt_utils.incremental;

import static com.yelp.nrtsearch.server.grpc.TestServer.S3_ENDPOINT;
import static com.yelp.nrtsearch.server.grpc.TestServer.SERVICE_NAME;
import static com.yelp.nrtsearch.server.grpc.TestServer.TEST_BUCKET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.yelp.nrtsearch.server.grpc.TestServer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class DeleteIncrementalSnapshotsCommandTest {
  private static final long HOUR_TO_MS = 60L * 60L * 1000L;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private AmazonS3 getS3() {
    AmazonS3 s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint(S3_ENDPOINT);
    s3.createBucket(TEST_BUCKET);
    return s3;
  }

  private CommandLine getInjectedCommand() {
    DeleteIncrementalSnapshotsCommand command = new DeleteIncrementalSnapshotsCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private static class TestSnapshotInfo {
    public final long timestampMs;
    public final Set<String> files;
    public final boolean hasMetadata;

    TestSnapshotInfo(long timestampMs, Set<String> files) {
      this(timestampMs, files, true);
    }

    TestSnapshotInfo(long timestampMs, Set<String> files, boolean hasMetadata) {
      this.timestampMs = timestampMs;
      this.files = files;
      this.hasMetadata = hasMetadata;
    }
  }

  private List<TestSnapshotInfo> createTestSnapshotData(String indexUniqueName) throws IOException {
    return createTestSnapshotData(indexUniqueName, true);
  }

  private List<TestSnapshotInfo> createTestSnapshotData(String indexUniqueName, boolean hasMetadata)
      throws IOException {
    return createTestSnapshotData(indexUniqueName, hasMetadata, SERVICE_NAME);
  }

  private List<TestSnapshotInfo> createTestSnapshotData(
      String indexUniqueName, boolean hasMetadata, String serviceName) throws IOException {
    long currentTime = System.currentTimeMillis() - 1;
    List<TestSnapshotInfo> testInfo = new ArrayList<>();
    testInfo.add(new TestSnapshotInfo(currentTime - HOUR_TO_MS, Set.of("f1", "f2", "f3")));
    testInfo.add(new TestSnapshotInfo(currentTime - (2L * HOUR_TO_MS), Set.of("f2", "f4")));
    testInfo.add(
        new TestSnapshotInfo(
            currentTime - (3L * HOUR_TO_MS), Set.of("f3", "f6", "f9"), hasMetadata));
    testInfo.add(
        new TestSnapshotInfo(currentTime - (4L * HOUR_TO_MS), Set.of("f10", "f11", "f12")));
    testInfo.add(new TestSnapshotInfo(currentTime - (5L * HOUR_TO_MS), Set.of("f1", "f5", "f10")));
    createSnapshotFiles(indexUniqueName, testInfo, serviceName);
    return testInfo;
  }

  private void createSnapshotFiles(
      String indexUniqueName, List<TestSnapshotInfo> snapshotInfos, String serviceName)
      throws IOException {
    Path metadataRootFolder = getMetadataRoot(indexUniqueName, serviceName);
    Files.createDirectories(metadataRootFolder);

    for (TestSnapshotInfo snapshotInfo : snapshotInfos) {
      if (snapshotInfo.hasMetadata) {
        Path metadataFile = metadataRootFolder.resolve(String.valueOf(snapshotInfo.timestampMs));
        Files.createFile(metadataFile);
      }
      Path dataFolder =
          getIndexSnapshotDataRoot(indexUniqueName, serviceName)
              .resolve(String.valueOf(snapshotInfo.timestampMs));
      Files.createDirectories(dataFolder);
      for (String file : snapshotInfo.files) {
        Path filePath = dataFolder.resolve(file);
        Files.createFile(filePath);
      }
    }
  }

  private Path getMetadataRoot(String indexUniqueName, String serviceName) {
    return folder
        .getRoot()
        .toPath()
        .resolve("s3")
        .resolve(TEST_BUCKET)
        .resolve(serviceName)
        .resolve(IncrementalCommandUtils.SNAPSHOT_DIR)
        .resolve(IncrementalCommandUtils.METADATA_DIR)
        .resolve(indexUniqueName);
  }

  private Path getIndexSnapshotDataRoot(String indexUniqueName, String serviceName) {
    return folder
        .getRoot()
        .toPath()
        .resolve("s3")
        .resolve(TEST_BUCKET)
        .resolve(serviceName)
        .resolve(IncrementalCommandUtils.SNAPSHOT_DIR)
        .resolve(indexUniqueName);
  }

  @Test
  public void testDeleteSnapshots() throws IOException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();
    List<TestSnapshotInfo> snapshotInfos = createTestSnapshotData(indexUniqueName);

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=210m");
    assertEquals(0, exitCode);

    verifySnapshots(
        indexUniqueName, snapshotInfos.get(0), snapshotInfos.get(1), snapshotInfos.get(2));
  }

  @Test
  public void testDeleteSnapshotsDryRun() throws IOException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();
    List<TestSnapshotInfo> snapshotInfos = createTestSnapshotData(indexUniqueName);

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=210m",
            "--dryRun");
    assertEquals(0, exitCode);

    verifySnapshots(
        indexUniqueName,
        snapshotInfos.get(0),
        snapshotInfos.get(1),
        snapshotInfos.get(2),
        snapshotInfos.get(3),
        snapshotInfos.get(4));
  }

  @Test
  public void testDeleteSnapshotsDifferentRoot() throws IOException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();
    List<TestSnapshotInfo> snapshotInfos =
        createTestSnapshotData(indexUniqueName, true, "different_root");

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--snapshotRoot=different_root/snapshots/",
            "--deleteAfter=210m");
    assertEquals(0, exitCode);

    verifySnapshots(
        indexUniqueName,
        "different_root",
        snapshotInfos.get(0),
        snapshotInfos.get(1),
        snapshotInfos.get(2));
  }

  @Test
  public void testDeleteSnapshotsKeepsN() throws IOException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();
    List<TestSnapshotInfo> snapshotInfos = createTestSnapshotData(indexUniqueName);

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=210m",
            "--minSnapshots=4");
    assertEquals(0, exitCode);

    verifySnapshots(
        indexUniqueName,
        snapshotInfos.get(0),
        snapshotInfos.get(1),
        snapshotInfos.get(2),
        snapshotInfos.get(3));
  }

  @Test
  public void testDeleteSnapshotsKeepMore() throws IOException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();
    List<TestSnapshotInfo> snapshotInfos = createTestSnapshotData(indexUniqueName);

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=210m",
            "--minSnapshots=7");
    assertEquals(0, exitCode);

    verifySnapshots(
        indexUniqueName,
        snapshotInfos.get(0),
        snapshotInfos.get(1),
        snapshotInfos.get(2),
        snapshotInfos.get(3),
        snapshotInfos.get(4));
  }

  @Test
  public void testDeleteAllSnapshots() throws IOException, InterruptedException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();
    List<TestSnapshotInfo> snapshotInfos = createTestSnapshotData(indexUniqueName);

    CommandLine cmd = getInjectedCommand();

    Thread.sleep(2000);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--minSnapshots=0");
    assertEquals(0, exitCode);

    verifySnapshots(indexUniqueName);
  }

  @Test
  public void testOnlyKeepN() throws IOException, InterruptedException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();
    List<TestSnapshotInfo> snapshotInfos = createTestSnapshotData(indexUniqueName);

    CommandLine cmd = getInjectedCommand();

    Thread.sleep(2000);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--minSnapshots=1");
    assertEquals(0, exitCode);

    verifySnapshots(indexUniqueName, snapshotInfos.get(0));
  }

  @Test
  public void testNoData() throws IOException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--minSnapshots=1");
    assertEquals(0, exitCode);

    verifySnapshots(indexUniqueName);
  }

  @Test
  public void testDeleteDataWithoutMetadata() throws IOException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();
    List<TestSnapshotInfo> snapshotInfos = createTestSnapshotData(indexUniqueName, false);

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=150m");
    assertEquals(0, exitCode);

    verifySnapshots(indexUniqueName, snapshotInfos.get(0), snapshotInfos.get(1));
  }

  @Test
  public void testKeepDataWithoutMetadata() throws IOException {
    TestServer.initS3(folder);
    String indexUniqueName = "test_index-" + UUID.randomUUID();
    List<TestSnapshotInfo> snapshotInfos = createTestSnapshotData(indexUniqueName, false);

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=" + indexUniqueName,
            "--exactResourceName",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=210m");
    assertEquals(0, exitCode);

    verifySnapshots(
        indexUniqueName, snapshotInfos.get(0), snapshotInfos.get(1), snapshotInfos.get(2));
  }

  @Test
  public void testIndexIdFromGlobalState() throws IOException {
    TestServer server = TestServer.builder(folder).withRemoteStateBackend(false).build();
    server.createIndex("test_index");
    String indexUniqueName = server.getGlobalState().getDataResourceForIndex("test_index");
    List<TestSnapshotInfo> snapshotInfos = createTestSnapshotData(indexUniqueName);

    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=210m");
    assertEquals(0, exitCode);

    verifySnapshots(
        indexUniqueName, snapshotInfos.get(0), snapshotInfos.get(1), snapshotInfos.get(2));
  }

  public void verifySnapshots(String indexUniqueName, TestSnapshotInfo... infos) {
    verifySnapshots(indexUniqueName, SERVICE_NAME, infos);
  }

  public void verifySnapshots(
      String indexUniqueName, String serviceName, TestSnapshotInfo... infos) {
    Map<String, Set<String>> fileMap =
        Stream.of(infos)
            .collect(Collectors.toMap(i -> String.valueOf(i.timestampMs), i -> i.files));

    Set<String> expectedMetadata = new HashSet<>();
    Set<String> expectedDataFolders = new HashSet<>();
    Set<String> metadataVersions = new HashSet<>();
    for (TestSnapshotInfo info : infos) {
      if (info.hasMetadata) {
        expectedMetadata.add(String.valueOf(info.timestampMs));
      }
      expectedDataFolders.add(String.valueOf(info.timestampMs));
    }
    File[] indexMetadataFiles = getMetadataRoot(indexUniqueName, serviceName).toFile().listFiles();
    if (indexMetadataFiles != null) {
      for (File file : indexMetadataFiles) {
        metadataVersions.add(file.getName());
      }
    }
    assertEquals(expectedMetadata, metadataVersions);

    Set<String> dataFolders = new HashSet<>();
    Path dataRoot = getIndexSnapshotDataRoot(indexUniqueName, serviceName);
    File[] indexDataVersions = dataRoot.toFile().listFiles();
    if (indexDataVersions != null) {
      for (File file : indexDataVersions) {
        dataFolders.add(file.getName());
      }
    }
    assertTrue(dataFolders.containsAll(expectedDataFolders));

    for (String folder : dataFolders) {
      File[] folderFiles = dataRoot.resolve(folder).toFile().listFiles();
      if (expectedDataFolders.contains(folder)) {
        Set<String> fileSet = new HashSet<>();
        for (File folderFile : folderFiles) {
          fileSet.add(folderFile.getName());
        }
        assertEquals(fileMap.get(folder), fileSet);
      } else {
        assertEquals(0, folderFiles.length);
      }
    }
  }
}
