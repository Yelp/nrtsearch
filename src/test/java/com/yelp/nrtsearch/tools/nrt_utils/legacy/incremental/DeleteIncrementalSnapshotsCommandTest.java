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
package com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental;

import static com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.IncrementalCommandUtils.toUTF8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.state.LegacyStateCommandUtils;
import java.io.ByteArrayInputStream;
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
import org.junit.Rule;
import org.junit.Test;
import picocli.CommandLine;

public class DeleteIncrementalSnapshotsCommandTest {
  private static final String TEST_BUCKET = "test-bucket";
  private static final String SERVICE_NAME = "test_service";
  private static final String GLOBAL_STATE_RESOURCE = "global_state";
  private static final long HOUR_TO_MS = 60L * 60L * 1000L;

  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(TEST_BUCKET);

  private AmazonS3 getS3() {
    return s3Provider.getAmazonS3();
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
    return Path.of(s3Provider.getS3DirectoryPath())
        .resolve(TEST_BUCKET)
        .resolve(serviceName)
        .resolve(IncrementalCommandUtils.SNAPSHOT_DIR)
        .resolve(IncrementalCommandUtils.METADATA_DIR)
        .resolve(indexUniqueName);
  }

  private Path getIndexSnapshotDataRoot(String indexUniqueName, String serviceName) {
    return Path.of(s3Provider.getS3DirectoryPath())
        .resolve(TEST_BUCKET)
        .resolve(serviceName)
        .resolve(IncrementalCommandUtils.SNAPSHOT_DIR)
        .resolve(indexUniqueName);
  }

  @Test
  public void testDeleteSnapshots() throws IOException {
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
    String indexName = "test_index";
    String indexId = "test_id";
    String indexUniqueName = LegacyStateCommandUtils.getUniqueIndexName(indexName, indexId);
    AmazonS3 s3Client = getS3();
    String stateFileId = UUID.randomUUID().toString();
    GlobalStateInfo globalStateInfo =
        GlobalStateInfo.newBuilder()
            .putIndices(
                indexName, IndexGlobalState.newBuilder().setId(indexId).setStarted(true).build())
            .build();
    String stateStr = JsonFormat.printer().print(globalStateInfo);
    byte[] stateData =
        LegacyStateCommandUtils.buildStateFileArchive(
            GLOBAL_STATE_RESOURCE, "state.json", toUTF8(stateStr));
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(stateData.length);

    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, GLOBAL_STATE_RESOURCE) + stateFileId,
        new ByteArrayInputStream(stateData),
        metadata);
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, GLOBAL_STATE_RESOURCE) + "1",
        stateFileId);
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, GLOBAL_STATE_RESOURCE)
            + IncrementalDataCleanupCommand.LATEST_VERSION_FILE,
        "1");

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
