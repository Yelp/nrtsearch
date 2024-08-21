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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.LegacyVersionManager;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.state.LegacyStateCommandUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import picocli.CommandLine;

public class SnapshotRestoreCommandTest {
  private static final String TEST_BUCKET = "test-bucket";
  private static final String SERVICE_NAME = "test_service";
  private static final String INDEX_STATE_FILE = "index_state.json";
  private static final String GLOBAL_STATE_RESOURCE = "global_state";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(TEST_BUCKET);

  private AmazonS3 getS3() {
    return s3Provider.getAmazonS3();
  }

  private CommandLine getInjectedSnapshotCommand() {
    SnapshotIncrementalCommand command = new SnapshotIncrementalCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private CommandLine getInjectedRestoreCommand() {
    RestoreIncrementalCommand command = new RestoreIncrementalCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private String createIndex(String indexId) throws IOException {
    return createIndex(getS3(), indexId);
  }

  public static String createIndex(AmazonS3 s3Client, String indexId) throws IOException {
    String indexName = "test_index";
    String indexUniqueName = LegacyStateCommandUtils.getUniqueIndexName(indexName, indexId);
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

    IndexStateInfo indexStateInfo =
        IndexStateInfo.newBuilder()
            .setIndexName(indexName)
            .putFields("field1", Field.newBuilder().setType(FieldType.ATOM).build())
            .putFields("field2", Field.newBuilder().setType(FieldType.ATOM).build())
            .putFields("field3", Field.newBuilder().setType(FieldType.ATOM).build())
            .build();
    stateStr = JsonFormat.printer().print(indexStateInfo);
    stateData =
        LegacyStateCommandUtils.buildStateFileArchive(
            indexUniqueName, "index_state.json", toUTF8(stateStr));
    metadata = new ObjectMetadata();
    metadata.setContentLength(stateData.length);

    String indexStateResource = LegacyStateCommandUtils.getIndexStateResource(indexUniqueName);
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexStateResource) + stateFileId,
        new ByteArrayInputStream(stateData),
        metadata);
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexStateResource) + "1",
        stateFileId);
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexStateResource)
            + IncrementalDataCleanupCommand.LATEST_VERSION_FILE,
        "1");

    Set<String> indexFiles = Set.of("_0.cfe", "_0.si", "_0.cfs", "segments_2");
    String indexDataResource = IncrementalCommandUtils.getIndexDataResource(indexUniqueName);
    for (String indexFile : indexFiles) {
      s3Client.putObject(
          TEST_BUCKET,
          IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexDataResource) + indexFile,
          "index_data");
    }
    String manifestFile = UUID.randomUUID().toString();
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexDataResource) + manifestFile,
        String.join("\n", indexFiles));
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource) + "1",
        manifestFile);
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource)
            + IncrementalDataCleanupCommand.LATEST_VERSION_FILE,
        "1");

    return indexUniqueName;
  }

  private void createWarmingQueries(String indexId) {
    AmazonS3 s3Client = getS3();
    String fileId = UUID.randomUUID().toString();
    String indexUniqueName = LegacyStateCommandUtils.getUniqueIndexName("test_index", indexId);
    String indexWarmingResource =
        IncrementalCommandUtils.getWarmingQueriesResource(indexUniqueName);
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexWarmingResource) + fileId,
        "");
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexWarmingResource) + "1",
        fileId);
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexWarmingResource)
            + IncrementalDataCleanupCommand.LATEST_VERSION_FILE,
        "1");
  }

  @Test
  public void testSnapshot() throws IOException {
    String indexUniqueName = createIndex("test_id");
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    assertSnapshotFiles(
        getS3(), indexUniqueName, SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR, false);
  }

  @Test
  public void testSnapshotDifferentRoot() throws IOException {
    String indexUniqueName = createIndex("test_id");
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--snapshotRoot=different/root/");
    assertEquals(0, exitCode);

    assertSnapshotFiles(getS3(), indexUniqueName, "different/root", false);
  }

  @Test
  public void testSnapshotWarmingQueries() throws IOException {
    String indexUniqueName = createIndex("test_id");
    createWarmingQueries("test_id");
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    assertSnapshotFiles(
        getS3(), indexUniqueName, SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR, true);
  }

  @Test
  public void testSnapshotWarmingQueriesDifferentRoot() throws IOException {
    String indexUniqueName = createIndex("test_id");
    createWarmingQueries("test_id");
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--snapshotRoot=different/root/");
    assertEquals(0, exitCode);

    assertSnapshotFiles(getS3(), indexUniqueName, "different/root", true);
  }

  @Test
  public void testRestore() throws IOException {
    String indexUniqueName = createIndex("test_id");
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    List<String> timestamps =
        getSnapshotTimestamps(
            getS3(), indexUniqueName, SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timestamps.size());
    String snapshotTimestamp = timestamps.getFirst();

    String restoreId = UUID.randomUUID().toString();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=" + SERVICE_NAME,
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexUniqueName,
            "--snapshotTimestamp=" + snapshotTimestamp);
    assertEquals(0, exitCode);
    assertRestoreFiles(
        getS3(),
        SERVICE_NAME,
        LegacyStateCommandUtils.getUniqueIndexName("restore_index", restoreId),
        false);
  }

  @Test
  public void testRestoreDifferentCluster() throws IOException {
    String indexUniqueName = createIndex("test_id");
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    List<String> timestamps =
        getSnapshotTimestamps(
            getS3(), indexUniqueName, SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timestamps.size());
    String snapshotTimestamp = timestamps.getFirst();

    String restoreId = UUID.randomUUID().toString();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=other_service",
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexUniqueName,
            "--snapshotTimestamp=" + snapshotTimestamp);
    assertEquals(0, exitCode);
    assertRestoreFiles(
        getS3(),
        "other_service",
        LegacyStateCommandUtils.getUniqueIndexName("restore_index", restoreId),
        false);
  }

  @Test
  public void testRestoreWarmingQueries() throws IOException {
    String indexUniqueName = createIndex("test_id");
    createWarmingQueries("test_id");
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    List<String> timestamps =
        getSnapshotTimestamps(
            getS3(), indexUniqueName, SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timestamps.size());
    String snapshotTimestamp = timestamps.getFirst();

    String restoreId = UUID.randomUUID().toString();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=" + SERVICE_NAME,
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexUniqueName,
            "--snapshotTimestamp=" + snapshotTimestamp);
    assertEquals(0, exitCode);
    assertRestoreFiles(
        getS3(),
        SERVICE_NAME,
        LegacyStateCommandUtils.getUniqueIndexName("restore_index", restoreId),
        true);
  }

  private void assertSnapshotFiles(
      AmazonS3 s3Client, String indexResource, String snapshotRoot, boolean withWarming) {
    List<String> timestamps = getSnapshotTimestamps(s3Client, indexResource, snapshotRoot);
    assertEquals(1, timestamps.size());
    String snapshotTimestamp = timestamps.get(0);

    Set<String> snapshotFiles =
        getSnapshotFiles(s3Client, indexResource, snapshotTimestamp, snapshotRoot);
    Set<String> expectedFiles =
        Set.of(
            "_0.cfe",
            "_0.si",
            "_0.cfs",
            "segments_2",
            IncrementalCommandUtils.SNAPSHOT_INDEX_FILES,
            IncrementalCommandUtils.SNAPSHOT_INDEX_STATE_FILE);
    if (withWarming) {
      expectedFiles =
          Sets.union(expectedFiles, Set.of(IncrementalCommandUtils.SNAPSHOT_WARMING_QUERIES));
    }
    assertEquals(expectedFiles, snapshotFiles);

    assertSnapshotMetadata(s3Client, indexResource, snapshotRoot, snapshotTimestamp);
  }

  private void assertSnapshotMetadata(
      AmazonS3 s3Client, String indexResource, String snapshotRoot, String snapshotTimestamp) {
    SnapshotMetadata snapshotMetadata =
        getSnapshotMetadata(s3Client, indexResource, snapshotTimestamp, snapshotRoot);
    assertEquals(SERVICE_NAME, snapshotMetadata.getServiceName());
    assertTrue(snapshotMetadata.getIndexName().startsWith("test_index-"));
    assertEquals(snapshotTimestamp, String.valueOf(snapshotMetadata.getTimestampMs()));
    assertTrue(snapshotMetadata.getIndexSizeBytes() > 0);
  }

  private void assertRestoreFiles(
      AmazonS3 s3Client, String serviceName, String indexResource, boolean withWarming)
      throws IOException {
    LegacyVersionManager versionManager = new LegacyVersionManager(s3Client, TEST_BUCKET);
    String indexDataResource = IncrementalCommandUtils.getIndexDataResource(indexResource);
    long dataVersion = versionManager.getLatestVersionNumber(serviceName, indexDataResource);
    assertTrue(dataVersion >= 0);
    String dataVersionId =
        versionManager.getVersionString(
            serviceName, indexDataResource, String.valueOf(dataVersion));

    String restoredDataKeyPrefix =
        IncrementalCommandUtils.getDataKeyPrefix(serviceName, indexDataResource);

    Set<String> expectedIndexFiles = Set.of("_0.cfe", "_0.si", "_0.cfs", "segments_2");
    Set<String> restoredFiles = getFiles(s3Client, restoredDataKeyPrefix);
    Set<String> expectedFiles = Sets.union(Set.of(dataVersionId), expectedIndexFiles);
    assertEquals(expectedFiles, restoredFiles);

    Set<String> versionFiles =
        IncrementalCommandUtils.getVersionFiles(
            s3Client, TEST_BUCKET, serviceName, indexDataResource, dataVersionId);
    assertEquals(expectedIndexFiles, versionFiles);

    String indexStateResource = LegacyStateCommandUtils.getIndexStateResource(indexResource);
    long stateVersion = versionManager.getLatestVersionNumber(serviceName, indexStateResource);
    assertTrue(stateVersion >= 0);
    String stateContent =
        LegacyStateCommandUtils.getStateFileContents(
            versionManager, serviceName, indexResource, INDEX_STATE_FILE);
    assertNotNull(stateContent);
    IndexStateInfo.Builder stateBuilder = IndexStateInfo.newBuilder();
    JsonFormat.parser().merge(stateContent, stateBuilder);
    IndexStateInfo indexStateInfo = stateBuilder.build();
    assertEquals("test_index", indexStateInfo.getIndexName());
    assertEquals(3, indexStateInfo.getFieldsMap().size());

    String indexWarmingQueriesResource =
        IncrementalCommandUtils.getWarmingQueriesResource(indexResource);
    long warmingQueriesVersion =
        versionManager.getLatestVersionNumber(serviceName, indexWarmingQueriesResource);
    if (withWarming) {
      assertTrue(warmingQueriesVersion >= 0);
      String warmingQueriesVersionId =
          versionManager.getVersionString(
              serviceName, indexWarmingQueriesResource, String.valueOf(warmingQueriesVersion));
      String warmingQueriesKey =
          IncrementalCommandUtils.getWarmingQueriesKeyPrefix(
                  serviceName, indexWarmingQueriesResource)
              + warmingQueriesVersionId;
      assertTrue(s3Client.doesObjectExist(TEST_BUCKET, warmingQueriesKey));
    } else {
      assertEquals(-1, warmingQueriesVersion);
    }
  }

  private Set<String> getSnapshotFiles(
      AmazonS3 s3Client, String indexResource, String timestamp, String snapshotRoot) {
    String snapshotKeyPrefix = String.join("/", snapshotRoot, indexResource, timestamp, "");
    return getFiles(s3Client, snapshotKeyPrefix);
  }

  private SnapshotMetadata getSnapshotMetadata(
      AmazonS3 s3Client, String indexResource, String timestamp, String snapshotRoot) {
    String snapshotMetadataKey =
        String.join(
            "/", snapshotRoot, IncrementalCommandUtils.METADATA_DIR, indexResource, timestamp);
    S3Object stateObject = s3Client.getObject(TEST_BUCKET, snapshotMetadataKey);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      IOUtils.copy(stateObject.getObjectContent(), byteArrayOutputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String fileContent = IncrementalCommandUtils.fromUTF8(byteArrayOutputStream.toByteArray());
    try {
      return OBJECT_MAPPER.readValue(fileContent, SnapshotMetadata.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> getSnapshotTimestamps(
      AmazonS3 s3Client, String indexResource, String snapshotRoot) {
    String indexMetadataKeyPrefix =
        String.join("/", snapshotRoot, IncrementalCommandUtils.METADATA_DIR, indexResource, "");
    ListObjectsV2Result result = s3Client.listObjectsV2(TEST_BUCKET, indexMetadataKeyPrefix);
    List<String> timestamps = new ArrayList<>();
    for (S3ObjectSummary summary : result.getObjectSummaries()) {
      String baseName = summary.getKey().split(indexMetadataKeyPrefix)[1];
      timestamps.add(baseName);
    }
    return timestamps;
  }

  private Set<String> getFiles(AmazonS3 s3Client, String prefix) {
    ListObjectsV2Result result = s3Client.listObjectsV2(TEST_BUCKET, prefix);
    Set<String> files = new HashSet<>();
    for (S3ObjectSummary summary : result.getObjectSummaries()) {
      String baseName = summary.getKey().split(prefix)[1];
      files.add(baseName);
    }
    return files;
  }
}
