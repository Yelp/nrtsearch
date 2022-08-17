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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.BackupWarmingQueriesRequest;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.luceneserver.state.BackendGlobalState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.tools.nrt_utils.state.StateCommandUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class SnapshotRestoreCommandTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
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

  private TestServer getTestServer() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .build();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2);
    server.refresh("test_index");
    server.commit("test_index");
    return server;
  }

  private void createWarmingQueries(TestServer server) throws IOException {
    TestServer replica =
        TestServer.builder(folder)
            .withAutoStartConfig(
                true, Mode.REPLICA, server.getReplicationPort(), IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .withAdditionalConfig(String.join("\n", "warmer:", "  maxWarmingQueries: 10"))
            .build();
    replica
        .getClient()
        .getBlockingStub()
        .backupWarmingQueries(
            BackupWarmingQueriesRequest.newBuilder()
                .setIndex("test_index")
                .setServiceName(SERVICE_NAME)
                .setNumQueriesThreshold(0)
                .setUptimeMinutesThreshold(0)
                .build());
  }

  @Test
  public void testSnapshot() throws IOException {
    TestServer server = getTestServer();
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    assertSnapshotFiles(
        getS3(),
        server.getGlobalState().getDataResourceForIndex("test_index"),
        SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR,
        false);
  }

  @Test
  public void testSnapshotDifferentRoot() throws IOException {
    TestServer server = getTestServer();
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--snapshotRoot=different/root/");
    assertEquals(0, exitCode);

    assertSnapshotFiles(
        getS3(),
        server.getGlobalState().getDataResourceForIndex("test_index"),
        "different/root",
        false);
  }

  @Test
  public void testSnapshotWarmingQueries() throws IOException {
    TestServer server = getTestServer();
    createWarmingQueries(server);
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    assertSnapshotFiles(
        getS3(),
        server.getGlobalState().getDataResourceForIndex("test_index"),
        SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR,
        true);
  }

  @Test
  public void testSnapshotWarmingQueriesDifferentRoot() throws IOException {
    TestServer server = getTestServer();
    createWarmingQueries(server);
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--snapshotRoot=different/root/");
    assertEquals(0, exitCode);

    assertSnapshotFiles(
        getS3(),
        server.getGlobalState().getDataResourceForIndex("test_index"),
        "different/root",
        true);
  }

  @Test
  public void testRestore() throws IOException {
    TestServer server = getTestServer();
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    String indexResource = server.getGlobalState().getDataResourceForIndex("test_index");
    List<String> timestamps =
        getSnapshotTimestamps(
            getS3(), indexResource, SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timestamps.size());
    String snapshotTimestamp = timestamps.get(0);

    String restoreId = UUID.randomUUID().toString();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=" + SERVICE_NAME,
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexResource,
            "--snapshotTimestamp=" + snapshotTimestamp);
    assertEquals(0, exitCode);
    assertRestoreFiles(
        getS3(),
        SERVICE_NAME,
        BackendGlobalState.getUniqueIndexName("restore_index", restoreId),
        false);
  }

  @Test
  public void testRestoreDifferentCluster() throws IOException {
    TestServer server = getTestServer();
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    String indexResource = server.getGlobalState().getDataResourceForIndex("test_index");
    List<String> timestamps =
        getSnapshotTimestamps(
            getS3(), indexResource, SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timestamps.size());
    String snapshotTimestamp = timestamps.get(0);

    String restoreId = UUID.randomUUID().toString();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=other_service",
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexResource,
            "--snapshotTimestamp=" + snapshotTimestamp);
    assertEquals(0, exitCode);
    assertRestoreFiles(
        getS3(),
        "other_service",
        BackendGlobalState.getUniqueIndexName("restore_index", restoreId),
        false);
  }

  @Test
  public void testRestoreWarmingQueries() throws IOException {
    TestServer server = getTestServer();
    createWarmingQueries(server);
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    String indexResource = server.getGlobalState().getDataResourceForIndex("test_index");
    List<String> timestamps =
        getSnapshotTimestamps(
            getS3(), indexResource, SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timestamps.size());
    String snapshotTimestamp = timestamps.get(0);

    String restoreId = UUID.randomUUID().toString();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=" + SERVICE_NAME,
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexResource,
            "--snapshotTimestamp=" + snapshotTimestamp);
    assertEquals(0, exitCode);
    assertRestoreFiles(
        getS3(),
        SERVICE_NAME,
        BackendGlobalState.getUniqueIndexName("restore_index", restoreId),
        true);
  }

  @Test
  public void testRestoreDocs() throws IOException {
    TestServer server = getTestServer();
    CommandLine cmd = getInjectedSnapshotCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET);
    assertEquals(0, exitCode);

    String indexResource = server.getGlobalState().getDataResourceForIndex("test_index");
    List<String> timestamps =
        getSnapshotTimestamps(
            getS3(), indexResource, SERVICE_NAME + "/" + IncrementalCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timestamps.size());
    String snapshotTimestamp = timestamps.get(0);

    String restoreId = UUID.randomUUID().toString();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=" + SERVICE_NAME,
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexResource,
            "--snapshotTimestamp=" + snapshotTimestamp);
    assertEquals(0, exitCode);

    server.createIndex(
        CreateIndexRequest.newBuilder()
            .setIndexName("restore_index")
            .setExistsWithId(restoreId)
            .build());
    server.startPrimaryIndex(
        "restore_index",
        -1,
        RestoreIndex.newBuilder()
            .setServiceName(SERVICE_NAME)
            .setResourceName("restore_index")
            .build());
    server.verifySimpleDocs("test_index", 2);
    server.verifySimpleDocs("restore_index", 2);
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
    VersionManager versionManager = new VersionManager(s3Client, TEST_BUCKET);
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

    String indexStateResource = StateCommandUtils.getIndexStateResource(indexResource);
    long stateVersion = versionManager.getLatestVersionNumber(serviceName, indexStateResource);
    assertTrue(stateVersion >= 0);
    String stateContent =
        StateCommandUtils.getStateFileContents(
            versionManager, serviceName, indexResource, StateUtils.INDEX_STATE_FILE);
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
    String fileContent = StateUtils.fromUTF8(byteArrayOutputStream.toByteArray());
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
