/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.tools.nrt_utils.backup;

import static com.yelp.nrtsearch.server.grpc.TestServer.S3_ENDPOINT;
import static com.yelp.nrtsearch.server.grpc.TestServer.SERVICE_NAME;
import static com.yelp.nrtsearch.server.grpc.TestServer.TEST_BUCKET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.BackupWarmingQueriesRequest;
import com.yelp.nrtsearch.server.grpc.CreateIndexRequest;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.RestoreIndex;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.state.BackendGlobalState;
import com.yelp.nrtsearch.server.state.StateUtils;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
    SnapshotCommand command = new SnapshotCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private CommandLine getInjectedRestoreCommand() {
    RestoreCommand command = new RestoreCommand();
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
        SERVICE_NAME + "/" + BackupCommandUtils.SNAPSHOT_DIR,
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
        SERVICE_NAME + "/" + BackupCommandUtils.SNAPSHOT_DIR,
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
    List<String> timeStrings =
        getSnapshotTimeStrings(
            getS3(), indexResource, SERVICE_NAME + "/" + BackupCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timeStrings.size());
    String snapshotTimeString = timeStrings.get(0);

    String restoreId = TimeStringUtils.generateTimeStringMs();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=" + SERVICE_NAME,
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexResource,
            "--snapshotTimeString=" + snapshotTimeString);
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
    List<String> timeStrings =
        getSnapshotTimeStrings(
            getS3(), indexResource, SERVICE_NAME + "/" + BackupCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timeStrings.size());
    String snapshotTimeString = timeStrings.get(0);

    String restoreId = TimeStringUtils.generateTimeStringMs();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=other_service",
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexResource,
            "--snapshotTimeString=" + snapshotTimeString);
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
    List<String> timeStrings =
        getSnapshotTimeStrings(
            getS3(), indexResource, SERVICE_NAME + "/" + BackupCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timeStrings.size());
    String snapshotTimeString = timeStrings.get(0);

    String restoreId = TimeStringUtils.generateTimeStringMs();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=" + SERVICE_NAME,
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexResource,
            "--snapshotTimeString=" + snapshotTimeString);
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
    List<String> timeStrings =
        getSnapshotTimeStrings(
            getS3(), indexResource, SERVICE_NAME + "/" + BackupCommandUtils.SNAPSHOT_DIR);
    assertEquals(1, timeStrings.size());
    String snapshotTimeString = timeStrings.get(0);

    String restoreId = TimeStringUtils.generateTimeStringMs();
    CommandLine restoreCmd = getInjectedRestoreCommand();
    exitCode =
        restoreCmd.execute(
            "--restoreServiceName=" + SERVICE_NAME,
            "--restoreIndexName=restore_index",
            "--restoreIndexId=" + restoreId,
            "--bucketName=" + TEST_BUCKET,
            "--snapshotServiceName=" + SERVICE_NAME,
            "--snapshotIndexIdentifier=" + indexResource,
            "--snapshotTimeString=" + snapshotTimeString);
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
      AmazonS3 s3Client, String indexResource, String snapshotRoot, boolean withWarming)
      throws IOException {
    List<String> timeStrings = getSnapshotTimeStrings(s3Client, indexResource, snapshotRoot);
    assertEquals(1, timeStrings.size());
    String snapshotTimeString = timeStrings.get(0);

    S3Backend s3Backend = new S3Backend(TEST_BUCKET, false, s3Client);
    NrtPointState pointState =
        RemoteUtils.pointStateFromUtf8(
            s3Backend.downloadPointState(SERVICE_NAME, indexResource).readAllBytes());
    assertEquals(Set.of("_0.cfe", "_0.si", "_0.cfs"), pointState.files.keySet());
    Set<String> pointBackendFiles =
        pointState.files.entrySet().stream()
            .map(e -> S3Backend.getIndexBackendFileName(e.getKey(), e.getValue()))
            .collect(Collectors.toSet());

    Set<String> snapshotFiles =
        getSnapshotFiles(s3Client, indexResource, snapshotTimeString, snapshotRoot);
    Set<String> expectedFiles =
        Sets.union(
            pointBackendFiles,
            Set.of(
                BackupCommandUtils.SNAPSHOT_POINT_STATE, BackupCommandUtils.SNAPSHOT_INDEX_STATE));
    if (withWarming) {
      expectedFiles =
          Sets.union(expectedFiles, Set.of(BackupCommandUtils.SNAPSHOT_WARMING_QUERIES));
    }
    assertEquals(expectedFiles, snapshotFiles);

    assertSnapshotMetadata(s3Client, indexResource, snapshotRoot, snapshotTimeString);
  }

  private void assertSnapshotMetadata(
      AmazonS3 s3Client, String indexResource, String snapshotRoot, String snapshotTimeString) {
    SnapshotMetadata snapshotMetadata =
        getSnapshotMetadata(s3Client, indexResource, snapshotTimeString, snapshotRoot);
    assertEquals(SERVICE_NAME, snapshotMetadata.getServiceName());
    assertTrue(snapshotMetadata.getIndexName().startsWith("test_index-"));
    assertEquals(snapshotTimeString, String.valueOf(snapshotMetadata.getTimeStringMs()));
    assertTrue(snapshotMetadata.getIndexSizeBytes() > 0);
  }

  private void assertRestoreFiles(
      AmazonS3 s3Client, String serviceName, String indexResource, boolean withWarming)
      throws IOException {
    S3Backend s3Backend = new S3Backend(TEST_BUCKET, false, s3Client);
    Set<String> expectedIndexFiles = Set.of("_0.cfe", "_0.si", "_0.cfs");

    NrtPointState pointState =
        RemoteUtils.pointStateFromUtf8(
            s3Backend.downloadPointState(serviceName, indexResource).readAllBytes());
    assertEquals(expectedIndexFiles, pointState.files.keySet());
    Set<String> localPointFiles =
        pointState.files.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    Set<String> backendPointFiles =
        pointState.files.entrySet().stream()
            .map(e -> S3Backend.getIndexBackendFileName(e.getKey(), e.getValue()))
            .collect(Collectors.toSet());

    String restoredDataKeyPrefix = S3Backend.getIndexDataPrefix(serviceName, indexResource);

    Set<String> restoredFiles = getFiles(s3Client, restoredDataKeyPrefix);
    assertEquals(backendPointFiles, restoredFiles);

    assertEquals(expectedIndexFiles, localPointFiles);

    IndexStateInfo indexStateInfo =
        StateUtils.indexStateFromUTF8(
            s3Backend.downloadIndexState(serviceName, indexResource).readAllBytes());
    assertEquals("test_index", indexStateInfo.getIndexName());
    assertEquals(3, indexStateInfo.getFieldsMap().size());

    boolean warmingQueriesExist =
        s3Backend.exists(
            serviceName, indexResource, RemoteBackend.IndexResourceType.WARMING_QUERIES);
    if (withWarming) {
      assertTrue(warmingQueriesExist);
      String warmingQueriesPrefix =
          S3Backend.getIndexResourcePrefix(
              serviceName, indexResource, RemoteBackend.IndexResourceType.WARMING_QUERIES);
      String warmingQueriesVersionId = s3Backend.getCurrentResourceName(warmingQueriesPrefix);
      assertTrue(
          s3Client.doesObjectExist(TEST_BUCKET, warmingQueriesPrefix + warmingQueriesVersionId));
    } else {
      assertFalse(warmingQueriesExist);
    }
  }

  private Set<String> getSnapshotFiles(
      AmazonS3 s3Client, String indexResource, String timeString, String snapshotRoot) {
    String snapshotKeyPrefix = String.join("/", snapshotRoot, indexResource, timeString, "");
    return getFiles(s3Client, snapshotKeyPrefix);
  }

  private SnapshotMetadata getSnapshotMetadata(
      AmazonS3 s3Client, String indexResource, String timeString, String snapshotRoot) {
    String snapshotMetadataKey =
        String.join("/", snapshotRoot, BackupCommandUtils.METADATA_DIR, indexResource, timeString);
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

  private List<String> getSnapshotTimeStrings(
      AmazonS3 s3Client, String indexResource, String snapshotRoot) {
    String indexMetadataKeyPrefix =
        String.join("/", snapshotRoot, BackupCommandUtils.METADATA_DIR, indexResource, "");
    ListObjectsV2Result result = s3Client.listObjectsV2(TEST_BUCKET, indexMetadataKeyPrefix);
    List<String> timeStrings = new ArrayList<>();
    for (S3ObjectSummary summary : result.getObjectSummaries()) {
      String baseName = summary.getKey().split(indexMetadataKeyPrefix)[1];
      timeStrings.add(baseName);
    }
    return timeStrings;
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
