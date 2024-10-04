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
package com.yelp.nrtsearch.tools.nrt_utils.cleanup;

import static com.yelp.nrtsearch.server.grpc.TestServer.S3_ENDPOINT;
import static com.yelp.nrtsearch.server.grpc.TestServer.SERVICE_NAME;
import static com.yelp.nrtsearch.server.grpc.TestServer.TEST_BUCKET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.luceneserver.nrt.state.NrtPointState;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.RemoteUtils;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.utils.TimeStringUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class CleanupDataCommandTest {
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
    CleanupDataCommand command = new CleanupDataCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private TestServer getTestServer() throws IOException {
    return TestServer.builder(folder)
        .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
        .withRemoteStateBackend(false)
        .build();
  }

  private List<String> initIndex(TestServer server, S3Backend s3Backend) throws IOException {
    List<String> versions = new ArrayList<>();
    server.createSimpleIndex("test_index");
    server.startPrimaryIndex("test_index", -1, null);
    String prefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME,
            server.getGlobalState().getDataResourceForIndex("test_index"),
            RemoteBackend.IndexResourceType.POINT_STATE);
    versions.add(s3Backend.getCurrentResourceName(prefix));
    for (int i = 0; i < 5; ++i) {
      server.addSimpleDocs("test_index", i * 2, i * 2 + 1);
      server.refresh("test_index");
      server.commit("test_index");
      versions.add(s3Backend.getCurrentResourceName(prefix));
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ignore) {
      }
    }
    return versions;
  }

  private void setCurrentVersion(S3Backend s3Backend, TestServer server, String currentVersion) {
    String indexResource = server.getGlobalState().getDataResourceForIndex("test_index");
    String versionPrefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, indexResource, RemoteBackend.IndexResourceType.POINT_STATE);
    s3Backend.setCurrentResource(versionPrefix, currentVersion);
  }

  @Test
  public void testKeepsRecentVersions() throws IOException {
    TestServer server = getTestServer();
    S3Backend s3Backend = new S3Backend(TEST_BUCKET, false, getS3());
    List<String> versions = initIndex(server, s3Backend);
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1h",
            "--gracePeriod=1s");
    assertEquals(0, exitCode);

    assertVersions(
        server.getGlobalState().getDataResourceForIndex("test_index"), versions, 0, 1, 2, 3, 4, 5);
  }

  @Test
  public void testDeletesUnneededVersions() throws IOException {
    TestServer server = getTestServer();
    S3Backend s3Backend = new S3Backend(TEST_BUCKET, false, getS3());
    List<String> versions = initIndex(server, s3Backend);
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1s");
    assertEquals(0, exitCode);

    assertVersions(server.getGlobalState().getDataResourceForIndex("test_index"), versions, 5);
  }

  @Test
  public void testKeepsFutureVersions() throws IOException {
    TestServer server = getTestServer();
    S3Backend s3Backend = new S3Backend(TEST_BUCKET, false, getS3());
    List<String> versions = initIndex(server, s3Backend);
    CommandLine cmd = getInjectedCommand();

    setCurrentVersion(s3Backend, server, versions.get(3));

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1s");
    assertEquals(0, exitCode);

    assertVersions(
        server.getGlobalState().getDataResourceForIndex("test_index"), versions, 3, 4, 5);
  }

  @Test
  public void testGracePeriod() throws IOException {
    TestServer server = getTestServer();
    S3Backend s3Backend = new S3Backend(TEST_BUCKET, false, getS3());
    List<String> versions = initIndex(server, s3Backend);
    CommandLine cmd = getInjectedCommand();
    AmazonS3 s3Client = getS3();
    String indexResource = server.getGlobalState().getDataResourceForIndex("test_index");

    Set<String> initialIndexFiles = getExistingDataFiles(s3Client, indexResource);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1h");
    assertEquals(0, exitCode);

    Set<String> cleanedUpFiles = getExistingDataFiles(s3Client, indexResource);
    assertEquals(initialIndexFiles, cleanedUpFiles);
    assertEquals(Set.of(versions.get(5)), getExistingVersions(s3Client, indexResource));

    exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1s");
    assertEquals(0, exitCode);
    assertVersions(server.getGlobalState().getDataResourceForIndex("test_index"), versions, 5);
  }

  private void assertVersions(String indexResource, List<String> versionNames, int... versions)
      throws IOException {
    AmazonS3 s3Client = getS3();
    Set<String> expectedVersions = new HashSet<>();
    for (int i : versions) {
      expectedVersions.add(versionNames.get(i));
    }
    Set<String> presentVersions = getExistingVersions(s3Client, indexResource);
    assertEquals(expectedVersions, presentVersions);

    String versionPrefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, indexResource, RemoteBackend.IndexResourceType.POINT_STATE);
    Set<String> requiredIndexFiles = new HashSet<>();
    for (int version : versions) {
      String versionId = versionNames.get(version);
      byte[] versionData =
          s3Client
              .getObject(TEST_BUCKET, versionPrefix + versionId)
              .getObjectContent()
              .readAllBytes();
      NrtPointState pointState = RemoteUtils.pointStateFromUtf8(versionData);
      Set<String> versionFiles =
          pointState.files.entrySet().stream()
              .map(e -> S3Backend.getIndexBackendFileName(e.getKey(), e.getValue()))
              .collect(Collectors.toSet());
      requiredIndexFiles.addAll(versionFiles);
    }

    Set<String> presentIndexFiles = getExistingDataFiles(s3Client, indexResource);
    assertEquals(requiredIndexFiles, presentIndexFiles);
  }

  private Set<String> getExistingVersions(AmazonS3 s3Client, String indexResource) {
    Set<String> versions = new HashSet<>();
    boolean currentVersionFileSeen = false;
    String versionPrefix =
        S3Backend.getIndexResourcePrefix(
            SERVICE_NAME, indexResource, RemoteBackend.IndexResourceType.POINT_STATE);
    ListObjectsV2Result result = s3Client.listObjectsV2(TEST_BUCKET, versionPrefix);
    for (S3ObjectSummary summary : result.getObjectSummaries()) {
      String baseName = summary.getKey().split(versionPrefix)[1];
      if (S3Backend.CURRENT_VERSION.equals(baseName)) {
        currentVersionFileSeen = true;
      } else {
        versions.add(baseName);
      }
    }
    assertTrue(currentVersionFileSeen);
    return versions;
  }

  private Set<String> getExistingDataFiles(AmazonS3 s3Client, String indexResource) {
    Set<String> indexFiles = new HashSet<>();
    String indexDataPrefix = S3Backend.getIndexDataPrefix(SERVICE_NAME, indexResource);
    ListObjectsV2Result result = s3Client.listObjectsV2(TEST_BUCKET, indexDataPrefix);
    for (S3ObjectSummary summary : result.getObjectSummaries()) {
      String baseName = summary.getKey().split(indexDataPrefix)[1];
      indexFiles.add(baseName);
    }
    return indexFiles;
  }

  @Test
  public void testValidatePSNameAndGetTimestampMs() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long expectedTimeMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    String testPSName = timeString + "-" + UUID.randomUUID() + "-1";
    long actualTimeMs = CleanupDataCommand.validatePSNameAndGetTimestampMs(testPSName);
    assertEquals(expectedTimeMs, actualTimeMs);
  }

  @Test
  public void testValidatePSNameAndGetTimestampMs_invalidStructure() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    String testPSName = timeString + "-2-1";
    try {
      CleanupDataCommand.validatePSNameAndGetTimestampMs(testPSName);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid point state name: " + testPSName));
    }
  }

  @Test
  public void testValidatePSNameAndGetTimestampMs_invalidTimeString() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    String testPSName = timeString + "000-" + UUID.randomUUID() + "-1";
    try {
      CleanupDataCommand.validatePSNameAndGetTimestampMs(testPSName);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid point state name: " + testPSName));
    }
  }

  @Test
  public void testValidatePSNameAndGetTimestampMs_invalidUUID() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    String testPSName = timeString + "-" + "5d65b454-fa30-49-invalid-8e0e5" + "-1";
    try {
      CleanupDataCommand.validatePSNameAndGetTimestampMs(testPSName);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid point state name: " + testPSName));
    }
  }

  @Test
  public void testValidatePSNameAndGetTimestampMs_invalidVersionNumber() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    String testPSName = timeString + "-" + UUID.randomUUID() + "-invalid";
    try {
      CleanupDataCommand.validatePSNameAndGetTimestampMs(testPSName);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid point state name: " + testPSName));
    }
  }

  @Test
  public void testValidateDataNameAndGetTimestampMs() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long expectedTimeMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    String testPSName = timeString + "-" + UUID.randomUUID() + "-_indexFile";
    long actualTimeMs = CleanupDataCommand.validateDataNameAndGetTimestampMs(testPSName);
    assertEquals(expectedTimeMs, actualTimeMs);
  }

  @Test
  public void testValidateDataNameAndGetTimestampMs_invalidStructure() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    String testPSName = timeString + "-2-1";
    try {
      CleanupDataCommand.validateDataNameAndGetTimestampMs(testPSName);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid data name: " + testPSName));
    }
  }

  @Test
  public void testValidateDataNameAndGetTimestampMs_invalidTimeString() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    String testPSName = timeString + "000-" + UUID.randomUUID() + "-_indexFile";
    try {
      CleanupDataCommand.validateDataNameAndGetTimestampMs(testPSName);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid data name: " + testPSName));
    }
  }

  @Test
  public void testValidateDataNameAndGetTimestampMs_invalidUUID() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    String testPSName = timeString + "-" + "5d65b454-fa30-49-invalid-8e0e5" + "-_indexFile";
    try {
      CleanupDataCommand.validateDataNameAndGetTimestampMs(testPSName);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid data name: " + testPSName));
    }
  }

  @Test
  public void testValidateDataNameAndGetTimestampMs_invalidIndexFile() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    String testPSName = timeString + "-" + UUID.randomUUID() + "-invalid";
    try {
      CleanupDataCommand.validateDataNameAndGetTimestampMs(testPSName);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid data name: " + testPSName));
    }
  }

  @Test
  public void testPointStateDeletionDecider_delete() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    CleanupDataCommand.PointStateDeletionDecider decider =
        new CleanupDataCommand.PointStateDeletionDecider(timestampMs + 1000);
    String fileName = timeString + "-" + UUID.randomUUID() + "-1";
    assertTrue(decider.shouldDelete(fileName, timestampMs));
    assertFalse(decider.isDone());
    assertNull(decider.getOldestRetainedFile());
  }

  @Test
  public void testPointStateDeletionDecider_retainGreaterTimeString() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    CleanupDataCommand.PointStateDeletionDecider decider =
        new CleanupDataCommand.PointStateDeletionDecider(timestampMs - 1000);
    String fileName = timeString + "-" + UUID.randomUUID() + "-1";
    assertFalse(decider.shouldDelete(fileName, timestampMs - 2000));
    assertTrue(decider.isDone());
    assertEquals(fileName, decider.getOldestRetainedFile());
  }

  @Test
  public void testPointStateDeletionDecider_retainGreaterTimestamp() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    CleanupDataCommand.PointStateDeletionDecider decider =
        new CleanupDataCommand.PointStateDeletionDecider(timestampMs + 1000);
    String fileName = timeString + "-" + UUID.randomUUID() + "-1";
    assertFalse(decider.shouldDelete(fileName, timestampMs + 2000));
    assertFalse(decider.isDone());
    assertNull(decider.getOldestRetainedFile());
  }

  @Test
  public void testPointStateDeletionDecider_retainCurrentVersion() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    CleanupDataCommand.PointStateDeletionDecider decider =
        new CleanupDataCommand.PointStateDeletionDecider(timestampMs);
    assertFalse(decider.shouldDelete(S3Backend.CURRENT_VERSION, 0));
    assertFalse(decider.isDone());
    assertNull(decider.getOldestRetainedFile());
  }

  @Test
  public void testPointStateDeletionDecider_invalidName() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    CleanupDataCommand.PointStateDeletionDecider decider =
        new CleanupDataCommand.PointStateDeletionDecider(timestampMs + 1000);
    String fileName = timeString + "-" + UUID.randomUUID() + "-invalid";
    try {
      decider.shouldDelete(fileName, timestampMs);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid point state name: " + fileName));
    }
  }

  @Test
  public void testIndexDataDeletionDecider_delete() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    CleanupDataCommand.IndexDataDeletionDecider decider =
        new CleanupDataCommand.IndexDataDeletionDecider(timestampMs + 1000, Set.of());
    String fileName = timeString + "-" + UUID.randomUUID() + "-_indexFile";
    assertTrue(decider.shouldDelete(fileName, timestampMs));
    assertFalse(decider.isDone());
  }

  @Test
  public void testIndexDataDeletionDecider_retainActiveFiles() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    String fileName = timeString + "-" + UUID.randomUUID() + "-_indexFile";
    CleanupDataCommand.IndexDataDeletionDecider decider =
        new CleanupDataCommand.IndexDataDeletionDecider(timestampMs + 1000, Set.of(fileName));
    assertFalse(decider.shouldDelete(fileName, timestampMs));
    assertFalse(decider.isDone());
  }

  @Test
  public void testIndexDataDeletionDecider_retainGreaterTimeString() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    CleanupDataCommand.IndexDataDeletionDecider decider =
        new CleanupDataCommand.IndexDataDeletionDecider(timestampMs - 1000, Set.of());
    String fileName = timeString + "-" + UUID.randomUUID() + "-_indexFile";
    assertFalse(decider.shouldDelete(fileName, timestampMs - 2000));
    assertTrue(decider.isDone());
  }

  @Test
  public void testIndexDataDeletionDecider_retainGreaterTimestamp() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    CleanupDataCommand.IndexDataDeletionDecider decider =
        new CleanupDataCommand.IndexDataDeletionDecider(timestampMs + 1000, Set.of());
    String fileName = timeString + "-" + UUID.randomUUID() + "-_indexFile";
    assertFalse(decider.shouldDelete(fileName, timestampMs + 2000));
    assertFalse(decider.isDone());
  }

  @Test
  public void testIndexDataDeletionDecider_invalidName() {
    String timeString = TimeStringUtils.generateTimeStringSec();
    long timestampMs = TimeStringUtils.parseTimeStringSec(timeString).toEpochMilli();

    CleanupDataCommand.IndexDataDeletionDecider decider =
        new CleanupDataCommand.IndexDataDeletionDecider(timestampMs + 1000, Set.of());
    String fileName = timeString + "-" + UUID.randomUUID() + "-invalid";
    try {
      decider.shouldDelete(fileName, timestampMs);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid data name: " + fileName));
    }
  }
}
