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
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.LegacyVersionManager;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.state.LegacyStateCommandUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import picocli.CommandLine;

public class IncrementalDataCleanupCommandTest {
  private static final String TEST_BUCKET = "test-bucket";
  private static final String SERVICE_NAME = "test_service";
  private static final String GLOBAL_STATE_RESOURCE = "global_state";

  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(TEST_BUCKET);

  private AmazonS3 getS3() {
    return s3Provider.getAmazonS3();
  }

  private CommandLine getInjectedCommand() {
    IncrementalDataCleanupCommand command = new IncrementalDataCleanupCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private void setUpIndex() throws IOException {
    AmazonS3 s3Client = getS3();
    String stateFileId = UUID.randomUUID().toString();
    GlobalStateInfo globalStateInfo =
        GlobalStateInfo.newBuilder()
            .putIndices(
                "test_index",
                IndexGlobalState.newBuilder().setId("test_id").setStarted(true).build())
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

    writeVersion(0);
    for (int i = 0; i < 5; ++i) {
      writeVersion(i + 1);
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ignore) {
      }
    }
  }

  private void writeVersion(int version) {
    String indexUniqueId = getUniqueIndexName();
    AmazonS3 s3Client = getS3();
    Set<String> indexFiles = new HashSet<>();
    indexFiles.add(String.format("segments_%d", version + 1));
    if (version > 0) {
      indexFiles.add(String.format("_%d.cfe", version));
      indexFiles.add(String.format("_%d.cfs", version));
      indexFiles.add(String.format("_%d.si", version));
    }

    String indexDataResource = IncrementalCommandUtils.getIndexDataResource(indexUniqueId);
    String versionId = UUID.randomUUID().toString();
    for (String indexFile : indexFiles) {
      s3Client.putObject(
          TEST_BUCKET,
          IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexDataResource) + indexFile,
          "index_data");
    }
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexDataResource) + versionId,
        String.join("\n", indexFiles));
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource) + version,
        versionId);
    s3Client.putObject(
        TEST_BUCKET,
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource)
            + IncrementalDataCleanupCommand.LATEST_VERSION_FILE,
        String.valueOf(version));
  }

  private String getUniqueIndexName() {
    return LegacyStateCommandUtils.getUniqueIndexName("test_index", "test_id");
  }

  private void setCurrentVersion(AmazonS3 s3Client, int currentVersion) {
    String indexResource = getUniqueIndexName();
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(
            SERVICE_NAME, IncrementalCommandUtils.getIndexDataResource(indexResource));
    String currentVersionKey = versionPrefix + IncrementalDataCleanupCommand.LATEST_VERSION_FILE;
    s3Client.putObject(TEST_BUCKET, currentVersionKey, String.valueOf(currentVersion));
  }

  private String getAndDeleteVersion(AmazonS3 s3Client, int currentVersion) throws IOException {
    String indexResource = getUniqueIndexName();
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(
            SERVICE_NAME, IncrementalCommandUtils.getIndexDataResource(indexResource));
    String currentId =
        IOUtils.toString(
            s3Client.getObject(TEST_BUCKET, versionPrefix + currentVersion).getObjectContent());
    s3Client.deleteObject(TEST_BUCKET, versionPrefix + currentVersion);
    return currentId;
  }

  private void putVersion(AmazonS3 s3Client, int version, String id) {
    String indexResource = getUniqueIndexName();
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(
            SERVICE_NAME, IncrementalCommandUtils.getIndexDataResource(indexResource));
    s3Client.putObject(TEST_BUCKET, versionPrefix + version, id);
  }

  @Test
  public void testKeepsRecentVersions() throws IOException {
    setUpIndex();
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1h",
            "--gracePeriod=1s",
            "--minVersions=1");
    assertEquals(0, exitCode);

    assertVersions(getUniqueIndexName(), 0, 1, 2, 3, 4, 5);
  }

  @Test
  public void testDeletesUnneededVersions() throws IOException {
    setUpIndex();
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1s",
            "--minVersions=1");
    assertEquals(0, exitCode);

    assertVersions(getUniqueIndexName(), 5);
  }

  @Test
  public void testKeepsFutureVersions() throws IOException {
    setUpIndex();
    CommandLine cmd = getInjectedCommand();
    AmazonS3 s3Client = getS3();

    // VersionManager will automatically advance back to the latest, so delete the next
    // version and replace after cleanup
    setCurrentVersion(s3Client, 3);
    String versionId = getAndDeleteVersion(s3Client, 4);

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1s",
            "--minVersions=1");
    assertEquals(0, exitCode);

    putVersion(s3Client, 4, versionId);

    assertVersions(getUniqueIndexName(), 3, 4, 5);
  }

  @Test
  public void testKeepsMinVersions() throws IOException {
    setUpIndex();
    CommandLine cmd = getInjectedCommand();

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1s",
            "--minVersions=3");
    assertEquals(0, exitCode);

    assertVersions(getUniqueIndexName(), 3, 4, 5);
  }

  @Test
  public void testGracePeriod() throws IOException {
    setUpIndex();
    CommandLine cmd = getInjectedCommand();
    AmazonS3 s3Client = getS3();
    String indexDataResource = IncrementalCommandUtils.getIndexDataResource(getUniqueIndexName());

    Set<String> initialIndexFiles = getExistingDataFiles(s3Client, indexDataResource);
    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1h",
            "--minVersions=1");
    assertEquals(0, exitCode);

    Set<String> cleanedUpFiles = getExistingDataFiles(s3Client, indexDataResource);
    assertEquals(initialIndexFiles, cleanedUpFiles);
    assertEquals(Set.of(5), getExistingVersions(s3Client, indexDataResource));

    exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1s",
            "--minVersions=1");
    assertEquals(0, exitCode);
    assertVersions(getUniqueIndexName(), 5);
  }

  private void assertVersions(String indexResource, int... versions) throws IOException {
    String indexDataResource = IncrementalCommandUtils.getIndexDataResource(indexResource);
    AmazonS3 s3Client = getS3();
    Set<Integer> expectedVersions = new HashSet<>();
    for (int i : versions) {
      expectedVersions.add(i);
    }
    Set<Integer> presentVersions = new HashSet<>();
    boolean latestVersionFileSeen = false;
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource);
    ListObjectsV2Result result = s3Client.listObjectsV2(TEST_BUCKET, versionPrefix);
    for (S3ObjectSummary summary : result.getObjectSummaries()) {
      String baseName = summary.getKey().split(versionPrefix)[1];
      if (IncrementalDataCleanupCommand.LATEST_VERSION_FILE.equals(baseName)) {
        latestVersionFileSeen = true;
      } else {
        presentVersions.add(Integer.parseInt(baseName));
      }
    }
    assertTrue(latestVersionFileSeen);
    assertEquals(expectedVersions, presentVersions);

    Set<String> requiredIndexFiles = new HashSet<>();
    LegacyVersionManager versionManager = new LegacyVersionManager(s3Client, TEST_BUCKET);
    for (int version : versions) {
      String versionId =
          versionManager.getVersionString(SERVICE_NAME, indexDataResource, String.valueOf(version));
      Set<String> versionFiles =
          IncrementalCommandUtils.getVersionFiles(
              s3Client, TEST_BUCKET, SERVICE_NAME, indexDataResource, versionId);
      requiredIndexFiles.addAll(versionFiles);
    }

    Set<String> presentIndexFiles = getExistingDataFiles(s3Client, indexDataResource);
    assertEquals(requiredIndexFiles, presentIndexFiles);
  }

  private Set<Integer> getExistingVersions(AmazonS3 s3Client, String indexDataResource) {
    Set<Integer> versions = new HashSet<>();
    boolean latestVersionFileSeen = false;
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource);
    ListObjectsV2Result result = s3Client.listObjectsV2(TEST_BUCKET, versionPrefix);
    for (S3ObjectSummary summary : result.getObjectSummaries()) {
      String baseName = summary.getKey().split(versionPrefix)[1];
      if (IncrementalDataCleanupCommand.LATEST_VERSION_FILE.equals(baseName)) {
        latestVersionFileSeen = true;
      } else {
        versions.add(Integer.parseInt(baseName));
      }
    }
    assertTrue(latestVersionFileSeen);
    return versions;
  }

  private Set<String> getExistingDataFiles(AmazonS3 s3Client, String indexDataResource) {
    Set<String> indexFiles = new HashSet<>();
    String indexDataPrefix =
        IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexDataResource);
    ListObjectsV2Result result = s3Client.listObjectsV2(TEST_BUCKET, indexDataPrefix);
    for (S3ObjectSummary summary : result.getObjectSummaries()) {
      String baseName = summary.getKey().split(indexDataPrefix)[1];
      if (!IncrementalCommandUtils.isManifestFile(baseName)) {
        indexFiles.add(baseName);
      }
    }
    return indexFiles;
  }
}
