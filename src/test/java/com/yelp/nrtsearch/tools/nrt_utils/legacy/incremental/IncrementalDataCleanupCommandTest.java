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

import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexGlobalState;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.LegacyVersionManager;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.state.LegacyStateCommandUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;
import picocli.CommandLine;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class IncrementalDataCleanupCommandTest {
  private static final String TEST_BUCKET = "test-bucket";
  private static final String SERVICE_NAME = "test_service";
  private static final String GLOBAL_STATE_RESOURCE = "global_state";

  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(TEST_BUCKET);

  private S3Client getS3() {
    return s3Provider.getAmazonS3();
  }

  private CommandLine getInjectedCommand() {
    IncrementalDataCleanupCommand command = new IncrementalDataCleanupCommand();
    command.setS3Client(getS3());
    return new CommandLine(command);
  }

  private void setUpIndex() throws IOException {
    S3Client s3Client = getS3();
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

    s3Client.putObject(
        PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(
                IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, GLOBAL_STATE_RESOURCE)
                    + stateFileId)
            .build(),
        RequestBody.fromBytes(stateData));
    s3Client.putObject(
        PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(
                IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, GLOBAL_STATE_RESOURCE)
                    + "1")
            .build(),
        RequestBody.fromString(stateFileId));
    s3Client.putObject(
        PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(
                IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, GLOBAL_STATE_RESOURCE)
                    + IncrementalDataCleanupCommand.LATEST_VERSION_FILE)
            .build(),
        RequestBody.fromString("1"));

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
    S3Client s3Client = getS3();
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
          PutObjectRequest.builder()
              .bucket(TEST_BUCKET)
              .key(
                  IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexDataResource)
                      + indexFile)
              .build(),
          RequestBody.fromString("index_data"));
    }
    s3Client.putObject(
        PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(
                IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexDataResource)
                    + versionId)
            .build(),
        RequestBody.fromString(String.join("\n", indexFiles)));
    s3Client.putObject(
        PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(
                IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource)
                    + version)
            .build(),
        RequestBody.fromString(versionId));
    s3Client.putObject(
        PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(
                IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource)
                    + IncrementalDataCleanupCommand.LATEST_VERSION_FILE)
            .build(),
        RequestBody.fromString(String.valueOf(version)));
  }

  private String getUniqueIndexName() {
    return LegacyStateCommandUtils.getUniqueIndexName("test_index", "test_id");
  }

  private void setCurrentVersion(S3Client s3Client, int currentVersion) {
    String indexResource = getUniqueIndexName();
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(
            SERVICE_NAME, IncrementalCommandUtils.getIndexDataResource(indexResource));
    String currentVersionKey = versionPrefix + IncrementalDataCleanupCommand.LATEST_VERSION_FILE;
    s3Client.putObject(
        PutObjectRequest.builder().bucket(TEST_BUCKET).key(currentVersionKey).build(),
        RequestBody.fromString(String.valueOf(currentVersion)));
  }

  private String getAndDeleteVersion(S3Client s3Client, int currentVersion) throws IOException {
    String indexResource = getUniqueIndexName();
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(
            SERVICE_NAME, IncrementalCommandUtils.getIndexDataResource(indexResource));
    byte[] currentIdBytes =
        s3Client
            .getObject(
                GetObjectRequest.builder()
                    .bucket(TEST_BUCKET)
                    .key(versionPrefix + currentVersion)
                    .build(),
                ResponseTransformer.toInputStream())
            .readAllBytes();
    s3Client.deleteObject(
        DeleteObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(versionPrefix + currentVersion)
            .build());
    return new String(currentIdBytes, StandardCharsets.UTF_8);
  }

  private void putVersion(S3Client s3Client, int version, String id) {
    String indexResource = getUniqueIndexName();
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(
            SERVICE_NAME, IncrementalCommandUtils.getIndexDataResource(indexResource));
    s3Client.putObject(
        PutObjectRequest.builder().bucket(TEST_BUCKET).key(versionPrefix + version).build(),
        RequestBody.fromString(id));
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
    S3Client s3Client = getS3();

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
    S3Client s3Client = getS3();
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
    S3Client s3Client = getS3();
    Set<Integer> expectedVersions = new HashSet<>();
    for (int i : versions) {
      expectedVersions.add(i);
    }
    Set<Integer> presentVersions = new HashSet<>();
    boolean latestVersionFileSeen = false;
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource);
    ListObjectsV2Response result =
        s3Client.listObjectsV2(
            ListObjectsV2Request.builder().bucket(TEST_BUCKET).prefix(versionPrefix).build());
    for (S3Object summary : result.contents()) {
      String baseName = summary.key().split(versionPrefix)[1];
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

  private Set<Integer> getExistingVersions(S3Client s3Client, String indexDataResource) {
    Set<Integer> versions = new HashSet<>();
    boolean latestVersionFileSeen = false;
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(SERVICE_NAME, indexDataResource);
    ListObjectsV2Response result =
        s3Client.listObjectsV2(
            ListObjectsV2Request.builder().bucket(TEST_BUCKET).prefix(versionPrefix).build());
    for (S3Object summary : result.contents()) {
      String baseName = summary.key().split(versionPrefix)[1];
      if (IncrementalDataCleanupCommand.LATEST_VERSION_FILE.equals(baseName)) {
        latestVersionFileSeen = true;
      } else {
        versions.add(Integer.parseInt(baseName));
      }
    }
    assertTrue(latestVersionFileSeen);
    return versions;
  }

  private Set<String> getExistingDataFiles(S3Client s3Client, String indexDataResource) {
    Set<String> indexFiles = new HashSet<>();
    String indexDataPrefix =
        IncrementalCommandUtils.getDataKeyPrefix(SERVICE_NAME, indexDataResource);
    ListObjectsV2Response result =
        s3Client.listObjectsV2(
            ListObjectsV2Request.builder().bucket(TEST_BUCKET).prefix(indexDataPrefix).build());
    for (S3Object summary : result.contents()) {
      String baseName = summary.key().split(indexDataPrefix)[1];
      if (!IncrementalCommandUtils.isManifestFile(baseName)) {
        indexFiles.add(baseName);
      }
    }
    return indexFiles;
  }
}
