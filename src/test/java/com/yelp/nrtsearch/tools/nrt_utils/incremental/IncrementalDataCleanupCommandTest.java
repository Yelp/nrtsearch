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
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class IncrementalDataCleanupCommandTest {
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
    IncrementalDataCleanupCommand command = new IncrementalDataCleanupCommand();
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
    for (int i = 0; i < 5; ++i) {
      server.addSimpleDocs("test_index", i * 2, i * 2 + 1);
      server.refresh("test_index");
      server.commit("test_index");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ignore) {
      }
    }
    return server;
  }

  private void setCurrentVersion(AmazonS3 s3Client, TestServer server, int currentVersion) {
    String indexResource = server.getGlobalState().getDataResourceForIndex("test_index");
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(
            SERVICE_NAME, IncrementalCommandUtils.getIndexDataResource(indexResource));
    String currentVersionKey = versionPrefix + IncrementalDataCleanupCommand.LATEST_VERSION_FILE;
    s3Client.putObject(TEST_BUCKET, currentVersionKey, String.valueOf(currentVersion));
  }

  private String getAndDeleteVersion(AmazonS3 s3Client, TestServer server, int currentVersion)
      throws IOException {
    String indexResource = server.getGlobalState().getDataResourceForIndex("test_index");
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(
            SERVICE_NAME, IncrementalCommandUtils.getIndexDataResource(indexResource));
    String currentId =
        IOUtils.toString(
            s3Client.getObject(TEST_BUCKET, versionPrefix + currentVersion).getObjectContent());
    s3Client.deleteObject(TEST_BUCKET, versionPrefix + currentVersion);
    return currentId;
  }

  private void putVersion(AmazonS3 s3Client, TestServer server, int version, String id) {
    String indexResource = server.getGlobalState().getDataResourceForIndex("test_index");
    String versionPrefix =
        IncrementalCommandUtils.getVersionKeyPrefix(
            SERVICE_NAME, IncrementalCommandUtils.getIndexDataResource(indexResource));
    s3Client.putObject(TEST_BUCKET, versionPrefix + version, id);
  }

  @Test
  public void testKeepsRecentVersions() throws IOException {
    TestServer server = getTestServer();
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

    assertVersions(server.getGlobalState().getDataResourceForIndex("test_index"), 0, 1, 2, 3, 4, 5);
  }

  @Test
  public void testDeletesUnneededVersions() throws IOException {
    TestServer server = getTestServer();
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

    assertVersions(server.getGlobalState().getDataResourceForIndex("test_index"), 5);
  }

  @Test
  public void testKeepsFutureVersions() throws IOException {
    TestServer server = getTestServer();
    CommandLine cmd = getInjectedCommand();
    AmazonS3 s3Client = getS3();

    // VersionManager will automatically advance back to the latest, so delete the next
    // version and replace after cleanup
    setCurrentVersion(s3Client, server, 3);
    String versionId = getAndDeleteVersion(s3Client, server, 4);

    int exitCode =
        cmd.execute(
            "--serviceName=" + SERVICE_NAME,
            "--indexName=test_index",
            "--bucketName=" + TEST_BUCKET,
            "--deleteAfter=1s",
            "--gracePeriod=1s",
            "--minVersions=1");
    assertEquals(0, exitCode);

    putVersion(s3Client, server, 4, versionId);

    assertVersions(server.getGlobalState().getDataResourceForIndex("test_index"), 3, 4, 5);
  }

  @Test
  public void testKeepsMinVersions() throws IOException {
    TestServer server = getTestServer();
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

    assertVersions(server.getGlobalState().getDataResourceForIndex("test_index"), 3, 4, 5);
  }

  @Test
  public void testGracePeriod() throws IOException {
    TestServer server = getTestServer();
    CommandLine cmd = getInjectedCommand();
    AmazonS3 s3Client = getS3();
    String indexDataResource =
        IncrementalCommandUtils.getIndexDataResource(
            server.getGlobalState().getDataResourceForIndex("test_index"));

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
    assertVersions(server.getGlobalState().getDataResourceForIndex("test_index"), 5);
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
    VersionManager versionManager = new VersionManager(s3Client, TEST_BUCKET);
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
