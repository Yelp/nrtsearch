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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import java.io.IOException;
import java.util.Set;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IncrementalCommandUtilsTest {
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

  private TestServer getTestServer() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
            .withRemoteStateBackend(false)
            .build();
    server.createSimpleIndex("test_index");
    return server;
  }

  @Test
  public void testGetIndexDataResource() {
    assertEquals(
        "test_index_data_index_data", IncrementalCommandUtils.getIndexDataResource("test_index"));
  }

  @Test
  public void testGetWarmingQueriesResource() {
    assertEquals(
        "test_index_warming_queries",
        IncrementalCommandUtils.getWarmingQueriesResource("test_index"));
  }

  @Test
  public void testGetVersionKeyPrefix() {
    assertEquals("a/_version/b/", IncrementalCommandUtils.getVersionKeyPrefix("a", "b"));
  }

  @Test
  public void testGetDataKeyPrefix() {
    assertEquals("a/b/", IncrementalCommandUtils.getDataKeyPrefix("a", "b"));
  }

  @Test
  public void testGetWarmingQueriesKeyPrefix() {
    assertEquals("a/b/", IncrementalCommandUtils.getWarmingQueriesKeyPrefix("a", "b"));
  }

  @Test
  public void testGetSnapshotRoot_rootProvided() {
    assertEquals("root/", IncrementalCommandUtils.getSnapshotRoot("root/", null));
    assertEquals("root/", IncrementalCommandUtils.getSnapshotRoot("root/", "service"));
    assertEquals("root/", IncrementalCommandUtils.getSnapshotRoot("root", "service"));
  }

  @Test
  public void testGetSnapshotRoot_serviceProvided() {
    assertEquals(
        "service_name/snapshots/", IncrementalCommandUtils.getSnapshotRoot(null, "service_name"));
  }

  @Test
  public void testGetSnapshotRoot_neitherProvided() {
    try {
      IncrementalCommandUtils.getSnapshotRoot(null, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Must specify snapshotRoot or serviceName", e.getMessage());
    }
  }

  @Test
  public void testGetSnapshotIndexDataRoot() {
    assertEquals(
        "root/test_index/1010/",
        IncrementalCommandUtils.getSnapshotIndexDataRoot("root/", "test_index", 1010));
  }

  @Test
  public void testGetSnapshotIndexMetadataKey() {
    assertEquals(
        "root/metadata/test_index/2002",
        IncrementalCommandUtils.getSnapshotIndexMetadataKey("root/", "test_index", 2002));
  }

  @Test
  public void testIsDataFile() {
    assertTrue(IncrementalCommandUtils.isDataFile("_3.cfs"));
    assertTrue(IncrementalCommandUtils.isDataFile("_3abcde.cfs"));
    assertTrue(IncrementalCommandUtils.isDataFile("segments"));
    assertTrue(IncrementalCommandUtils.isDataFile("segments_15"));

    assertFalse(IncrementalCommandUtils.isDataFile("other_file"));
    assertFalse(IncrementalCommandUtils.isDataFile("09d9c9e4-483e-4a90-9c4f-d342c8da1210"));
  }

  @Test
  public void testIsManifestFile() {
    assertTrue(IncrementalCommandUtils.isManifestFile("09d9c9e4-483e-4a90-9c4f-d342c8da1210"));
    assertTrue(IncrementalCommandUtils.isManifestFile("09d9c9e4-483e-4a90-9c4F-D342c8da1210"));

    assertFalse(IncrementalCommandUtils.isManifestFile("09d9c9e4-483e-4a90-D342c8da1210"));
    assertFalse(IncrementalCommandUtils.isManifestFile("other_file"));
    assertFalse(IncrementalCommandUtils.isManifestFile("_3.cfs"));
    assertFalse(IncrementalCommandUtils.isManifestFile("segments"));
  }

  @Test
  public void testIsUUID() {
    assertTrue(IncrementalCommandUtils.isUUID("09d9c9e4-483e-4a90-9c4f-d342c8da1210"));
    assertTrue(IncrementalCommandUtils.isUUID("09d9c9e4-483e-4a90-9c4F-D342c8da1210"));

    assertFalse(IncrementalCommandUtils.isUUID("09d9c9e4-483e-4a90-D342c8da1210"));
    assertFalse(IncrementalCommandUtils.isUUID("other_file"));
    assertFalse(IncrementalCommandUtils.isUUID("_3.cfs"));
    assertFalse(IncrementalCommandUtils.isUUID("segments"));
  }

  @Test
  public void testGetVersionFiles() throws IOException {
    TestServer server = getTestServer();
    server.startPrimaryIndex("test_index", -1, null);
    server.addSimpleDocs("test_index", 1, 2);
    server.refresh("test_index");
    server.commit("test_index");

    String indexDataResource =
        IncrementalCommandUtils.getIndexDataResource(
            server.getGlobalState().getDataResourceForIndex("test_index"));
    AmazonS3 s3Client = getS3();
    VersionManager versionManager = new VersionManager(s3Client, TEST_BUCKET);
    long indexVersion = versionManager.getLatestVersionNumber(SERVICE_NAME, indexDataResource);
    String versionId =
        versionManager.getVersionString(
            SERVICE_NAME, indexDataResource, String.valueOf(indexVersion));
    Set<String> indexFiles =
        IncrementalCommandUtils.getVersionFiles(
            s3Client, TEST_BUCKET, SERVICE_NAME, indexDataResource, versionId);
    assertEquals(Set.of("_0.cfe", "_0.si", "_0.cfs", "segments_2"), indexFiles);
  }

  @Test
  public void testGetVersionFiles_notExist() throws IOException {
    TestServer.initS3(folder);
    try {
      IncrementalCommandUtils.getVersionFiles(
          getS3(), TEST_BUCKET, SERVICE_NAME, "test_index", "not_exist");
      fail();
    } catch (AmazonS3Exception e) {
      assertTrue(e.getMessage().startsWith("The resource you requested does not exist"));
    }
  }
}
