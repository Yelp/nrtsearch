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
package com.yelp.nrtsearch.tools.nrt_utils.legacy.state;

import static com.yelp.nrtsearch.tools.nrt_utils.legacy.incremental.SnapshotRestoreCommandTest.createIndex;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.amazonaws.services.s3.AmazonS3;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.grpc.*;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import com.yelp.nrtsearch.tools.nrt_utils.legacy.LegacyVersionManager;
import java.io.IOException;
import java.util.UUID;
import org.junit.Rule;
import org.junit.Test;

public class LegacyStateCommandUtilsTest {
  private static final String TEST_BUCKET = "test-bucket";
  private static final String SERVICE_NAME = "test_service";
  private static final String GLOBAL_STATE_FILE = "state.json";
  private static final String INDEX_STATE_FILE = "index_state.json";
  private static final String GLOBAL_STATE_RESOURCE = "global_state";

  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(TEST_BUCKET);

  private AmazonS3 getS3() {
    return s3Provider.getAmazonS3();
  }

  private LegacyVersionManager getVersionManager() {
    return new LegacyVersionManager(getS3(), TEST_BUCKET);
  }

  @Test
  public void testGetStateFileContents_GlobalNotExist() throws IOException {
    String contents =
        LegacyStateCommandUtils.getStateFileContents(
            getVersionManager(), SERVICE_NAME, GLOBAL_STATE_RESOURCE, GLOBAL_STATE_FILE);
    assertNull(contents);
  }

  @Test
  public void testGetStateFileContents_IndexNotExist() throws IOException {
    String contents =
        LegacyStateCommandUtils.getStateFileContents(
            getVersionManager(), SERVICE_NAME, "test_index", INDEX_STATE_FILE);
    assertNull(contents);
  }

  @Test
  public void testGetStateFileContents_ResourceNotExist() throws IOException {
    LegacyVersionManager mockVersionManager = mock(LegacyVersionManager.class);
    String version = UUID.randomUUID().toString();
    String indexResource = LegacyStateCommandUtils.getIndexStateResource("test_index");
    when(mockVersionManager.getLatestVersionNumber(SERVICE_NAME, indexResource)).thenReturn(5L);
    when(mockVersionManager.getVersionString(SERVICE_NAME, indexResource, String.valueOf(5L)))
        .thenReturn(version);
    when(mockVersionManager.getS3()).thenReturn(getS3());
    when(mockVersionManager.getBucketName()).thenReturn(TEST_BUCKET);

    String contents =
        LegacyStateCommandUtils.getStateFileContents(
            mockVersionManager, SERVICE_NAME, "test_index", INDEX_STATE_FILE);
    assertNull(contents);

    verify(mockVersionManager, times(1)).getLatestVersionNumber(SERVICE_NAME, indexResource);
    verify(mockVersionManager, times(1))
        .getVersionString(SERVICE_NAME, indexResource, String.valueOf(5L));
    verify(mockVersionManager, times(1)).getS3();
    verify(mockVersionManager, times(1)).getBucketName();
    verifyNoMoreInteractions(mockVersionManager);
  }

  @Test
  public void testGetStateFileContents_GlobalState() throws IOException {
    createIndex(getS3(), UUID.randomUUID().toString());
    String contents =
        LegacyStateCommandUtils.getStateFileContents(
            getVersionManager(), SERVICE_NAME, GLOBAL_STATE_RESOURCE, GLOBAL_STATE_FILE);
    GlobalStateInfo.Builder builder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, builder);
    GlobalStateInfo stateInfo = builder.build();
    assertEquals(1, stateInfo.getIndicesMap().size());
    assertTrue(stateInfo.getIndicesMap().containsKey("test_index"));
    assertTrue(stateInfo.getIndicesMap().get("test_index").getStarted());
  }

  @Test
  public void testGetStateFileContents_FileNotInTar() throws IOException {
    createIndex(getS3(), UUID.randomUUID().toString());
    String contents =
        LegacyStateCommandUtils.getStateFileContents(
            getVersionManager(), SERVICE_NAME, GLOBAL_STATE_RESOURCE, "not_state_file");
    assertNull(contents);
  }

  @Test
  public void testGetResourceName_GlobalState() throws IOException {
    String resourceName =
        LegacyStateCommandUtils.getResourceName(
            mock(LegacyVersionManager.class), SERVICE_NAME, GLOBAL_STATE_RESOURCE, false);
    assertEquals(GLOBAL_STATE_RESOURCE, resourceName);
  }

  @Test
  public void testGetResourceName_GlobalStateExact() throws IOException {
    String resourceName =
        LegacyStateCommandUtils.getResourceName(
            mock(LegacyVersionManager.class), SERVICE_NAME, GLOBAL_STATE_RESOURCE, true);
    assertEquals(GLOBAL_STATE_RESOURCE, resourceName);
  }

  @Test
  public void testGetResourceName_IndexStateExact() throws IOException {
    String resourceName =
        LegacyStateCommandUtils.getResourceName(
            mock(LegacyVersionManager.class), SERVICE_NAME, "exact-resource-name", true);
    assertEquals("exact-resource-name", resourceName);
  }

  @Test
  public void testGetResourceName_NoGlobalState() throws IOException {
    LegacyVersionManager versionManager = getVersionManager();
    try {
      LegacyStateCommandUtils.getResourceName(versionManager, SERVICE_NAME, "test_index", false);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Unable to load global state for cluster: \"test_service\"", e.getMessage());
    }
  }

  @Test
  public void testGetResourceName_IndexNotExists() throws IOException {
    createIndex(getS3(), UUID.randomUUID().toString());
    LegacyVersionManager versionManager = getVersionManager();
    try {
      LegacyStateCommandUtils.getResourceName(
          versionManager, SERVICE_NAME, "invalid_test_index", false);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Unable to find index: \"invalid_test_index\" in cluster: \"test_service\"",
          e.getMessage());
    }
  }

  @Test
  public void testGetResourceName_IndexResource() throws IOException {
    String indexUniqueName = createIndex(getS3(), UUID.randomUUID().toString());
    LegacyVersionManager versionManager = getVersionManager();
    String resourceName =
        LegacyStateCommandUtils.getResourceName(versionManager, SERVICE_NAME, "test_index", false);
    assertEquals(indexUniqueName, resourceName);
  }

  @Test
  public void testGetIndexStateResource() {
    assertEquals("test_index-state", LegacyStateCommandUtils.getIndexStateResource("test_index"));
  }

  @Test
  public void testGetStateKey() {
    assertEquals("a/b/c", LegacyStateCommandUtils.getStateKey("a", "b", "c"));
  }
}
