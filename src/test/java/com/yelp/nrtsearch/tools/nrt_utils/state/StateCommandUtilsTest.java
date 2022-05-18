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
package com.yelp.nrtsearch.tools.nrt_utils.state;

import static com.yelp.nrtsearch.server.grpc.TestServer.S3_ENDPOINT;
import static com.yelp.nrtsearch.server.grpc.TestServer.SERVICE_NAME;
import static com.yelp.nrtsearch.server.grpc.TestServer.TEST_BUCKET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.GlobalStateInfo;
import com.yelp.nrtsearch.server.grpc.IndexLiveSettings;
import com.yelp.nrtsearch.server.grpc.IndexStateInfo;
import com.yelp.nrtsearch.server.grpc.Mode;
import com.yelp.nrtsearch.server.grpc.TestServer;
import com.yelp.nrtsearch.server.luceneserver.IndexBackupUtils;
import com.yelp.nrtsearch.server.luceneserver.index.ImmutableIndexState;
import com.yelp.nrtsearch.server.luceneserver.state.StateUtils;
import com.yelp.nrtsearch.server.luceneserver.state.backend.RemoteStateBackend;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StateCommandUtilsTest {
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

  private VersionManager getVersionManager() {
    return new VersionManager(getS3(), TEST_BUCKET);
  }

  private TestServer getTestServer() throws IOException {
    TestServer server =
        TestServer.builder(folder)
            .withAutoStartConfig(true, Mode.PRIMARY, 0, IndexDataLocationType.LOCAL)
            .withRemoteStateBackend(false)
            .build();
    server.createSimpleIndex("test_index");
    return server;
  }

  @Test
  public void testGetStateFileContents_GlobalNotExist() throws IOException {
    TestServer.initS3(folder);
    String contents =
        StateCommandUtils.getStateFileContents(
            getVersionManager(),
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            StateUtils.GLOBAL_STATE_FILE);
    assertNull(contents);
  }

  @Test
  public void testGetStateFileContents_IndexNotExist() throws IOException {
    TestServer.initS3(folder);
    String contents =
        StateCommandUtils.getStateFileContents(
            getVersionManager(), SERVICE_NAME, "test_index", StateUtils.INDEX_STATE_FILE);
    assertNull(contents);
  }

  @Test
  public void testGetStateFileContents_ResourceNotExist() throws IOException {
    TestServer.initS3(folder);
    VersionManager mockVersionManager = mock(VersionManager.class);
    String version = UUID.randomUUID().toString();
    String indexResource = "test_index" + IndexBackupUtils.INDEX_STATE_SUFFIX;
    when(mockVersionManager.getLatestVersionNumber(SERVICE_NAME, indexResource)).thenReturn(5L);
    when(mockVersionManager.getVersionString(SERVICE_NAME, indexResource, String.valueOf(5L)))
        .thenReturn(version);
    when(mockVersionManager.getS3()).thenReturn(getS3());
    when(mockVersionManager.getBucketName()).thenReturn(TEST_BUCKET);

    String contents =
        StateCommandUtils.getStateFileContents(
            mockVersionManager, SERVICE_NAME, "test_index", StateUtils.INDEX_STATE_FILE);
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
    getTestServer();
    String contents =
        StateCommandUtils.getStateFileContents(
            getVersionManager(),
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            StateUtils.GLOBAL_STATE_FILE);
    GlobalStateInfo.Builder builder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, builder);
    GlobalStateInfo stateInfo = builder.build();
    assertEquals(1, stateInfo.getIndicesMap().size());
    assertTrue(stateInfo.getIndicesMap().containsKey("test_index"));
    assertFalse(stateInfo.getIndicesMap().get("test_index").getStarted());
  }

  @Test
  public void testGetStateFileContents_IndexState() throws IOException {
    TestServer server = getTestServer();
    String contents =
        StateCommandUtils.getStateFileContents(
            getVersionManager(),
            SERVICE_NAME,
            server.getGlobalState().getDataResourceForIndex("test_index"),
            StateUtils.INDEX_STATE_FILE);
    IndexStateInfo.Builder builder = IndexStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, builder);
    IndexStateInfo stateInfo = builder.build();
    IndexStateInfo expected =
        ((ImmutableIndexState) server.getGlobalState().getIndex("test_index"))
            .getCurrentStateInfo();
    assertEquals(expected, stateInfo);
  }

  @Test
  public void testGetStateFileContents_FileNotInTar() throws IOException {
    getTestServer();
    String contents =
        StateCommandUtils.getStateFileContents(
            getVersionManager(),
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            "not_state_file");
    assertNull(contents);
  }

  @Test
  public void testWriteStringToFile() throws IOException {
    File file = folder.newFile();
    String testString = "This is a test string \u0394";
    StateCommandUtils.writeStringToFile(testString, file);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (FileInputStream fileInputStream = new FileInputStream(file)) {
      IOUtils.copy(fileInputStream, byteArrayOutputStream);
    }
    String fileString = StateUtils.fromUTF8(byteArrayOutputStream.toByteArray());
    assertEquals(testString, fileString);
  }

  @Test
  public void testWriteStateDataToBackend_GlobalState() throws IOException {
    TestServer server = getTestServer();
    VersionManager versionManager = getVersionManager();
    String contents =
        StateCommandUtils.getStateFileContents(
            versionManager,
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            StateUtils.GLOBAL_STATE_FILE);
    assertEquals(1, server.indices().size());

    GlobalStateInfo.Builder builder = GlobalStateInfo.newBuilder();
    JsonFormat.parser().merge(contents, builder);
    builder.removeIndices("test_index");
    GlobalStateInfo stateInfo = builder.build();

    byte[] stateBytes = StateUtils.toUTF8(JsonFormat.printer().print(stateInfo));
    StateCommandUtils.writeStateDataToBackend(
        versionManager,
        SERVICE_NAME,
        RemoteStateBackend.GLOBAL_STATE_RESOURCE,
        StateUtils.GLOBAL_STATE_FILE,
        stateBytes);

    server.restart();
    assertEquals(0, server.indices().size());
  }

  @Test
  public void testWriteStateDataToBackend_IndexState() throws IOException {
    TestServer server = getTestServer();
    IndexStateInfo currentState =
        ((ImmutableIndexState) server.getGlobalState().getIndex("test_index"))
            .getCurrentStateInfo();
    IndexStateInfo updatedState =
        currentState
            .toBuilder()
            .setLiveSettings(
                IndexLiveSettings.newBuilder()
                    .setSliceMaxSegments(Int32Value.newBuilder().setValue(1).build())
                    .build())
            .build();
    byte[] stateBytes = StateUtils.toUTF8(JsonFormat.printer().print(updatedState));
    StateCommandUtils.writeStateDataToBackend(
        getVersionManager(),
        SERVICE_NAME,
        server.getGlobalState().getDataResourceForIndex("test_index"),
        StateUtils.INDEX_STATE_FILE,
        stateBytes);

    server.restart();
    IndexStateInfo newCurrentState =
        ((ImmutableIndexState) server.getGlobalState().getIndex("test_index"))
            .getCurrentStateInfo();
    assertEquals(updatedState, newCurrentState);
    assertNotEquals(currentState, newCurrentState);
  }

  @Test
  public void testValidateConfigData_ValidGlobalState() throws IOException {
    String stateStr =
        "{\"indices\":{\"test_index\":{\"id\":\"09d9c9e4-483e-4a90-9c4f-d342c8da1210\",\"started\":true}}}";
    StateCommandUtils.validateConfigData(stateStr.getBytes(StandardCharsets.UTF_8), true);
  }

  @Test
  public void testValidateConfigData_InvalidGlobalState() throws IOException {
    String stateStr =
        "{\"ind\":{\"test_index\":{\"id\":\"09d9c9e4-483e-4a90-9c4f-d342c8da1210\",\"started\":true}}}";
    try {
      StateCommandUtils.validateConfigData(stateStr.getBytes(StandardCharsets.UTF_8), false);
      fail();
    } catch (InvalidProtocolBufferException ignore) {
    }
  }

  @Test
  public void testValidateConfigData_GlobalStateAsIndexState() throws IOException {
    String stateStr =
        "{\"indices\":{\"test_index\":{\"id\":\"09d9c9e4-483e-4a90-9c4f-d342c8da1210\",\"started\":true}}}";
    try {
      StateCommandUtils.validateConfigData(stateStr.getBytes(StandardCharsets.UTF_8), false);
      fail();
    } catch (InvalidProtocolBufferException ignore) {
    }
  }

  @Test
  public void testValidateConfigData_ValidIndexState() throws IOException {
    String stateStr =
        "{\"indexName\":\"test_index\",\"gen\":\"10\",\"committed\":true,\"fields\":{\"field1\":{\"name\":\"field1\",\"type\":\"INT\",\"storeDocValues\":true}}}";
    StateCommandUtils.validateConfigData(stateStr.getBytes(StandardCharsets.UTF_8), false);
  }

  @Test
  public void testValidateConfigData_InvalidIndexState() throws IOException {
    String stateStr =
        "{\"indexName\":\"test_index\",\"gen\":\"10\",\"committed\":true,\"fields\":{\"field1\":{\"name\":\"field1\",\"type\":\"INT\",\"docValues\":true}}}";
    try {
      StateCommandUtils.validateConfigData(stateStr.getBytes(StandardCharsets.UTF_8), false);
      fail();
    } catch (InvalidProtocolBufferException ignore) {
    }
  }

  @Test
  public void testValidateConfigData_IndexStateAsGlobalState() throws IOException {
    String stateStr =
        "{\"indexName\":\"test_index\",\"gen\":\"10\",\"committed\":true,\"fields\":{\"field1\":{\"name\":\"field1\",\"type\":\"INT\",\"storeDocValues\":true}}}";
    try {
      StateCommandUtils.validateConfigData(stateStr.getBytes(StandardCharsets.UTF_8), true);
      fail();
    } catch (InvalidProtocolBufferException ignore) {
    }
  }

  @Test
  public void testValidateConfigData_BadEncoding() throws IOException {
    byte[] invalidUtf8 = new byte[1];
    invalidUtf8[0] = (byte) 0xFE;
    try {
      StateCommandUtils.validateConfigData(invalidUtf8, true);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("java.nio.charset.MalformedInputException: Input length = 1", e.getMessage());
    }
  }

  @Test
  public void testGetResourceName_GlobalState() throws IOException {
    String resourceName =
        StateCommandUtils.getResourceName(
            mock(VersionManager.class),
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            false);
    assertEquals(RemoteStateBackend.GLOBAL_STATE_RESOURCE, resourceName);
  }

  @Test
  public void testGetResourceName_GlobalStateExact() throws IOException {
    String resourceName =
        StateCommandUtils.getResourceName(
            mock(VersionManager.class),
            SERVICE_NAME,
            RemoteStateBackend.GLOBAL_STATE_RESOURCE,
            true);
    assertEquals(RemoteStateBackend.GLOBAL_STATE_RESOURCE, resourceName);
  }

  @Test
  public void testGetResourceName_IndexStateExact() throws IOException {
    String resourceName =
        StateCommandUtils.getResourceName(
            mock(VersionManager.class), SERVICE_NAME, "exact-resource-name", true);
    assertEquals("exact-resource-name", resourceName);
  }

  @Test
  public void testGetResourceName_NoGlobalState() throws IOException {
    TestServer.initS3(folder);
    VersionManager versionManager = getVersionManager();
    try {
      StateCommandUtils.getResourceName(versionManager, SERVICE_NAME, "test_index", false);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Unable to load global state for cluster: \"test_server\"", e.getMessage());
    }
  }

  @Test
  public void testGetResourceName_IndexNotExists() throws IOException {
    getTestServer();
    VersionManager versionManager = getVersionManager();
    try {
      StateCommandUtils.getResourceName(versionManager, SERVICE_NAME, "invalid_test_index", false);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(
          "Unable to find index: \"invalid_test_index\" in cluster: \"test_server\"",
          e.getMessage());
    }
  }

  @Test
  public void testGetResourceName_IndexResource() throws IOException {
    TestServer server = getTestServer();
    VersionManager versionManager = getVersionManager();
    String resourceName =
        StateCommandUtils.getResourceName(versionManager, SERVICE_NAME, "test_index", false);
    assertEquals(server.getGlobalState().getDataResourceForIndex("test_index"), resourceName);
  }

  @Test
  public void testGetIndexStateResource() {
    assertEquals("test_index-state", StateCommandUtils.getIndexStateResource("test_index"));
  }

  @Test
  public void testGetStateKey() {
    assertEquals("a/b/c", StateCommandUtils.getStateKey("a", "b", "c"));
  }

  @Test
  public void testGetStateKeyPrefix() {
    assertEquals("a/b/", StateCommandUtils.getStateKeyPrefix("a", "b"));
  }
}
