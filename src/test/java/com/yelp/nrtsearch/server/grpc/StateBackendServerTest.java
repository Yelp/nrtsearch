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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.BoolValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt64Value;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.clientlib.Node;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.index.ImmutableIndexState;
import com.yelp.nrtsearch.server.script.js.JsScriptEngine;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StateBackendServerTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private TestServer primaryServer;
  private TestServer replicaServer;

  @After
  public void cleanup() {
    TestServer.cleanupAll();
  }

  private TestServer buildLocalPrimary() throws IOException {
    return TestServer.builder(folder).build();
  }

  private TestServer buildRemotePrimary() throws IOException {
    return TestServer.builder(folder)
        .withRemoteStateBackend(false)
        .withAutoStartConfig(false, Mode.PRIMARY, 0, IndexDataLocationType.REMOTE)
        .build();
  }

  private TestServer buildLocalReplica() throws IOException {
    return TestServer.builder(folder).build();
  }

  private TestServer buildRemoteReplica() throws IOException {
    return TestServer.builder(folder)
        .withRemoteStateBackend(true)
        .withSyncInitialNrtPoint(false)
        .withAutoStartConfig(false, Mode.REPLICA, 0, IndexDataLocationType.REMOTE)
        .build();
  }

  private StartIndexResponse startIndexOnServer(TestServer server, Mode mode) {
    return startIndexOnServer(server, mode, 0);
  }

  private StartIndexResponse startIndexOnServer(TestServer server, Mode mode, long primaryGen) {
    StartIndexRequest.Builder builder =
        StartIndexRequest.newBuilder().setIndexName("test_index").setMode(mode);
    if (mode == Mode.REPLICA) {
      builder
          .setPrimaryAddress("localhost")
          .setPort(primaryServer.getReplicationPort())
          .setPrimaryGen(primaryGen);
    } else {
      builder.setPrimaryGen(primaryGen);
    }
    return server.startIndex(builder.build());
  }

  private StartIndexResponse startIndexWithRestore(
      TestServer server, Mode mode, boolean deleteExistingData) {
    StartIndexRequest.Builder builder =
        StartIndexRequest.newBuilder()
            .setIndexName("test_index")
            .setMode(mode)
            .setRestore(
                RestoreIndex.newBuilder()
                    .setServiceName(TestServer.SERVICE_NAME)
                    .setResourceName("test_index")
                    .setDeleteExistingData(deleteExistingData)
                    .build());
    if (mode == Mode.REPLICA) {
      builder.setPrimaryAddress("localhost").setPort(primaryServer.getReplicationPort());
    }
    return server.startIndex(builder.build());
  }

  private final List<Field> fields1 =
      List.of(
          Field.newBuilder()
              .setName("id")
              .setType(FieldType._ID)
              .setStoreDocValues(true)
              .setSearch(true)
              .build(),
          Field.newBuilder()
              .setName("field1")
              .setStoreDocValues(true)
              .setType(FieldType.FLOAT)
              .build(),
          Field.newBuilder()
              .setName("field2")
              .setStoreDocValues(true)
              .setSearch(true)
              .setType(FieldType.ATOM)
              .build());

  private final List<Field> fields2 =
      List.of(
          Field.newBuilder()
              .setName("field3")
              .setStoreDocValues(true)
              .setSearch(true)
              .setType(FieldType.LONG)
              .build(),
          Field.newBuilder()
              .setName("field4")
              .setType(FieldType.VIRTUAL)
              .setScript(
                  Script.newBuilder()
                      .setLang(JsScriptEngine.LANG)
                      .setSource("field1 * 2.0")
                      .build())
              .build());

  private final List<AddDocumentRequest> docs1 =
      List.of(
          AddDocumentRequest.newBuilder()
              .setIndexName("test_index")
              .putFields("id", MultiValuedField.newBuilder().addValue("1").build())
              .putFields("field1", MultiValuedField.newBuilder().addValue("1.1").build())
              .putFields("field2", MultiValuedField.newBuilder().addValue("atom_1").build())
              .putFields("field3", MultiValuedField.newBuilder().addValue("11").build())
              .build());

  private final List<AddDocumentRequest> docs2 =
      List.of(
          AddDocumentRequest.newBuilder()
              .setIndexName("test_index")
              .putFields("id", MultiValuedField.newBuilder().addValue("2").build())
              .putFields("field1", MultiValuedField.newBuilder().addValue("3.0").build())
              .putFields("field2", MultiValuedField.newBuilder().addValue("atom_2").build())
              .putFields("field3", MultiValuedField.newBuilder().addValue("22").build())
              .build());

  private final List<AddDocumentRequest> docs3 =
      List.of(
          AddDocumentRequest.newBuilder()
              .setIndexName("test_index")
              .putFields("id", MultiValuedField.newBuilder().addValue("3").build())
              .putFields("field1", MultiValuedField.newBuilder().addValue("5.0").build())
              .putFields("field2", MultiValuedField.newBuilder().addValue("atom_3").build())
              .build());

  private final List<String> fieldList = List.of("id", "field1", "field2", "field3", "field4");
  private final List<String> subFieldList = List.of("id", "field1", "field2");

  private void verifyDocs(int expectedCount, TestServer server) {
    SearchResponse response =
        server
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName("test_index")
                    .addAllRetrieveFields(fieldList)
                    .setTopHits(expectedCount + 1)
                    .setStartHit(0)
                    .build());
    assertEquals(expectedCount, response.getHitsCount());
    for (Hit hit : response.getHitsList()) {
      String id = hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue();
      if ("1".equals(id)) {
        assertEquals(
            1.1f, hit.getFieldsOrThrow("field1").getFieldValue(0).getFloatValue(), Math.ulp(1.1f));
        assertEquals("atom_1", hit.getFieldsOrThrow("field2").getFieldValue(0).getTextValue());
        assertEquals(11, hit.getFieldsOrThrow("field3").getFieldValue(0).getLongValue());
        assertEquals(
            2.2f, hit.getFieldsOrThrow("field4").getFieldValue(0).getDoubleValue(), Math.ulp(2.2f));
      } else if ("2".equals(id)) {
        assertEquals(
            3.0f, hit.getFieldsOrThrow("field1").getFieldValue(0).getFloatValue(), Math.ulp(3.0f));
        assertEquals("atom_2", hit.getFieldsOrThrow("field2").getFieldValue(0).getTextValue());
        assertEquals(22, hit.getFieldsOrThrow("field3").getFieldValue(0).getLongValue());
        assertEquals(
            6.0f, hit.getFieldsOrThrow("field4").getFieldValue(0).getDoubleValue(), Math.ulp(6.0f));
      } else if ("3".equals(id)) {
        assertEquals(
            5.0f, hit.getFieldsOrThrow("field1").getFieldValue(0).getFloatValue(), Math.ulp(5.0f));
        assertEquals("atom_3", hit.getFieldsOrThrow("field2").getFieldValue(0).getTextValue());
        assertEquals(0, hit.getFieldsOrThrow("field3").getFieldValueCount());
        assertEquals(
            10.0f,
            hit.getFieldsOrThrow("field4").getFieldValue(0).getDoubleValue(),
            Math.ulp(10.0f));
      } else {
        throw new RuntimeException("Unknown hit: " + hit);
      }
    }
  }

  private void verifySubFieldDocs(int expectedCount, TestServer server) {
    SearchResponse response =
        server
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName("test_index")
                    .addAllRetrieveFields(subFieldList)
                    .setTopHits(expectedCount + 1)
                    .setStartHit(0)
                    .build());
    assertEquals(expectedCount, response.getHitsCount());
    for (Hit hit : response.getHitsList()) {
      String id = hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue();
      if ("1".equals(id)) {
        assertEquals(
            1.1f, hit.getFieldsOrThrow("field1").getFieldValue(0).getFloatValue(), Math.ulp(1.1f));
        assertEquals("atom_1", hit.getFieldsOrThrow("field2").getFieldValue(0).getTextValue());
      } else if ("2".equals(id)) {
        assertEquals(
            3.0f, hit.getFieldsOrThrow("field1").getFieldValue(0).getFloatValue(), Math.ulp(3.0f));
        assertEquals("atom_2", hit.getFieldsOrThrow("field2").getFieldValue(0).getTextValue());
      } else if ("3".equals(id)) {
        assertEquals(
            5.0f, hit.getFieldsOrThrow("field1").getFieldValue(0).getFloatValue(), Math.ulp(5.0f));
        assertEquals("atom_3", hit.getFieldsOrThrow("field2").getFieldValue(0).getTextValue());
      } else {
        throw new RuntimeException("Unknown hit: " + hit);
      }
    }
  }

  private IndexStateInfo getIndexState(String indexName, TestServer server) throws IOException {
    StateResponse response =
        server
            .getClient()
            .getBlockingStub()
            .state(StateRequest.newBuilder().setIndexName(indexName).build());
    JsonObject root = JsonParser.parseString(response.getResponse()).getAsJsonObject();
    String indexStateJson = root.get("state").toString();
    IndexStateInfo.Builder builder = IndexStateInfo.newBuilder();
    JsonFormat.parser().merge(indexStateJson, builder);
    return builder.build();
  }

  private Map<String, Field> getFieldMap(String jsonFieldMap) throws IOException {
    JsonObject root = JsonParser.parseString(jsonFieldMap).getAsJsonObject();
    Map<String, Field> resultsMap = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : root.entrySet()) {
      Field.Builder builder = Field.newBuilder();
      JsonFormat.parser().merge(entry.getValue().toString(), builder);
      resultsMap.put(entry.getKey(), builder.build());
    }
    return resultsMap;
  }

  private void createIndices() {
    assertEquals(
        "Created Index name: test_index", primaryServer.createIndex("test_index").getResponse());
    assertEquals(
        "Created Index name: test_index_2",
        primaryServer.createIndex("test_index_2").getResponse());
    assertEquals(
        "Created Index name: test_index_3",
        primaryServer.createIndex("test_index_3").getResponse());
  }

  private void createIndex() {
    assertEquals(
        "Created Index name: test_index", primaryServer.createIndex("test_index").getResponse());
  }

  private void createIndexWithFields() {
    createIndex();
    primaryServer.registerFields("test_index", fields1);
    primaryServer.registerFields("test_index", fields2);
  }

  private void writeNodeFile(List<Node> nodes, String filePath) throws IOException {
    String fileStr = new ObjectMapper().writeValueAsString(nodes);
    try (FileOutputStream outputStream = new FileOutputStream(filePath)) {
      outputStream.write(fileStr.getBytes());
    }
  }

  @Test
  public void testStartServer() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
  }

  @Test
  public void testRestartServer() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
  }

  @Test
  public void testCreateIndices() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndices();

    assertEquals(Set.of("test_index", "test_index_2", "test_index_3"), primaryServer.indices());
  }

  @Test
  public void testIndicesPersistRestart() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndices();
    primaryServer.restart();

    assertEquals(Set.of("test_index", "test_index_2", "test_index_3"), primaryServer.indices());
  }

  @Test
  public void testSetIndexSettings() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();
    SettingsV2Response response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .settingsV2(SettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_SETTINGS, response.getSettings());

    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .settingsV2(
                SettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setSettings(
                        IndexSettings.newBuilder()
                            .setNrtCachingDirectoryMaxSizeMB(
                                DoubleValue.newBuilder().setValue(120.0).build())
                            .setNrtCachingDirectoryMaxMergeSizeMB(
                                DoubleValue.newBuilder().setValue(60.0).build())
                            .setIndexMergeSchedulerAutoThrottle(
                                BoolValue.newBuilder().setValue(true).build())
                            .build())
                    .build());
    IndexSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS.toBuilder()
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(120.0).build())
            .setNrtCachingDirectoryMaxMergeSizeMB(DoubleValue.newBuilder().setValue(60.0).build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .build();
    assertEquals(expectedSettings, response.getSettings());
  }

  @Test
  public void testIndexSettingsPersistRestart() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();

    primaryServer
        .getClient()
        .getBlockingStub()
        .settingsV2(
            SettingsV2Request.newBuilder()
                .setIndexName("test_index")
                .setSettings(
                    IndexSettings.newBuilder()
                        .setNrtCachingDirectoryMaxSizeMB(
                            DoubleValue.newBuilder().setValue(120.0).build())
                        .setNrtCachingDirectoryMaxMergeSizeMB(
                            DoubleValue.newBuilder().setValue(60.0).build())
                        .setIndexMergeSchedulerAutoThrottle(
                            BoolValue.newBuilder().setValue(true).build())
                        .build())
                .build());
    IndexSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS.toBuilder()
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(120.0).build())
            .setNrtCachingDirectoryMaxMergeSizeMB(DoubleValue.newBuilder().setValue(60.0).build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .build();

    primaryServer.restart();
    SettingsV2Response response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .settingsV2(SettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(expectedSettings, response.getSettings());
  }

  @Test
  public void testSetIndexLiveSettings() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();
    LiveSettingsV2Response response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());

    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(
                LiveSettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setLiveSettings(
                        IndexLiveSettings.newBuilder()
                            .setDefaultTerminateAfter(
                                Int32Value.newBuilder().setValue(1000).build())
                            .setDefaultTerminateAfterMaxRecallCount(
                                Int32Value.newBuilder().setValue(1000).build())
                            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
                            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
                            .setDefaultSearchTimeoutSec(
                                DoubleValue.newBuilder().setValue(5.1).build())
                            .build())
                    .build());
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
            .setDefaultTerminateAfterMaxRecallCount(Int32Value.newBuilder().setValue(1000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .build();
    assertEquals(expectedSettings, response.getLiveSettings());
  }

  @Test
  public void testIndexLiveSettingsPersistRestart() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();

    primaryServer
        .getClient()
        .getBlockingStub()
        .liveSettingsV2(
            LiveSettingsV2Request.newBuilder()
                .setIndexName("test_index")
                .setLiveSettings(
                    IndexLiveSettings.newBuilder()
                        .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
                        .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
                        .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
                        .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
                        .build())
                .build());
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .build();

    primaryServer.restart();
    LiveSettingsV2Response response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(expectedSettings, response.getLiveSettings());
  }

  @Test
  public void testSetLocalIndexLiveSettings() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();
    LiveSettingsV2Response response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());

    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(
                LiveSettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setLiveSettings(
                        IndexLiveSettings.newBuilder()
                            .setDefaultTerminateAfter(
                                Int32Value.newBuilder().setValue(1000).build())
                            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
                            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
                            .setDefaultSearchTimeoutSec(
                                DoubleValue.newBuilder().setValue(5.1).build())
                            .build())
                    .setLocal(true)
                    .build());
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .build();
    assertEquals(expectedSettings, response.getLiveSettings());
    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(
                LiveSettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setLocal(true)
                    .build());
    assertEquals(expectedSettings, response.getLiveSettings());

    // live settings without local
    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());
  }

  @Test
  public void testUpdateLocalIndexLiveSettings() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();
    LiveSettingsV2Response response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());

    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(
                LiveSettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setLiveSettings(
                        IndexLiveSettings.newBuilder()
                            .setDefaultTerminateAfter(
                                Int32Value.newBuilder().setValue(1000).build())
                            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
                            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
                            .setDefaultSearchTimeoutSec(
                                DoubleValue.newBuilder().setValue(5.1).build())
                            .build())
                    .setLocal(true)
                    .build());
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .build();
    assertEquals(expectedSettings, response.getLiveSettings());

    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(
                LiveSettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setLiveSettings(
                        IndexLiveSettings.newBuilder()
                            .setDefaultTerminateAfter(
                                Int32Value.newBuilder().setValue(2000).build())
                            .setSliceMaxDocs(Int32Value.newBuilder().setValue(500).build())
                            .build())
                    .setLocal(true)
                    .build());
    expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(2000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .setSliceMaxDocs(Int32Value.newBuilder().setValue(500).build())
            .build();
    assertEquals(expectedSettings, response.getLiveSettings());

    // live settings without local
    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());
  }

  @Test
  public void testSetLocalIndexLiveSettingsEphemeral() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();
    LiveSettingsV2Response response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());

    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(
                LiveSettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setLiveSettings(
                        IndexLiveSettings.newBuilder()
                            .setDefaultTerminateAfter(
                                Int32Value.newBuilder().setValue(1000).build())
                            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
                            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
                            .setDefaultSearchTimeoutSec(
                                DoubleValue.newBuilder().setValue(5.1).build())
                            .build())
                    .setLocal(true)
                    .build());
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .build();
    assertEquals(expectedSettings, response.getLiveSettings());

    // live settings without local
    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());

    primaryServer.restart();

    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());
    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(
                LiveSettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setLocal(true)
                    .build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());
  }

  @Test
  public void testSetLocalIndexLiveSettingsReplica() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    startIndexOnServer(primaryServer, Mode.PRIMARY);

    replicaServer = buildLocalReplica();
    startIndexOnServer(replicaServer, Mode.REPLICA);

    LiveSettingsV2Response response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());

    response =
        replicaServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());

    response =
        replicaServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(
                LiveSettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setLiveSettings(
                        IndexLiveSettings.newBuilder()
                            .setDefaultTerminateAfter(
                                Int32Value.newBuilder().setValue(1000).build())
                            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
                            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
                            .setDefaultSearchTimeoutSec(
                                DoubleValue.newBuilder().setValue(5.1).build())
                            .build())
                    .setLocal(true)
                    .build());
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .build();
    assertEquals(expectedSettings, response.getLiveSettings());

    // live settings without local
    response =
        replicaServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());

    // primary unaffected
    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());
    response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .liveSettingsV2(
                LiveSettingsV2Request.newBuilder()
                    .setIndexName("test_index")
                    .setLocal(true)
                    .build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());
  }

  @Test
  public void testSetIndexFields() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();

    FieldDefResponse response = primaryServer.registerFields("test_index", fields1);
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryServer);
    Map<String, Field> fieldMap = getFieldMap(response.getResponse());

    assertEquals(3, indexStateInfo.getFieldsCount());
    assertEquals(fields1.get(0), indexStateInfo.getFieldsMap().get("id"));
    assertEquals(fields1.get(1), indexStateInfo.getFieldsMap().get("field1"));
    assertEquals(fields1.get(2), indexStateInfo.getFieldsMap().get("field2"));
    assertEquals(fieldMap, indexStateInfo.getFieldsMap());

    response = primaryServer.registerFields("test_index", fields2);
    indexStateInfo = getIndexState("test_index", primaryServer);
    fieldMap = getFieldMap(response.getResponse());

    assertEquals(5, indexStateInfo.getFieldsCount());
    assertEquals(fields1.get(0), indexStateInfo.getFieldsMap().get("id"));
    assertEquals(fields1.get(1), indexStateInfo.getFieldsMap().get("field1"));
    assertEquals(fields1.get(2), indexStateInfo.getFieldsMap().get("field2"));
    assertEquals(fields2.get(0), indexStateInfo.getFieldsMap().get("field3"));
    assertEquals(fields2.get(1), indexStateInfo.getFieldsMap().get("field4"));
    assertEquals(fieldMap, indexStateInfo.getFieldsMap());
  }

  @Test
  public void testIndexFieldsPersistRestart() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();

    FieldDefResponse response = primaryServer.registerFields("test_index", fields1);

    primaryServer.restart();

    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryServer);
    Map<String, Field> fieldMap = getFieldMap(response.getResponse());

    assertEquals(3, indexStateInfo.getFieldsCount());
    assertEquals(fields1.get(0), indexStateInfo.getFieldsMap().get("id"));
    assertEquals(fields1.get(1), indexStateInfo.getFieldsMap().get("field1"));
    assertEquals(fields1.get(2), indexStateInfo.getFieldsMap().get("field2"));
    assertEquals(fieldMap, indexStateInfo.getFieldsMap());

    response = primaryServer.registerFields("test_index", fields2);

    primaryServer.restart();

    indexStateInfo = getIndexState("test_index", primaryServer);
    fieldMap = getFieldMap(response.getResponse());

    assertEquals(5, indexStateInfo.getFieldsCount());
    assertEquals(fields1.get(0), indexStateInfo.getFieldsMap().get("id"));
    assertEquals(fields1.get(1), indexStateInfo.getFieldsMap().get("field1"));
    assertEquals(fields1.get(2), indexStateInfo.getFieldsMap().get("field2"));
    assertEquals(fields2.get(0), indexStateInfo.getFieldsMap().get("field3"));
    assertEquals(fields2.get(1), indexStateInfo.getFieldsMap().get("field4"));
    assertEquals(fieldMap, indexStateInfo.getFieldsMap());
  }

  @Test
  public void testCompleteState() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();

    IndexLiveSettings liveSettings =
        IndexLiveSettings.newBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .build();
    IndexSettings settings =
        IndexSettings.newBuilder()
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(120.0).build())
            .setNrtCachingDirectoryMaxMergeSizeMB(DoubleValue.newBuilder().setValue(60.0).build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .build();

    primaryServer.registerFields("test_index", fields1);
    primaryServer.registerFields("test_index", fields2);
    primaryServer
        .getClient()
        .getBlockingStub()
        .liveSettingsV2(
            LiveSettingsV2Request.newBuilder()
                .setIndexName("test_index")
                .setLiveSettings(liveSettings)
                .build());
    primaryServer
        .getClient()
        .getBlockingStub()
        .settingsV2(
            SettingsV2Request.newBuilder()
                .setIndexName("test_index")
                .setSettings(settings)
                .build());

    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(5, indexStateInfo.getFieldsCount());
    assertEquals(fields1.get(0), indexStateInfo.getFieldsMap().get("id"));
    assertEquals(fields1.get(1), indexStateInfo.getFieldsMap().get("field1"));
    assertEquals(fields1.get(2), indexStateInfo.getFieldsMap().get("field2"));
    assertEquals(fields2.get(0), indexStateInfo.getFieldsMap().get("field3"));
    assertEquals(fields2.get(1), indexStateInfo.getFieldsMap().get("field4"));
    assertEquals(liveSettings, indexStateInfo.getLiveSettings());
    assertEquals(settings, indexStateInfo.getSettings());

    primaryServer.restart();

    indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(5, indexStateInfo.getFieldsCount());
    assertEquals(fields1.get(0), indexStateInfo.getFieldsMap().get("id"));
    assertEquals(fields1.get(1), indexStateInfo.getFieldsMap().get("field1"));
    assertEquals(fields1.get(2), indexStateInfo.getFieldsMap().get("field2"));
    assertEquals(fields2.get(0), indexStateInfo.getFieldsMap().get("field3"));
    assertEquals(fields2.get(1), indexStateInfo.getFieldsMap().get("field4"));
    assertEquals(liveSettings, indexStateInfo.getLiveSettings());
    assertEquals(settings, indexStateInfo.getSettings());
  }

  @Test
  public void testIndexAlreadyExists() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();
    try {
      createIndex();
      fail();
    } catch (StatusRuntimeException e) {
      assertEquals(Status.ALREADY_EXISTS.getCode(), e.getStatus().getCode());
      assertEquals("ALREADY_EXISTS: Index test_index already exists", e.getMessage());
    }
  }

  @Test
  public void testRecreateIndex() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();

    primaryServer.registerFields("test_index", fields1);
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(3, indexStateInfo.getFieldsCount());
    Path dataDir = primaryServer.getGlobalState().getIndexDirBase();
    File[] indexFolders = dataDir.toFile().listFiles();
    assertEquals(1, indexFolders.length);
    String indexUniqueName = indexFolders[0].getName();
    assertTrue(indexUniqueName.startsWith("test_index"));

    DeleteIndexResponse response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .deleteIndex(DeleteIndexRequest.newBuilder().setIndexName("test_index").build());
    assertEquals("ok", response.getOk());
    indexFolders = dataDir.toFile().listFiles();
    assertEquals(0, indexFolders.length);

    createIndex();
    indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(1, indexStateInfo.getGen());
    assertTrue(indexStateInfo.getFieldsMap().isEmpty());

    indexFolders = dataDir.toFile().listFiles();
    assertEquals(1, indexFolders.length);
    String newIndexUniqueName = indexFolders[0].getName();
    assertTrue(newIndexUniqueName.startsWith("test_index"));
    assertNotEquals(newIndexUniqueName, indexUniqueName);

    primaryServer.registerFields("test_index", fields1);
    indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(3, indexStateInfo.getFieldsCount());
  }

  @Test
  public void testStartIndexPrimary() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryServer);
    assertTrue(indexStateInfo.getCommitted());
  }

  @Test
  public void testStartIndexStandalone() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.STANDALONE);
    assertEquals(0, response.getNumDocs());
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryServer);
    assertTrue(indexStateInfo.getCommitted());
  }

  @Test
  public void testStartIndexReplica() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryServer);
    assertTrue(indexStateInfo.getCommitted());

    replicaServer = buildLocalReplica();
    response = startIndexOnServer(replicaServer, Mode.REPLICA);
    assertEquals(0, response.getNumDocs());
    indexStateInfo = getIndexState("test_index", replicaServer);
    assertTrue(indexStateInfo.getCommitted());
    assertEquals(5, indexStateInfo.getFieldsMap().size());
  }

  @Test
  public void testInitialNrtPointSync() throws Exception {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    primaryServer.addDocs(docs1.stream());
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    replicaServer = buildLocalReplica();
    response = startIndexOnServer(replicaServer, Mode.REPLICA);
    assertEquals(1, response.getNumDocs());
    verifyDocs(1, replicaServer);
  }

  @Test
  public void testIndexStopStart() throws Exception {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    primaryServer.stopIndex("test_index");

    response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    primaryServer.addDocs(docs1.stream());
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);
  }

  @Test
  public void testIndexStopStartExistingDoc() throws Exception {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    primaryServer.addDocs(docs1.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    primaryServer.stopIndex("test_index");

    response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(1, response.getNumDocs());

    primaryServer.addDocs(docs2.stream());
    primaryServer.refresh("test_index");
    verifyDocs(2, primaryServer);
  }

  @Test
  public void testIndexStopStartExistingDocStandalone() throws Exception {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.STANDALONE);
    assertEquals(0, response.getNumDocs());

    primaryServer.addDocs(docs1.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    primaryServer.stopIndex("test_index");

    response = startIndexOnServer(primaryServer, Mode.STANDALONE);
    assertEquals(1, response.getNumDocs());

    primaryServer.addDocs(docs2.stream());
    primaryServer.refresh("test_index");
    verifyDocs(2, primaryServer);
  }

  @Test
  public void testIndexStopStartReplica() throws Exception {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    primaryServer.addDocs(docs1.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    replicaServer = buildLocalReplica();
    response = startIndexOnServer(replicaServer, Mode.REPLICA);
    assertEquals(1, response.getNumDocs());
    verifyDocs(1, replicaServer);

    replicaServer.stopIndex("test_index");

    response = startIndexOnServer(replicaServer, Mode.REPLICA);
    assertEquals(1, response.getNumDocs());
    verifyDocs(1, replicaServer);
  }

  @Test
  public void testIndexAlreadyStarted() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    try {
      startIndexOnServer(primaryServer, Mode.PRIMARY);
      fail();
    } catch (StatusRuntimeException e) {
      assertEquals(
          "INTERNAL: Error handling startIndex request\nIndex test_index is already started",
          e.getMessage());
    }
  }

  @Test
  public void testRecreateStartedIndex() throws Exception {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();

    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(5, indexStateInfo.getFieldsCount());
    Path dataDir = primaryServer.getGlobalState().getIndexDirBase();
    File[] indexFolders = dataDir.toFile().listFiles();
    assertEquals(1, indexFolders.length);
    String indexUniqueName = indexFolders[0].getName();
    assertTrue(indexUniqueName.startsWith("test_index"));

    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());
    primaryServer.addDocs(docs1.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    DeleteIndexResponse delResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .deleteIndex(DeleteIndexRequest.newBuilder().setIndexName("test_index").build());
    assertEquals("ok", delResponse.getOk());
    indexFolders = dataDir.toFile().listFiles();
    assertEquals(0, indexFolders.length);

    createIndex();
    indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(1, indexStateInfo.getGen());
    assertTrue(indexStateInfo.getFieldsMap().isEmpty());

    indexFolders = dataDir.toFile().listFiles();
    assertEquals(1, indexFolders.length);
    String newIndexUniqueName = indexFolders[0].getName();
    assertTrue(newIndexUniqueName.startsWith("test_index"));
    assertNotEquals(newIndexUniqueName, indexUniqueName);

    primaryServer.registerFields("test_index", fields1);
    indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(3, indexStateInfo.getFieldsCount());
    primaryServer.registerFields("test_index", fields2);
    indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(5, indexStateInfo.getFieldsCount());

    response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());
    primaryServer.addDocs(docs1.stream());
    primaryServer.addDocs(docs2.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(2, primaryServer);
  }

  @Test
  public void testSchemaChangeWithIndexing() throws Exception {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();

    startIndexOnServer(primaryServer, Mode.PRIMARY);

    primaryServer.registerFields("test_index", fields1);
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(3, indexStateInfo.getFieldsCount());

    primaryServer.addDocs(docs3.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");

    primaryServer.registerFields("test_index", fields2);
    indexStateInfo = getIndexState("test_index", primaryServer);
    assertEquals(5, indexStateInfo.getFieldsCount());

    primaryServer.addDocs(docs1.stream());
    primaryServer.addDocs(docs2.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(3, primaryServer);
  }

  @Test
  public void testSettingsV1All() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();

    SettingsRequest request =
        SettingsRequest.newBuilder()
            .setIndexName("test_index")
            .setNrtCachingDirectoryMaxSizeMB(101.0)
            .setNrtCachingDirectoryMaxMergeSizeMB(51.0)
            .setConcurrentMergeSchedulerMaxMergeCount(10)
            .setConcurrentMergeSchedulerMaxThreadCount(5)
            .setIndexSort(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("field1").setReverse(true).build())
                    .build())
            .setIndexMergeSchedulerAutoThrottle(true)
            .setDirectory("MMapDirectory")
            .build();

    SettingsResponse response = primaryServer.getClient().getBlockingStub().settings(request);
    IndexSettings expectedSettings =
        IndexSettings.newBuilder()
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(101.0).build())
            .setNrtCachingDirectoryMaxMergeSizeMB(DoubleValue.newBuilder().setValue(51.0).build())
            .setConcurrentMergeSchedulerMaxMergeCount(Int32Value.newBuilder().setValue(10).build())
            .setConcurrentMergeSchedulerMaxThreadCount(Int32Value.newBuilder().setValue(5).build())
            .setIndexSort(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("field1").setReverse(true).build())
                    .build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .setDirectory(StringValue.newBuilder().setValue("MMapDirectory").build())
            .setMaxFullFlushMergeWaitMillis(UInt64Value.newBuilder().setValue(500).build())
            .build();

    IndexSettings.Builder builder = IndexSettings.newBuilder();
    JsonFormat.parser().merge(response.getResponse(), builder);
    assertEquals(expectedSettings, builder.build());
  }

  @Test
  public void testSettingsV1Partial() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();

    SettingsRequest request =
        SettingsRequest.newBuilder()
            .setIndexName("test_index")
            .setNrtCachingDirectoryMaxSizeMB(101.0)
            .setNrtCachingDirectoryMaxMergeSizeMB(51.0)
            .setIndexSort(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("field1").setReverse(true).build())
                    .build())
            .setIndexMergeSchedulerAutoThrottle(true)
            .build();

    SettingsResponse response = primaryServer.getClient().getBlockingStub().settings(request);
    IndexSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS.toBuilder()
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(101.0).build())
            .setNrtCachingDirectoryMaxMergeSizeMB(DoubleValue.newBuilder().setValue(51.0).build())
            .setIndexSort(
                SortFields.newBuilder()
                    .addSortedFields(
                        SortType.newBuilder().setFieldName("field1").setReverse(true).build())
                    .build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .build();

    IndexSettings.Builder builder = IndexSettings.newBuilder();
    JsonFormat.parser().merge(response.getResponse(), builder);
    assertEquals(expectedSettings, builder.build());
  }

  @Test
  public void testLiveSettingsV1All() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();

    LiveSettingsRequest request =
        LiveSettingsRequest.newBuilder()
            .setIndexName("test_index")
            .setMaxRefreshSec(30.0)
            .setMinRefreshSec(10.0)
            .setMaxSearcherAgeSec(120.0)
            .setIndexRamBufferSizeMB(128.0)
            .setAddDocumentsMaxBufferLen(250)
            .setSliceMaxDocs(100)
            .setSliceMaxSegments(50)
            .setVirtualShards(3)
            .setMaxMergedSegmentMB(150)
            .setSegmentsPerTier(25)
            .setDefaultSearchTimeoutSec(13.0)
            .setDefaultSearchTimeoutCheckEvery(500)
            .setDefaultTerminateAfter(5000)
            .setDefaultTerminateAfterMaxRecallCount(6000)
            .setDeletePctAllowed(20.0)
            .build();

    LiveSettingsResponse response =
        primaryServer.getClient().getBlockingStub().liveSettings(request);
    IndexLiveSettings expectedSettings =
        IndexLiveSettings.newBuilder()
            .setMaxRefreshSec(DoubleValue.newBuilder().setValue(30.0).build())
            .setMinRefreshSec(DoubleValue.newBuilder().setValue(10.0).build())
            .setMaxSearcherAgeSec(DoubleValue.newBuilder().setValue(120.0).build())
            .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(128.0).build())
            .setAddDocumentsMaxBufferLen(Int32Value.newBuilder().setValue(250).build())
            .setSliceMaxDocs(Int32Value.newBuilder().setValue(100).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setVirtualShards(Int32Value.newBuilder().setValue(3).build())
            .setMaxMergedSegmentMB(Int32Value.newBuilder().setValue(150).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(25).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(13.0).build())
            .setDefaultSearchTimeoutCheckEvery(Int32Value.newBuilder().setValue(500).build())
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(5000).build())
            .setDefaultTerminateAfterMaxRecallCount(Int32Value.newBuilder().setValue(6000).build())
            .setMaxMergePreCopyDurationSec(UInt64Value.newBuilder().setValue(0))
            .setVerboseMetrics(BoolValue.newBuilder().setValue(false).build())
            .setParallelFetchByField(BoolValue.newBuilder().setValue(false).build())
            .setParallelFetchChunkSize(Int32Value.newBuilder().setValue(50).build())
            .setDeletePctAllowed(DoubleValue.newBuilder().setValue(20.0).build())
            .build();

    IndexLiveSettings.Builder builder = IndexLiveSettings.newBuilder();
    JsonFormat.parser().merge(response.getResponse(), builder);
    assertEquals(expectedSettings, builder.build());
  }

  @Test
  public void testLiveSettingsV1Partial() throws IOException {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();

    LiveSettingsRequest request =
        LiveSettingsRequest.newBuilder()
            .setIndexName("test_index")
            .setMaxRefreshSec(30.0)
            .setMaxSearcherAgeSec(120.0)
            .setIndexRamBufferSizeMB(128.0)
            .setSliceMaxSegments(50)
            .setMaxMergedSegmentMB(150)
            .setDefaultSearchTimeoutSec(13.0)
            .setDefaultSearchTimeoutCheckEvery(500)
            .setDefaultTerminateAfter(5000)
            .build();

    LiveSettingsResponse response =
        primaryServer.getClient().getBlockingStub().liveSettings(request);
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS.toBuilder()
            .setMaxRefreshSec(DoubleValue.newBuilder().setValue(30.0).build())
            .setMaxSearcherAgeSec(DoubleValue.newBuilder().setValue(120.0).build())
            .setIndexRamBufferSizeMB(DoubleValue.newBuilder().setValue(128.0).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setMaxMergedSegmentMB(Int32Value.newBuilder().setValue(150).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(13.0).build())
            .setDefaultSearchTimeoutCheckEvery(Int32Value.newBuilder().setValue(500).build())
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(5000).build())
            .build();

    IndexLiveSettings.Builder builder = IndexLiveSettings.newBuilder();
    JsonFormat.parser().merge(response.getResponse(), builder);
    assertEquals(expectedSettings, builder.build());
  }

  @Test
  public void testStartServerWithRemote() throws IOException {
    primaryServer = buildRemotePrimary();
    assertTrue(primaryServer.indices().isEmpty());
  }

  @Test
  public void testStartWithRestore() throws Exception {
    primaryServer = buildRemotePrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    startIndexOnServer(primaryServer, Mode.PRIMARY);
    primaryServer.addDocs(docs1.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    primaryServer.stopIndex("test_index");

    primaryServer.restart();
    startIndexWithRestore(primaryServer, Mode.PRIMARY, true);
    verifyDocs(1, primaryServer);
  }

  @Test
  public void testStartRestoreNoCommit() throws Exception {
    primaryServer = buildRemotePrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    startIndexWithRestore(primaryServer, Mode.PRIMARY, false);
    primaryServer.addDocs(docs1.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    primaryServer.stopIndex("test_index");

    primaryServer.restart();
    startIndexWithRestore(primaryServer, Mode.PRIMARY, true);
    verifyDocs(1, primaryServer);
  }

  @Test
  public void testReplicaRestore() throws Exception {
    primaryServer = buildRemotePrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    startIndexOnServer(primaryServer, Mode.PRIMARY);
    primaryServer.addDocs(docs1.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    primaryServer.stopIndex("test_index");

    replicaServer = buildRemoteReplica();
    startIndexWithRestore(replicaServer, Mode.REPLICA, true);
    verifyDocs(1, replicaServer);
  }

  @Test
  public void testReplicaReDownloadsIndexData() throws Exception {
    primaryServer = buildRemotePrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    startIndexOnServer(primaryServer, Mode.PRIMARY);
    primaryServer.addDocs(docs1.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    replicaServer = buildRemoteReplica();
    startIndexWithRestore(replicaServer, Mode.REPLICA, true);
    verifyDocs(1, replicaServer);
    replicaServer.stopIndex("test_index");

    // commit more docs on primary
    primaryServer.addDocs(docs2.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(2, primaryServer);

    // start index and pull latest restore
    startIndexWithRestore(replicaServer, Mode.REPLICA, true);
    verifyDocs(2, replicaServer);
  }

  @Test
  public void testReplicaRestoreSchemaChange() throws Exception {
    primaryServer = buildRemotePrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndex();
    primaryServer.registerFields("test_index", fields1);

    replicaServer = buildRemoteReplica();

    primaryServer.registerFields("test_index", fields2);
    startIndexWithRestore(primaryServer, Mode.PRIMARY, true);
    primaryServer.addDocs(docs1.stream());
    primaryServer.addDocs(docs2.stream());
    primaryServer.addDocs(docs3.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(3, primaryServer);

    startIndexWithRestore(replicaServer, Mode.REPLICA, false);
    verifySubFieldDocs(3, replicaServer);

    replicaServer.stopIndex("test_index");
    replicaServer.restart();
    startIndexWithRestore(replicaServer, Mode.REPLICA, true);
    verifyDocs(3, replicaServer);
  }

  @Test
  public void testStartReplicaNoGlobalState() throws IOException {
    try {
      replicaServer = buildRemoteReplica();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Cannot update remote state when configured as read only", e.getMessage());
    }
  }

  @Test
  public void testAutoPrimaryGeneration() throws Exception {
    primaryServer = buildRemotePrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    startIndexOnServer(primaryServer, Mode.PRIMARY, -1);
    primaryServer.addDocs(docs1.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);
    primaryServer.stopIndex("test_index");
    primaryServer.stop();

    replicaServer = buildRemoteReplica();
    replicaServer.startIndex(
        StartIndexRequest.newBuilder()
            .setIndexName("test_index")
            .setMode(Mode.REPLICA)
            .setPrimaryAddress("localhost")
            .setPort(0)
            .setPrimaryGen(-1)
            .setRestore(
                RestoreIndex.newBuilder()
                    .setServiceName(TestServer.SERVICE_NAME)
                    .setResourceName("test_index")
                    .setDeleteExistingData(true)
                    .build())
            .build());

    verifyDocs(1, replicaServer);

    primaryServer.restart();
    startIndexOnServer(primaryServer, Mode.PRIMARY, -1);
    primaryServer.addDocs(docs2.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(2, primaryServer);
    primaryServer.stopIndex("test_index");
    primaryServer.stop();
    replicaServer.stopIndex("test_index");

    replicaServer.restart();
    replicaServer.startIndex(
        StartIndexRequest.newBuilder()
            .setIndexName("test_index")
            .setMode(Mode.REPLICA)
            .setPrimaryAddress("localhost")
            .setPrimaryGen(-1)
            .setRestore(
                RestoreIndex.newBuilder()
                    .setServiceName(TestServer.SERVICE_NAME)
                    .setResourceName("test_index")
                    .setDeleteExistingData(true)
                    .build())
            .build());
    verifyDocs(2, replicaServer);
    replicaServer.stopIndex("test_index");

    primaryServer.restart();
    startIndexOnServer(primaryServer, Mode.PRIMARY, -1);
    primaryServer.addDocs(docs3.stream());
    primaryServer.commit("test_index");
    primaryServer.refresh("test_index");
    verifyDocs(3, primaryServer);

    replicaServer.restart();
    replicaServer.startIndex(
        StartIndexRequest.newBuilder()
            .setIndexName("test_index")
            .setMode(Mode.REPLICA)
            .setPrimaryAddress("localhost")
            .setPrimaryGen(-1)
            .setRestore(
                RestoreIndex.newBuilder()
                    .setServiceName(TestServer.SERVICE_NAME)
                    .setResourceName("test_index")
                    .setDeleteExistingData(true)
                    .build())
            .build());
    verifyDocs(3, replicaServer);
  }

  @Test
  public void testStartIndexFromDiscoveryFile() throws Exception {
    primaryServer = buildLocalPrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    primaryServer.addDocs(docs1.stream());
    primaryServer.refresh("test_index");
    verifyDocs(1, primaryServer);

    replicaServer = buildLocalReplica();
    String discoveryFilePath =
        Paths.get(folder.getRoot().toString(), "test_discovery_file.json").toString();
    writeNodeFile(
        Collections.singletonList(new Node("localhost", primaryServer.getReplicationPort())),
        discoveryFilePath);
    response =
        replicaServer.startIndex(
            StartIndexRequest.newBuilder()
                .setIndexName("test_index")
                .setMode(Mode.REPLICA)
                .setPrimaryDiscoveryFile(discoveryFilePath)
                .setPrimaryGen(0)
                .build());
    assertEquals(1, response.getNumDocs());
    verifyDocs(1, replicaServer);
  }

  @Test
  public void testStartIndexNoDiscovery() throws Exception {
    primaryServer = buildRemotePrimary();
    assertTrue(primaryServer.indices().isEmpty());
    createIndexWithFields();
    StartIndexResponse response = startIndexOnServer(primaryServer, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    replicaServer = buildRemoteReplica();
    replicaServer.startIndex(
        StartIndexRequest.newBuilder()
            .setIndexName("test_index")
            .setMode(Mode.REPLICA)
            .setPrimaryGen(0)
            .build());
    assertEquals(Set.of("test_index"), replicaServer.indices());
  }
}
