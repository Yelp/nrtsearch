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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
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
import com.yelp.nrtsearch.server.backup.BackupDiffManager;
import com.yelp.nrtsearch.server.backup.ContentDownloader;
import com.yelp.nrtsearch.server.backup.ContentDownloaderImpl;
import com.yelp.nrtsearch.server.backup.FileCompressAndUploader;
import com.yelp.nrtsearch.server.backup.IndexArchiver;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.LuceneServer.LuceneServerImpl;
import com.yelp.nrtsearch.server.grpc.LuceneServer.ReplicationServerImpl;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.index.ImmutableIndexState;
import com.yelp.nrtsearch.server.luceneserver.script.js.JsScriptEngine;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.CollectorRegistry;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StateBackendServerTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(TEST_BUCKET);

  private Server primaryServer;
  private Server primaryReplicationServer;
  private LuceneServerClient primaryClient;

  private Server replicaServer;
  private Server replicaReplicationServer;
  private LuceneServerClient replicaClient;

  private static final String TEST_BUCKET = "state-backend-server-test";
  private static final String TEST_SERVICE_NAME = "state-backend-test-service";
  private IndexArchiver archiverPrimary;
  private IndexArchiver archiverReplica;

  @After
  public void cleanup() throws InterruptedException {
    cleanupPrimary();
    cleanupReplica();
    archiverPrimary = null;
    archiverReplica = null;
  }

  private void cleanupPrimary() {
    if (primaryClient != null) {
      try {
        primaryClient.shutdown();
      } catch (InterruptedException ignore) {
      }
      primaryClient = null;
    }
    if (primaryServer != null) {
      primaryServer.shutdown();
      try {
        primaryServer.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignore) {
      }
      primaryServer = null;
    }
    if (primaryReplicationServer != null) {
      primaryReplicationServer.shutdown();
      try {
        primaryReplicationServer.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignore) {
      }
      primaryReplicationServer = null;
    }
  }

  private void cleanupReplica() {
    if (replicaClient != null) {
      try {
        replicaClient.shutdown();
      } catch (InterruptedException ignore) {
      }
      replicaClient = null;
    }
    if (replicaServer != null) {
      replicaServer.shutdown();
      try {
        replicaServer.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignore) {
      }
      replicaServer = null;
    }
    if (replicaReplicationServer != null) {
      replicaReplicationServer.shutdown();
      try {
        replicaReplicationServer.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignore) {
      }
      replicaReplicationServer = null;
    }
  }

  private void initArchiver() throws IOException {
    Files.createDirectories(getPrimaryArchiveDir());
    Files.createDirectories(getReplicaIndexDir());

    AmazonS3 s3 = s3Provider.getAmazonS3();
    TransferManager transferManager =
        TransferManagerBuilder.standard().withS3Client(s3).withShutDownThreadPools(false).build();

    ContentDownloader contentDownloader =
        new ContentDownloaderImpl(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, TEST_BUCKET, true);
    FileCompressAndUploader fileCompressAndUploader =
        new FileCompressAndUploader(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, TEST_BUCKET);
    VersionManager versionManager = new VersionManager(s3, TEST_BUCKET);
    BackupDiffManager backupDiffManagerPrimary =
        new BackupDiffManager(
            contentDownloader, fileCompressAndUploader, versionManager, getPrimaryArchiveDir());
    archiverPrimary =
        new IndexArchiver(
            backupDiffManagerPrimary,
            fileCompressAndUploader,
            contentDownloader,
            versionManager,
            getPrimaryArchiveDir());

    BackupDiffManager backupDiffManagerReplica =
        new BackupDiffManager(
            contentDownloader, fileCompressAndUploader, versionManager, getPrimaryArchiveDir());
    archiverReplica =
        new IndexArchiver(
            backupDiffManagerReplica,
            fileCompressAndUploader,
            contentDownloader,
            versionManager,
            getReplicaArchiveDir());
  }

  private LuceneServerConfiguration getPrimaryConfig() {
    String configStr =
        String.join(
            "\n",
            "nodeName: 'test_node'",
            "serviceName: " + TEST_SERVICE_NAME,
            "stateDir: " + getStateDir(),
            "indexDir: " + getPrimaryIndexDir(),
            "archiveDirectory: " + getPrimaryArchiveDir(),
            "stateConfig:",
            "  backendType: LOCAL");
    return new LuceneServerConfiguration(new ByteArrayInputStream(configStr.getBytes()));
  }

  private LuceneServerConfiguration getPrimaryArchiverConfig() {
    String configStr =
        String.join(
            "\n",
            "nodeName: 'test_node'",
            "serviceName: " + TEST_SERVICE_NAME,
            "stateDir: " + getStateDir(),
            "indexDir: " + getPrimaryIndexDir(),
            "archiveDirectory: " + getPrimaryArchiveDir(),
            "backupWithIncArchiver: true",
            "restoreFromIncArchiver: true",
            "stateConfig:",
            "  backendType: REMOTE",
            "  remote:",
            "    readOnly: false");
    return new LuceneServerConfiguration(new ByteArrayInputStream(configStr.getBytes()));
  }

  private LuceneServerConfiguration getReplicaConfig() {
    String configStr =
        String.join(
            "\n",
            "nodeName: 'test_node_replica'",
            "serviceName: " + TEST_SERVICE_NAME,
            "stateDir: " + getStateDir(),
            "indexDir: " + getReplicaIndexDir(),
            "archiveDirectory: " + getReplicaArchiveDir(),
            "syncInitialNrtPoint: true",
            "stateConfig:",
            "  backendType: LOCAL");
    return new LuceneServerConfiguration(new ByteArrayInputStream(configStr.getBytes()));
  }

  private LuceneServerConfiguration getReplicaArchiverConfig() {
    String configStr =
        String.join(
            "\n",
            "nodeName: 'test_node_replica'",
            "serviceName: " + TEST_SERVICE_NAME,
            "stateDir: " + getStateDir(),
            "indexDir: " + getReplicaIndexDir(),
            "archiveDirectory: " + getReplicaArchiveDir(),
            // don't sync on start to make restore testing easier
            "syncInitialNrtPoint: false",
            "restoreFromIncArchiver: true",
            "stateConfig:",
            "  backendType: REMOTE");
    return new LuceneServerConfiguration(new ByteArrayInputStream(configStr.getBytes()));
  }

  private void restartPrimary() throws IOException {
    cleanupPrimary();
    LuceneServerImpl serverImpl =
        new LuceneServerImpl(
            getPrimaryConfig(), null, null, new CollectorRegistry(), Collections.emptyList());

    primaryReplicationServer =
        ServerBuilder.forPort(0)
            .addService(new ReplicationServerImpl(serverImpl.getGlobalState()))
            .build()
            .start();
    primaryServer = ServerBuilder.forPort(0).addService(serverImpl).build().start();
    primaryClient = new LuceneServerClient("localhost", primaryServer.getPort());
  }

  private void restartPrimaryWithArchiver() throws IOException {
    cleanupPrimary();
    LuceneServerImpl serverImpl =
        new LuceneServerImpl(
            getPrimaryArchiverConfig(),
            null,
            archiverPrimary,
            new CollectorRegistry(),
            Collections.emptyList());

    primaryReplicationServer =
        ServerBuilder.forPort(0)
            .addService(new ReplicationServerImpl(serverImpl.getGlobalState()))
            .build()
            .start();
    primaryServer = ServerBuilder.forPort(0).addService(serverImpl).build().start();
    primaryClient = new LuceneServerClient("localhost", primaryServer.getPort());
  }

  private void restartReplica() throws IOException {
    cleanupReplica();
    LuceneServerImpl serverImpl =
        new LuceneServerImpl(
            getReplicaConfig(), null, null, new CollectorRegistry(), Collections.emptyList());

    replicaReplicationServer =
        ServerBuilder.forPort(0)
            .addService(new ReplicationServerImpl(serverImpl.getGlobalState()))
            .build()
            .start();
    replicaServer = ServerBuilder.forPort(0).addService(serverImpl).build().start();
    replicaClient = new LuceneServerClient("localhost", replicaServer.getPort());
  }

  private void restartReplicaWithArchiver() throws IOException {
    cleanupReplica();
    LuceneServerImpl serverImpl =
        new LuceneServerImpl(
            getReplicaArchiverConfig(),
            null,
            archiverReplica,
            new CollectorRegistry(),
            Collections.emptyList());

    replicaReplicationServer =
        ServerBuilder.forPort(0)
            .addService(new ReplicationServerImpl(serverImpl.getGlobalState()))
            .build()
            .start();
    replicaServer = ServerBuilder.forPort(0).addService(serverImpl).build().start();
    replicaClient = new LuceneServerClient("localhost", replicaServer.getPort());
  }

  private Path getStateDir() {
    return Paths.get(folder.getRoot().toString(), "state_dir");
  }

  private Path getPrimaryIndexDir() {
    return Paths.get(folder.getRoot().toString(), "primary_index_dir");
  }

  private Path getPrimaryArchiveDir() {
    return Paths.get(folder.getRoot().toString(), "primary_archive_dir");
  }

  private Path getReplicaIndexDir() {
    return Paths.get(folder.getRoot().toString(), "replica_index_dir");
  }

  private Path getReplicaArchiveDir() {
    return Paths.get(folder.getRoot().toString(), "replica_archive_dir");
  }

  private void initPrimary() throws IOException {
    restartPrimary();
    IndicesResponse response =
        primaryClient.getBlockingStub().indices(IndicesRequest.newBuilder().build());
    assertTrue(response.getIndicesResponseList().isEmpty());
  }

  private void initPrimaryWithArchiver() throws IOException {
    restartPrimaryWithArchiver();
    IndicesResponse response =
        primaryClient.getBlockingStub().indices(IndicesRequest.newBuilder().build());
    assertTrue(response.getIndicesResponseList().isEmpty());
  }

  private void createIndices() {
    CreateIndexResponse response =
        primaryClient
            .getBlockingStub()
            .createIndex(CreateIndexRequest.newBuilder().setIndexName("test_index").build());
    assertEquals("Created Index name: test_index", response.getResponse());
    response =
        primaryClient
            .getBlockingStub()
            .createIndex(CreateIndexRequest.newBuilder().setIndexName("test_index_2").build());
    assertEquals("Created Index name: test_index_2", response.getResponse());
    response =
        primaryClient
            .getBlockingStub()
            .createIndex(CreateIndexRequest.newBuilder().setIndexName("test_index_3").build());
    assertEquals("Created Index name: test_index_3", response.getResponse());
  }

  private void createIndex() {
    CreateIndexResponse response =
        primaryClient
            .getBlockingStub()
            .createIndex(CreateIndexRequest.newBuilder().setIndexName("test_index").build());
    assertEquals("Created Index name: test_index", response.getResponse());
  }

  private void createIndexWithFields() {
    createIndex();
    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields1).build());
    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields2).build());
  }

  private IndexStateInfo getIndexState(String indexName, LuceneServerClient client)
      throws IOException {
    StateResponse response =
        client.getBlockingStub().state(StateRequest.newBuilder().setIndexName(indexName).build());
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

  private void verifyDocs(int expectedCount, LuceneServerClient client) {
    SearchResponse response =
        client
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

  private void verifySubFieldDocs(int expectedCount, LuceneServerClient client) {
    SearchResponse response =
        client
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

  private AddDocumentResponse addDocs(Stream<AddDocumentRequest> requestStream) throws Exception {
    CountDownLatch finishLatch = new CountDownLatch(1);
    // observers responses from Server(should get one onNext and oneCompleted)
    final AtomicReference<AddDocumentResponse> response = new AtomicReference<>();
    final AtomicReference<Exception> exception = new AtomicReference<>();
    StreamObserver<AddDocumentResponse> responseStreamObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(AddDocumentResponse value) {
            response.set(value);
          }

          @Override
          public void onError(Throwable t) {
            exception.set(new RuntimeException(t));
            finishLatch.countDown();
          }

          @Override
          public void onCompleted() {
            finishLatch.countDown();
          }
        };
    // requestObserver sends requests to Server (one onNext per AddDocumentRequest and one
    // onCompleted)
    StreamObserver<AddDocumentRequest> requestObserver =
        primaryClient.getAsyncStub().addDocuments(responseStreamObserver);
    // parse CSV into a stream of AddDocumentRequest
    try {
      requestStream.forEach(requestObserver::onNext);
    } catch (RuntimeException e) {
      // Cancel RPC
      requestObserver.onError(e);
      throw e;
    }
    // Mark the end of requests
    requestObserver.onCompleted();
    // Receiving happens asynchronously, so block here 20 seconds
    if (!finishLatch.await(20, TimeUnit.SECONDS)) {
      throw new RuntimeException("addDocuments can not finish within 20 seconds");
    }
    // Re-throw exception
    if (exception.get() != null) {
      throw exception.get();
    }
    return response.get();
  }

  private StartIndexResponse startIndex(LuceneServerClient client, Mode mode) {
    return startIndex(client, mode, 0);
  }

  private StartIndexResponse startIndex(LuceneServerClient client, Mode mode, long primaryGen) {
    if (mode.equals(Mode.REPLICA)) {
      return client
          .getBlockingStub()
          .startIndex(
              StartIndexRequest.newBuilder()
                  .setIndexName("test_index")
                  .setMode(Mode.REPLICA)
                  .setPrimaryAddress("localhost")
                  .setPort(primaryReplicationServer.getPort())
                  .setPrimaryGen(primaryGen)
                  .build());
    } else {
      return client
          .getBlockingStub()
          .startIndex(
              StartIndexRequest.newBuilder().setIndexName("test_index").setMode(mode).build());
    }
  }

  private StartIndexResponse startIndexWithRestore(
      LuceneServerClient client, Mode mode, boolean deleteExistingData) {
    if (mode.equals(Mode.REPLICA)) {
      return client
          .getBlockingStub()
          .startIndex(
              StartIndexRequest.newBuilder()
                  .setIndexName("test_index")
                  .setMode(Mode.REPLICA)
                  .setPrimaryAddress("localhost")
                  .setPort(primaryReplicationServer.getPort())
                  .setRestore(
                      RestoreIndex.newBuilder()
                          .setServiceName(TEST_SERVICE_NAME)
                          .setResourceName("test_index")
                          .setDeleteExistingData(deleteExistingData)
                          .build())
                  .build());
    } else {
      return client
          .getBlockingStub()
          .startIndex(
              StartIndexRequest.newBuilder()
                  .setIndexName("test_index")
                  .setMode(mode)
                  .setRestore(
                      RestoreIndex.newBuilder()
                          .setServiceName(TEST_SERVICE_NAME)
                          .setResourceName("test_index")
                          .setDeleteExistingData(deleteExistingData)
                          .build())
                  .build());
    }
  }

  private DummyResponse stopIndex(LuceneServerClient client) {
    DummyResponse response =
        client
            .getBlockingStub()
            .stopIndex(StopIndexRequest.newBuilder().setIndexName("test_index").build());
    assertEquals("ok", response.getOk());
    return response;
  }

  private CommitResponse commitIndex(LuceneServerClient client) {
    return client
        .getBlockingStub()
        .commit(CommitRequest.newBuilder().setIndexName("test_index").build());
  }

  private RefreshResponse refreshIndex(LuceneServerClient client) {
    return client
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName("test_index").build());
  }

  @Test
  public void testStartServer() throws IOException {
    initPrimary();
  }

  @Test
  public void testRestartServer() throws IOException {
    initPrimary();
  }

  @Test
  public void testCreateIndices() throws IOException {
    initPrimary();
    createIndices();

    IndicesResponse response =
        primaryClient.getBlockingStub().indices(IndicesRequest.newBuilder().build());
    Set<String> indices = new HashSet<>();
    for (IndexStatsResponse statsResponse : response.getIndicesResponseList()) {
      indices.add(statsResponse.getIndexName());
    }
    assertEquals(Set.of("test_index", "test_index_2", "test_index_3"), indices);
  }

  @Test
  public void testIndicesPersistRestart() throws IOException {
    initPrimary();
    createIndices();
    restartPrimary();

    IndicesResponse response =
        primaryClient.getBlockingStub().indices(IndicesRequest.newBuilder().build());
    Set<String> indices = new HashSet<>();
    for (IndexStatsResponse statsResponse : response.getIndicesResponseList()) {
      indices.add(statsResponse.getIndexName());
    }
    assertEquals(Set.of("test_index", "test_index_2", "test_index_3"), indices);
  }

  @Test
  public void testSetIndexSettings() throws IOException {
    initPrimary();
    createIndex();
    SettingsV2Response response =
        primaryClient
            .getBlockingStub()
            .settingsV2(SettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_SETTINGS, response.getSettings());

    response =
        primaryClient
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
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS
            .toBuilder()
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(120.0).build())
            .setNrtCachingDirectoryMaxMergeSizeMB(DoubleValue.newBuilder().setValue(60.0).build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .build();
    assertEquals(expectedSettings, response.getSettings());
  }

  @Test
  public void testIndexSettingsPersistRestart() throws IOException {
    initPrimary();
    createIndex();

    SettingsV2Response response =
        primaryClient
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
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS
            .toBuilder()
            .setNrtCachingDirectoryMaxSizeMB(DoubleValue.newBuilder().setValue(120.0).build())
            .setNrtCachingDirectoryMaxMergeSizeMB(DoubleValue.newBuilder().setValue(60.0).build())
            .setIndexMergeSchedulerAutoThrottle(BoolValue.newBuilder().setValue(true).build())
            .build();
    assertEquals(expectedSettings, response.getSettings());

    restartPrimary();
    response =
        primaryClient
            .getBlockingStub()
            .settingsV2(SettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(expectedSettings, response.getSettings());
  }

  @Test
  public void testSetIndexLiveSettings() throws IOException {
    initPrimary();
    createIndex();
    LiveSettingsV2Response response =
        primaryClient
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS, response.getLiveSettings());

    response =
        primaryClient
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
                    .build());
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS
            .toBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .build();
    assertEquals(expectedSettings, response.getLiveSettings());
  }

  @Test
  public void testIndexLiveSettingsPersistRestart() throws IOException {
    initPrimary();
    createIndex();

    LiveSettingsV2Response response =
        primaryClient
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
                    .build());
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS
            .toBuilder()
            .setDefaultTerminateAfter(Int32Value.newBuilder().setValue(1000).build())
            .setSegmentsPerTier(Int32Value.newBuilder().setValue(4).build())
            .setSliceMaxSegments(Int32Value.newBuilder().setValue(50).build())
            .setDefaultSearchTimeoutSec(DoubleValue.newBuilder().setValue(5.1).build())
            .build();
    assertEquals(expectedSettings, response.getLiveSettings());

    restartPrimary();
    response =
        primaryClient
            .getBlockingStub()
            .liveSettingsV2(LiveSettingsV2Request.newBuilder().setIndexName("test_index").build());
    assertEquals(expectedSettings, response.getLiveSettings());
  }

  @Test
  public void testSetIndexFields() throws IOException {
    initPrimary();
    createIndex();

    FieldDefResponse response =
        primaryClient
            .getBlockingStub()
            .registerFields(
                FieldDefRequest.newBuilder()
                    .setIndexName("test_index")
                    .addAllField(fields1)
                    .build());
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryClient);
    Map<String, Field> fieldMap = getFieldMap(response.getResponse());

    assertEquals(3, indexStateInfo.getFieldsCount());
    assertEquals(fields1.get(0), indexStateInfo.getFieldsMap().get("id"));
    assertEquals(fields1.get(1), indexStateInfo.getFieldsMap().get("field1"));
    assertEquals(fields1.get(2), indexStateInfo.getFieldsMap().get("field2"));
    assertEquals(fieldMap, indexStateInfo.getFieldsMap());

    response =
        primaryClient
            .getBlockingStub()
            .registerFields(
                FieldDefRequest.newBuilder()
                    .setIndexName("test_index")
                    .addAllField(fields2)
                    .build());
    indexStateInfo = getIndexState("test_index", primaryClient);
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
    initPrimary();
    createIndex();

    FieldDefResponse response =
        primaryClient
            .getBlockingStub()
            .registerFields(
                FieldDefRequest.newBuilder()
                    .setIndexName("test_index")
                    .addAllField(fields1)
                    .build());

    restartPrimary();

    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryClient);
    Map<String, Field> fieldMap = getFieldMap(response.getResponse());

    assertEquals(3, indexStateInfo.getFieldsCount());
    assertEquals(fields1.get(0), indexStateInfo.getFieldsMap().get("id"));
    assertEquals(fields1.get(1), indexStateInfo.getFieldsMap().get("field1"));
    assertEquals(fields1.get(2), indexStateInfo.getFieldsMap().get("field2"));
    assertEquals(fieldMap, indexStateInfo.getFieldsMap());

    response =
        primaryClient
            .getBlockingStub()
            .registerFields(
                FieldDefRequest.newBuilder()
                    .setIndexName("test_index")
                    .addAllField(fields2)
                    .build());

    restartPrimary();

    indexStateInfo = getIndexState("test_index", primaryClient);
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
    initPrimary();
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

    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields1).build());
    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields2).build());
    primaryClient
        .getBlockingStub()
        .liveSettingsV2(
            LiveSettingsV2Request.newBuilder()
                .setIndexName("test_index")
                .setLiveSettings(liveSettings)
                .build());
    primaryClient
        .getBlockingStub()
        .settingsV2(
            SettingsV2Request.newBuilder()
                .setIndexName("test_index")
                .setSettings(settings)
                .build());

    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(5, indexStateInfo.getFieldsCount());
    assertEquals(fields1.get(0), indexStateInfo.getFieldsMap().get("id"));
    assertEquals(fields1.get(1), indexStateInfo.getFieldsMap().get("field1"));
    assertEquals(fields1.get(2), indexStateInfo.getFieldsMap().get("field2"));
    assertEquals(fields2.get(0), indexStateInfo.getFieldsMap().get("field3"));
    assertEquals(fields2.get(1), indexStateInfo.getFieldsMap().get("field4"));
    assertEquals(liveSettings, indexStateInfo.getLiveSettings());
    assertEquals(settings, indexStateInfo.getSettings());

    restartPrimary();

    indexStateInfo = getIndexState("test_index", primaryClient);
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
    initPrimary();
    createIndex();
    try {
      createIndex();
      fail();
    } catch (StatusRuntimeException e) {
      assertEquals(Status.ALREADY_EXISTS.getCode(), e.getStatus().getCode());
      assertEquals(
          "ALREADY_EXISTS: invalid indexName: test_index\nIllegalArgumentException()",
          e.getMessage());
    }
  }

  @Test
  public void testRecreateIndex() throws IOException {
    initPrimary();
    createIndex();

    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields1).build());
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(3, indexStateInfo.getFieldsCount());
    Path dataDir = getPrimaryIndexDir();
    File[] indexFolders = dataDir.toFile().listFiles();
    assertEquals(1, indexFolders.length);
    String indexUniqueName = indexFolders[0].getName();
    assertTrue(indexUniqueName.startsWith("test_index"));

    DeleteIndexResponse response =
        primaryClient
            .getBlockingStub()
            .deleteIndex(DeleteIndexRequest.newBuilder().setIndexName("test_index").build());
    assertEquals("ok", response.getOk());
    indexFolders = dataDir.toFile().listFiles();
    assertEquals(0, indexFolders.length);

    createIndex();
    indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(1, indexStateInfo.getGen());
    assertTrue(indexStateInfo.getFieldsMap().isEmpty());

    indexFolders = dataDir.toFile().listFiles();
    assertEquals(1, indexFolders.length);
    String newIndexUniqueName = indexFolders[0].getName();
    assertTrue(newIndexUniqueName.startsWith("test_index"));
    assertNotEquals(newIndexUniqueName, indexUniqueName);

    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields1).build());
    indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(3, indexStateInfo.getFieldsCount());
  }

  @Test
  public void testStartIndexPrimary() throws IOException {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryClient);
    assertTrue(indexStateInfo.getCommitted());
  }

  @Test
  public void testStartIndexStandalone() throws IOException {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.STANDALONE);
    assertEquals(0, response.getNumDocs());
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryClient);
    assertTrue(indexStateInfo.getCommitted());
  }

  @Test
  public void testStartIndexReplica() throws IOException {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryClient);
    assertTrue(indexStateInfo.getCommitted());

    restartReplica();
    response = startIndex(replicaClient, Mode.REPLICA);
    assertEquals(0, response.getNumDocs());
    indexStateInfo = getIndexState("test_index", replicaClient);
    assertTrue(indexStateInfo.getCommitted());
    assertEquals(5, indexStateInfo.getFieldsMap().size());
  }

  @Test
  public void testInitialNrtPointSync() throws Exception {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    addDocs(docs1.stream());
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    restartReplica();
    response = startIndex(replicaClient, Mode.REPLICA);
    assertEquals(1, response.getNumDocs());
    verifyDocs(1, replicaClient);
  }

  @Test
  public void testIndexStopStart() throws Exception {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    stopIndex(primaryClient);

    response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    addDocs(docs1.stream());
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);
  }

  @Test
  public void testIndexStopStartExistingDoc() throws Exception {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    addDocs(docs1.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    stopIndex(primaryClient);

    response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(1, response.getNumDocs());

    addDocs(docs2.stream());
    refreshIndex(primaryClient);
    verifyDocs(2, primaryClient);
  }

  @Test
  public void testIndexStopStartExistingDocStandalone() throws Exception {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.STANDALONE);
    assertEquals(0, response.getNumDocs());

    addDocs(docs1.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    stopIndex(primaryClient);

    response = startIndex(primaryClient, Mode.STANDALONE);
    assertEquals(1, response.getNumDocs());

    addDocs(docs2.stream());
    refreshIndex(primaryClient);
    verifyDocs(2, primaryClient);
  }

  @Test
  public void testIndexStopStartReplica() throws Exception {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    addDocs(docs1.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    restartReplica();
    response = startIndex(replicaClient, Mode.REPLICA);
    assertEquals(1, response.getNumDocs());
    verifyDocs(1, replicaClient);

    stopIndex(replicaClient);

    response = startIndex(replicaClient, Mode.REPLICA);
    assertEquals(1, response.getNumDocs());
    verifyDocs(1, replicaClient);
  }

  @Test
  public void testIndexAlreadyStarted() throws IOException {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    try {
      startIndex(primaryClient, Mode.PRIMARY);
      fail();
    } catch (StatusRuntimeException e) {
      assertEquals(
          "INVALID_ARGUMENT: error while trying to start index: test_index\nIndex test_index is already started",
          e.getMessage());
    }
  }

  @Test
  public void testRecreateStartedIndex() throws Exception {
    initPrimary();
    createIndexWithFields();

    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(5, indexStateInfo.getFieldsCount());
    Path dataDir = getPrimaryIndexDir();
    File[] indexFolders = dataDir.toFile().listFiles();
    assertEquals(1, indexFolders.length);
    String indexUniqueName = indexFolders[0].getName();
    assertTrue(indexUniqueName.startsWith("test_index"));

    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());
    addDocs(docs1.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    DeleteIndexResponse delResponse =
        primaryClient
            .getBlockingStub()
            .deleteIndex(DeleteIndexRequest.newBuilder().setIndexName("test_index").build());
    assertEquals("ok", delResponse.getOk());
    indexFolders = dataDir.toFile().listFiles();
    assertEquals(0, indexFolders.length);

    createIndex();
    indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(1, indexStateInfo.getGen());
    assertTrue(indexStateInfo.getFieldsMap().isEmpty());

    indexFolders = dataDir.toFile().listFiles();
    assertEquals(1, indexFolders.length);
    String newIndexUniqueName = indexFolders[0].getName();
    assertTrue(newIndexUniqueName.startsWith("test_index"));
    assertNotEquals(newIndexUniqueName, indexUniqueName);

    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields1).build());
    indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(3, indexStateInfo.getFieldsCount());
    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields2).build());
    indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(5, indexStateInfo.getFieldsCount());

    response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());
    addDocs(docs1.stream());
    addDocs(docs2.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(2, primaryClient);
  }

  @Test
  public void testSchemaChangeWithIndexing() throws Exception {
    initPrimary();
    createIndex();

    startIndex(primaryClient, Mode.PRIMARY);

    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields1).build());
    IndexStateInfo indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(3, indexStateInfo.getFieldsCount());

    addDocs(docs3.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);

    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields2).build());
    indexStateInfo = getIndexState("test_index", primaryClient);
    assertEquals(5, indexStateInfo.getFieldsCount());

    addDocs(docs1.stream());
    addDocs(docs2.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(3, primaryClient);
  }

  @Test
  public void testSettingsV1All() throws IOException {
    initPrimary();
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

    SettingsResponse response = primaryClient.getBlockingStub().settings(request);
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
            .build();

    IndexSettings.Builder builder = IndexSettings.newBuilder();
    JsonFormat.parser().merge(response.getResponse(), builder);
    assertEquals(expectedSettings, builder.build());
  }

  @Test
  public void testSettingsV1Partial() throws IOException {
    initPrimary();
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

    SettingsResponse response = primaryClient.getBlockingStub().settings(request);
    IndexSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_SETTINGS
            .toBuilder()
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
    initPrimary();
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
            .build();

    LiveSettingsResponse response = primaryClient.getBlockingStub().liveSettings(request);
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
            .setMaxMergePreCopyDurationSec(UInt64Value.newBuilder().setValue(0))
            .build();

    IndexLiveSettings.Builder builder = IndexLiveSettings.newBuilder();
    JsonFormat.parser().merge(response.getResponse(), builder);
    assertEquals(expectedSettings, builder.build());
  }

  @Test
  public void testLiveSettingsV1Partial() throws IOException {
    initPrimary();
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

    LiveSettingsResponse response = primaryClient.getBlockingStub().liveSettings(request);
    IndexLiveSettings expectedSettings =
        ImmutableIndexState.DEFAULT_INDEX_LIVE_SETTINGS
            .toBuilder()
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
  public void testStartServerWithArchiver() throws IOException {
    initArchiver();
    initPrimaryWithArchiver();
  }

  @Test
  public void testStartWithRestore() throws Exception {
    initArchiver();
    initPrimaryWithArchiver();
    createIndexWithFields();
    startIndex(primaryClient, Mode.PRIMARY);
    addDocs(docs1.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    stopIndex(primaryClient);

    restartPrimaryWithArchiver();
    startIndexWithRestore(primaryClient, Mode.PRIMARY, true);
    verifyDocs(1, primaryClient);
  }

  @Test
  public void testStartRestoreNoCommit() throws Exception {
    initArchiver();
    initPrimaryWithArchiver();
    createIndexWithFields();
    startIndexWithRestore(primaryClient, Mode.PRIMARY, false);
    addDocs(docs1.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    stopIndex(primaryClient);

    restartPrimaryWithArchiver();
    startIndexWithRestore(primaryClient, Mode.PRIMARY, true);
    verifyDocs(1, primaryClient);
  }

  @Test
  public void testReplicaRestore() throws Exception {
    initArchiver();
    initPrimaryWithArchiver();
    createIndexWithFields();
    startIndex(primaryClient, Mode.PRIMARY);
    addDocs(docs1.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    stopIndex(primaryClient);

    restartReplicaWithArchiver();
    startIndexWithRestore(replicaClient, Mode.REPLICA, true);
    verifyDocs(1, replicaClient);
  }

  @Test
  public void testReplicaReDownloadsIndexData() throws Exception {
    initArchiver();
    initPrimaryWithArchiver();
    createIndexWithFields();
    startIndex(primaryClient, Mode.PRIMARY);
    addDocs(docs1.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    restartReplicaWithArchiver();
    startIndexWithRestore(replicaClient, Mode.REPLICA, true);
    verifyDocs(1, replicaClient);
    stopIndex(replicaClient);

    // commit more docs on primary
    addDocs(docs2.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(2, primaryClient);

    // start index and pull latest restore
    startIndexWithRestore(replicaClient, Mode.REPLICA, true);
    verifyDocs(2, replicaClient);
  }

  @Test
  public void testReplicaRestoreSchemaChange() throws Exception {
    initArchiver();
    initPrimaryWithArchiver();
    createIndex();
    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields1).build());

    restartReplicaWithArchiver();

    primaryClient
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName("test_index").addAllField(fields2).build());
    startIndexWithRestore(primaryClient, Mode.PRIMARY, true);
    addDocs(docs1.stream());
    addDocs(docs2.stream());
    addDocs(docs3.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(3, primaryClient);

    startIndexWithRestore(replicaClient, Mode.REPLICA, false);
    verifySubFieldDocs(3, replicaClient);

    stopIndex(replicaClient);
    restartReplicaWithArchiver();
    startIndexWithRestore(replicaClient, Mode.REPLICA, true);
    verifyDocs(3, replicaClient);
  }

  @Test
  public void testStartReplicaNoGlobalState() throws IOException {
    initArchiver();
    try {
      restartReplicaWithArchiver();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Cannot update remote state when configured as read only", e.getMessage());
    }
  }

  @Test
  public void testAutoPrimaryGeneration() throws Exception {
    initArchiver();
    initPrimaryWithArchiver();
    createIndexWithFields();
    startIndex(primaryClient, Mode.PRIMARY, -1);
    addDocs(docs1.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);
    stopIndex(primaryClient);
    cleanupPrimary();

    restartReplicaWithArchiver();
    replicaClient
        .getBlockingStub()
        .startIndex(
            StartIndexRequest.newBuilder()
                .setIndexName("test_index")
                .setMode(Mode.REPLICA)
                .setPrimaryAddress("localhost")
                .setPort(0)
                .setPrimaryGen(-1)
                .setRestore(
                    RestoreIndex.newBuilder()
                        .setServiceName(TEST_SERVICE_NAME)
                        .setResourceName("test_index")
                        .setDeleteExistingData(true)
                        .build())
                .build());

    verifyDocs(1, replicaClient);

    restartPrimaryWithArchiver();
    startIndex(primaryClient, Mode.PRIMARY, -1);
    addDocs(docs2.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(2, primaryClient);
    stopIndex(primaryClient);
    cleanupPrimary();
    stopIndex(replicaClient);

    restartReplicaWithArchiver();
    replicaClient
        .getBlockingStub()
        .startIndex(
            StartIndexRequest.newBuilder()
                .setIndexName("test_index")
                .setMode(Mode.REPLICA)
                .setPrimaryAddress("localhost")
                .setPrimaryGen(-1)
                .setRestore(
                    RestoreIndex.newBuilder()
                        .setServiceName(TEST_SERVICE_NAME)
                        .setResourceName("test_index")
                        .setDeleteExistingData(true)
                        .build())
                .build());
    verifyDocs(2, replicaClient);
    stopIndex(replicaClient);

    restartPrimaryWithArchiver();
    startIndex(primaryClient, Mode.PRIMARY, -1);
    addDocs(docs3.stream());
    commitIndex(primaryClient);
    refreshIndex(primaryClient);
    verifyDocs(3, primaryClient);

    restartReplicaWithArchiver();
    replicaClient
        .getBlockingStub()
        .startIndex(
            StartIndexRequest.newBuilder()
                .setIndexName("test_index")
                .setMode(Mode.REPLICA)
                .setPrimaryAddress("localhost")
                .setPrimaryGen(-1)
                .setRestore(
                    RestoreIndex.newBuilder()
                        .setServiceName(TEST_SERVICE_NAME)
                        .setResourceName("test_index")
                        .setDeleteExistingData(true)
                        .build())
                .build());
    verifyDocs(3, replicaClient);
  }

  @Test
  public void testStartIndexFromDiscoveryFile() throws Exception {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    addDocs(docs1.stream());
    refreshIndex(primaryClient);
    verifyDocs(1, primaryClient);

    restartReplica();
    String discoveryFilePath =
        Paths.get(folder.getRoot().toString(), "test_discovery_file.json").toString();
    writeNodeFile(
        Collections.singletonList(new Node("localhost", primaryReplicationServer.getPort())),
        discoveryFilePath);
    response =
        replicaClient
            .getBlockingStub()
            .startIndex(
                StartIndexRequest.newBuilder()
                    .setIndexName("test_index")
                    .setMode(Mode.REPLICA)
                    .setPrimaryDiscoveryFile(discoveryFilePath)
                    .setPrimaryGen(0)
                    .build());
    assertEquals(1, response.getNumDocs());
    verifyDocs(1, replicaClient);
  }

  private void writeNodeFile(List<Node> nodes, String filePath) throws IOException {
    String filePathStr = filePath;
    String fileStr = new ObjectMapper().writeValueAsString(nodes);
    try (FileOutputStream outputStream = new FileOutputStream(filePathStr)) {
      outputStream.write(fileStr.getBytes());
    }
  }

  @Test
  public void testStartIndexNoDiscovery() throws IOException {
    initPrimary();
    createIndexWithFields();
    StartIndexResponse response = startIndex(primaryClient, Mode.PRIMARY);
    assertEquals(0, response.getNumDocs());

    restartReplica();
    try {
      replicaClient
          .getBlockingStub()
          .startIndex(
              StartIndexRequest.newBuilder()
                  .setIndexName("test_index")
                  .setMode(Mode.REPLICA)
                  .setPrimaryGen(0)
                  .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage()
              .contains("Unable to initialize primary replication client for start request:"));
    }
  }
}
