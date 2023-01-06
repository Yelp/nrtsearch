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

import static com.yelp.nrtsearch.server.grpc.ReplicationServerClient.BINARY_MAGIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.yelp.nrtsearch.clientlib.Node;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.ArchiverImpl;
import com.yelp.nrtsearch.server.backup.BackupDiffManager;
import com.yelp.nrtsearch.server.backup.ContentDownloader;
import com.yelp.nrtsearch.server.backup.ContentDownloaderImpl;
import com.yelp.nrtsearch.server.backup.FileCompressAndUploader;
import com.yelp.nrtsearch.server.backup.IndexArchiver;
import com.yelp.nrtsearch.server.backup.NoTarImpl;
import com.yelp.nrtsearch.server.backup.Tar;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.backup.VersionManager;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.config.StateConfig.StateBackendType;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.LuceneServer.LuceneServerImpl;
import com.yelp.nrtsearch.server.grpc.LuceneServer.ReplicationServerImpl;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.utils.FileUtil;
import io.findify.s3mock.S3Mock;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.CollectorRegistry;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.rules.TemporaryFolder;

public class TestServer {
  private static final List<TestServer> createdServers = new ArrayList<>();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final Gson gson = new GsonBuilder().serializeNulls().create();
  public static final String SERVICE_NAME = "test_server";
  public static final String TEST_BUCKET = "test-server-data-bucket";
  public static final String S3_ENDPOINT = "http://127.0.0.1:8011";
  public static final String DISCOVERY_FILE = "primary_node.json";
  public static final long DEFAULT_REPLICATION_WAIT_TIMEOUT_MS = 30000;
  public static final long DEFAULT_PRIMARY_REGISTER_TIMEOUT_MS = 30000;
  public static final List<String> simpleFieldNames = List.of("id", "field1", "field2");
  public static final List<Field> simpleFields =
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
              .setType(FieldType.INT)
              .build(),
          Field.newBuilder()
              .setName("field2")
              .setStoreDocValues(true)
              .setSearch(true)
              .setType(FieldType.ATOM)
              .build());

  private static S3Mock api;

  private final LuceneServerConfiguration configuration;
  private final boolean writeDiscoveryFile;
  private final Path discoveryFilePath;
  private Server server;
  private Server replicationServer;
  private LuceneServerClient client;
  private LuceneServerImpl serverImpl;
  private Archiver legacyArchiver;
  private Archiver indexArchiver;

  public static void initS3(TemporaryFolder folder) throws IOException {
    if (api == null) {
      Path s3Directory = folder.newFolder("s3").toPath();
      api = S3Mock.create(8011, s3Directory.toAbsolutePath().toString());
      api.start();
    }
  }

  public static void cleanupAll() {
    createdServers.forEach(TestServer::cleanup);
    createdServers.clear();
    if (api != null) {
      api.shutdown();
      api = null;
    }
  }

  public TestServer(
      LuceneServerConfiguration configuration, boolean writeDiscoveryFile, Path discoveryFilePath)
      throws IOException {
    this.configuration = configuration;
    this.writeDiscoveryFile = writeDiscoveryFile;
    this.discoveryFilePath = discoveryFilePath;
    createdServers.add(this);
    restart();
  }

  private IndexArchiver createIndexArchiver(Path archiverDir) throws IOException {
    Files.createDirectories(archiverDir);

    AmazonS3 s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint(S3_ENDPOINT);
    s3.createBucket(TEST_BUCKET);
    TransferManager transferManager =
        TransferManagerBuilder.standard().withS3Client(s3).withShutDownThreadPools(false).build();

    ContentDownloader contentDownloader =
        new ContentDownloaderImpl(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, TEST_BUCKET, true);
    FileCompressAndUploader fileCompressAndUploader =
        new FileCompressAndUploader(
            new TarImpl(TarImpl.CompressionMode.LZ4), transferManager, TEST_BUCKET);
    ContentDownloader contentDownloaderNoTar =
        new ContentDownloaderImpl(new NoTarImpl(), transferManager, TEST_BUCKET, true);
    FileCompressAndUploader fileCompressAndUploaderNoTar =
        new FileCompressAndUploader(new NoTarImpl(), transferManager, TEST_BUCKET);
    VersionManager versionManager = new VersionManager(s3, TEST_BUCKET);
    BackupDiffManager backupDiffManagerPrimary =
        new BackupDiffManager(
            contentDownloaderNoTar, fileCompressAndUploaderNoTar, versionManager, archiverDir);

    return new IndexArchiver(
        backupDiffManagerPrimary,
        fileCompressAndUploader,
        contentDownloader,
        versionManager,
        archiverDir);
  }

  private Archiver createLegacyArchiver(Path archiverDir) throws IOException {
    Files.createDirectories(archiverDir);

    AmazonS3 s3 = new AmazonS3Client(new AnonymousAWSCredentials());
    s3.setEndpoint(S3_ENDPOINT);
    s3.createBucket(TEST_BUCKET);
    return new ArchiverImpl(
        s3, TEST_BUCKET, archiverDir, new TarImpl(Tar.CompressionMode.LZ4), true);
  }

  public void restart() throws IOException {
    restart(false);
  }

  public void restart(boolean clearData) throws IOException {
    cleanup(clearData);
    legacyArchiver = createLegacyArchiver(Paths.get(configuration.getArchiveDirectory()));
    indexArchiver = createIndexArchiver(Paths.get(configuration.getArchiveDirectory()));
    serverImpl =
        new LuceneServerImpl(
            configuration,
            legacyArchiver,
            indexArchiver,
            new CollectorRegistry(),
            Collections.emptyList());

    replicationServer =
        ServerBuilder.forPort(0)
            .addService(new ReplicationServerImpl(serverImpl.getGlobalState()))
            .build()
            .start();
    serverImpl.getGlobalState().replicationStarted(replicationServer.getPort());

    if (writeDiscoveryFile) {
      writeDiscoveryFile(replicationServer.getPort());
    }

    server = ServerBuilder.forPort(0).addService(serverImpl).build().start();
    client = new LuceneServerClient("localhost", server.getPort());
  }

  private void writeDiscoveryFile(int replicationPort) throws IOException {
    Node serverNode = new Node("localhost", replicationPort);
    writeNodeFile(Collections.singletonList(serverNode));
  }

  private void writeNodeFile(List<Node> nodes) throws IOException {
    writeFile(OBJECT_MAPPER.writeValueAsString(nodes));
  }

  private void writeFile(String contents) throws IOException {
    String filePathStr = discoveryFilePath.toString();
    try (FileOutputStream outputStream = new FileOutputStream(filePathStr)) {
      outputStream.write(contents.getBytes());
    }
  }

  public int getPort() {
    return server.getPort();
  }

  public int getReplicationPort() {
    return replicationServer.getPort();
  }

  public String getServiceName() {
    return serverImpl.getGlobalState().getConfiguration().getServiceName();
  }

  public GlobalState getGlobalState() {
    return serverImpl.getGlobalState();
  }

  public LuceneServerClient getClient() {
    return client;
  }

  public Archiver getLegacyArchiver() {
    return legacyArchiver;
  }

  public Archiver getIndexArchiver() {
    return indexArchiver;
  }

  public void cleanup() {
    cleanup(false);
  }

  public void cleanup(boolean clearData) {
    if (serverImpl != null) {
      GlobalState globalState = serverImpl.getGlobalState();
      for (String indexName : globalState.getIndexNames()) {
        try {
          IndexState indexState = globalState.getIndex(indexName);
          if (indexState.isStarted()) {
            indexState.close();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      if (clearData) {
        try {
          FileUtil.deleteAllFiles(globalState.getIndexDirBase());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      serverImpl = null;
    }
    if (client != null) {
      try {
        client.shutdown();
      } catch (InterruptedException ignore) {
      }
      client = null;
    }
    if (server != null) {
      server.shutdown();
      try {
        server.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignore) {
      }
      server = null;
    }
    if (replicationServer != null) {
      replicationServer.shutdown();
      try {
        replicationServer.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignore) {
      }
      replicationServer = null;
    }
  }

  public Set<String> indices() {
    IndicesResponse response =
        client.getBlockingStub().indices(IndicesRequest.newBuilder().build());
    return response.getIndicesResponseList().stream()
        .map(IndexStatsResponse::getIndexName)
        .collect(Collectors.toSet());
  }

  public boolean isReady() {
    try {
      client.getBlockingStub().ready(ReadyCheckRequest.newBuilder().build());
      return true;
    } catch (StatusRuntimeException ignore) {
    }
    return false;
  }

  public boolean isStarted(String indexName) {
    try {
      StatsResponse response =
          client.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(indexName).build());
      return response.getState().equals("started");
    } catch (StatusRuntimeException e) {
      // hacky, we should make the stats call handle this better
      if (e.getMessage().contains("isn't started")) {
        return false;
      }
      throw e;
    }
  }

  public CreateIndexResponse createIndex(CreateIndexRequest request) {
    return client.getBlockingStub().createIndex(request);
  }

  public CreateIndexResponse createIndex(String indexName) {
    return createIndex(CreateIndexRequest.newBuilder().setIndexName(indexName).build());
  }

  public void createSimpleIndex(String indexName) {
    createIndex(indexName);
    registerFields(indexName, simpleFields);
  }

  public FieldDefResponse registerFields(String indexName, List<Field> fields) {
    return client
        .getBlockingStub()
        .registerFields(
            FieldDefRequest.newBuilder().setIndexName(indexName).addAllField(fields).build());
  }

  public ReloadStateResponse reloadState() {
    return client.getBlockingStub().reloadState(ReloadStateRequest.newBuilder().build());
  }

  public StartIndexResponse startIndex(StartIndexRequest startIndexRequest) {
    return client.getBlockingStub().startIndex(startIndexRequest);
  }

  public void startStandaloneIndex(String indexName, RestoreIndex maybeRestore) {
    StartIndexRequest.Builder builder =
        StartIndexRequest.newBuilder().setIndexName(indexName).setMode(Mode.STANDALONE);
    if (maybeRestore != null) {
      builder.setRestore(maybeRestore);
    }
    startIndex(builder.build());
  }

  public void startPrimaryIndex(String indexName, long gen, RestoreIndex maybeRestore) {
    StartIndexRequest.Builder builder =
        StartIndexRequest.newBuilder()
            .setIndexName(indexName)
            .setMode(Mode.PRIMARY)
            .setPrimaryGen(gen);
    if (maybeRestore != null) {
      builder.setRestore(maybeRestore);
    }
    startIndex(builder.build());
  }

  public void startReplicaIndex(
      String indexName, long gen, int primaryPort, RestoreIndex maybeRestore) {
    StartIndexRequest.Builder builder =
        StartIndexRequest.newBuilder()
            .setIndexName(indexName)
            .setMode(Mode.REPLICA)
            .setPrimaryGen(gen)
            .setPrimaryAddress("localhost")
            .setPort(primaryPort);
    if (maybeRestore != null) {
      builder.setRestore(maybeRestore);
    }
    startIndex(builder.build());
  }

  public StartIndexResponse startIndexV2(StartIndexV2Request startIndexRequest) {
    return client.getBlockingStub().startIndexV2(startIndexRequest);
  }

  public DummyResponse stopIndex(StopIndexRequest stopIndexRequest) {
    return client.getBlockingStub().stopIndex(stopIndexRequest);
  }

  public void stopIndex(String indexName) {
    stopIndex(StopIndexRequest.newBuilder().setIndexName(indexName).build());
  }

  public AddDocumentResponse addDocs(Stream<AddDocumentRequest> requestStream) {
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
        client.getAsyncStub().addDocuments(responseStreamObserver);
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
    try {
      if (!finishLatch.await(20, TimeUnit.SECONDS)) {
        throw new RuntimeException("addDocuments can not finish within 20 seconds");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    // Re-throw exception
    if (exception.get() != null) {
      throw new RuntimeException(exception.get());
    }
    return response.get();
  }

  public void addSimpleDocs(String indexName, int... ids) {
    List<AddDocumentRequest> requests = new ArrayList<>();
    for (int id : ids) {
      requests.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(indexName)
              .putFields("id", MultiValuedField.newBuilder().addValue(String.valueOf(id)).build())
              .putFields(
                  "field1", MultiValuedField.newBuilder().addValue(String.valueOf(id * 3)).build())
              .putFields(
                  "field2", MultiValuedField.newBuilder().addValue(String.valueOf(id * 5)).build())
              .build());
    }
    addDocs(requests.stream());
  }

  public void verifyFieldName(String indexName, String fieldName) {
    StateResponse rawResponse =
        client.getBlockingStub().state(StateRequest.newBuilder().setIndexName(indexName).build());
    JsonObject response = gson.fromJson(rawResponse.getResponse(), JsonObject.class);
    JsonObject state = response.getAsJsonObject("state");
    JsonObject fields = state.getAsJsonObject("fields");
    assertTrue(fields.has(fieldName));
  }

  public void verifySimpleDocs(String indexName, int expectedCount) {
    SearchResponse response =
        client
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(indexName)
                    .addAllRetrieveFields(simpleFieldNames)
                    .setTopHits(expectedCount + 1)
                    .setStartHit(0)
                    .build());

    assertEquals(expectedCount, response.getHitsCount());
    for (Hit hit : response.getHitsList()) {
      int id = Integer.parseInt(hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());
      int f1 = hit.getFieldsOrThrow("field1").getFieldValue(0).getIntValue();
      int f2 = Integer.parseInt(hit.getFieldsOrThrow("field2").getFieldValue(0).getTextValue());
      assertEquals(id * 3, f1);
      assertEquals(id * 5, f2);
    }
  }

  public void verifySimpleDocIds(String indexName, int... ids) {
    Set<Integer> uniqueIds = new HashSet<>();
    for (int id : ids) {
      uniqueIds.add(id);
    }
    SearchResponse response =
        client
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(indexName)
                    .addAllRetrieveFields(simpleFieldNames)
                    .setTopHits(uniqueIds.size() + 1)
                    .setStartHit(0)
                    .build());
    assertEquals(uniqueIds.size(), response.getHitsCount());
    Set<Integer> uniqueHitIds = new HashSet<>();
    for (Hit hit : response.getHitsList()) {
      int id = Integer.parseInt(hit.getFieldsOrThrow("id").getFieldValue(0).getTextValue());
      int f1 = hit.getFieldsOrThrow("field1").getFieldValue(0).getIntValue();
      int f2 = Integer.parseInt(hit.getFieldsOrThrow("field2").getFieldValue(0).getTextValue());
      assertEquals(id * 3, f1);
      assertEquals(id * 5, f2);
      uniqueHitIds.add(id);
    }
    assertEquals(uniqueIds, uniqueHitIds);
  }

  public void refresh(String indexName) {
    client.refresh(indexName);
  }

  public void commit(String indexName) {
    client.commit(indexName);
  }

  public void deleteAllDocuments(String indexName) {
    client.deleteAllDocuments(indexName);
  }

  public void deleteIndex(String indexName) {
    client.deleteIndex(indexName);
  }

  public void waitForReplication(String indexName) throws IOException {
    waitForReplication(indexName, DEFAULT_REPLICATION_WAIT_TIMEOUT_MS);
  }

  public void waitForReplication(String indexName, long timeoutMs) throws IOException {
    ShardState shardState = getGlobalState().getIndex(indexName).getShard(0);
    if (!shardState.isReplica()) {
      throw new IllegalStateException("Must be called on replica index");
    }
    long start = System.currentTimeMillis();
    while (shardState.nrtReplicaNode.isCopying()
        && (System.currentTimeMillis() - start) < timeoutMs) {
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    if (shardState.nrtReplicaNode.isCopying()) {
      throw new RuntimeException("Timed out waiting for replication");
    }
  }

  public void registerWithPrimary(String indexName) throws IOException {
    registerWithPrimary(indexName, DEFAULT_PRIMARY_REGISTER_TIMEOUT_MS);
  }

  public void registerWithPrimary(String indexName, long timeoutMs) throws IOException {
    ShardState shardState = getGlobalState().getIndex(indexName).getShard(0);
    if (!shardState.isReplica()) {
      throw new IllegalStateException("Must be called on replica index");
    }

    AddReplicaRequest addReplicaRequest =
        AddReplicaRequest.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName(indexName)
            .setReplicaId(ShardState.REPLICA_ID)
            .setHostName("localhost")
            .setPort(getGlobalState().getReplicationPort())
            .build();

    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < timeoutMs) {
      try {
        AddReplicaResponse response =
            shardState
                .nrtReplicaNode
                .getPrimaryAddress()
                .getBlockingStub()
                .addReplicas(addReplicaRequest);
        if (response.getOk().equals("ok")) {
          return;
        }
      } catch (StatusRuntimeException ignored) {
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException("Timed out trying to register with primary");
  }

  public static Builder builder(TemporaryFolder folder) {
    return new Builder(folder);
  }

  public static class Builder {
    private final TemporaryFolder folder;
    private final String uuid = UUID.randomUUID().toString();
    private String serviceName = SERVICE_NAME;

    private boolean autoStart = false;
    private boolean decInitialCommit = false;
    private Mode mode = Mode.STANDALONE;
    private int port = 0;
    private IndexDataLocationType locationType = IndexDataLocationType.LOCAL;
    private boolean fileDiscovery = false;

    private StateBackendType stateBackendType = StateBackendType.LOCAL;
    private boolean backendReadOnly = true;

    private boolean incArchiver = true;

    private boolean syncInitialNrtPoint = true;

    private int maxWarmingQueries = 0;
    private int warmingParallelism = 1;
    private boolean warmOnStartup = false;
    private boolean writeDiscoveryFile = false;

    private String additionalConfig = "";

    Builder(TemporaryFolder folder) {
      this.folder = folder;
    }

    public Builder withServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public Builder withAutoStartConfig(
        boolean autoStart, Mode mode, int port, IndexDataLocationType locationType) {
      this.autoStart = autoStart;
      this.mode = mode;
      this.port = port;
      this.locationType = locationType;
      this.fileDiscovery = mode == Mode.REPLICA && port <= 0;
      return this;
    }

    public Builder withAdditionalConfig(String additionalConfig) {
      this.additionalConfig = additionalConfig;
      return this;
    }

    public Builder withLocalStateBackend() {
      this.stateBackendType = StateBackendType.LOCAL;
      this.backendReadOnly = true;
      return this;
    }

    public Builder withRemoteStateBackend(boolean readOnly) {
      this.stateBackendType = StateBackendType.REMOTE;
      this.backendReadOnly = readOnly;
      return this;
    }

    public Builder withIncArchiver(boolean enabled) {
      this.incArchiver = enabled;
      return this;
    }

    public Builder withDecInitialCommit(boolean enabled) {
      this.decInitialCommit = enabled;
      return this;
    }

    public Builder withSyncInitialNrtPoint(boolean enable) {
      this.syncInitialNrtPoint = enable;
      return this;
    }

    public Builder withWarming(
        int maxWarmingQueries, int warmingParallelism, boolean warmOnStartup) {
      this.maxWarmingQueries = maxWarmingQueries;
      this.warmingParallelism = warmingParallelism;
      this.warmOnStartup = warmOnStartup;
      return this;
    }

    public Builder withWriteDiscoveryFile(boolean writeDiscoveryFile) {
      this.writeDiscoveryFile = writeDiscoveryFile;
      return this;
    }

    public TestServer build() throws IOException {
      initS3(folder);
      String configFile =
          String.join(
              "\n",
              baseConfig(),
              backendConfig(),
              autoStartConfig(),
              archiverConfig(),
              warmingConfig(),
              "syncInitialNrtPoint: " + syncInitialNrtPoint,
              additionalConfig);
      return new TestServer(
          new LuceneServerConfiguration(new ByteArrayInputStream(configFile.getBytes())),
          writeDiscoveryFile,
          Paths.get(folder.getRoot().toString(), DISCOVERY_FILE));
    }

    private String archiverConfig() {
      return String.join(
          "\n", "backupWithIncArchiver: " + incArchiver, "restoreFromIncArchiver: " + incArchiver);
    }

    private String backendConfig() {
      if (StateBackendType.LOCAL.equals(stateBackendType)) {
        return String.join("\n", "stateConfig:", "  backendType: LOCAL");
      } else {
        return String.join(
            "\n",
            "stateConfig:",
            "  backendType: REMOTE",
            "  remote:",
            "    readOnly: " + backendReadOnly);
      }
    }

    private String autoStartConfig() {
      String config =
          String.join(
              "\n",
              "indexStartConfig:",
              "  autoStart: " + autoStart,
              "  dataLocationType: " + locationType,
              "  mode: " + mode,
              "  primaryDiscovery:");
      if (fileDiscovery) {
        return String.join(
            "\n", config, "    file: " + Paths.get(folder.getRoot().toString(), DISCOVERY_FILE));
      } else {
        return String.join("\n", config, "    host: localhost", "    port: " + port);
      }
    }

    private String warmingConfig() {
      return String.join(
          "\n",
          "warmer:",
          "  maxWarmingQueries: " + maxWarmingQueries,
          "  warmingParallelism: " + warmingParallelism,
          "  warmOnStartup: " + warmOnStartup);
    }

    private String baseConfig() {
      return String.join(
          "\n",
          "nodeName: test_node-" + uuid,
          "serviceName: " + serviceName,
          "stateDir: " + Paths.get(folder.getRoot().toString(), "state_dir"),
          "indexDir: " + Paths.get(folder.getRoot().toString(), "index_dir-" + uuid),
          "archiveDirectory: " + Paths.get(folder.getRoot().toString(), "archive_dir-" + uuid),
          "decInitialCommit: " + decInitialCommit,
          "syncInitialNrtPoint: true");
    }
  }
}
