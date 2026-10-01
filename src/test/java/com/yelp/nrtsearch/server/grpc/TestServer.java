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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.yelp.nrtsearch.clientlib.Node;
import com.yelp.nrtsearch.server.concurrent.ExecutorFactory;
import com.yelp.nrtsearch.server.config.IndexStartConfig.IndexDataLocationType;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.config.StateConfig.StateBackendType;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.NrtsearchServer.LuceneServerImpl;
import com.yelp.nrtsearch.server.grpc.NrtsearchServer.ReplicationServerImpl;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.index.ShardState;
import com.yelp.nrtsearch.server.monitoring.Configuration;
import com.yelp.nrtsearch.server.monitoring.NrtsearchMonitoringServerInterceptor;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.server.state.GlobalState;
import com.yelp.nrtsearch.server.utils.FileUtils;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import com.yelp.nrtsearch.test_utils.PortUtils;
import com.yelp.nrtsearch.test_utils.TestDocumentHelper;
import io.findify.s3mock.S3Mock;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.StatusRuntimeException;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

public class TestServer {
  private static final List<TestServer> createdServers = new ArrayList<>();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final Gson gson = new GsonBuilder().serializeNulls().create();
  public static final String SERVICE_NAME = "test_server";
  public static final String TEST_BUCKET = "test-server-data-bucket";
  public static String S3_ENDPOINT = null;
  public static Path s3TempDir = null;
  public static final String DISCOVERY_FILE = "primary_node.json";
  public static final long DEFAULT_REPLICATION_WAIT_TIMEOUT_MS = 60000;
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

  private final NrtsearchConfig configuration;
  private final boolean writeDiscoveryFile;
  private final Path discoveryFilePath;
  private final List<Plugin> plugins;
  private Server server;
  private Server replicationServer;
  private NrtsearchClient client;
  private ReplicationServerClient replicationClient;
  private LuceneServerImpl serverImpl;
  private ExecutorFactory executorFactory;
  private RemoteBackend remoteBackend;
  private PrometheusRegistry prometheusRegistry;

  public static void initS3(TemporaryFolder folder) throws IOException {
    if (api == null) {
      // Use an independent temp dir (not the per-test @Rule folder) so that
      // S3Mock's FileProvider storage survives across multiple tests in the same
      // class.  The per-test folder is deleted by @Rule after each test, which
      // was silently breaking S3Mock for every test after the first.
      s3TempDir = Files.createTempDirectory("nrtsearch-s3mock-");
      Path s3Directory = s3TempDir;
      for (int attempt = 0; attempt < 5; attempt++) {
        int port = PortUtils.findAvailablePort();
        S3Mock mock = S3Mock.create(port, s3Directory.toAbsolutePath().toString());
        try {
          mock.start();
          api = mock;
          S3_ENDPOINT = "http://127.0.0.1:" + port;
          // Phase 1: wait for Akka's HTTP layer to accept any connection.
          for (int readyAttempt = 0; readyAttempt < 100; readyAttempt++) {
            try {
              HttpURLConnection conn =
                  (HttpURLConnection) new URL(S3_ENDPOINT + "/").openConnection();
              conn.setConnectTimeout(500);
              conn.setReadTimeout(500);
              try {
                conn.getResponseCode();
                break;
              } finally {
                conn.disconnect();
              }
            } catch (IOException ignored) {
              Thread.sleep(100);
            }
          }
          // Phase 2: wait for the bucket-creation route to be registered.
          // After Phase 1 the HTTP layer is up but the storage actor may not have finished
          // registering its routes yet.
          S3Client probeS3 = AmazonS3Provider.createTestS3Client(S3_ENDPOINT);
          for (int bucketAttempt = 0; bucketAttempt < 100; bucketAttempt++) {
            try {
              probeS3.createBucket(
                  CreateBucketRequest.builder().bucket("s3mock-readiness-probe").build());
              break;
            } catch (Exception e) {
              try {
                Thread.sleep(200);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
              }
            }
          }
          return;
        } catch (Exception e) {
          if (attempt == 4) {
            throw new IOException("Failed to start S3Mock after 5 attempts", e);
          }
        }
      }
    }
  }

  public static void cleanupServers() {
    createdServers.forEach(TestServer::stop);
    createdServers.forEach(
        s -> {
          if (s.executorFactory != null) {
            try {
              s.executorFactory.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
    createdServers.clear();
    // Reset S3 bucket between tests so committed index state from one test doesn't bleed into the
    // next.
    resetS3Bucket();
  }

  public static void cleanupAll() {
    cleanupServers();
    if (api != null) {
      int shutdownPort = S3_ENDPOINT != null ? Integer.parseInt(S3_ENDPOINT.split(":")[2]) : -1;
      api.shutdown();
      api = null;
      S3_ENDPOINT = null;
      // Wait up to 2s for Akka to fully release the port. When the old system is
      // still cleaning up its threads, the new S3Mock's Akka starts slower, causing
      // BindExceptions and createBucket failures. A clean port release means the
      // next initS3() can start an Akka system without thread-pool contention.
      if (shutdownPort > 0) {
        for (int i = 0; i < 20; i++) {
          try (java.net.Socket s = new java.net.Socket("127.0.0.1", shutdownPort)) {
            Thread.sleep(100);
          } catch (IOException e) {
            break; // port closed — Akka done
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
      if (s3TempDir != null) {
        try {
          FileUtils.deleteAllFiles(s3TempDir);
        } catch (IOException ignored) {
        }
        s3TempDir = null;
      }
    }
  }

  /**
   * Deletes all objects from TEST_BUCKET and recreates it. Call from @Before to give each test a
   * clean bucket without restarting S3Mock.
   */
  public static void resetS3Bucket() {
    if (S3_ENDPOINT == null) return;
    S3Client s3 = AmazonS3Provider.createTestS3Client(S3_ENDPOINT);
    // Only delete objects — do NOT delete/recreate the bucket. Deleting and recreating
    // the bucket causes S3Mock to transiently return 404 on the next createBucket call
    // under JVM load, which cascades into failures across all subsequent test classes.
    try {
      List<ObjectIdentifier> objects =
          s3.listObjectsV2(r -> r.bucket(TEST_BUCKET)).contents().stream()
              .map(o -> ObjectIdentifier.builder().key(o.key()).build())
              .collect(Collectors.toList());
      if (!objects.isEmpty()) {
        s3.deleteObjects(r -> r.bucket(TEST_BUCKET).delete(d -> d.objects(objects)));
      }
    } catch (Exception ignored) {
    }
  }

  public TestServer(
      NrtsearchConfig configuration, boolean writeDiscoveryFile, Path discoveryFilePath)
      throws IOException {
    this(configuration, writeDiscoveryFile, discoveryFilePath, Collections.emptyList());
  }

  public TestServer(
      NrtsearchConfig configuration,
      boolean writeDiscoveryFile,
      Path discoveryFilePath,
      List<Plugin> plugins)
      throws IOException {
    this.configuration = configuration;
    this.writeDiscoveryFile = writeDiscoveryFile;
    this.discoveryFilePath = discoveryFilePath;
    this.plugins = plugins;
    createdServers.add(this);
    restart();
  }

  public NrtsearchConfig getConfiguration() {
    return configuration;
  }

  public PrometheusRegistry getPrometheusRegistry() {
    return prometheusRegistry;
  }

  private RemoteBackend createRemoteBackend() throws IOException {
    S3Client s3 = AmazonS3Provider.createTestS3Client(S3_ENDPOINT);
    // S3Mock 0.2.6 FileProvider.createBucket uses createDirectory(), which throws
    // FileAlreadyExistsException (wrapped as 500) when the bucket already exists.
    // HeadBucket is not implemented in S3Mock 0.2.6. Use listBuckets() to distinguish
    // "not ready" (listBuckets also fails) from "already exists" (listBuckets succeeds).
    Exception lastBucketException = null;
    for (int attempt = 0; attempt < 30; attempt++) {
      try {
        s3.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET).build());
        lastBucketException = null;
        break;
      } catch (Exception e) {
        try {
          boolean exists =
              s3.listBuckets().buckets().stream().anyMatch(b -> b.name().equals(TEST_BUCKET));
          if (exists) {
            lastBucketException = null;
            break;
          }
        } catch (Exception ignored) {
        }
        lastBucketException = e;
        try {
          Thread.sleep(200);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted waiting for S3Mock bucket creation", ie);
        }
      }
    }
    if (lastBucketException != null) {
      throw new IOException("S3 bucket creation failed after retries", lastBucketException);
    }
    software.amazon.awssdk.services.s3.S3AsyncClient s3Async =
        AmazonS3Provider.createTestS3AsyncClient(S3_ENDPOINT);
    software.amazon.awssdk.transfer.s3.S3TransferManager transferManager =
        software.amazon.awssdk.transfer.s3.S3TransferManager.builder().s3Client(s3Async).build();
    return new S3Backend(configuration, new S3Util.S3ClientBundle(s3, s3Async));
  }

  public void restart() throws IOException {
    restart(false);
  }

  public void restart(boolean clearData) throws IOException {
    stop(clearData);
    if (executorFactory == null) {
      executorFactory = new ExecutorFactory(configuration.getThreadPoolConfiguration());
    }
    remoteBackend = createRemoteBackend();
    prometheusRegistry = new PrometheusRegistry();
    serverImpl =
        new LuceneServerImpl(
            configuration, remoteBackend, prometheusRegistry, executorFactory, plugins);

    replicationServer =
        ServerBuilder.forPort(0)
            .addService(
                new ReplicationServerImpl(
                    serverImpl.getGlobalState(), configuration.getVerifyReplicationIndexId()))
            .build()
            .start();
    serverImpl.getGlobalState().replicationStarted(replicationServer.getPort());

    if (writeDiscoveryFile) {
      writeDiscoveryFile(replicationServer.getPort());
    }

    NrtsearchMonitoringServerInterceptor monitoringInterceptor =
        NrtsearchMonitoringServerInterceptor.create(
            Configuration.allMetrics().withPrometheusRegistry(prometheusRegistry));
    // On macOS/BSD, SO_REUSEADDR allows a second socket to bind to a port already in LISTEN
    // state. Two rapid forPort(0) calls can therefore land on the same port, causing the OS
    // to load-balance connections between the replication server (ReplicationServerImpl) and
    // the main server (LuceneServerImpl). When the client's channel is routed to the
    // replication server, createIndex gets UNIMPLEMENTED. Detect and retry until the main
    // server is assigned a distinct port.
    for (int attempt = 0; attempt < 10; attempt++) {
      if (server != null) {
        server.shutdown();
        try {
          server.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        server = null;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      server =
          ServerBuilder.forPort(configuration.getPort())
              .addService(
                  ServerInterceptors.intercept(
                      serverImpl, new NrtsearchHeaderInterceptor(), monitoringInterceptor))
              .build()
              .start();
      if (server.getPort() != replicationServer.getPort()) {
        break;
      }
    }
    client = new NrtsearchClient("localhost", server.getPort());
    replicationClient = new ReplicationServerClient("localhost", replicationServer.getPort());
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

  public NrtsearchClient getClient() {
    return client;
  }

  public ReplicationServerClient getReplicationClient() {
    return replicationClient;
  }

  public RemoteBackend getRemoteBackend() {
    return remoteBackend;
  }

  public void stop() {
    stop(false);
  }

  public void stop(boolean clearData) {
    if (serverImpl != null) {
      GlobalState globalState = serverImpl.getGlobalState();
      for (String indexName : globalState.getIndexNames()) {
        try {
          IndexState indexState = globalState.getIndexOrThrow(indexName);
          if (indexState.isStarted()) {
            indexState.close();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      if (clearData) {
        try {
          FileUtils.deleteAllFiles(globalState.getIndexDirBase());
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
    if (replicationClient != null) {
      replicationClient.close();
      replicationClient = null;
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
    return TestDocumentHelper.addDocuments(client.getAsyncStub(), requestStream);
  }

  public void addSimpleDocs(String indexName, int... ids) {
    List<AddDocumentRequest> requests = new ArrayList<>();
    for (int id : ids) {
      requests.add(getSimpleDocRequest(indexName, id));
    }
    addDocs(requests.stream());
  }

  public AddDocumentRequest getSimpleDocRequest(String indexName, int id) {
    return AddDocumentRequest.newBuilder()
        .setIndexName(indexName)
        .putFields("id", MultiValuedField.newBuilder().addValue(String.valueOf(id)).build())
        .putFields("field1", MultiValuedField.newBuilder().addValue(String.valueOf(id * 3)).build())
        .putFields("field2", MultiValuedField.newBuilder().addValue(String.valueOf(id * 5)).build())
        .build();
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

  public void waitForReplication(String indexName, TestServer primaryServer) throws IOException {
    waitForReplication(indexName, primaryServer, DEFAULT_REPLICATION_WAIT_TIMEOUT_MS);
  }

  public void waitForReplication(String indexName, TestServer primaryServer, long timeoutMs)
      throws IOException {
    ShardState replicaShardState = getGlobalState().getIndexOrThrow(indexName).getShard(0);
    if (!replicaShardState.isReplica()) {
      throw new IllegalStateException("Must be called on replica index");
    }
    long targetVersion =
        primaryServer
            .getGlobalState()
            .getIndexOrThrow(indexName)
            .getShard(0)
            .nrtPrimaryNode
            .getCurrentSearchingVersion();
    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < timeoutMs) {
      if (replicaShardState.nrtReplicaNode.getCurrentSearchingVersion() >= targetVersion) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException(
        String.format(
            "Timed out waiting for replication to version %d, replica at %d",
            targetVersion, replicaShardState.nrtReplicaNode.getCurrentSearchingVersion()));
  }

  public void registerWithPrimary(String indexName) throws IOException {
    registerWithPrimary(indexName, DEFAULT_PRIMARY_REGISTER_TIMEOUT_MS);
  }

  public void registerWithPrimary(String indexName, long timeoutMs) throws IOException {
    IndexStateManager indexStateManager = getGlobalState().getIndexStateManagerOrThrow(indexName);
    ShardState shardState = indexStateManager.getCurrent().getShard(0);
    if (!shardState.isReplica()) {
      throw new IllegalStateException("Must be called on replica index");
    }

    AddReplicaRequest addReplicaRequest =
        AddReplicaRequest.newBuilder()
            .setMagicNumber(BINARY_MAGIC)
            .setIndexName(indexName)
            .setIndexId(indexStateManager.getIndexId())
            .setNodeName(getGlobalState().getNodeName())
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
    private boolean withoutPrimary = false;

    private StateBackendType stateBackendType = StateBackendType.LOCAL;
    private boolean backendReadOnly = true;

    private boolean syncInitialNrtPoint = true;

    private int maxWarmingQueries = 0;
    private int warmingParallelism = 1;
    private boolean warmOnStartup = false;
    private boolean writeDiscoveryFile = false;

    private String additionalConfig = "";
    private List<Plugin> plugins = Collections.emptyList();
    private final int serverPort = 0;

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

    public Builder withoutPrimary() {
      this.withoutPrimary = true;
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

    public Builder withPlugins(List<Plugin> plugins) {
      this.plugins = plugins;
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
              warmingConfig(),
              "syncInitialNrtPoint: " + syncInitialNrtPoint,
              additionalConfig);
      return new TestServer(
          new NrtsearchConfig(new ByteArrayInputStream(configFile.getBytes())),
          writeDiscoveryFile,
          Paths.get(folder.getRoot().toString(), DISCOVERY_FILE),
          plugins);
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
              "  mode: " + mode);
      if (!withoutPrimary) {
        config = String.join("\n", config, "  primaryDiscovery:");
        if (fileDiscovery) {
          return String.join(
              "\n", config, "    file: " + Paths.get(folder.getRoot().toString(), DISCOVERY_FILE));
        } else {
          return String.join("\n", config, "    host: localhost", "    port: " + port);
        }
      } else {
        return config;
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
          "bucketName: " + TEST_BUCKET,
          "stateDir: " + Paths.get(folder.getRoot().toString(), "state_dir"),
          "indexDir: " + Paths.get(folder.getRoot().toString(), "index_dir-" + uuid),
          "port: " + serverPort,
          "decInitialCommit: " + decInitialCommit,
          "syncInitialNrtPoint: true");
    }
  }
}
