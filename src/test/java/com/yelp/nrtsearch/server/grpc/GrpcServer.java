/*
 * Copyright 2020 Yelp Inc.
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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.NrtsearchServer.LuceneServerImpl;
import com.yelp.nrtsearch.server.monitoring.Configuration;
import com.yelp.nrtsearch.server.monitoring.NrtsearchMonitoringServerInterceptor;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.rules.TemporaryFolder;

public class GrpcServer {
  // TODO: use this everywhere instead of importing test index name from
  // ReplicationFailureScenariosTest
  public static final String TEST_INDEX = "test_index";

  private final GrpcCleanupRule grpcCleanup;
  private final TemporaryFolder temporaryFolder;
  private final RemoteBackend remoteBackend;
  private String indexDir;
  private String testIndex;
  private LuceneServerGrpc.LuceneServerBlockingStub blockingStub;
  private LuceneServerGrpc.LuceneServerStub stub;
  private ReplicationServerGrpc.ReplicationServerBlockingStub replicationServerBlockingStub;
  private ReplicationServerGrpc.ReplicationServerStub replicationServerStub;

  private GlobalState globalState;
  private NrtsearchConfig configuration;
  private Server server;
  private ManagedChannel luceneServerManagedChannel;
  private Server replicationServer;
  private ManagedChannel replicationServerManagedChannel;

  public GrpcServer(
      PrometheusRegistry prometheusRegistry,
      GrpcCleanupRule grpcCleanup,
      NrtsearchConfig configuration,
      TemporaryFolder temporaryFolder,
      GlobalState replicationGlobalState,
      String indexDir,
      String index,
      int port,
      RemoteBackend remoteBackend,
      List<Plugin> plugins)
      throws IOException {
    this.grpcCleanup = grpcCleanup;
    this.temporaryFolder = temporaryFolder;
    this.configuration = configuration;
    this.globalState = replicationGlobalState;
    this.indexDir = indexDir;
    this.testIndex = index;
    this.remoteBackend = remoteBackend;
    invoke(prometheusRegistry, replicationGlobalState != null, port, remoteBackend, plugins);
  }

  public GrpcServer(
      GrpcCleanupRule grpcCleanup,
      NrtsearchConfig configuration,
      TemporaryFolder temporaryFolder,
      GlobalState replicationGlobalState,
      String indexDir,
      String index,
      int port,
      RemoteBackend remoteBackend)
      throws IOException {
    this(
        null,
        grpcCleanup,
        configuration,
        temporaryFolder,
        replicationGlobalState,
        indexDir,
        index,
        port,
        remoteBackend,
        Collections.emptyList());
  }

  public GrpcServer(
      GrpcCleanupRule grpcCleanup,
      NrtsearchConfig configuration,
      TemporaryFolder temporaryFolder,
      GlobalState replicationGlobalState,
      String indexDir,
      String index,
      int port)
      throws IOException {
    this(
        grpcCleanup,
        configuration,
        temporaryFolder,
        replicationGlobalState,
        indexDir,
        index,
        port,
        null);
  }

  public String getIndexDir() {
    return indexDir;
  }

  public String getTestIndex() {
    return testIndex;
  }

  public LuceneServerGrpc.LuceneServerBlockingStub getBlockingStub() {
    return blockingStub;
  }

  public LuceneServerGrpc.LuceneServerStub getStub() {
    return stub;
  }

  public ReplicationServerGrpc.ReplicationServerBlockingStub getReplicationServerBlockingStub() {
    return replicationServerBlockingStub;
  }

  public ReplicationServerGrpc.ReplicationServerStub getReplicationServerStub() {
    return replicationServerStub;
  }

  public GlobalState getGlobalState() {
    return globalState;
  }

  private RemoteBackend getRemoteBackend() {
    return remoteBackend;
  }

  public void shutdown() {
    if (server != null && luceneServerManagedChannel != null) {
      server.shutdown();
      luceneServerManagedChannel.shutdown();
      server = null;
      luceneServerManagedChannel = null;
    }
    if (replicationServer != null && replicationServerManagedChannel != null) {
      replicationServer.shutdown();
      replicationServerManagedChannel.shutdown();
      replicationServer = null;
      replicationServerStub = null;
    }
  }

  public void forceShutdown() {
    if (server != null && luceneServerManagedChannel != null) {
      server.shutdownNow();
      luceneServerManagedChannel.shutdownNow();
      server = null;
      luceneServerManagedChannel = null;
    }
    if (replicationServer != null && replicationServerManagedChannel != null) {
      replicationServer.shutdownNow();
      replicationServerManagedChannel.shutdownNow();
      replicationServer = null;
      replicationServerStub = null;
    }
  }

  /**
   * To test the server, make calls with a real stub using the in-process channel, and verify
   * behaviors or state changes from the client side.
   */
  private void invoke(
      PrometheusRegistry prometheusRegistry,
      boolean isReplication,
      int port,
      RemoteBackend remoteBackend,
      List<Plugin> plugins)
      throws IOException {
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();
    if (!isReplication) {
      Server server;
      if (prometheusRegistry == null) {
        LuceneServerImpl serverImpl =
            new NrtsearchServer.LuceneServerImpl(
                configuration, remoteBackend, prometheusRegistry, plugins);
        globalState = serverImpl.getGlobalState();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        server =
            ServerBuilder.forPort(port)
                .addService(
                    ServerInterceptors.intercept(
                        serverImpl.bindService(), new NrtsearchHeaderInterceptor()))
                .compressorRegistry(LuceneServerStubBuilder.COMPRESSOR_REGISTRY)
                .decompressorRegistry(LuceneServerStubBuilder.DECOMPRESSOR_REGISTRY)
                .build()
                .start();
      } else {
        NrtsearchMonitoringServerInterceptor monitoringInterceptor =
            NrtsearchMonitoringServerInterceptor.create(
                Configuration.allMetrics().withPrometheusRegistry(prometheusRegistry));
        LuceneServerImpl serverImpl =
            new NrtsearchServer.LuceneServerImpl(
                configuration, remoteBackend, prometheusRegistry, plugins);
        globalState = serverImpl.getGlobalState();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        server =
            ServerBuilder.forPort(port)
                .addService(
                    ServerInterceptors.intercept(
                        serverImpl, new NrtsearchHeaderInterceptor(), monitoringInterceptor))
                .compressorRegistry(LuceneServerStubBuilder.COMPRESSOR_REGISTRY)
                .decompressorRegistry(LuceneServerStubBuilder.DECOMPRESSOR_REGISTRY)
                .build()
                .start();
      }
      grpcCleanup.register(server);

      // Create a client channel and register for automatic graceful shutdown.
      LuceneServerStubBuilder stubBuilder = new LuceneServerStubBuilder("localhost", port);
      grpcCleanup.register(stubBuilder.channel);
      this.server = server;
      luceneServerManagedChannel = stubBuilder.channel;
      blockingStub = stubBuilder.createBlockingStub();
      stub = stubBuilder.createAsyncStub();

      replicationServerBlockingStub = null;
      replicationServerStub = null;
      replicationServer = null;
      replicationServerManagedChannel = null;

    } else {
      // Create a server, add service, start, and register for automatic graceful shutdown.
      Server server =
          ServerBuilder.forPort(port)
              .addService(new NrtsearchServer.ReplicationServerImpl(globalState, false))
              .compressorRegistry(LuceneServerStubBuilder.COMPRESSOR_REGISTRY)
              .decompressorRegistry(LuceneServerStubBuilder.DECOMPRESSOR_REGISTRY)
              .build()
              .start();
      grpcCleanup.register(server);

      // Create a client channel and register for automatic graceful shutdown.
      ManagedChannel managedChannel =
          ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
      grpcCleanup.register(managedChannel);
      replicationServer = server;
      replicationServerManagedChannel = managedChannel;
      replicationServerBlockingStub = ReplicationServerGrpc.newBlockingStub(managedChannel);
      replicationServerStub = ReplicationServerGrpc.newStub(managedChannel);

      blockingStub = null;
      stub = null;
      this.server = null;
      luceneServerManagedChannel = null;
    }
  }

  // TODO fix server to not need to use specific named directories?
  public static void rmDir(Path dir) throws IOException {
    if (Files.exists(dir)) {
      if (Files.isRegularFile(dir)) {
        Files.delete(dir);
      } else {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
          for (Path path : stream) {
            if (Files.isDirectory(path)) {
              rmDir(path);
            } else {
              Files.delete(path);
            }
          }
        }
        Files.delete(dir);
      }
    }
  }

  public static class TestServer {
    private final GrpcServer grpcServer;
    public AddDocumentResponse addDocumentResponse;
    public boolean completed = false;
    public boolean error = false;
    public Throwable throwable = null;

    public TestServer(
        GrpcServer grpcServer, boolean startIndex, Mode mode, int primaryGen, boolean startOldIndex)
        throws IOException {
      this.grpcServer = grpcServer;
      if (startIndex) {
        new IndexAndRoleManager(grpcServer)
            .createStartIndexAndRegisterFields(mode, primaryGen, startOldIndex);
      }
    }

    public TestServer(GrpcServer grpcServer, boolean startIndex, Mode mode, int primaryGen)
        throws IOException {
      this(grpcServer, startIndex, mode, primaryGen, false);
    }

    public TestServer(GrpcServer grpcServer, boolean startIndex, Mode mode) throws IOException {
      this(grpcServer, startIndex, mode, 0);
    }

    public AddDocumentResponse addDocuments() throws IOException, InterruptedException {
      return addDocuments(null);
    }

    public AddDocumentResponse addDocuments(String fileName)
        throws IOException, InterruptedException {
      Stream<AddDocumentRequest> addDocumentRequestStream = getAddDocumentRequestStream(fileName);
      addDocumentsFromStream(addDocumentRequestStream);
      if (!error) {
        refresh();
      }
      return addDocumentResponse;
    }

    public void addDocumentsFromStream(Stream<AddDocumentRequest> addDocumentRequestStream)
        throws InterruptedException {
      CountDownLatch finishLatch = new CountDownLatch(1);
      // observers responses from Server(should get one onNext and oneCompleted)
      StreamObserver<AddDocumentResponse> responseStreamObserver =
          new StreamObserver<AddDocumentResponse>() {
            @Override
            public void onNext(AddDocumentResponse value) {
              addDocumentResponse = value;
            }

            @Override
            public void onError(Throwable t) {
              error = true;
              throwable = t;
              finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
              completed = true;
              finishLatch.countDown();
            }
          };
      // requestObserver sends requests to Server (one onNext per AddDocumentRequest and one
      // onCompleted)
      StreamObserver<AddDocumentRequest> requestObserver =
          grpcServer.getStub().addDocuments(responseStreamObserver);
      // parse CSV into a stream of AddDocumentRequest
      try {
        addDocumentRequestStream.forEach(
            addDocumentRequest -> requestObserver.onNext(addDocumentRequest));
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
    }

    public void refresh() {
      grpcServer
          .getBlockingStub()
          .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    }

    private Stream<AddDocumentRequest> getAddDocumentRequestStream(String fileName)
        throws IOException {
      String addDocsFile = fileName == null ? "addDocs.csv" : fileName;
      Path filePath = Paths.get("src", "test", "resources", addDocsFile);
      Reader reader = Files.newBufferedReader(filePath);
      CSVParser csvParser =
          new CSVParser(
              reader, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build());
      return new NrtsearchClientBuilder.AddDocumentsClientBuilder(
              grpcServer.getTestIndex(), csvParser)
          .buildRequest(filePath);
    }
  }

  public static class IndexAndRoleManager {

    private GrpcServer grpcServer;

    public IndexAndRoleManager(GrpcServer grpcServer) {
      this.grpcServer = grpcServer;
    }

    public FieldDefResponse createStartIndexAndRegisterFields(Mode mode) throws IOException {
      return createStartIndexAndRegisterFields(mode, 0);
    }

    public FieldDefResponse createStartIndexAndRegisterFields(Mode mode, int primaryGen)
        throws IOException {
      return createStartIndexAndRegisterFields(mode, primaryGen, false);
    }

    public FieldDefResponse createStartIndexAndRegisterFields(
        Mode mode, int primaryGen, boolean startOldIndex) throws IOException {
      return createStartIndexAndRegisterFields(mode, primaryGen, startOldIndex, null);
    }

    public FieldDefResponse createStartIndexAndRegisterFields(
        Mode mode, int primaryGen, boolean startOldIndex, String registerFieldsFileName)
        throws IOException {
      String testIndex = grpcServer.getTestIndex();
      LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();
      if (!startOldIndex) {
        // create the index
        blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(testIndex).build());
      }
      // start the index
      StartIndexRequest.Builder startIndexBuilder =
          StartIndexRequest.newBuilder().setIndexName(testIndex);

      if (mode.equals(Mode.PRIMARY)) {
        startIndexBuilder.setMode(Mode.PRIMARY);
        startIndexBuilder.setPrimaryGen(primaryGen);
      } else if (mode.equals(Mode.REPLICA)) {
        startIndexBuilder.setMode(Mode.REPLICA);
        startIndexBuilder.setPrimaryAddress("localhost");
        startIndexBuilder.setPort(9001); // primary port for replication server
      }
      if (startOldIndex) {
        blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(testIndex).build());
        RestoreIndex restoreIndex =
            RestoreIndex.newBuilder()
                .setServiceName("testservice")
                .setResourceName("testresource")
                .build();
        startIndexBuilder.setRestore(restoreIndex);
      }
      blockingStub.startIndex(startIndexBuilder.build());

      String registerFields =
          registerFieldsFileName == null ? "registerFieldsBasic.json" : registerFieldsFileName;
      // register the fields
      FieldDefRequest fieldDefRequest =
          buildFieldDefRequest(Paths.get("src", "test", "resources", registerFields));
      return blockingStub.registerFields(fieldDefRequest);
    }

    private FieldDefRequest buildFieldDefRequest(Path filePath) throws IOException {
      return getFieldDefRequest(Files.readString(filePath));
    }

    private FieldDefRequest getFieldDefRequest(String jsonStr) {
      FieldDefRequest.Builder fieldDefRequestBuilder = FieldDefRequest.newBuilder();
      try {
        JsonFormat.parser().merge(jsonStr, fieldDefRequestBuilder);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      FieldDefRequest fieldDefRequest = fieldDefRequestBuilder.build();
      return fieldDefRequest;
    }
  }
}
