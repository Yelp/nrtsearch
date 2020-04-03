package com.yelp.nrtsearch.server.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import me.dinowernli.grpc.prometheus.Configuration;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import com.yelp.nrtsearch.server.utils.Archiver;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;


public class GrpcServer {
    private final GrpcCleanupRule grpcCleanup;
    private final TemporaryFolder temporaryFolder;
    private String indexDir;
    private String testIndex;
    private LuceneServerGrpc.LuceneServerBlockingStub blockingStub;
    private LuceneServerGrpc.LuceneServerStub stub;
    private ReplicationServerGrpc.ReplicationServerBlockingStub replicationServerBlockingStub;
    private ReplicationServerGrpc.ReplicationServerStub replicationServerStub;

    private GlobalState globalState;
    private LuceneServerClient luceneServerClient;
    private Server luceneServer;
    private ManagedChannel luceneServerManagedChannel;
    private Server replicationServer;
    private ManagedChannel replicationServerManagedChannel;


    public GrpcServer(CollectorRegistry collectorRegistry, GrpcCleanupRule grpcCleanup, TemporaryFolder temporaryFolder,
                      boolean isReplication, GlobalState globalState,
                      String indexDir, String index, int port, Archiver archiver) throws IOException {
        this.grpcCleanup = grpcCleanup;
        this.temporaryFolder = temporaryFolder;
        this.globalState = globalState;
        this.indexDir = indexDir;
        this.testIndex = index;
        invoke(collectorRegistry, isReplication, port, archiver);
    }


    public GrpcServer(GrpcCleanupRule grpcCleanup, TemporaryFolder temporaryFolder,
                      boolean isReplication, GlobalState globalState,
                      String indexDir, String index, int port, Archiver archiver) throws IOException {
        this(null, grpcCleanup, temporaryFolder, isReplication, globalState, indexDir, index, port, archiver);
    }

    public GrpcServer(GrpcCleanupRule grpcCleanup, TemporaryFolder temporaryFolder,
                      boolean isReplication, GlobalState globalState,
                      String indexDir, String index, int port) throws IOException {
        this(grpcCleanup, temporaryFolder, isReplication, globalState, indexDir, index, port, null);
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

    public void shutdown() {
        if (luceneServer != null && luceneServerManagedChannel != null) {
            luceneServer.shutdown();
            luceneServerManagedChannel.shutdown();
            luceneServer = null;
            luceneServerManagedChannel = null;
        }
        if (replicationServer != null && replicationServerManagedChannel != null) {
            replicationServer.shutdown();
            replicationServerManagedChannel.shutdown();
            replicationServer = null;
            replicationServerStub = null;
        }
    }

    /**
     * To test the server, make calls with a real stub using the in-process channel, and verify
     * behaviors or state changes from the client side.
     */
    private void invoke(CollectorRegistry collectorRegistry, boolean isReplication, int port, Archiver archiver) throws IOException {
        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();
        if (!isReplication) {
            Server server;
            if (collectorRegistry == null) {
                // Create a server, add service, start, and register for automatic graceful shutdown.
                server = ServerBuilder.forPort(port)
                        .addService(new LuceneServer.LuceneServerImpl(globalState, archiver, collectorRegistry).bindService())
                        .build()
                        .start();
            } else {
                MonitoringServerInterceptor monitoringInterceptor =
                        MonitoringServerInterceptor.create(Configuration
                                .allMetrics()
                                .withCollectorRegistry(collectorRegistry));
                // Create a server, add service, start, and register for automatic graceful shutdown.
                server = ServerBuilder.forPort(port)
                        .addService(ServerInterceptors.intercept(new LuceneServer.LuceneServerImpl(globalState, archiver, collectorRegistry), monitoringInterceptor))
                        .build()
                        .start();
            }
            grpcCleanup.register(server);

            // Create a client channel and register for automatic graceful shutdown.
            ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            grpcCleanup.register(managedChannel);
            luceneServer = server;
            luceneServerManagedChannel = managedChannel;
            blockingStub = LuceneServerGrpc.newBlockingStub(managedChannel);
            stub = LuceneServerGrpc.newStub(managedChannel);

            replicationServerBlockingStub = null;
            replicationServerStub = null;
            replicationServer = null;
            replicationServerManagedChannel = null;

        } else {
            // Create a server, add service, start, and register for automatic graceful shutdown.
            Server server = ServerBuilder.forPort(port)
                    .addService(new LuceneServer.ReplicationServerImpl(globalState))
                    .build()
                    .start();
            grpcCleanup.register(server);

            // Create a client channel and register for automatic graceful shutdown.
            ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            grpcCleanup.register(managedChannel);
            replicationServer = server;
            replicationServerManagedChannel = managedChannel;
            replicationServerBlockingStub = ReplicationServerGrpc.newBlockingStub(managedChannel);
            replicationServerStub = ReplicationServerGrpc.newStub(managedChannel);

            blockingStub = null;
            stub = null;
            luceneServer = null;
            luceneServerManagedChannel = null;
        }

    }

    //TODO fix server to not need to use specific named directories?
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


        TestServer(GrpcServer grpcServer, boolean startIndex, Mode mode, int primaryGen, boolean startOldIndex) throws IOException {
            this.grpcServer = grpcServer;
            if (startIndex) {
                new IndexAndRoleManager(grpcServer).createStartIndexAndRegisterFields(mode, primaryGen, startOldIndex);
            }
        }

        TestServer(GrpcServer grpcServer, boolean startIndex, Mode mode, int primaryGen) throws IOException {
            this(grpcServer, startIndex, mode, primaryGen, false);
        }

        TestServer(GrpcServer grpcServer, boolean startIndex, Mode mode) throws IOException {
            this(grpcServer, startIndex, mode, 0);
        }

        public void addDocuments() throws IOException, InterruptedException {
            addDocuments(null);
        }

        public AddDocumentResponse addDocuments(String fileName) throws IOException, InterruptedException {
            CountDownLatch finishLatch = new CountDownLatch(1);
            //observers responses from Server(should get one onNext and oneCompleted)
            StreamObserver<AddDocumentResponse> responseStreamObserver = new StreamObserver<AddDocumentResponse>() {
                @Override
                public void onNext(AddDocumentResponse value) {
                    addDocumentResponse = value;
                }

                @Override
                public void onError(Throwable t) {
                    error = true;
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    completed = true;
                    finishLatch.countDown();
                }
            };
            //requestObserver sends requests to Server (one onNext per AddDocumentRequest and one onCompleted)
            StreamObserver<AddDocumentRequest> requestObserver = grpcServer.getStub().addDocuments(responseStreamObserver);
            //parse CSV into a stream of AddDocumentRequest
            Stream<AddDocumentRequest> addDocumentRequestStream = getAddDocumentRequestStream(fileName);
            try {
                addDocumentRequestStream.forEach(addDocumentRequest -> requestObserver.onNext(addDocumentRequest));
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
            return addDocumentResponse;

        }

        private Stream<AddDocumentRequest> getAddDocumentRequestStream(String fileName) throws IOException {
            String addDocsFile = fileName == null ? "addDocs.csv" : fileName;
            Path filePath = Paths.get("src", "test", "resources", addDocsFile);
            Reader reader = Files.newBufferedReader(filePath);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            return new LuceneServerClientBuilder.AddDcoumentsClientBuilder(grpcServer.getTestIndex(), csvParser).buildRequest(filePath);
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

        public FieldDefResponse createStartIndexAndRegisterFields(Mode mode, int primaryGen) throws IOException {
            return createStartIndexAndRegisterFields(mode, primaryGen, false);
        }

        public FieldDefResponse createStartIndexAndRegisterFields(Mode mode, int primaryGen, boolean startOldIndex) throws IOException {
            return createStartIndexAndRegisterFields(mode, primaryGen, startOldIndex, null);
        }

        public FieldDefResponse createStartIndexAndRegisterFields(Mode mode, int primaryGen, boolean startOldIndex, String registerFieldsFileName) throws IOException {
            String registerFields = registerFieldsFileName == null ? "registerFieldsBasic.json" : registerFieldsFileName;
            String rootDirName = grpcServer.getIndexDir();
            String testIndex = grpcServer.getTestIndex();
            LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();
            if (!startOldIndex) {
                //create the index
                blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(testIndex).setRootDir(rootDirName).build());

            }
            //start the index
            StartIndexRequest.Builder startIndexBuilder = StartIndexRequest.newBuilder().setIndexName(testIndex);
            RestoreIndex restoreIndex = RestoreIndex.newBuilder().setServiceName("testservice").setResourceName("testresource").build();
            if (mode.equals(Mode.PRIMARY)) {
                startIndexBuilder.setMode(Mode.PRIMARY);
                startIndexBuilder.setPrimaryGen(primaryGen);
            } else if (mode.equals(Mode.REPLICA)) {
                startIndexBuilder.setMode(Mode.REPLICA);
                startIndexBuilder.setPrimaryAddress("localhost");
                startIndexBuilder.setPort(9001); //primary port for replication server
            }
            if (startOldIndex) {
                startIndexBuilder.setRestore(restoreIndex);
            }
            blockingStub.startIndex(startIndexBuilder.build());

            if (!startOldIndex) {
                //register the fields
                FieldDefRequest fieldDefRequest = buildFieldDefRequest(Paths.get("src", "test", "resources", registerFields));
                return blockingStub.registerFields(fieldDefRequest);
            } else {//dummy
                return FieldDefResponse.newBuilder().build();
            }
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
