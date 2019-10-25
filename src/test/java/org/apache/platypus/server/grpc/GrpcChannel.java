package org.apache.platypus.server.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.platypus.server.GlobalState;
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


public class GrpcChannel {
    private final GrpcCleanupRule grpcCleanup;
    private final TemporaryFolder temporaryFolder;
    private String rootDirName;
    private String testIndex;
    private LuceneServerGrpc.LuceneServerBlockingStub blockingStub;
    private LuceneServerGrpc.LuceneServerStub stub;
    private ReplicationServerGrpc.ReplicationServerBlockingStub replicationServerBlockingStub;
    private ReplicationServerGrpc.ReplicationServerStub replicationServerStub;

    private GlobalState globalState;

    public GrpcChannel(GrpcCleanupRule grpcCleanup, TemporaryFolder temporaryFolder, boolean isReplication, GlobalState globalState, String rootDirName, String index) throws IOException {
        this.grpcCleanup = grpcCleanup;
        this.temporaryFolder = temporaryFolder;
        this.globalState = globalState;
        this.rootDirName = rootDirName;
        this.testIndex = index;
        invoke(isReplication);
    }

    public String getRootDirName() {
        return rootDirName;
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

    /**
     * To test the server, make calls with a real stub using the in-process channel, and verify
     * behaviors or state changes from the client side.
     */
    private void invoke(boolean isReplication) throws IOException {
        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();
        if (!isReplication) {
            // Create a server, add service, start, and register for automatic graceful shutdown.
            grpcCleanup.register(InProcessServerBuilder
                    .forName(serverName).directExecutor().addService(new LuceneServer.LuceneServerImpl(globalState)).build().start());
            blockingStub = LuceneServerGrpc.newBlockingStub(
                    // Create a client channel and register for automatic graceful shutdown.
                    grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
            stub = LuceneServerGrpc.newStub(
                    grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
            replicationServerBlockingStub = null;
            replicationServerStub = null;

        } else {
            // Create a server, add service, start, and register for automatic graceful shutdown.
            grpcCleanup.register(InProcessServerBuilder
                    .forName(serverName).directExecutor().addService(new LuceneServer.ReplicationServerImpl(globalState)).build().start());
            replicationServerBlockingStub = ReplicationServerGrpc.newBlockingStub(
                    // Create a client channel and register for automatic graceful shutdown.
                    grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
            replicationServerStub = ReplicationServerGrpc.newStub(
                    grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
            blockingStub = null;
            stub = null;
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
        private final GrpcChannel grpcChannel;
        public AddDocumentResponse addDocumentResponse;
        public boolean completed = false;
        public boolean error = false;

        TestServer(GrpcChannel grpcChannel, boolean startIndex, Mode mode) throws IOException {
            this.grpcChannel = grpcChannel;
            if (startIndex) {
                new IndexAndRoleManager(grpcChannel).createStartIndexAndRegisterFields(mode);
            }
        }

        public void addDocuments() throws IOException, InterruptedException {
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
            StreamObserver<AddDocumentRequest> requestObserver = grpcChannel.getStub().addDocuments(responseStreamObserver);
            //parse CSV into a stream of AddDocumentRequest
            Stream<AddDocumentRequest> addDocumentRequestStream = getAddDocumentRequestStream();
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
        }

        private Stream<AddDocumentRequest> getAddDocumentRequestStream() throws IOException {
            Path filePath = Paths.get("src", "test", "resources", "addDocs.csv");
            Reader reader = Files.newBufferedReader(filePath);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            return new LuceneServerClientBuilder.AddDcoumentsClientBuilder(grpcChannel.getTestIndex(), csvParser).buildRequest(filePath);
        }


    }

    public static class IndexAndRoleManager {

        private GrpcChannel grpcChannel;

        public IndexAndRoleManager(GrpcChannel grpcChannel) {
            this.grpcChannel = grpcChannel;
        }

        public FieldDefResponse createStartIndexAndRegisterFields(Mode mode) throws IOException {
            String rootDirName = grpcChannel.getRootDirName();
            String testIndex = grpcChannel.getTestIndex();
            LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcChannel.getBlockingStub();
            //create the index
            blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(testIndex).setRootDir(rootDirName).build());
            //start the index
            StartIndexRequest.Builder startIndexBuilder = StartIndexRequest.newBuilder().setIndexName(testIndex);
            if (mode.equals(Mode.PRIMARY)) {
                startIndexBuilder.setMode(Mode.PRIMARY);
                startIndexBuilder.setPrimaryGen(0);
            }
            //TODO: these are not used in tests ?
            else if (mode.equals(Mode.REPLICA)) {
                startIndexBuilder.setPrimaryAddress("replica:localhost");
                startIndexBuilder.setPort(9999);
            }
            blockingStub.startIndex(startIndexBuilder.build());
            //register the fields
            FieldDefRequest fieldDefRequest = buildFieldDefRequest(Paths.get("src", "test", "resources", "registerFieldsBasic.json"));
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
