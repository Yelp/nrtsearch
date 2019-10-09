package org.apache.platypus.server.grpc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import net.bytebuddy.agent.builder.AgentBuilder;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.platypus.server.GlobalState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.lucene.util.LuceneTestCase.createTempDir;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class LuceneServerTest {
    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the
     * end of test.
     */
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    /**
     * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private StartServer startServer;

    @After
    public void tearDown() throws IOException {
        startServer.getGlobalState().close();
        rmDir(Paths.get(startServer.getRootDirName()));
    }

    @Before
    public void setUp() throws IOException {
        startServer = new StartServer().invoke();
    }

    @Test
    public void testCreateIndex() throws Exception {
        String rootDirName = startServer.getRootDirName();
        String testIndex = startServer.getTestIndex();
        LuceneServerGrpc.LuceneServerBlockingStub blockingStub = startServer.getBlockingStub();
        CreateIndexResponse reply = blockingStub.createIndex(CreateIndexRequest.newBuilder().build().newBuilder().setIndexName(testIndex).setRootDir(rootDirName).build());
        assertEquals(reply.getResponse(), String.format("Created Index name: %s, at rootDir: %s", testIndex, rootDirName));
    }

    @Test
    public void testStartShard() throws IOException {
        String rootDirName = startServer.getRootDirName();
        String testIndex = startServer.getTestIndex();
        LuceneServerGrpc.LuceneServerBlockingStub blockingStub = startServer.getBlockingStub();
        //create the index
        blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(testIndex).setRootDir(rootDirName).build());
        //start the index
        StartIndexResponse reply = blockingStub.startIndex(StartIndexRequest.newBuilder().setIndexName(testIndex).build());
        assertEquals(0, reply.getMaxDoc());
        assertEquals(0, reply.getNumDocs());
        assertTrue(!reply.getSegments().isEmpty());
    }

    @Test
    public void testRegisterFieldsBasic() throws Exception {
        String rootDirName = startServer.getRootDirName();
        String testIndex = startServer.getTestIndex();
        LuceneServerGrpc.LuceneServerBlockingStub blockingStub = startServer.getBlockingStub();
        //create the index
        blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(testIndex).setRootDir(rootDirName).build());
        //start the index
        blockingStub.startIndex(StartIndexRequest.newBuilder().setIndexName(testIndex).build());
        //register the fields
        FieldDefRequest fieldDefRequest = buildFieldDefRequest(Paths.get("src", "test", "resources", "registerFieldsBasic.json"));
        FieldDefResponse reply = blockingStub.registerFields(fieldDefRequest);
        assertTrue(reply.getResponse().contains("vendor_name"));
        assertTrue(reply.getResponse().contains("vendor_name_atom"));
        assertTrue(reply.getResponse().contains("license_no"));
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

    private class StartServer {
        private String rootDirName;
        private String testIndex;
        private LuceneServerGrpc.LuceneServerBlockingStub blockingStub;
        private LuceneServerGrpc.LuceneServerStub stub;
        private GlobalState globalState;

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

        public GlobalState getGlobalState() {
            return globalState;
        }

        /**
         * To test the server, make calls with a real stub using the in-process channel, and verify
         * behaviors or state changes from the client side.
         */
        public StartServer invoke() throws IOException {
            String nodeName = "server1";
            rootDirName = "server1RootDirName1";
            Path rootDir = folder.newFolder(rootDirName).toPath();
            testIndex = "test_index";
            globalState = new GlobalState(nodeName, rootDir);
            // Generate a unique in-process server name.
            String serverName = InProcessServerBuilder.generateName();
            // Create a server, add service, start, and register for automatic graceful shutdown.
            grpcCleanup.register(InProcessServerBuilder
                    .forName(serverName).directExecutor().addService(new LuceneServer.LuceneServerImpl(globalState)).build().start());

            blockingStub = LuceneServerGrpc.newBlockingStub(
                    // Create a client channel and register for automatic graceful shutdown.
                    grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
            stub = LuceneServerGrpc.newStub(
                    grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
            return this;
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