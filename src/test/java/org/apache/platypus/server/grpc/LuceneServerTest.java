package org.apache.platypus.server.grpc;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.platypus.server.GlobalState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class LuceneServerTest {
    public static final List<String> RETRIEVED_VALUES = Arrays.asList("doc_id", "license_no", "vendor_name", "vendor_name_atom");
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
        FieldDefResponse reply = setUpIndexWithFields();
        assertTrue(reply.getResponse().contains("vendor_name"));
        assertTrue(reply.getResponse().contains("vendor_name_atom"));
        assertTrue(reply.getResponse().contains("license_no"));
    }

    @Test
    public void testAddDocumentsBasic() throws IOException, InterruptedException {
        TestAddDocuments testAddDocs = new TestAddDocuments(true);
        testAddDocs.addDocuments();
        assertEquals(false, testAddDocs.error);
        assertEquals(true, testAddDocs.completed);

        /* Tricky to check genId for exact match on a standalone node (one that does both indexing and real-time searching.
         *  The ControlledRealTimeReopenThread is running in the background which refreshes the searcher and updates the sequence_number
         *  each time maybeRefresh is invoked depending on frequency set in indexState.maxRefreshSec
         *  Overall: sequence_number(genID) is increased for each operation
         *  - open index writer
         *  - open taxo writer
         *  - once for each invocation of SearcherTaxonomyManager as explained above
         *  - once per commit
         */
        assert 3 <= Integer.parseInt(testAddDocs.addDocumentResponse.getGenId());
        testAddDocs.addDocuments();
        assert 4 <= Integer.parseInt(testAddDocs.addDocumentResponse.getGenId());

    }

    @Test
    public void testStats() throws IOException, InterruptedException {
        setUpIndexWithFields();
        StatsResponse statsResponse = startServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        assertEquals(0, statsResponse.getNumDocs());
        assertEquals(0, statsResponse.getMaxDoc());
        assertEquals(0, statsResponse.getOrd());
        assertEquals(0, statsResponse.getCurrentSearcher().getNumDocs());
        assertEquals("started", statsResponse.getState());
        TestAddDocuments testAddDocs = new TestAddDocuments(false);
        testAddDocs.addDocuments();
        statsResponse = startServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());
        assertEquals(0, statsResponse.getOrd());
        //searcher is not refreshed so searcher returns zeroDocs
        //Note: (does refresh in background thread eventually every indexState.indexMaxRefreshSec)
        assertEquals(0, statsResponse.getCurrentSearcher().getNumDocs());
        assertEquals("started", statsResponse.getState());
    }

    @Test
    public void testRefresh() throws IOException, InterruptedException {
        new TestAddDocuments(true).addDocuments();
        StatsResponse statsResponse = startServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());
        assertEquals(0, statsResponse.getOrd());
        //searcher is not refreshed so searcher returns zeroDocs
        //Note: (does refresh in background thread eventually every indexState.indexMaxRefreshSec)
        assertEquals(0, statsResponse.getCurrentSearcher().getNumDocs());
        //manual refresh
        startServer.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        //check status on currentSearchAgain
        statsResponse = startServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
    }

    @Test
    public void testDelete() throws IOException, InterruptedException {
        TestAddDocuments testAddDocs = new TestAddDocuments(true);
        //add 2 docs
        testAddDocs.addDocuments();
        //check stats numDocs for 2 docs
        StatsResponse statsResponse = startServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());

        //delete 1 doc
        AddDocumentRequest.Builder addDocumentRequestBuilder = AddDocumentRequest.newBuilder();
        addDocumentRequestBuilder.setIndexName("test_index");
        AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder = AddDocumentRequest.MultiValuedField.newBuilder();
        addDocumentRequestBuilder.putFields("doc_id", multiValuedFieldsBuilder.addValue("1").build());
        AddDocumentResponse addDocumentResponse = startServer.getBlockingStub().delete(addDocumentRequestBuilder.build());

        //manual refresh needed to depict changes in buffered deletes (i.e. not committed yet)
        startServer.getBlockingStub().refresh(RefreshRequest.newBuilder().build().newBuilder().setIndexName(startServer.getTestIndex()).build());

        //check stats numDocs for 1 docs
        statsResponse = startServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        assertEquals(1, statsResponse.getNumDocs());
        //note maxDoc stays 2 since it does not include delete documents
        assertEquals(2, statsResponse.getMaxDoc());

    }

    @Test
    public void testDeleteAllDocuments() throws IOException, InterruptedException {
        TestAddDocuments testAddDocs = new TestAddDocuments(true);
        //add 2 docs
        testAddDocs.addDocuments();
        //check stats numDocs for 2 docs
        StatsResponse statsResponse = startServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());

        //deleteAll documents
        DeleteAllDocumentsRequest.Builder deleteAllDocumentsBuilder = DeleteAllDocumentsRequest.newBuilder();
        DeleteAllDocumentsRequest deleteAllDocumentsRequest = deleteAllDocumentsBuilder.setIndexName("test_index").build();
        startServer.getBlockingStub().deleteAll(deleteAllDocumentsRequest);

        //check stats numDocs for 1 docs
        statsResponse = startServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        assertEquals(0, statsResponse.getNumDocs());
        assertEquals(0, statsResponse.getMaxDoc());

    }

    @Test
    public void testDeleteIndex() throws IOException, InterruptedException {
        TestAddDocuments testAddDocs = new TestAddDocuments(true);
        //add 2 docs
        testAddDocs.addDocuments();
        //check stats numDocs for 2 docs
        StatsResponse statsResponse = startServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());

        //deleteIndex
        DeleteIndexRequest deleteIndexRequest = DeleteIndexRequest.newBuilder().setIndexName("test_index").build();
        DeleteIndexResponse deleteIndexResponse = startServer.getBlockingStub().deleteIndex(deleteIndexRequest);

        assertEquals("ok", deleteIndexResponse.getOk());

    }

    @Test
    public void testSearchBasic() throws IOException, InterruptedException {
        TestAddDocuments testAddDocs = new TestAddDocuments(true);
        //2 docs addDocuments
        testAddDocs.addDocuments();
        //manual refresh
        startServer.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName(startServer.getTestIndex()).build());

        SearchResponse searchResponse = startServer.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(startServer.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .build());

        String response = searchResponse.getResponse();
        Map<String, Object> resultMap = new Gson().fromJson(response, Map.class);
        assertEquals(2.0, (double) resultMap.get("totalHits"), 0.01);
        List<Map<String, Object>> hits = (List<Map<String, Object>>) resultMap.get("hits");
        assertEquals(2, ((List<Map<String, Object>>) resultMap.get("hits")).size());
        Map<String, Object> firstHit = hits.get(0);
        checkHits(firstHit);
        Map<String, Object> secondHit = hits.get(1);
        checkHits(secondHit);
    }

    public void checkHits(Map<String, Object> hit) {
        List<String> expectedHitFields = Arrays.asList("doc", "score", "fields");
        checkFieldNames(expectedHitFields, hit);
        Map<String, Object> fields = (Map<String, Object>) hit.get("fields");
        List<String> expectedRetrieveFields = RETRIEVED_VALUES;
        checkFieldNames(expectedRetrieveFields, (Map<String, Object>) hit.get("fields"));

        String docId = ((List<String>) fields.get("doc_id")).get(0);
        if(docId.equals("1")){
            checkPerFieldValues(Arrays.asList("300", "3100"), (List<String>) fields.get("license_no"));
            checkPerFieldValues(Arrays.asList("first vendor", "first again"), (List<String>) fields.get("vendor_name"));
            checkPerFieldValues(Arrays.asList("first atom vendor", "first atom again"), (List<String>) fields.get("vendor_name_atom"));
        } else if (docId.equals("2")) {
            checkPerFieldValues(Arrays.asList("411", "4222"), (List<String>) fields.get("license_no"));
            checkPerFieldValues(Arrays.asList("second vendor", "second again"), (List<String>) fields.get("vendor_name"));
            checkPerFieldValues(Arrays.asList("second atom vendor", "second atom again"), (List<String>) fields.get("vendor_name_atom"));

        } else {
            assertFalse(String.format("docIdd %s not indexed", docId), true);
        }
    }

    public void checkFieldNames(List<String> expectedNames, Map<String, Object> actualNames) {
        List<String> hitFields = new ArrayList<>();
        hitFields.addAll(actualNames.keySet());
        Collections.sort(expectedNames);
        Collections.sort(hitFields);
        assertEquals(expectedNames, hitFields);
    }

    public void checkPerFieldValues(List<String> expectedValues, List<String> actualValues) {
        Collections.sort(expectedValues);
        Collections.sort(actualValues);
        assertEquals(expectedValues, actualValues);
    }


    private FieldDefResponse setUpIndexWithFields() throws IOException {
        String rootDirName = startServer.getRootDirName();
        String testIndex = startServer.getTestIndex();
        LuceneServerGrpc.LuceneServerBlockingStub blockingStub = startServer.getBlockingStub();
        //create the index
        blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(testIndex).setRootDir(rootDirName).build());
        //start the index
        blockingStub.startIndex(StartIndexRequest.newBuilder().setIndexName(testIndex).build());
        //register the fields
        FieldDefRequest fieldDefRequest = buildFieldDefRequest(Paths.get("src", "test", "resources", "registerFieldsBasic.json"));
        return blockingStub.registerFields(fieldDefRequest);
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

    private class TestAddDocuments {
        public AddDocumentResponse addDocumentResponse;
        public boolean completed = false;
        public boolean error = false;

        TestAddDocuments(boolean startIndex) throws IOException {
            if (startIndex) {
                setUpIndexWithFields();
            }
        }

        void addDocuments() throws IOException, InterruptedException {
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
            StreamObserver<AddDocumentRequest> requestObserver = startServer.getStub().addDocuments(responseStreamObserver);
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
    }

    private Stream<AddDocumentRequest> getAddDocumentRequestStream() throws IOException {
        Path filePath = Paths.get("src", "test", "resources", "addDocs.csv");
        Reader reader = Files.newBufferedReader(filePath);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
        return new LuceneServerClientBuilder.AddDcoumentsClientBuilder(startServer.getTestIndex(), csvParser).buildRequest(filePath);
    }

}
