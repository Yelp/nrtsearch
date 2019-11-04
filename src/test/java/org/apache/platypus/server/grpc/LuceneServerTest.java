package org.apache.platypus.server.grpc;

import com.google.gson.Gson;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.platypus.server.luceneserver.GlobalState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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

    private GrpcServer grpcServer;

    @After
    public void tearDown() throws IOException {
        grpcServer.getGlobalState().close();
        rmDir(Paths.get(grpcServer.getRootDirName()));
    }

    @Before
    public void setUp() throws IOException {
        String nodeName = "server1";
        String rootDirName = "server1RootDirName1";
        Path rootDir = folder.newFolder(rootDirName).toPath();
        String testIndex = "test_index";
        GlobalState globalState = new GlobalState(nodeName, rootDir, null, 9000, 9000);
        grpcServer = new GrpcServer(grpcCleanup, folder, false, globalState,  rootDirName, testIndex, globalState.getPort());
    }

    @Test
    public void testCreateIndex() throws Exception {
        String rootDirName = grpcServer.getRootDirName();
        String testIndex = grpcServer.getTestIndex();
        LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();
        CreateIndexResponse reply = blockingStub.createIndex(CreateIndexRequest.newBuilder().build().newBuilder().setIndexName(testIndex).setRootDir(rootDirName).build());
        assertEquals(reply.getResponse(), String.format("Created Index name: %s, at rootDir: %s", testIndex, rootDirName));
    }

    @Test
    public void testStartShard() throws IOException {
        String rootDirName = grpcServer.getRootDirName();
        String testIndex = grpcServer.getTestIndex();
        LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();
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
        FieldDefResponse reply = new GrpcServer.IndexAndRoleManager(grpcServer).createStartIndexAndRegisterFields(Mode.STANDALONE);
        assertTrue(reply.getResponse().contains("vendor_name"));
        assertTrue(reply.getResponse().contains("vendor_name_atom"));
        assertTrue(reply.getResponse().contains("license_no"));
    }

    @Test
    public void testAddDocumentsBasic() throws IOException, InterruptedException {
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
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
        new GrpcServer.IndexAndRoleManager(grpcServer).createStartIndexAndRegisterFields(Mode.STANDALONE);
        StatsResponse statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(0, statsResponse.getNumDocs());
        assertEquals(0, statsResponse.getMaxDoc());
        assertEquals(0, statsResponse.getOrd());
        assertEquals(0, statsResponse.getCurrentSearcher().getNumDocs());
        assertEquals("started", statsResponse.getState());
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, false,Mode.STANDALONE);
        testAddDocs.addDocuments();
        statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
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
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE).addDocuments();
        StatsResponse statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());
        assertEquals(0, statsResponse.getOrd());
        //searcher is not refreshed so searcher returns zeroDocs
        //Note: (does refresh in background thread eventually every indexState.indexMaxRefreshSec)
        assertEquals(0, statsResponse.getCurrentSearcher().getNumDocs());
        //manual refresh
        grpcServer.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        //check status on currentSearchAgain
        statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
    }

    @Test
    public void testDelete() throws IOException, InterruptedException {
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
        //add 2 docs
        testAddDocs.addDocuments();
        //check stats numDocs for 2 docs
        StatsResponse statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());

        //delete 1 doc
        AddDocumentRequest.Builder addDocumentRequestBuilder = AddDocumentRequest.newBuilder();
        addDocumentRequestBuilder.setIndexName("test_index");
        AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder = AddDocumentRequest.MultiValuedField.newBuilder();
        addDocumentRequestBuilder.putFields("doc_id", multiValuedFieldsBuilder.addValue("1").build());
        AddDocumentResponse addDocumentResponse = grpcServer.getBlockingStub().delete(addDocumentRequestBuilder.build());

        //manual refresh needed to depict changes in buffered deletes (i.e. not committed yet)
        grpcServer.getBlockingStub().refresh(RefreshRequest.newBuilder().build().newBuilder().setIndexName(grpcServer.getTestIndex()).build());

        //check stats numDocs for 1 docs
        statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(1, statsResponse.getNumDocs());
        //note maxDoc stays 2 since it does not include delete documents
        assertEquals(2, statsResponse.getMaxDoc());

    }

    @Test
    public void testDeleteAllDocuments() throws IOException, InterruptedException {
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true,Mode.STANDALONE);
        //add 2 docs
        testAddDocs.addDocuments();
        //check stats numDocs for 2 docs
        StatsResponse statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());

        //deleteAll documents
        DeleteAllDocumentsRequest.Builder deleteAllDocumentsBuilder = DeleteAllDocumentsRequest.newBuilder();
        DeleteAllDocumentsRequest deleteAllDocumentsRequest = deleteAllDocumentsBuilder.setIndexName("test_index").build();
        grpcServer.getBlockingStub().deleteAll(deleteAllDocumentsRequest);

        //check stats numDocs for 1 docs
        statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(0, statsResponse.getNumDocs());
        assertEquals(0, statsResponse.getMaxDoc());

    }

    @Test
    public void testDeleteIndex() throws IOException, InterruptedException {
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer,true, Mode.STANDALONE);
        //add 2 docs
        testAddDocs.addDocuments();
        //check stats numDocs for 2 docs
        StatsResponse statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());

        //deleteIndex
        DeleteIndexRequest deleteIndexRequest = DeleteIndexRequest.newBuilder().setIndexName("test_index").build();
        DeleteIndexResponse deleteIndexResponse = grpcServer.getBlockingStub().deleteIndex(deleteIndexRequest);

        assertEquals("ok", deleteIndexResponse.getOk());

    }

    @Test
    public void testSearchBasic() throws IOException, InterruptedException {
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
        //2 docs addDocuments
        testAddDocs.addDocuments();
        //manual refresh
        grpcServer.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

        SearchResponse searchResponse = grpcServer.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(grpcServer.getTestIndex())
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

    public static void checkHits(Map<String, Object> hit) {
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

    public static void checkFieldNames(List<String> expectedNames, Map<String, Object> actualNames) {
        List<String> hitFields = new ArrayList<>();
        hitFields.addAll(actualNames.keySet());
        Collections.sort(expectedNames);
        Collections.sort(hitFields);
        assertEquals(expectedNames, hitFields);
    }

    public static void checkPerFieldValues(List<String> expectedValues, List<String> actualValues) {
        Collections.sort(expectedValues);
        Collections.sort(actualValues);
        assertEquals(expectedValues, actualValues);
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


    private Stream<AddDocumentRequest> getAddDocumentRequestStream() throws IOException {
        Path filePath = Paths.get("src", "test", "resources", "addDocs.csv");
        Reader reader = Files.newBufferedReader(filePath);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
        return new LuceneServerClientBuilder.AddDcoumentsClientBuilder(grpcServer.getTestIndex(), csvParser).buildRequest(filePath);
    }

}
