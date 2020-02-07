package org.apache.platypus.server.grpc;

import io.grpc.testing.GrpcCleanupRule;
import org.apache.commons.io.FileUtils;
import org.apache.platypus.server.LuceneServerTestConfigurationFactory;
import org.apache.platypus.server.config.LuceneServerConfiguration;
import org.apache.platypus.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import org.apache.platypus.server.luceneserver.GlobalState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.platypus.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class LuceneServerTest {
    public static final List<String> RETRIEVED_VALUES = Arrays.asList("doc_id", "license_no", "vendor_name", "vendor_name_atom", "count");
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
        tearDownGrpcServer();
    }

    private void tearDownGrpcServer() throws IOException {
        grpcServer.getGlobalState().close();
        grpcServer.shutdown();
        rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
    }

    @Before
    public void setUp() throws IOException {
        grpcServer = setUpGrpcServer(null);
    }

    private GrpcServer setUpGrpcServer(Path rootDir) throws IOException {
        String testIndex = "test_index";
        LuceneServerConfiguration luceneServerConfiguration = LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE);
        GlobalState globalState = new GlobalState(luceneServerConfiguration);
        return new GrpcServer(grpcCleanup, folder, false, globalState, luceneServerConfiguration.getIndexDir(), testIndex, globalState.getPort());
    }

    @Test
    public void testCreateIndex() throws Exception {
        LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();
        CreateIndexResponse reply = blockingStub.createIndex(CreateIndexRequest.newBuilder()
                .setIndexName(grpcServer.getTestIndex())
                .setRootDir(grpcServer.getIndexDir())
                .build());
        assertEquals(reply.getResponse(), String.format("Created Index name: %s, at rootDir: %s", grpcServer.getTestIndex(), grpcServer.getIndexDir()));
    }

    @Test
    public void testStartShard() throws IOException {
        String rootDirName = grpcServer.getIndexDir();
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
    public void testAddDocumentsLatLon() throws IOException, InterruptedException {
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
        new GrpcServer.IndexAndRoleManager(grpcServer).createStartIndexAndRegisterFields(
                Mode.STANDALONE, 0, false, "registerFieldsLatLon.json");
        AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocsLatLon.csv");
        assertEquals(false, testAddDocs.error);
        assertEquals(true, testAddDocs.completed);

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
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
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
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
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
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
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

        assertEquals(2, searchResponse.getTotalHits());
        assertEquals(2, searchResponse.getHitsList().size());
        SearchResponse.Hit firstHit = searchResponse.getHits(0);
        checkHits(firstHit);
        SearchResponse.Hit secondHit = searchResponse.getHits(1);
        checkHits(secondHit);
    }

    @Test
    public void testSearchBooleanQuery() throws IOException, InterruptedException {
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
                .setQuery(Query.newBuilder()
                        .setQueryType(QueryType.BOOLEAN_QUERY)
                        .setBooleanQuery(BooleanQuery.newBuilder()
                                .addClauses(BooleanClause.newBuilder()
                                        .setQuery(Query.newBuilder()
                                                .setQueryType(QueryType.PHRASE_QUERY)
                                                .setPhraseQuery(PhraseQuery.newBuilder()
                                                        .setSlop(0)
                                                        .setField("vendor_name")
                                                        .addTerms("first")
                                                        .addTerms("again")
                                                        .build())
                                                .build())
                                        .setOccur(BooleanClause.Occur.MUST)
                                        .build())
                                .build())
                        .build())
                .build());

        assertEquals(1, searchResponse.getTotalHits());
        assertEquals(1, searchResponse.getHitsList().size());
        SearchResponse.Hit hit = searchResponse.getHits(0);
        String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
        assertEquals("1", docId);
        checkHits(hit);
    }

    @Test
    public void testSearchPhraseQuery() throws IOException, InterruptedException {
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
                .setQuery(Query.newBuilder()
                        .setQueryType(QueryType.PHRASE_QUERY)
                        .setPhraseQuery(PhraseQuery.newBuilder()
                                .setSlop(0)
                                .setField("vendor_name")
                                .addTerms("second")
                                .addTerms("again")
                                .build())
                        .build())
                .build());

        assertEquals(1, searchResponse.getTotalHits());
        assertEquals(1, searchResponse.getHitsList().size());
        SearchResponse.Hit hit = searchResponse.getHits(0);
        String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
        assertEquals("2", docId);
        checkHits(hit);
    }

    @Test
    public void testSearchFunctionScoreQuery() throws IOException, InterruptedException {
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
                .setQuery(Query.newBuilder()
                        .setQueryType(QueryType.FUNCTION_SCORE_QUERY)
                        .setFunctionScoreQuery(FunctionScoreQuery.newBuilder()
                                .setFunction("sqrt(4) * count")
                                .setQuery(Query.newBuilder()
                                        .setQueryType(QueryType.PHRASE_QUERY)
                                        .setPhraseQuery(PhraseQuery.newBuilder()
                                                .setSlop(0)
                                                .setField("vendor_name")
                                                .addTerms("second")
                                                .addTerms("again")
                                                .build()))
                                .build())
                        .build())
                .build());

        assertEquals(1, searchResponse.getTotalHits());
        assertEquals(1, searchResponse.getHitsList().size());
        SearchResponse.Hit hit = searchResponse.getHits(0);
        String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
        assertEquals("2", docId);
        assertEquals(14.0, hit.getScore(), 0.0);
        checkHits(hit);
    }

    @Test
    public void testSearchTermQuery() throws IOException, InterruptedException {
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
                .setQuery(Query.newBuilder()
                        .setQueryType(QueryType.TERM_QUERY)
                        .setTermQuery(TermQuery.newBuilder()
                                .setField("vendor_name")
                                .setTerm("second")))
                .build());

        assertEquals(1, searchResponse.getTotalHits());
        assertEquals(1, searchResponse.getHitsList().size());
        SearchResponse.Hit hit = searchResponse.getHits(0);
        String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
        assertEquals("2", docId);
        checkHits(hit);
    }

    @Test
    public void testSearchTermInSetQuery() throws IOException, InterruptedException {
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
                .setQuery(Query.newBuilder()
                        .setQueryType(QueryType.TERM_IN_SET_QUERY)
                        .setTermInSetQuery(TermInSetQuery.newBuilder()
                                .setField("vendor_name")
                                .addTerms("second")
                                .addTerms("first")))
                .build());

        assertEquals(2, searchResponse.getTotalHits());
        assertEquals(2, searchResponse.getHitsList().size());
        SearchResponse.Hit firstHit = searchResponse.getHits(0);
        checkHits(firstHit);
        SearchResponse.Hit secondHit = searchResponse.getHits(1);
        checkHits(secondHit);
    }

    @Test
    public void testSearchDisjunctionMaxQuery() throws IOException, InterruptedException {
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
                .setQuery(Query.newBuilder()
                        .setQueryType(QueryType.DISJUNCTION_MAX)
                        .setDisjunctionMaxQuery(DisjunctionMaxQuery.newBuilder()
                                .addDisjuncts(Query.newBuilder()
                                        .setQueryType(QueryType.TERM_QUERY)
                                        .setTermQuery(TermQuery.newBuilder()
                                                .setField("vendor_name")
                                                .setTerm("second")))
                                .addDisjuncts(Query.newBuilder()
                                        .setQueryType(QueryType.FUNCTION_SCORE_QUERY)
                                        .setFunctionScoreQuery(FunctionScoreQuery.newBuilder()
                                                .setFunction("sqrt(4) * count")
                                                .setQuery(Query.newBuilder()
                                                        .setQueryType(QueryType.TERM_QUERY)
                                                        .setTermQuery(TermQuery.newBuilder()
                                                                .setField("vendor_name")
                                                                .setTerm("second")))))
                                .setTieBreakerMultiplier(0)))
                .build());

        assertEquals(1, searchResponse.getTotalHits());
        assertEquals(1, searchResponse.getHitsList().size());
        SearchResponse.Hit hit = searchResponse.getHits(0);
        String docId = hit.getFieldsMap().get("doc_id").getFieldValue(0).getTextValue();
        assertEquals("2", docId);
        assertEquals(14.0, hit.getScore(), 0.0);
        checkHits(hit);
    }

    public static void checkHits(SearchResponse.Hit hit) {
        Map<String, CompositeFieldValue> fields = hit.getFieldsMap();
        List<String> expectedRetrieveFields = RETRIEVED_VALUES;
        checkFieldNames(expectedRetrieveFields, fields);

        String docId = fields.get("doc_id").getFieldValue(0).getTextValue();

        List<String> expectedLicenseNo = null;
        List<String> expectedVendorName = null;
        List<String> expectedVendorNameAtom = null;
        int expectedCount = 0;

        if (docId.equals("1")) {
            expectedLicenseNo = Arrays.asList("300", "3100");
            expectedVendorName = Arrays.asList("first vendor", "first again");
            expectedVendorNameAtom = Arrays.asList("first atom vendor", "first atom again");
            expectedCount = 3;
        } else if (docId.equals("2")) {
            expectedLicenseNo = Arrays.asList("411", "4222");
            expectedVendorName = Arrays.asList("second vendor", "second again");
            expectedVendorNameAtom = Arrays.asList("second atom vendor", "second atom again");
            expectedCount = 7;
        } else {
            fail(String.format("docId %s not indexed", docId));
        }

        checkPerFieldValues(expectedLicenseNo, getLongFieldValuesListAsString(fields.get("license_no").getFieldValueList()));
        checkPerFieldValues(expectedVendorName, getStringFieldValuesList(fields.get("vendor_name").getFieldValueList()));
        checkPerFieldValues(expectedVendorNameAtom, getStringFieldValuesList(fields.get("vendor_name_atom").getFieldValueList()));
        assertEquals(expectedCount, fields.get("count").getFieldValueList().get(0).getLongValue());
    }

    public static void checkFieldNames(List<String> expectedNames, Map<String, CompositeFieldValue> actualNames) {
        List<String> hitFields = new ArrayList<>(actualNames.keySet());
        Collections.sort(expectedNames);
        Collections.sort(hitFields);
        assertEquals(expectedNames, hitFields);
    }

    public static void checkPerFieldValues(List<String> expectedValues, List<String> actualValues) {
        Collections.sort(expectedValues);
        Collections.sort(actualValues);
        assertEquals(expectedValues, actualValues);
    }

    private static List<String> getLongFieldValuesListAsString(List<SearchResponse.Hit.FieldValue> fieldValues) {
        return fieldValues.stream()
                .map(fieldValue -> String.valueOf(fieldValue.getLongValue()))
                .collect(Collectors.toList());
    }

    private static List<String> getStringFieldValuesList(List<SearchResponse.Hit.FieldValue> fieldValues) {
        return fieldValues.stream()
                .map(SearchResponse.Hit.FieldValue::getTextValue)
                .collect(Collectors.toList());
    }

}
