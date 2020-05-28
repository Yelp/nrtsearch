package com.yelp.nrtsearch.server.grpc;

import com.google.api.HttpBody;
import com.google.protobuf.Empty;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class LuceneServerTest {
    public static final List<String> RETRIEVED_VALUES = Arrays.asList("doc_id", "license_no", "vendor_name", "vendor_name_atom",
            "count", "long_field", "double_field_multi", "double_field", "float_field_multi", "float_field", "boolean_field_multi", "boolean_field",
            "description", "date");
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
    private CollectorRegistry collectorRegistry;

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
        collectorRegistry = new CollectorRegistry();
        grpcServer = setUpGrpcServer(collectorRegistry);
    }

    private GrpcServer setUpGrpcServer(CollectorRegistry collectorRegistry) throws IOException {
        String testIndex = "test_index";
        LuceneServerConfiguration luceneServerConfiguration = LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE);
        GlobalState globalState = new GlobalState(luceneServerConfiguration);
        return new GrpcServer(
                collectorRegistry, grpcCleanup, folder, false, globalState, luceneServerConfiguration.getIndexDir(), testIndex, globalState.getPort(), null, Collections.emptyList()
        );
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
    public void testUpdateFieldsBasic() throws Exception {
        FieldDefResponse reply = new GrpcServer.IndexAndRoleManager(grpcServer).createStartIndexAndRegisterFields(Mode.STANDALONE);
        assertTrue(reply.getResponse().contains("vendor_name"));
        assertTrue(reply.getResponse().contains("vendor_name_atom"));
        assertTrue(reply.getResponse().contains("license_no"));
        reply = grpcServer.getBlockingStub().updateFields(FieldDefRequest.newBuilder()
                .setIndexName("test_index")
                .addField(Field.newBuilder()
                        .setName("new_text_field")
                        .setType(FieldType.TEXT)
                        .setStoreDocValues(true)
                        .setSearch(true)
                        .setMultiValued(true)
                        .setTokenize(true)
                        .build())
                .build());
        assertTrue(reply.getResponse().contains("new_text_field"));
    }

    @Test
    public void testSearchPostUpdate() throws IOException, InterruptedException {
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
        //2 docs addDocuments
        testAddDocs.addDocuments();

        //update schema: add a new field
        grpcServer.getBlockingStub().updateFields(FieldDefRequest.newBuilder()
                .setIndexName("test_index")
                .addField(Field.newBuilder()
                        .setName("new_text_field")
                        .setType(FieldType.TEXT)
                        .setStoreDocValues(true)
                        .setSearch(true)
                        .setMultiValued(true)
                        .setTokenize(true)
                        .build())
                .build());

        //2 docs addDocuments
        AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocsUpdated.csv");
        assertEquals(false, testAddDocs.error);
        assertEquals(true, testAddDocs.completed);
        List<String> RETRIEVE = Arrays.asList("doc_id", "new_text_field");

        Query query = Query.newBuilder()
                .setQueryType(QueryType.TERM_QUERY)
                .setTermQuery(TermQuery.newBuilder()
                        .setField("new_text_field")
                        .setTextValue("updated"))
                .build();

        SearchResponse searchResponse = grpcServer.getBlockingStub().search(SearchRequest.newBuilder()
                .setIndexName(grpcServer.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .setQuery(query)
                .addAllRetrieveFields(RETRIEVE)
                .build());

        assertEquals(2, searchResponse.getTotalHits().getValue());
        assertEquals(2, searchResponse.getHitsList().size());
        SearchResponse.Hit firstHit = searchResponse.getHits(0);

        Map<String, CompositeFieldValue> fields = firstHit.getFieldsMap();
        String docId = fields.get("doc_id").getFieldValue(0).getTextValue();
        String newTextField = fields.get("new_text_field").getFieldValue(0).getTextValue();
        assertEquals("3", docId);
        assertEquals("new updated first", newTextField);

        SearchResponse.Hit secondHit = searchResponse.getHits(1);
        fields = secondHit.getFieldsMap();
        docId = fields.get("doc_id").getFieldValue(0).getTextValue();
        newTextField = fields.get("new_text_field").getFieldValue(0).getTextValue();
        assertEquals("4", docId);
        assertEquals("new updated second", newTextField);

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
        assertEquals(0, statsResponse.getDirSize());
        assertEquals("started", statsResponse.getState());
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
        testAddDocs.addDocuments();
        statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());
        assertEquals(0, statsResponse.getOrd());
        assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
        assertEquals(6610, statsResponse.getDirSize(), 1000);
        assertEquals("started", statsResponse.getState());
    }

    @Test
    public void testRefresh() throws IOException, InterruptedException {
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE).addDocuments();
        StatsResponse statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());
        assertEquals(0, statsResponse.getOrd());
        assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
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
        grpcServer.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

        //check stats numDocs for 1 docs
        statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(1, statsResponse.getNumDocs());
        //note maxDoc stays 2 since it does not include delete documents
        assertEquals(2, statsResponse.getMaxDoc());

    }

    @Test
    public void testDeleteByQuery() throws IOException, InterruptedException {
        GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
        //add 2 docs
        testAddDocs.addDocuments();
        //check stats numDocs for 2 docs
        StatsResponse statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(2, statsResponse.getNumDocs());
        assertEquals(2, statsResponse.getMaxDoc());

        Query query = Query.newBuilder()
                .setQueryType(QueryType.TERM_QUERY)
                .setTermQuery(TermQuery.newBuilder()
                        .setField("count")
                        .setIntValue(7))
                .build();
        SearchRequest searchRequest = SearchRequest.newBuilder()
                .setIndexName(grpcServer.getTestIndex())
                .setStartHit(0)
                .setTopHits(10)
                .addAllRetrieveFields(RETRIEVED_VALUES)
                .setQuery(query)
                .build();
        SearchResponse searchResponse = grpcServer.getBlockingStub()
                .search(searchRequest);
        assertEquals(searchResponse.getHitsCount(), 1);

        //delete 1 doc

        DeleteByQueryRequest deleteByQueryRequest = DeleteByQueryRequest.newBuilder()
                .setIndexName("test_index")
                .addQuery(query)
                .build();
        AddDocumentResponse addDocumentResponse = grpcServer.getBlockingStub().deleteByQuery(deleteByQueryRequest);

        //manual refresh needed to depict changes in buffered deletes (i.e. not committed yet)
        grpcServer.getBlockingStub().refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

        //check stats numDocs for 1 docs
        statsResponse = grpcServer.getBlockingStub().stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
        assertEquals(1, statsResponse.getNumDocs());
        //note maxDoc stays 2 since it does not include delete documents
        assertEquals(2, statsResponse.getMaxDoc());
        //deleted document does not show up in search response now
        searchResponse = grpcServer.getBlockingStub()
                .search(searchRequest);
        assertEquals(searchResponse.getHitsCount(), 0);

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

        assertEquals(2, searchResponse.getTotalHits().getValue());
        assertEquals(2, searchResponse.getHitsList().size());
        SearchResponse.Hit firstHit = searchResponse.getHits(0);
        checkHits(firstHit);
        SearchResponse.Hit secondHit = searchResponse.getHits(1);
        checkHits(secondHit);
    }

    @Test
    public void testMetrics() {
        HttpBody response = grpcServer.getBlockingStub().metrics(Empty.newBuilder().build());
        HashSet expectedSampleNames = new HashSet(Arrays.asList("grpc_server_started_total", "grpc_server_handled_total", "grpc_server_msg_received_total",
                "grpc_server_msg_sent_total", "grpc_server_handled_latency_seconds"));
        assertEquals("text/plain", response.getContentType());
        String data = new String(response.getData().toByteArray());
        String[] arr = data.split("\n");
        Set<String> labelsHelp = new HashSet<>();
        Set<String> labelsType = new HashSet<>();
        for (int i = 0; i < arr.length; i++) {
            if (arr[i].startsWith("# HELP")) {
                labelsHelp.add(arr[i].split(" ")[2]);
            } else if (arr[i].startsWith("# TYPE")) {
                labelsType.add(arr[i].split(" ")[2]);
            }
        }
        assertEquals(true, labelsHelp.equals(labelsType));
        assertEquals(true, labelsHelp.equals(expectedSampleNames));
    }

    public static void checkHits(SearchResponse.Hit hit) {
        Map<String, CompositeFieldValue> fields = hit.getFieldsMap();
        checkFieldNames(RETRIEVED_VALUES, fields);

        String docId = fields.get("doc_id").getFieldValue(0).getTextValue();

        List<String> expectedLicenseNo = null;
        List<String> expectedVendorName = null;
        List<String> expectedVendorNameAtom = null;
        List<String> expectedDescription = null;
        List<String> expectedDoubleFieldMulti = null;
        List<String> expectedDoubleField = null;
        List<String> expectedFloatFieldMulti = null;
        List<String> expectedFloatField = null;
        List<String> expectedBooleanFieldMulti = Arrays.asList("true", "false");
        List<String> expectedBooleanField = Arrays.asList("false");
        long expectedDate = 0;

        int expectedCount = 0;
        long expectedLongField = 0;

        if (docId.equals("1")) {
            expectedLicenseNo = Arrays.asList("300", "3100");
            expectedVendorName = Arrays.asList("first vendor", "first again");
            expectedVendorNameAtom = Arrays.asList("first atom vendor", "first atom again");
            expectedCount = 3;
            expectedLongField = 12;
            expectedDoubleFieldMulti = Arrays.asList("1.1", "1.11");
            expectedDoubleField = Arrays.asList("1.01");
            expectedFloatFieldMulti = Arrays.asList("100.1", "100.11");
            expectedFloatField = Arrays.asList("100.01");
            expectedDescription = Collections.singletonList("FIRST food");
            expectedDate = getStringDateTimeAsListOfStringMillis("2019-10-12 15:30:41");
        } else if (docId.equals("2")) {
            expectedLicenseNo = Arrays.asList("411", "4222");
            expectedVendorName = Arrays.asList("second vendor", "second again");
            expectedVendorNameAtom = Arrays.asList("second atom vendor", "second atom again");
            expectedCount = 7;
            expectedLongField = 16;
            expectedDoubleFieldMulti = Arrays.asList("2.2", "2.22");
            expectedDoubleField = Arrays.asList("2.01");
            expectedFloatFieldMulti = Arrays.asList("200.2", "200.22");
            expectedFloatField = Arrays.asList("200.02");
            expectedDescription = Collections.singletonList("SECOND gas");
            expectedDate = getStringDateTimeAsListOfStringMillis("2020-03-05 01:03:05");
        } else {
            fail(String.format("docId %s not indexed", docId));
        }

        checkPerFieldValues(expectedLicenseNo, getIntFieldValuesListAsString(fields.get("license_no").getFieldValueList()));
        checkPerFieldValues(expectedVendorName, getStringFieldValuesList(fields.get("vendor_name").getFieldValueList()));
        checkPerFieldValues(expectedVendorNameAtom, getStringFieldValuesList(fields.get("vendor_name_atom").getFieldValueList()));
        assertEquals(expectedCount, fields.get("count").getFieldValueList().get(0).getIntValue());
        assertEquals(expectedLongField, fields.get("long_field").getFieldValueList().get(0).getLongValue());
        checkPerFieldValues(expectedDoubleFieldMulti, getDoubleFieldValuesListAsString(fields.get("double_field_multi").getFieldValueList()));
        checkPerFieldValues(expectedDoubleField, getDoubleFieldValuesListAsString(fields.get("double_field").getFieldValueList()));
        checkPerFieldValues(expectedFloatFieldMulti, getFloatFieldValuesListAsString(fields.get("float_field_multi").getFieldValueList()));
        checkPerFieldValues(expectedFloatField, getFloatFieldValuesListAsString(fields.get("float_field").getFieldValueList()));
        checkPerFieldValues(expectedBooleanFieldMulti, getBooleanFieldValuesListAsString(fields.get("boolean_field_multi").getFieldValueList()));
        checkPerFieldValues(expectedBooleanField, getBooleanFieldValuesListAsString(fields.get("boolean_field").getFieldValueList()));
        checkPerFieldValues(expectedDescription, getStringFieldValuesList(fields.get("description").getFieldValueList()));
        assertEquals(expectedDate, fields.get("date").getFieldValueList().get(0).getLongValue());
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

    private static List<String> getIntFieldValuesListAsString(List<SearchResponse.Hit.FieldValue> fieldValues) {
        return fieldValues.stream()
                .map(fieldValue -> String.valueOf(fieldValue.getIntValue()))
                .collect(Collectors.toList());
    }

    private static List<String> getDoubleFieldValuesListAsString(List<SearchResponse.Hit.FieldValue> fieldValues) {
        return fieldValues.stream()
                .map(fieldValue -> String.valueOf(fieldValue.getDoubleValue()))
                .collect(Collectors.toList());
    }

    private static List<String> getFloatFieldValuesListAsString(List<SearchResponse.Hit.FieldValue> fieldValues) {
        return fieldValues.stream()
                .map(fieldValue -> String.valueOf(fieldValue.getFloatValue()))
                .collect(Collectors.toList());
    }

    private static List<String> getBooleanFieldValuesListAsString(List<SearchResponse.Hit.FieldValue> fieldValues) {
        return fieldValues.stream()
                .map(fieldValue -> String.valueOf(fieldValue.getBooleanValue()))
                .collect(Collectors.toList());
    }

    private static List<String> getStringFieldValuesList(List<SearchResponse.Hit.FieldValue> fieldValues) {
        return fieldValues.stream()
                .map(SearchResponse.Hit.FieldValue::getTextValue)
                .collect(Collectors.toList());
    }

    private static long getStringDateTimeAsListOfStringMillis(String dateTime) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.parse(dateTime, dateTimeFormatter).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
