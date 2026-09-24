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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.HttpBody;
import com.google.protobuf.Empty;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.NrtsearchServer.LuceneServerImpl;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.remote.RemoteBackend;
import com.yelp.nrtsearch.server.remote.s3.S3Backend;
import com.yelp.nrtsearch.server.remote.s3.S3Util;
import com.yelp.nrtsearch.server.search.cache.NrtQueryCache;
import com.yelp.nrtsearch.server.state.StateUtils;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import com.yelp.nrtsearch.test_utils.TestDocumentHelper;
import com.yelp.nrtsearch.test_utils.TestResourceHelper;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.s3.S3AsyncClient;

@RunWith(JUnit4.class)
public class NrtsearchServerTest {
  public static final List<String> RETRIEVED_VALUES =
      Arrays.asList(
          "doc_id",
          "license_no",
          "vendor_name",
          "vendor_name_atom",
          "count",
          "long_field",
          "long_field_multi",
          "double_field_multi",
          "double_field",
          "float_field_multi",
          "float_field",
          "boolean_field_multi",
          "boolean_field",
          "description",
          "date",
          "date_multi");
  public static final List<String> INDEX_VIRTUAL_FIELDS =
      Arrays.asList("virtual_field", "virtual_field_w_score");
  public static final List<String> QUERY_VIRTUAL_FIELDS =
      Arrays.asList("query_virtual_field", "query_virtual_field_w_score");
  static final List<String> LAT_LON_VALUES =
      Arrays.asList(
          "doc_id", "vendor_name", "vendor_name_atom", "license_no", "lat_lon", "lat_lon_multi");

  private static final String TEST_INDEX = "test_index";
  private static final String TEST_SERVICE_NAME = "TEST_SERVICE_NAME";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private TestServer primaryServer;
  private TestServer replicaServer;
  private RemoteBackend remoteBackend;

  @After
  public void tearDown() {
    TestServer.cleanupAll();
  }

  @Before
  public void setUp() throws Exception {
    primaryServer = TestServer.builder(folder).build();
    replicaServer =
        TestServer.builder(folder)
            .withServiceName(TEST_SERVICE_NAME)
            .withWarming(10, 1, true)
            .withAdditionalConfig("syncInitialNrtPoint: false")
            .build();
    remoteBackend = createRemoteBackend(replicaServer.getConfiguration());
    setUpWarmer();
  }

  private RemoteBackend createRemoteBackend(NrtsearchConfig config) {
    S3AsyncClient s3Async = AmazonS3Provider.createTestS3AsyncClient(TestServer.S3_ENDPOINT);
    return new S3Backend(
        config,
        new S3Util.S3ClientBundle(
            AmazonS3Provider.createTestS3Client(TestServer.S3_ENDPOINT), s3Async));
  }

  private void setUpWarmer() throws Exception {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (OutputStreamWriter writer =
        new OutputStreamWriter(byteArrayOutputStream, StateUtils.getValidatingUTF8Encoder())) {
      for (String line : getTestSearchRequestsAsJsonStrings()) {
        writer.write(line);
        writer.write("\n");
      }
    }
    remoteBackend.uploadWarmingQueries(
        TEST_SERVICE_NAME, TEST_INDEX, byteArrayOutputStream.toByteArray());
  }

  private List<String> getTestSearchRequestsAsJsonStrings() {
    return List.of(
        "{\"indexName\":\"test_index\",\"query\":{\"termQuery\":{\"field\":\"field0\"}}}",
        "{\"indexName\":\"test_index\",\"query\":{\"termQuery\":{\"field\":\"field1\"}}}");
  }

  private void setupIndex(String registerFieldsFile, String addDocsFile) throws Exception {
    LuceneServerGrpc.LuceneServerBlockingStub stub = primaryServer.getClient().getBlockingStub();
    stub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.startIndex(StartIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.registerFields(
        TestResourceHelper.getFieldsFromResourceFile("/" + registerFieldsFile).toBuilder()
            .setIndexName(TEST_INDEX)
            .build());
    TestDocumentHelper.addDocuments(
        primaryServer.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/" + addDocsFile));
    stub.refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());
  }

  private FieldDefResponse setupIndexNoDocuments(String registerFieldsFile) throws Exception {
    LuceneServerGrpc.LuceneServerBlockingStub stub = primaryServer.getClient().getBlockingStub();
    stub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.startIndex(StartIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    return stub.registerFields(
        TestResourceHelper.getFieldsFromResourceFile("/" + registerFieldsFile).toBuilder()
            .setIndexName(TEST_INDEX)
            .build());
  }

  private void setupIndexPrimary() throws Exception {
    LuceneServerGrpc.LuceneServerBlockingStub stub = primaryServer.getClient().getBlockingStub();
    stub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.startIndex(
        StartIndexRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setMode(Mode.PRIMARY)
            .setPrimaryGen(0)
            .build());
    stub.registerFields(
        TestResourceHelper.getFieldsFromResourceFile("/registerFieldsBasic.json").toBuilder()
            .setIndexName(TEST_INDEX)
            .build());
    // Add and commit an initial set of documents so the Lucene gen is at 1 before the
    // test's own commit calls; the test expects its first explicit commit to produce gen=2.
    TestDocumentHelper.addDocuments(
        primaryServer.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    stub.commit(CommitRequest.newBuilder().setIndexName(TEST_INDEX).build());
  }

  @Test
  public void testCreateIndex() {
    List<String> validIndexNames =
        List.of("idx", "idx1", "idx_1", "idx-3", "123", "IDX123", "iD1x23", "_");
    List<String> invalidIndexNames = List.of("id@x", "idx,1", "#", "", "(idx)");

    LuceneServerGrpc.LuceneServerBlockingStub blockingStub =
        primaryServer.getClient().getBlockingStub();

    for (String indexName : validIndexNames) {
      CreateIndexRequest request = CreateIndexRequest.newBuilder().setIndexName(indexName).build();
      CreateIndexResponse reply = blockingStub.createIndex(request);
      assertEquals(
          String.format(
              "Created Index name: %s",
              indexName, primaryServer.getGlobalState().getIndexDirBase().toString()),
          reply.getResponse());
    }

    for (String indexName : invalidIndexNames) {
      CreateIndexRequest request = CreateIndexRequest.newBuilder().setIndexName(indexName).build();
      try {
        blockingStub.createIndex(request);
        fail("The above line must throw an exception");
      } catch (StatusRuntimeException e) {
        assertEquals(
            String.format(
                "INVALID_ARGUMENT: Index name %s is invalid - must contain only a-z, A-Z or 0-9",
                indexName),
            e.getMessage());
      }
    }
  }

  @Test
  public void testStartShard() throws Exception {
    String testIndex = TEST_INDEX;
    LuceneServerGrpc.LuceneServerBlockingStub blockingStub =
        primaryServer.getClient().getBlockingStub();
    // create the index
    blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(testIndex).build());
    // start the index
    StartIndexResponse reply =
        blockingStub.startIndex(StartIndexRequest.newBuilder().setIndexName(testIndex).build());
    assertEquals(0, reply.getMaxDoc());
    assertEquals(0, reply.getNumDocs());
    assertTrue(!reply.getSegments().isEmpty());
  }

  @Test
  public void testStartIndexWithEmptyString() {
    LuceneServerGrpc.LuceneServerBlockingStub blockingStub =
        primaryServer.getClient().getBlockingStub();
    try {
      // start the index
      String emptyTestIndex = "";
      blockingStub.startIndex(StartIndexRequest.newBuilder().setIndexName(emptyTestIndex).build());
      fail("The above line must throw an exception");
    } catch (StatusRuntimeException e) {
      assertEquals(
          String.format(
              "INVALID_ARGUMENT: error while trying to start index since indexName was empty."),
          e.getMessage());
    }
    try {
      // start the index
      blockingStub.startIndex(StartIndexRequest.newBuilder().build());
      fail("The above line must throw an exception");
    } catch (StatusRuntimeException e) {
      assertEquals(
          String.format(
              "INVALID_ARGUMENT: error while trying to start index since indexName was empty."),
          e.getMessage());
    }
  }

  @Test
  public void testRegisterFieldsBasic() throws Exception {
    FieldDefResponse reply = setupIndexNoDocuments("registerFieldsBasic.json");
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
  }

  @Test
  public void testUpdateFieldsBasic() throws Exception {
    FieldDefResponse reply = setupIndexNoDocuments("registerFieldsBasic.json");
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
    reply =
        primaryServer
            .getClient()
            .getBlockingStub()
            .updateFields(
                FieldDefRequest.newBuilder()
                    .setIndexName("test_index")
                    .addField(
                        Field.newBuilder()
                            .setName("new_text_field")
                            .setType(FieldType.TEXT)
                            .setStoreDocValues(true)
                            .setSearch(true)
                            .setMultiValued(true)
                            .build())
                    .build());
    assertTrue(reply.getResponse().contains("new_text_field"));
  }

  @Test
  public void testRegisterVirtualFields() throws Exception {
    FieldDefResponse reply = setupIndexNoDocuments("registerFieldsBasic.json");
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
    reply =
        primaryServer
            .getClient()
            .getBlockingStub()
            .updateFields(
                FieldDefRequest.newBuilder()
                    .setIndexName("test_index")
                    .addField(
                        Field.newBuilder()
                            .setName("new_virtual_field")
                            .setType(FieldType.VIRTUAL)
                            .setScript(
                                Script.newBuilder()
                                    .setLang("js")
                                    .setSource("long_field*2.0")
                                    .build())
                            .build())
                    .build());
    assertTrue(reply.getResponse().contains("new_virtual_field"));
  }

  @Test
  public void testRegisterVirtualAndNonVirtualFields() throws Exception {
    FieldDefResponse reply = setupIndexNoDocuments("registerFieldsBasic.json");
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
    reply =
        primaryServer
            .getClient()
            .getBlockingStub()
            .updateFields(
                FieldDefRequest.newBuilder()
                    .setIndexName("test_index")
                    .addField(
                        Field.newBuilder()
                            .setName("new_virtual_field")
                            .setType(FieldType.VIRTUAL)
                            .setScript(
                                Script.newBuilder()
                                    .setLang("js")
                                    .setSource("long_field*2.0")
                                    .build())
                            .build())
                    .addField(
                        Field.newBuilder()
                            .setName("new_text_field")
                            .setType(FieldType.TEXT)
                            .setStoreDocValues(true)
                            .setSearch(true)
                            .setMultiValued(true)
                            .build())
                    .build());
    assertTrue(reply.getResponse().contains("new_virtual_field"));
    assertTrue(reply.getResponse().contains("new_text_field"));
  }

  @Test
  public void testRegisterVirtualWithDependentField() throws Exception {
    FieldDefResponse reply = setupIndexNoDocuments("registerFieldsBasic.json");
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
    reply =
        primaryServer
            .getClient()
            .getBlockingStub()
            .updateFields(
                FieldDefRequest.newBuilder()
                    .setIndexName("test_index")
                    .addField(
                        Field.newBuilder()
                            .setName("new_virtual_field")
                            .setType(FieldType.VIRTUAL)
                            .setScript(
                                Script.newBuilder()
                                    .setLang("js")
                                    .setSource("needed_field*2.0")
                                    .build())
                            .build())
                    .addField(
                        Field.newBuilder()
                            .setName("needed_field")
                            .setType(FieldType.INT)
                            .setStoreDocValues(true)
                            .setMultiValued(false)
                            .build())
                    .build());
    assertTrue(reply.getResponse().contains("new_virtual_field"));
    assertTrue(reply.getResponse().contains("needed_field"));
  }

  @Test
  public void testSearchPostUpdate() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");

    // update schema: add a new field
    primaryServer
        .getClient()
        .getBlockingStub()
        .updateFields(
            FieldDefRequest.newBuilder()
                .setIndexName("test_index")
                .addField(
                    Field.newBuilder()
                        .setName("new_text_field")
                        .setType(FieldType.TEXT)
                        .setStoreDocValues(true)
                        .setSearch(true)
                        .setMultiValued(true)
                        .build())
                .build());

    // 2 docs addDocuments
    TestDocumentHelper.addDocuments(
        primaryServer.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocsUpdated.csv"));
    primaryServer
        .getClient()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());
    List<String> RETRIEVE = Arrays.asList("doc_id", "new_text_field");

    Query query =
        Query.newBuilder()
            .setTermQuery(TermQuery.newBuilder().setField("new_text_field").setTextValue("updated"))
            .build();

    SearchResponse searchResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
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
  public void testAddDocumentsBasic() throws Exception {
    setupIndexNoDocuments("registerFieldsBasic.json");

    /* Tricky to check genId for exact match on a standalone node (one that does both indexing and real-time searching.
     *  The ControlledRealTimeReopenThread is running in the background which refreshes the searcher and updates the sequence_number
     *  each time maybeRefresh is invoked depending on frequency set in indexState.maxRefreshSec
     *  Overall: sequence_number(genID) is increased for each operation
     *  - open index writer
     *  - open taxo writer
     *  - once for each invocation of SearcherTaxonomyManager as explained above
     *  - once per commit
     */
    AddDocumentResponse resp1 =
        TestDocumentHelper.addDocuments(
            primaryServer.getClient().getAsyncStub(),
            TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    assert 3 <= Integer.parseInt(resp1.getGenId());
    AddDocumentResponse resp2 =
        TestDocumentHelper.addDocuments(
            primaryServer.getClient().getAsyncStub(),
            TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    assert 4 <= Integer.parseInt(resp2.getGenId());
  }

  @Test
  public void testAddDocumentsLatLon() throws Exception {
    setupIndex("registerFieldsLatLon.json", "addDocsLatLon.csv");
  }

  @Test
  public void testAddNoDocuments() throws Exception {
    setupIndexNoDocuments("registerFieldsLatLon.json");
    // Adding an empty stream should succeed without error
    TestDocumentHelper.addDocuments(primaryServer.getClient().getAsyncStub(), Stream.empty());
  }

  @Test
  public void testStats() throws Exception {
    setupIndexNoDocuments("registerFieldsBasic.json");
    StatsResponse statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(0, statsResponse.getNumDocs());
    assertEquals(0, statsResponse.getMaxDoc());
    assertEquals(0, statsResponse.getOrd());
    assertEquals(0, statsResponse.getCurrentSearcher().getNumDocs());
    assertTrue(statsResponse.getDirSize() > 0);
    assertEquals("started", statsResponse.getState());
    TestDocumentHelper.addDocuments(
        primaryServer.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    primaryServer
        .getClient()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());
    statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());
    assertEquals(0, statsResponse.getOrd());
    assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
    assertEquals(1, statsResponse.getCurrentSearcher().getNumSegments());
    assertEquals(8537, statsResponse.getDirSize(), 1500);
    assertEquals("started", statsResponse.getState());
  }

  @Test
  public void testRefresh() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");
    StatsResponse statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());
    assertEquals(0, statsResponse.getOrd());
    assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
    // check status on currentSearchAgain
    statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
  }

  @Test
  public void testDelete() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");
    // check stats numDocs for 2 docs
    StatsResponse statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());

    // delete 1 doc
    AddDocumentRequest.Builder addDocumentRequestBuilder = AddDocumentRequest.newBuilder();
    addDocumentRequestBuilder.setIndexName("test_index");
    AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder =
        AddDocumentRequest.MultiValuedField.newBuilder();
    addDocumentRequestBuilder.putFields("doc_id", multiValuedFieldsBuilder.addValue("1").build());
    AddDocumentResponse addDocumentResponse =
        primaryServer.getClient().getBlockingStub().delete(addDocumentRequestBuilder.build());
    assertFalse(addDocumentResponse.getPrimaryId().isEmpty());

    // manual refresh needed to depict changes in buffered deletes (i.e. not committed yet)
    primaryServer
        .getClient()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    // check stats numDocs for 1 docs
    statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(1, statsResponse.getNumDocs());
    // the refresh triggers a merge, so there is only one doc in the segment
    assertEquals(1, statsResponse.getMaxDoc());
  }

  @Test
  public void testDeleteByQuery() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");
    // check stats numDocs for 2 docs
    StatsResponse statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());

    Query query =
        Query.newBuilder()
            .setTermQuery(TermQuery.newBuilder().setField("count").setIntValue(7))
            .build();
    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setStartHit(0)
            .setTopHits(10)
            .addAllRetrieveFields(RETRIEVED_VALUES)
            .setQuery(query)
            .build();
    SearchResponse searchResponse =
        primaryServer.getClient().getBlockingStub().search(searchRequest);
    assertEquals(searchResponse.getHitsCount(), 1);

    // delete 1 doc

    DeleteByQueryRequest deleteByQueryRequest =
        DeleteByQueryRequest.newBuilder().setIndexName("test_index").addQuery(query).build();
    AddDocumentResponse addDocumentResponse =
        primaryServer.getClient().getBlockingStub().deleteByQuery(deleteByQueryRequest);
    assertFalse(addDocumentResponse.getPrimaryId().isEmpty());

    // manual refresh needed to depict changes in buffered deletes (i.e. not committed yet)
    primaryServer
        .getClient()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    // check stats numDocs for 1 docs
    statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(1, statsResponse.getNumDocs());
    // the refresh triggers a merge, so there is only one doc in the segment
    assertEquals(1, statsResponse.getMaxDoc());
    // deleted document does not show up in search response now
    searchResponse = primaryServer.getClient().getBlockingStub().search(searchRequest);
    assertEquals(searchResponse.getHitsCount(), 0);
  }

  @Test
  public void testDeleteAllDocuments() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");
    // check stats numDocs for 2 docs
    StatsResponse statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());

    // deleteAll documents
    DeleteAllDocumentsRequest.Builder deleteAllDocumentsBuilder =
        DeleteAllDocumentsRequest.newBuilder();
    DeleteAllDocumentsRequest deleteAllDocumentsRequest =
        deleteAllDocumentsBuilder.setIndexName("test_index").build();
    primaryServer.getClient().getBlockingStub().deleteAll(deleteAllDocumentsRequest);

    // check stats numDocs for 1 docs
    statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(0, statsResponse.getNumDocs());
    assertEquals(0, statsResponse.getMaxDoc());
  }

  @Test
  public void testDeleteIndex() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");
    // check stats numDocs for 2 docs
    StatsResponse statsResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());

    String indexName = "test_index";
    // deleteIndex
    DeleteIndexRequest deleteIndexRequest =
        DeleteIndexRequest.newBuilder().setIndexName(indexName).build();
    DeleteIndexResponse deleteIndexResponse =
        primaryServer.getClient().getBlockingStub().deleteIndex(deleteIndexRequest);

    Path indexRootDir = primaryServer.getGlobalState().getIndexDirBase().resolve(indexName);
    assertEquals(false, Files.exists(indexRootDir));

    assertEquals("ok", deleteIndexResponse.getOk());
  }

  @Test
  public void testSearchBasic() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");
    // manual refresh
    primaryServer
        .getClient()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    SearchResponse searchResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
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
  public void testSearchFetchingAllFieldsWithWildcard() throws Exception {
    setupIndex("registerFieldsWildcardRetrieval.json", "addDocsWildcardRetrieval.csv");

    primaryServer
        .getClient()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    SearchResponse searchResponseWithWildcard =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addRetrieveFields("*")
                    .build());

    assertEquals(2, searchResponseWithWildcard.getTotalHits().getValue());
    assertEquals(2, searchResponseWithWildcard.getHitsList().size());
    checkHits(searchResponseWithWildcard.getHits(0));
    checkHits(searchResponseWithWildcard.getHits(1));
  }

  @Test
  public void testSearchLatLong() throws Exception {
    setupIndex("registerFieldsLatLon.json", "addDocsLatLon.csv");

    SearchResponse searchResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(LAT_LON_VALUES)
                    .build());

    assertEquals(2, searchResponse.getTotalHits().getValue());
    assertEquals(2, searchResponse.getHitsList().size());
    SearchResponse.Hit firstHit = searchResponse.getHits(0);
    checkHitsLatLon(firstHit);
    SearchResponse.Hit secondHit = searchResponse.getHits(1);
    checkHitsLatLon(secondHit);
  }

  @Test
  public void testSearchIndexVirtualFields() throws Exception {
    setupIndex("registerFieldsVirtual.json", "addDocs.csv");

    List<String> queryFields = new ArrayList<>(RETRIEVED_VALUES);
    queryFields.addAll(INDEX_VIRTUAL_FIELDS);

    SearchResponse searchResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(queryFields)
                    .setQueryText("vendor_name:first vendor")
                    .build());

    assertEquals(2, searchResponse.getTotalHits().getValue());
    assertEquals(2, searchResponse.getHitsList().size());
    SearchResponse.Hit firstHit = searchResponse.getHits(0);
    checkHitsVirtual(firstHit, true, false);
    SearchResponse.Hit secondHit = searchResponse.getHits(1);
    checkHitsVirtual(secondHit, true, false);
  }

  @Test
  public void testSearchQueryVirtualFields() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");

    List<String> queryFields = new ArrayList<>(RETRIEVED_VALUES);
    queryFields.addAll(QUERY_VIRTUAL_FIELDS);

    SearchResponse searchResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(queryFields)
                    .addAllVirtualFields(getQueryVirtualFields())
                    .setQueryText("vendor_name:first vendor")
                    .build());

    assertEquals(2, searchResponse.getTotalHits().getValue());
    assertEquals(2, searchResponse.getHitsList().size());
    SearchResponse.Hit firstHit = searchResponse.getHits(0);
    checkHitsVirtual(firstHit, false, true);
    SearchResponse.Hit secondHit = searchResponse.getHits(1);
    checkHitsVirtual(secondHit, false, true);
  }

  @Test
  public void testSearchBothVirtualFields() throws Exception {
    setupIndex("registerFieldsVirtual.json", "addDocs.csv");

    List<String> queryFields = new ArrayList<>(RETRIEVED_VALUES);
    queryFields.addAll(INDEX_VIRTUAL_FIELDS);
    queryFields.addAll(QUERY_VIRTUAL_FIELDS);

    SearchResponse searchResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(queryFields)
                    .addAllVirtualFields(getQueryVirtualFields())
                    .setQueryText("vendor_name:first vendor")
                    .build());

    assertEquals(2, searchResponse.getTotalHits().getValue());
    assertEquals(2, searchResponse.getHitsList().size());
    SearchResponse.Hit firstHit = searchResponse.getHits(0);
    checkHitsVirtual(firstHit, true, true);
    SearchResponse.Hit secondHit = searchResponse.getHits(1);
    checkHitsVirtual(secondHit, true, true);
  }

  @Test
  public void testBackupWarmingQueries() throws Exception {
    LuceneServerGrpc.LuceneServerBlockingStub replicaStub =
        replicaServer.getClient().getBlockingStub();
    replicaStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    replicaStub.startIndex(StartIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    replicaStub.registerFields(
        TestResourceHelper.getFieldsFromResourceFile("/registerFieldsBasic.json").toBuilder()
            .setIndexName(TEST_INDEX)
            .build());
    replicaServer.getGlobalState().getIndexOrThrow(TEST_INDEX).initWarmer(remoteBackend);
    assertNotNull(replicaServer.getGlobalState().getIndexOrThrow(TEST_INDEX).getWarmer());
    // Average case should pass
    replicaStub.backupWarmingQueries(
        BackupWarmingQueriesRequest.newBuilder()
            .setIndex(TEST_INDEX)
            .setServiceName(TEST_SERVICE_NAME)
            .build());
    // Should fail; does not meet UptimeMinutesThreshold
    try {
      replicaStub.backupWarmingQueries(
          BackupWarmingQueriesRequest.newBuilder()
              .setIndex(TEST_INDEX)
              .setServiceName(TEST_SERVICE_NAME)
              .setUptimeMinutesThreshold(1000)
              .build());
      fail("Expecting exception on the previous line");
    } catch (StatusRuntimeException e) {
      Pattern pattern =
          Pattern.compile(
              "UNKNOWN: Unable to backup warming queries since uptime is [0-9] minutes, which is less than threshold 1000");
      Matcher m = pattern.matcher(e.getMessage());
      assertTrue(m.matches());
    }

    // Should fail; does not meet NumQueriesThreshold
    try {
      replicaStub.backupWarmingQueries(
          BackupWarmingQueriesRequest.newBuilder()
              .setIndex(TEST_INDEX)
              .setServiceName(TEST_SERVICE_NAME)
              .setNumQueriesThreshold(1000)
              .build());
      fail("Expecting exception on the previous line");
    } catch (StatusRuntimeException e) {
      assertEquals(
          "UNKNOWN: Unable to backup warming queries since warmer has 0 requests, which is less than threshold 1000",
          e.getMessage());
    }
  }

  @Test
  public void testMetrics() {
    // make rpc calls to populate some metrics
    primaryServer.getClient().getBlockingStub().status(HealthCheckRequest.newBuilder().build());

    HttpBody response =
        primaryServer.getClient().getBlockingStub().metrics(Empty.newBuilder().build());
    HashSet expectedSampleNames =
        new HashSet(
            Arrays.asList(
                "grpc_server_handled_latency_seconds",
                "grpc_server_handled_total",
                "grpc_server_started_total"));
    assertEquals("text/plain", response.getContentType());
    String data = new String(response.getData().toByteArray());
    String[] arr = data.split("\n");
    System.out.println(Arrays.toString(arr));
    Set<String> labelsHelp = new HashSet<>();
    Set<String> labelsType = new HashSet<>();
    for (int i = 0; i < arr.length; i++) {
      if (arr[i].startsWith("# HELP")) {
        labelsHelp.add(arr[i].split(" ")[2]);
      } else if (arr[i].startsWith("# TYPE")) {
        labelsType.add(arr[i].split(" ")[2]);
      }
    }
    assertEquals(labelsType, labelsHelp);
    assertEquals(expectedSampleNames, labelsHelp);
  }

  @Test
  public void testIndexState() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");
    IndexStateResponse indexStateResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .indexState(IndexStateRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(
        primaryServer.getGlobalState().getIndex(TEST_INDEX).getIndexStateInfo(),
        indexStateResponse.getIndexState());
  }

  @Test
  public void testForceMerge() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");
    // add more documents to a different segment
    TestDocumentHelper.addDocuments(
        primaryServer.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    primaryServer
        .getClient()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    StatsResponse stats =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(4, stats.getNumDocs());
    assertEquals(2, stats.getCurrentSearcher().getNumSegments());

    ForceMergeResponse response =
        primaryServer
            .getClient()
            .getBlockingStub()
            .forceMerge(
                ForceMergeRequest.newBuilder()
                    .setIndexName(TEST_INDEX)
                    .setMaxNumSegments(1)
                    .setDoWait(true)
                    .build());
    assertEquals(ForceMergeResponse.Status.FORCE_MERGE_COMPLETED, response.getStatus());

    primaryServer
        .getClient()
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    stats =
        primaryServer
            .getClient()
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    assertEquals(4, stats.getNumDocs());
    assertEquals(1, stats.getCurrentSearcher().getNumSegments());
  }

  @Test
  public void testReleaseSnapshotOnPrimary() throws Exception {
    setupIndexPrimary();
    CommitRequest commitRequest = CommitRequest.newBuilder().setIndexName(TEST_INDEX).build();
    primaryServer.getClient().getBlockingStub().commit(commitRequest);

    // create a snapshot
    CreateSnapshotRequest createSnapshotRequest =
        CreateSnapshotRequest.newBuilder().setIndexName(TEST_INDEX).build();
    CreateSnapshotResponse createSnapshotResponse =
        primaryServer.getClient().getBlockingStub().createSnapshot(createSnapshotRequest);
    assertEquals(2, createSnapshotResponse.getSnapshotId().getIndexGen());
    assertEquals(-1, createSnapshotResponse.getSnapshotId().getStateGen());

    // add more documents and another commit to create another index gen
    TestDocumentHelper.addDocuments(
        primaryServer.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    primaryServer.getClient().getBlockingStub().commit(commitRequest);

    // create another snapshot
    createSnapshotResponse =
        primaryServer.getClient().getBlockingStub().createSnapshot(createSnapshotRequest);
    assertEquals(3, createSnapshotResponse.getSnapshotId().getIndexGen());
    assertEquals(-1, createSnapshotResponse.getSnapshotId().getStateGen());

    // Release the first snapshot with index and state gen
    ReleaseSnapshotRequest releaseSnapshotRequest =
        ReleaseSnapshotRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setSnapshotId(SnapshotId.newBuilder().setIndexGen(2).setStateGen(-1))
            .build();
    ReleaseSnapshotResponse releaseSnapshotResponse =
        primaryServer.getClient().getBlockingStub().releaseSnapshot(releaseSnapshotRequest);
    assertTrue(releaseSnapshotResponse.getSuccess());

    // Release the second snapshot's index gen and already released state gen
    releaseSnapshotRequest =
        ReleaseSnapshotRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .setSnapshotId(SnapshotId.newBuilder().setIndexGen(3).setStateGen(-1))
            .build();
    releaseSnapshotResponse =
        primaryServer.getClient().getBlockingStub().releaseSnapshot(releaseSnapshotRequest);
    assertTrue(releaseSnapshotResponse.getSuccess());

    // Verify both index gens released
    GetAllSnapshotGenRequest getAllSnapshotGenRequest =
        GetAllSnapshotGenRequest.newBuilder().setIndexName(TEST_INDEX).build();
    GetAllSnapshotGenResponse getAllSnapshotGenResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .getAllSnapshotIndexGen(getAllSnapshotGenRequest);
    assertEquals(0, getAllSnapshotGenResponse.getIndexGensCount());
  }

  @Test
  public void testGetAllSnapshotIndexGen() throws Exception {
    setupIndex("registerFieldsBasic.json", "addDocs.csv");
    CommitRequest commitRequest = CommitRequest.newBuilder().setIndexName(TEST_INDEX).build();
    primaryServer.getClient().getBlockingStub().commit(commitRequest);

    // create a snapshot
    CreateSnapshotRequest createSnapshotRequest =
        CreateSnapshotRequest.newBuilder().setIndexName(TEST_INDEX).build();
    CreateSnapshotResponse createSnapshotResponse =
        primaryServer.getClient().getBlockingStub().createSnapshot(createSnapshotRequest);
    assertEquals(2, createSnapshotResponse.getSnapshotId().getIndexGen());

    // add more documents and another commit to create another index gen
    TestDocumentHelper.addDocuments(
        primaryServer.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    primaryServer.getClient().getBlockingStub().commit(commitRequest);

    // create another snapshot
    createSnapshotResponse =
        primaryServer.getClient().getBlockingStub().createSnapshot(createSnapshotRequest);
    assertEquals(3, createSnapshotResponse.getSnapshotId().getIndexGen());

    GetAllSnapshotGenRequest getAllSnapshotGenRequest =
        GetAllSnapshotGenRequest.newBuilder().setIndexName(TEST_INDEX).build();
    GetAllSnapshotGenResponse getAllSnapshotGenResponse =
        primaryServer
            .getClient()
            .getBlockingStub()
            .getAllSnapshotIndexGen(getAllSnapshotGenRequest);

    assertTrue(getAllSnapshotGenResponse.getIndexGensList().contains(2L));
    assertTrue(getAllSnapshotGenResponse.getIndexGensList().contains(3L));
  }

  @Test
  public void testAddDocsHasPrimaryId() throws Exception {
    setupIndexPrimary();
    AddDocumentResponse response =
        TestDocumentHelper.addDocuments(
            primaryServer.getClient().getAsyncStub(),
            TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    assertFalse(response.getPrimaryId().isEmpty());
  }

  @Test
  public void testCommitHasPrimaryId() throws Exception {
    setupIndexPrimary();
    TestDocumentHelper.addDocuments(
        primaryServer.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    CommitRequest commitRequest = CommitRequest.newBuilder().setIndexName(TEST_INDEX).build();
    CommitResponse response = primaryServer.getClient().getBlockingStub().commit(commitRequest);
    assertFalse(response.getPrimaryId().isEmpty());
  }

  @Test
  public void testReady() throws Exception {
    LuceneServerGrpc.LuceneServerBlockingStub blockingStub =
        primaryServer.getClient().getBlockingStub();
    String index1 = "index1";
    String index2 = "index2";
    String index3 = "index3";

    // Create all indices
    for (String indexName : List.of(index1, index2, index3)) {
      CreateIndexResponse createIndexResponse =
          blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(indexName).build());
      String expectedResponse =
          String.format(
              "Created Index name: %s",
              indexName, primaryServer.getGlobalState().getIndexDirBase().toString());
      assertEquals(expectedResponse, createIndexResponse.getResponse());
    }

    // Start indices 2 and 3
    for (String indexName : List.of(index2, index3)) {
      StartIndexRequest startIndexRequest =
          StartIndexRequest.newBuilder().setIndexName(indexName).setMode(Mode.STANDALONE).build();
      StartIndexResponse startIndexResponse = blockingStub.startIndex(startIndexRequest);
      assertEquals(0, startIndexResponse.getNumDocs());
    }

    primaryServer.getGlobalState().getIndexOrThrow(index3).getShard(0).writer.close();

    try {
      blockingStub.ready(ReadyCheckRequest.newBuilder().setIndexNames("").build());
      fail("Expecting exception on the previous line");
    } catch (StatusRuntimeException e) {
      assertEquals(e.getMessage(), "UNAVAILABLE: Indices not started: [index3]");
    }

    for (String indexNames : Arrays.asList("index1", "index1,index2")) {
      try {
        blockingStub.ready(ReadyCheckRequest.newBuilder().setIndexNames(indexNames).build());
        fail("Expecting exception on the previous line");
      } catch (StatusRuntimeException e) {
        assertEquals(e.getMessage(), "UNAVAILABLE: Indices not started: [index1]");
      }
    }

    for (String indexNames : Arrays.asList("index3", "index2,index3")) {
      try {
        blockingStub.ready(ReadyCheckRequest.newBuilder().setIndexNames(indexNames).build());
        fail("Expecting exception on the previous line");
      } catch (StatusRuntimeException e) {
        assertEquals(e.getMessage(), "UNAVAILABLE: Indices not started: [index3]");
      }
    }

    for (String indexNames : Arrays.asList("index4", "index1,index4", "index4,index2,index1")) {
      try {
        blockingStub.ready(ReadyCheckRequest.newBuilder().setIndexNames(indexNames).build());
        fail("Expecting exception on the previous line");
      } catch (StatusRuntimeException e) {
        assertEquals(e.getMessage(), "UNAVAILABLE: Indices do not exist: [index4]");
      }
    }

    HealthCheckResponse response =
        blockingStub.ready(ReadyCheckRequest.newBuilder().setIndexNames("index2").build());
    assertEquals(response.getHealth(), TransferStatusCode.Done);
  }

  @Test
  public void testQueryCache() {
    QueryCache queryCache = IndexSearcher.getDefaultQueryCache();
    assertTrue(queryCache instanceof NrtQueryCache);

    String configStr = String.join("\n", "queryCache:", "  enabled: false");
    NrtsearchConfig configuration =
        new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    LuceneServerImpl.initQueryCache(configuration);
    assertNull(IndexSearcher.getDefaultQueryCache());

    configStr = String.join("\n", "queryCache:", "  enabled: true");
    configuration = new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    LuceneServerImpl.initQueryCache(configuration);
    queryCache = IndexSearcher.getDefaultQueryCache();
    assertTrue(queryCache instanceof NrtQueryCache);
  }

  @Test
  public void testInitMaxClauseCount() {
    int originalMaxClauseCount = IndexSearcher.getMaxClauseCount();
    try {
      String defaultConfig = "nodeName: \"test_node\"";
      NrtsearchConfig defaultConfiguration =
          new NrtsearchConfig(new ByteArrayInputStream(defaultConfig.getBytes()));

      LuceneServerImpl.initMaxClauseCount(defaultConfiguration);
      assertEquals(1024, IndexSearcher.getMaxClauseCount()); // Default value

      String customConfig = String.join("\n", "nodeName: \"test_node\"", "maxClauseCount: 2048");
      NrtsearchConfig customConfiguration =
          new NrtsearchConfig(new ByteArrayInputStream(customConfig.getBytes()));

      LuceneServerImpl.initMaxClauseCount(customConfiguration);
      assertEquals(2048, IndexSearcher.getMaxClauseCount());

    } finally {
      IndexSearcher.setMaxClauseCount(originalMaxClauseCount);
    }
  }

  public static List<VirtualField> getQueryVirtualFields() {
    List<VirtualField> fields = new ArrayList<>();
    fields.add(
        VirtualField.newBuilder()
            .setName("query_virtual_field")
            .setScript(
                Script.newBuilder()
                    .setLang("js")
                    .setSource("4.0*float_field+2.0*double_field")
                    .build())
            .build());
    fields.add(
        VirtualField.newBuilder()
            .setName("query_virtual_field_w_score")
            .setScript(Script.newBuilder().setLang("js").setSource("5.0*_score").build())
            .build());
    return fields;
  }

  @Test
  public void testCancellationDefaultEnabled() {
    assertTrue(DeadlineUtils.getCancellationEnabled());
  }

  public static void checkHits(SearchResponse.Hit hit) {
    Map<String, CompositeFieldValue> fields = hit.getFieldsMap();
    checkFieldNames(RETRIEVED_VALUES, fields);
    checkBasicFields(fields);
  }

  public static void checkBasicFields(Map<String, CompositeFieldValue> fields) {
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

    checkPerFieldValues(
        expectedLicenseNo,
        getIntFieldValuesListAsString(fields.get("license_no").getFieldValueList()));
    checkPerFieldValues(
        expectedVendorName,
        getStringFieldValuesList(fields.get("vendor_name").getFieldValueList()));
    checkPerFieldValues(
        expectedVendorNameAtom,
        getStringFieldValuesList(fields.get("vendor_name_atom").getFieldValueList()));
    assertEquals(expectedCount, fields.get("count").getFieldValueList().get(0).getIntValue());
    assertEquals(
        expectedLongField, fields.get("long_field").getFieldValueList().get(0).getLongValue());
    checkPerFieldValues(
        expectedDoubleFieldMulti,
        getDoubleFieldValuesListAsString(fields.get("double_field_multi").getFieldValueList()));
    checkPerFieldValues(
        expectedDoubleField,
        getDoubleFieldValuesListAsString(fields.get("double_field").getFieldValueList()));
    checkPerFieldValues(
        expectedFloatFieldMulti,
        getFloatFieldValuesListAsString(fields.get("float_field_multi").getFieldValueList()));
    checkPerFieldValues(
        expectedFloatField,
        getFloatFieldValuesListAsString(fields.get("float_field").getFieldValueList()));
    checkPerFieldValues(
        expectedBooleanFieldMulti,
        getBooleanFieldValuesListAsString(fields.get("boolean_field_multi").getFieldValueList()));
    checkPerFieldValues(
        expectedBooleanField,
        getBooleanFieldValuesListAsString(fields.get("boolean_field").getFieldValueList()));
    checkPerFieldValues(
        expectedDescription,
        getStringFieldValuesList(fields.get("description").getFieldValueList()));
    assertEquals(expectedDate, fields.get("date").getFieldValueList().get(0).getLongValue());
  }

  public static void checkHitsVirtual(
      SearchResponse.Hit hit, boolean withIndexVirtualFields, boolean withQueryVirtualFields) {
    int totalFields = 0;

    Map<String, CompositeFieldValue> fields = hit.getFieldsMap();

    List<String> basicFields =
        fields.keySet().stream().filter(RETRIEVED_VALUES::contains).collect(Collectors.toList());
    Collections.sort(RETRIEVED_VALUES);
    Collections.sort(basicFields);
    assertEquals(RETRIEVED_VALUES, basicFields);
    checkBasicFields(fields);
    totalFields += RETRIEVED_VALUES.size();

    if (withIndexVirtualFields) {
      List<String> indexVirtualFields =
          fields.keySet().stream()
              .filter(INDEX_VIRTUAL_FIELDS::contains)
              .collect(Collectors.toList());
      Collections.sort(INDEX_VIRTUAL_FIELDS);
      Collections.sort(indexVirtualFields);
      assertEquals(INDEX_VIRTUAL_FIELDS, indexVirtualFields);
      checkIndexVirtualFields(fields, hit.getScore());
      totalFields += INDEX_VIRTUAL_FIELDS.size();
    }

    if (withQueryVirtualFields) {
      List<String> queryVirtualFields =
          fields.keySet().stream()
              .filter(QUERY_VIRTUAL_FIELDS::contains)
              .collect(Collectors.toList());
      Collections.sort(QUERY_VIRTUAL_FIELDS);
      Collections.sort(queryVirtualFields);
      assertEquals(QUERY_VIRTUAL_FIELDS, queryVirtualFields);
      checkQueryVirtualFields(fields, hit.getScore());
      totalFields += QUERY_VIRTUAL_FIELDS.size();
    }
    assertEquals(totalFields, fields.size());
  }

  public static void checkIndexVirtualFields(
      Map<String, CompositeFieldValue> fields, double score) {
    String docId = fields.get("doc_id").getFieldValue(0).getTextValue();

    double expectedVirtualField = 0.0;
    double expectedVirtualWithScore = 0.0;
    double expectedScore = 0.0;

    if (docId.equals("1")) {
      expectedVirtualField = 236.02;
      expectedScore = 0.516;
      expectedVirtualWithScore = 3.0 * expectedScore;
    } else if (docId.equals("2")) {
      expectedVirtualField = 448.04;
      expectedScore = 0.0828;
      expectedVirtualWithScore = 3.0 * expectedScore;
    } else {
      fail(String.format("docId %s not indexed", docId));
    }

    assertEquals(
        expectedVirtualField, fields.get("virtual_field").getFieldValue(0).getDoubleValue(), 0.001);
    assertEquals(expectedScore, score, 0.001);
    assertEquals(
        expectedVirtualWithScore,
        fields.get("virtual_field_w_score").getFieldValue(0).getDoubleValue(),
        0.001);
  }

  public static void checkQueryVirtualFields(
      Map<String, CompositeFieldValue> fields, double score) {
    String docId = fields.get("doc_id").getFieldValue(0).getTextValue();

    double expectedVirtualField = 0.0;
    double expectedVirtualWithScore = 0.0;
    double expectedScore = 0.0;

    if (docId.equals("1")) {
      expectedVirtualField = 402.06;
      expectedScore = 0.516;
      expectedVirtualWithScore = 5.0 * expectedScore;
    } else if (docId.equals("2")) {
      expectedVirtualField = 804.1;
      expectedScore = 0.0828;
      expectedVirtualWithScore = 5.0 * expectedScore;
    } else {
      fail(String.format("docId %s not indexed", docId));
    }

    assertEquals(
        expectedVirtualField,
        fields.get("query_virtual_field").getFieldValue(0).getDoubleValue(),
        0.001);
    assertEquals(expectedScore, score, 0.001);
    assertEquals(
        expectedVirtualWithScore,
        fields.get("query_virtual_field_w_score").getFieldValue(0).getDoubleValue(),
        0.001);
  }

  public static void checkHitsLatLon(SearchResponse.Hit hit) {
    Map<String, CompositeFieldValue> fields = hit.getFieldsMap();
    checkFieldNames(LAT_LON_VALUES, fields);

    String docId = fields.get("doc_id").getFieldValue(0).getTextValue();

    List<String> expectedLicenseNo = null;
    List<String> expectedVendorName = null;
    List<String> expectedVendorNameAtom = null;
    List<Double> expectedLat = null;
    List<Double> expectedLon = null;
    List<Double> expectedMultiLat = null;
    List<Double> expectedMultiLon = null;

    if (docId.equals("1")) {
      expectedLicenseNo = Arrays.asList("300", "3100");
      expectedVendorName = Arrays.asList("first vendor", "first again");
      expectedVendorNameAtom = Arrays.asList("first atom vendor", "first atom again");
      expectedLat = Arrays.asList(37.7749);
      expectedLon = Arrays.asList(-122.393990);
      expectedMultiLat = Arrays.asList(30.9988, 40.1748);
      expectedMultiLon = Arrays.asList(-120.33977, -142.453490);
    } else if (docId.equals("2")) {
      expectedLicenseNo = Arrays.asList("411", "4222");
      expectedVendorName = Arrays.asList("second vendor", "second again");
      expectedVendorNameAtom = Arrays.asList("second atom vendor", "second atom again");
      expectedLat = Arrays.asList(37.5485);
      expectedLon = Arrays.asList(-121.9886);
      expectedMultiLat = Arrays.asList(29.9988, 39.1748);
      expectedMultiLon = Arrays.asList(-119.33977, -141.453490);
    } else {
      fail(String.format("docId %s not indexed", docId));
    }

    checkPerFieldValues(
        expectedLicenseNo,
        getIntFieldValuesListAsString(fields.get("license_no").getFieldValueList()));
    checkPerFieldValues(
        expectedVendorName,
        getStringFieldValuesList(fields.get("vendor_name").getFieldValueList()));
    checkPerFieldValues(
        expectedVendorNameAtom,
        getStringFieldValuesList(fields.get("vendor_name_atom").getFieldValueList()));
    List<SearchResponse.Hit.FieldValue> latLonList = fields.get("lat_lon").getFieldValueList();
    assertEquals(latLonList.size(), expectedLat.size());
    for (int i = 0; i < latLonList.size(); ++i) {
      assertEquals(expectedLat.get(i), latLonList.get(i).getLatLngValue().getLatitude(), 0.00001);
      assertEquals(expectedLon.get(i), latLonList.get(i).getLatLngValue().getLongitude(), 0.00001);
    }
    List<SearchResponse.Hit.FieldValue> latLonMultiList =
        fields.get("lat_lon_multi").getFieldValueList();
    assertEquals(latLonMultiList.size(), expectedMultiLat.size());
    for (int i = 0; i < latLonMultiList.size(); ++i) {
      assertEquals(
          expectedMultiLat.get(i), latLonMultiList.get(i).getLatLngValue().getLatitude(), 0.00001);
      assertEquals(
          expectedMultiLon.get(i), latLonMultiList.get(i).getLatLngValue().getLongitude(), 0.00001);
    }
  }

  public static void checkFieldNames(
      List<String> expectedNames, Map<String, CompositeFieldValue> actualNames) {
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

  private static List<String> getIntFieldValuesListAsString(
      List<SearchResponse.Hit.FieldValue> fieldValues) {
    return fieldValues.stream()
        .map(fieldValue -> String.valueOf(fieldValue.getIntValue()))
        .collect(Collectors.toList());
  }

  private static List<String> getDoubleFieldValuesListAsString(
      List<SearchResponse.Hit.FieldValue> fieldValues) {
    return fieldValues.stream()
        .map(fieldValue -> String.valueOf(fieldValue.getDoubleValue()))
        .collect(Collectors.toList());
  }

  private static List<String> getFloatFieldValuesListAsString(
      List<SearchResponse.Hit.FieldValue> fieldValues) {
    return fieldValues.stream()
        .map(fieldValue -> String.valueOf(fieldValue.getFloatValue()))
        .collect(Collectors.toList());
  }

  private static List<String> getBooleanFieldValuesListAsString(
      List<SearchResponse.Hit.FieldValue> fieldValues) {
    return fieldValues.stream()
        .map(fieldValue -> String.valueOf(fieldValue.getBooleanValue()))
        .collect(Collectors.toList());
  }

  private static List<String> getStringFieldValuesList(
      List<SearchResponse.Hit.FieldValue> fieldValues) {
    return fieldValues.stream()
        .map(SearchResponse.Hit.FieldValue::getTextValue)
        .collect(Collectors.toList());
  }

  private static long getStringDateTimeAsListOfStringMillis(String dateTime) {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    return LocalDateTime.parse(dateTime, dateTimeFormatter)
        .toInstant(ZoneOffset.UTC)
        .toEpochMilli();
  }
}
