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

import static com.yelp.nrtsearch.server.grpc.GrpcServer.rmDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazonaws.services.s3.AmazonS3;
import com.google.api.HttpBody;
import com.google.protobuf.Empty;
import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.backup.Archiver;
import com.yelp.nrtsearch.server.backup.ArchiverImpl;
import com.yelp.nrtsearch.server.backup.TarImpl;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.LuceneServer.LuceneServerImpl;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.luceneserver.search.cache.NrtQueryCache;
import com.yelp.nrtsearch.test_utils.AmazonS3Provider;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LuceneServerTest {
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

  private final String bucketName = "lucene-server-unittest";

  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Rule public final AmazonS3Provider s3Provider = new AmazonS3Provider(bucketName);

  private GrpcServer grpcServer;
  private GrpcServer replicaGrpcServer;
  private CollectorRegistry collectorRegistry;
  private Archiver archiver;
  private AmazonS3 s3;
  private final String TEST_SERVICE_NAME = "TEST_SERVICE_NAME";

  @After
  public void tearDown() throws IOException {
    tearDownGrpcServer();
    tearDownReplicaGrpcServer();
  }

  private void tearDownGrpcServer() throws IOException {
    grpcServer.getGlobalState().close();
    grpcServer.shutdown();
    rmDir(Paths.get(grpcServer.getIndexDir()).getParent());
  }

  private void tearDownReplicaGrpcServer() throws IOException {
    replicaGrpcServer.getGlobalState().close();
    replicaGrpcServer.shutdown();
    rmDir(Paths.get(replicaGrpcServer.getIndexDir()).getParent());
  }

  @Before
  public void setUp() throws IOException {
    collectorRegistry = new CollectorRegistry();
    archiver = setUpArchiver();
    grpcServer = setUpGrpcServer(collectorRegistry);
    replicaGrpcServer = setUpReplicaGrpcServer(collectorRegistry);
    setUpWarmer();
  }

  private Archiver setUpArchiver() throws IOException {
    Path archiverDirectory = folder.newFolder("archiver").toPath();

    s3 = s3Provider.getAmazonS3();

    return new ArchiverImpl(
        s3, bucketName, archiverDirectory, new TarImpl(TarImpl.CompressionMode.LZ4));
  }

  private void setUpWarmer() throws IOException {
    Path warmingQueriesDir = folder.newFolder("warming_queries").toPath();
    try (BufferedWriter writer =
        Files.newBufferedWriter(warmingQueriesDir.resolve("warming_queries.txt"))) {
      List<String> testSearchRequestsJson = getTestSearchRequestsAsJsonStrings();
      for (String line : testSearchRequestsJson) {
        writer.write(line);
        writer.newLine();
      }
      writer.flush();
    }
    String resourceName = "test_index_warming_queries";
    String versionHash =
        archiver.upload(
            TEST_SERVICE_NAME, resourceName, warmingQueriesDir, List.of(), List.of(), false);
    archiver.blessVersion(TEST_SERVICE_NAME, resourceName, versionHash);
  }

  private List<String> getTestSearchRequestsAsJsonStrings() {
    return List.of(
        "{\"indexName\":\"test_index\",\"query\":{\"termQuery\":{\"field\":\"field0\"}}}",
        "{\"indexName\":\"test_index\",\"query\":{\"termQuery\":{\"field\":\"field1\"}}}");
  }

  private GrpcServer setUpGrpcServer(CollectorRegistry collectorRegistry) throws IOException {
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(Mode.STANDALONE, folder.getRoot());
    return new GrpcServer(
        collectorRegistry,
        grpcCleanup,
        luceneServerConfiguration,
        folder,
        null,
        luceneServerConfiguration.getIndexDir(),
        testIndex,
        luceneServerConfiguration.getPort(),
        archiver,
        Collections.emptyList());
  }

  private GrpcServer setUpReplicaGrpcServer(CollectorRegistry collectorRegistry)
      throws IOException {
    String testIndex = "test_index";
    LuceneServerConfiguration luceneServerReplicaConfiguration =
        LuceneServerTestConfigurationFactory.getConfig(
            Mode.REPLICA, folder.getRoot(), getExtraConfig());

    return new GrpcServer(
        grpcCleanup,
        luceneServerReplicaConfiguration,
        folder,
        null,
        luceneServerReplicaConfiguration.getIndexDir(),
        testIndex,
        luceneServerReplicaConfiguration.getPort(),
        archiver);
  }

  private String getExtraConfig() {
    return String.join(
        "\n",
        "warmer:",
        "  maxWarmingQueries: 10",
        "  warmOnStartup: true",
        "  warmingParallelism: 1",
        "syncInitialNrtPoint: false");
  }

  @Test
  public void testCreateIndex() {
    List<String> validIndexNames =
        List.of("idx", "idx1", "idx_1", "idx-3", "123", "IDX123", "iD1x23", "_");
    List<String> invalidIndexNames = List.of("id@x", "idx,1", "#", "", "(idx)");

    LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();

    for (String indexName : validIndexNames) {
      CreateIndexRequest request = CreateIndexRequest.newBuilder().setIndexName(indexName).build();
      CreateIndexResponse reply = blockingStub.createIndex(request);
      assertEquals(
          String.format("Created Index name: %s", indexName, grpcServer.getIndexDir()),
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
  public void testStartShard() throws IOException {
    String testIndex = grpcServer.getTestIndex();
    LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();
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
    LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();
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
    FieldDefResponse reply =
        new GrpcServer.IndexAndRoleManager(grpcServer)
            .createStartIndexAndRegisterFields(Mode.STANDALONE);
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
  }

  @Test
  public void testUpdateFieldsBasic() throws Exception {
    FieldDefResponse reply =
        new GrpcServer.IndexAndRoleManager(grpcServer)
            .createStartIndexAndRegisterFields(Mode.STANDALONE);
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
    reply =
        grpcServer
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
                            .setTokenize(true)
                            .build())
                    .build());
    assertTrue(reply.getResponse().contains("new_text_field"));
  }

  @Test
  public void testRegisterVirtualFields() throws Exception {
    FieldDefResponse reply =
        new GrpcServer.IndexAndRoleManager(grpcServer)
            .createStartIndexAndRegisterFields(Mode.STANDALONE);
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
    reply =
        grpcServer
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
    FieldDefResponse reply =
        new GrpcServer.IndexAndRoleManager(grpcServer)
            .createStartIndexAndRegisterFields(Mode.STANDALONE);
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
    reply =
        grpcServer
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
                            .setTokenize(true)
                            .build())
                    .build());
    assertTrue(reply.getResponse().contains("new_virtual_field"));
    assertTrue(reply.getResponse().contains("new_text_field"));
  }

  @Test
  public void testRegisterVirtualWithDependentField() throws Exception {
    FieldDefResponse reply =
        new GrpcServer.IndexAndRoleManager(grpcServer)
            .createStartIndexAndRegisterFields(Mode.STANDALONE);
    assertTrue(reply.getResponse().contains("vendor_name"));
    assertTrue(reply.getResponse().contains("vendor_name_atom"));
    assertTrue(reply.getResponse().contains("license_no"));
    reply =
        grpcServer
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
                            .setSort(true)
                            .build())
                    .build());
    assertTrue(reply.getResponse().contains("new_virtual_field"));
    assertTrue(reply.getResponse().contains("needed_field"));
  }

  @Test
  public void testSearchPostUpdate() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();

    // update schema: add a new field
    grpcServer
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
                        .setTokenize(true)
                        .build())
                .build());

    // 2 docs addDocuments
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocsUpdated.csv");
    assertEquals(false, testAddDocs.error);
    assertEquals(true, testAddDocs.completed);
    List<String> RETRIEVE = Arrays.asList("doc_id", "new_text_field");

    Query query =
        Query.newBuilder()
            .setTermQuery(TermQuery.newBuilder().setField("new_text_field").setTextValue("updated"))
            .build();

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
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
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
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
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE, 0, false, "registerFieldsLatLon.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocsLatLon.csv");
    assertEquals(false, testAddDocs.error);
    assertEquals(true, testAddDocs.completed);
  }

  @Test
  public void testAddNoDocuments() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE, 0, false, "registerFieldsLatLon.json");

    testAddDocs.addDocumentsFromStream(Stream.empty());
    assertFalse(testAddDocs.error);
    assertTrue(testAddDocs.completed);
  }

  @Test
  public void testStats() throws IOException, InterruptedException {
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE);
    StatsResponse statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(0, statsResponse.getNumDocs());
    assertEquals(0, statsResponse.getMaxDoc());
    assertEquals(0, statsResponse.getOrd());
    assertEquals(0, statsResponse.getCurrentSearcher().getNumDocs());
    assertTrue(statsResponse.getDirSize() > 0);
    assertEquals("started", statsResponse.getState());
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    testAddDocs.addDocuments();
    statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());
    assertEquals(0, statsResponse.getOrd());
    assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
    assertEquals(1, statsResponse.getCurrentSearcher().getNumSegments());
    assertEquals(6610, statsResponse.getDirSize(), 1500);
    assertEquals("started", statsResponse.getState());
  }

  @Test
  public void testRefresh() throws IOException, InterruptedException {
    new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE).addDocuments();
    StatsResponse statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());
    assertEquals(0, statsResponse.getOrd());
    assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
    // check status on currentSearchAgain
    statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(2, statsResponse.getCurrentSearcher().getNumDocs());
  }

  @Test
  public void testDelete() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // add 2 docs
    testAddDocs.addDocuments();
    // check stats numDocs for 2 docs
    StatsResponse statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());

    // delete 1 doc
    AddDocumentRequest.Builder addDocumentRequestBuilder = AddDocumentRequest.newBuilder();
    addDocumentRequestBuilder.setIndexName("test_index");
    AddDocumentRequest.MultiValuedField.Builder multiValuedFieldsBuilder =
        AddDocumentRequest.MultiValuedField.newBuilder();
    addDocumentRequestBuilder.putFields("doc_id", multiValuedFieldsBuilder.addValue("1").build());
    AddDocumentResponse addDocumentResponse =
        grpcServer.getBlockingStub().delete(addDocumentRequestBuilder.build());
    assertFalse(addDocumentResponse.getPrimaryId().isEmpty());

    // manual refresh needed to depict changes in buffered deletes (i.e. not committed yet)
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    // check stats numDocs for 1 docs
    statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(1, statsResponse.getNumDocs());
    // note maxDoc stays 2 since it does not include delete documents
    assertEquals(2, statsResponse.getMaxDoc());
  }

  @Test
  public void testDeleteByQuery() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // add 2 docs
    testAddDocs.addDocuments();
    // check stats numDocs for 2 docs
    StatsResponse statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());

    Query query =
        Query.newBuilder()
            .setTermQuery(TermQuery.newBuilder().setField("count").setIntValue(7))
            .build();
    SearchRequest searchRequest =
        SearchRequest.newBuilder()
            .setIndexName(grpcServer.getTestIndex())
            .setStartHit(0)
            .setTopHits(10)
            .addAllRetrieveFields(RETRIEVED_VALUES)
            .setQuery(query)
            .build();
    SearchResponse searchResponse = grpcServer.getBlockingStub().search(searchRequest);
    assertEquals(searchResponse.getHitsCount(), 1);

    // delete 1 doc

    DeleteByQueryRequest deleteByQueryRequest =
        DeleteByQueryRequest.newBuilder().setIndexName("test_index").addQuery(query).build();
    AddDocumentResponse addDocumentResponse =
        grpcServer.getBlockingStub().deleteByQuery(deleteByQueryRequest);
    assertFalse(addDocumentResponse.getPrimaryId().isEmpty());

    // manual refresh needed to depict changes in buffered deletes (i.e. not committed yet)
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    // check stats numDocs for 1 docs
    statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(1, statsResponse.getNumDocs());
    // note maxDoc stays 2 since it does not include delete documents
    assertEquals(2, statsResponse.getMaxDoc());
    // deleted document does not show up in search response now
    searchResponse = grpcServer.getBlockingStub().search(searchRequest);
    assertEquals(searchResponse.getHitsCount(), 0);
  }

  @Test
  public void testDeleteAllDocuments() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // add 2 docs
    testAddDocs.addDocuments();
    // check stats numDocs for 2 docs
    StatsResponse statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());

    // deleteAll documents
    DeleteAllDocumentsRequest.Builder deleteAllDocumentsBuilder =
        DeleteAllDocumentsRequest.newBuilder();
    DeleteAllDocumentsRequest deleteAllDocumentsRequest =
        deleteAllDocumentsBuilder.setIndexName("test_index").build();
    grpcServer.getBlockingStub().deleteAll(deleteAllDocumentsRequest);

    // check stats numDocs for 1 docs
    statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(0, statsResponse.getNumDocs());
    assertEquals(0, statsResponse.getMaxDoc());
  }

  @Test
  public void testDeleteIndex() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // add 2 docs
    testAddDocs.addDocuments();
    // check stats numDocs for 2 docs
    StatsResponse statsResponse =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(2, statsResponse.getNumDocs());
    assertEquals(2, statsResponse.getMaxDoc());

    String indexName = "test_index";
    // deleteIndex
    DeleteIndexRequest deleteIndexRequest =
        DeleteIndexRequest.newBuilder().setIndexName(indexName).build();
    DeleteIndexResponse deleteIndexResponse =
        grpcServer.getBlockingStub().deleteIndex(deleteIndexRequest);

    Path indexRootDir = Paths.get(grpcServer.getIndexDir(), indexName);
    assertEquals(false, Files.exists(indexRootDir));

    assertEquals("ok", deleteIndexResponse.getOk());
  }

  @Test
  public void testSearchBasic() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
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
  public void testSearchFetchingAllFieldsWithWildcard() throws IOException, InterruptedException {
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsWildcardRetrieval.json");
    new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE)
        .addDocuments("addDocsWildcardRetrieval.csv");

    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponseWithWildcard =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
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
  public void testSearchLatLong() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE, 0, false, "registerFieldsLatLon.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocsLatLon.csv");
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
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
  public void testSearchIndexVirtualFields() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE, 0, false, "registerFieldsVirtual.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocs.csv");
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    List<String> queryFields = new ArrayList<>(RETRIEVED_VALUES);
    queryFields.addAll(INDEX_VIRTUAL_FIELDS);

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
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
  public void testSearchQueryVirtualFields() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE, 0, false, "registerFieldsBasic.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocs.csv");
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    List<String> queryFields = new ArrayList<>(RETRIEVED_VALUES);
    queryFields.addAll(QUERY_VIRTUAL_FIELDS);

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
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
  public void testSearchBothVirtualFields() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(Mode.STANDALONE, 0, false, "registerFieldsVirtual.json");
    AddDocumentResponse addDocumentResponse = testAddDocs.addDocuments("addDocs.csv");
    // manual refresh
    grpcServer
        .getBlockingStub()
        .refresh(RefreshRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    List<String> queryFields = new ArrayList<>(RETRIEVED_VALUES);
    queryFields.addAll(INDEX_VIRTUAL_FIELDS);
    queryFields.addAll(QUERY_VIRTUAL_FIELDS);

    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
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
  public void testBackupWarmingQueries() throws IOException, InterruptedException {
    GrpcServer.TestServer testServerReplica =
        new GrpcServer.TestServer(replicaGrpcServer, true, Mode.REPLICA);
    replicaGrpcServer
        .getGlobalState()
        .getIndex(replicaGrpcServer.getTestIndex())
        .initWarmer(archiver);
    assertNotNull(
        replicaGrpcServer.getGlobalState().getIndex(replicaGrpcServer.getTestIndex()).getWarmer());
    // Average case should pass
    replicaGrpcServer
        .getBlockingStub()
        .backupWarmingQueries(
            BackupWarmingQueriesRequest.newBuilder()
                .setIndex(replicaGrpcServer.getTestIndex())
                .setServiceName(TEST_SERVICE_NAME)
                .build());
    // Should fail; does not meet UptimeMinutesThreshold
    try {
      replicaGrpcServer
          .getBlockingStub()
          .backupWarmingQueries(
              BackupWarmingQueriesRequest.newBuilder()
                  .setIndex(replicaGrpcServer.getTestIndex())
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
      replicaGrpcServer
          .getBlockingStub()
          .backupWarmingQueries(
              BackupWarmingQueriesRequest.newBuilder()
                  .setIndex(replicaGrpcServer.getTestIndex())
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
    HttpBody response = grpcServer.getBlockingStub().metrics(Empty.newBuilder().build());
    HashSet expectedSampleNames =
        new HashSet(
            Arrays.asList(
                "grpc_server_started_total",
                "grpc_server_handled_total",
                "grpc_server_msg_received_total",
                "grpc_server_msg_sent_total",
                "grpc_server_handled_latency_seconds"));
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

  @Test
  public void testForceMerge() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    // add more documents to different segment
    testAddDocs.addDocuments();

    StatsResponse stats =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(4, stats.getNumDocs());
    assertEquals(2, stats.getCurrentSearcher().getNumSegments());

    ForceMergeResponse response =
        grpcServer
            .getBlockingStub()
            .forceMerge(
                ForceMergeRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setMaxNumSegments(1)
                    .setDoWait(true)
                    .build());
    assertEquals(ForceMergeResponse.Status.FORCE_MERGE_COMPLETED, response.getStatus());

    testAddDocs.refresh();

    stats =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());
    assertEquals(4, stats.getNumDocs());
    assertEquals(1, stats.getCurrentSearcher().getNumSegments());
  }

  @Test
  public void testReleaseSnapshotOnPrimary() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.PRIMARY);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    CommitRequest commitRequest =
        CommitRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build();
    grpcServer.getBlockingStub().commit(commitRequest);

    // create a snapshot
    CreateSnapshotRequest createSnapshotRequest =
        CreateSnapshotRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build();
    CreateSnapshotResponse createSnapshotResponse =
        grpcServer.getBlockingStub().createSnapshot(createSnapshotRequest);
    assertEquals(2, createSnapshotResponse.getSnapshotId().getIndexGen());
    assertEquals(-1, createSnapshotResponse.getSnapshotId().getStateGen());

    // add more documents and another commit to create another index gen
    testAddDocs.addDocuments();
    grpcServer.getBlockingStub().commit(commitRequest);

    // create another snapshot
    createSnapshotResponse = grpcServer.getBlockingStub().createSnapshot(createSnapshotRequest);
    assertEquals(3, createSnapshotResponse.getSnapshotId().getIndexGen());
    assertEquals(-1, createSnapshotResponse.getSnapshotId().getStateGen());

    // Release the first snapshot with index and state gen
    ReleaseSnapshotRequest releaseSnapshotRequest =
        ReleaseSnapshotRequest.newBuilder()
            .setIndexName(grpcServer.getTestIndex())
            .setSnapshotId(SnapshotId.newBuilder().setIndexGen(2).setStateGen(-1))
            .build();
    ReleaseSnapshotResponse releaseSnapshotResponse =
        grpcServer.getBlockingStub().releaseSnapshot(releaseSnapshotRequest);
    assertTrue(releaseSnapshotResponse.getSuccess());

    // Release the second snapshot's index gen and already released state gen
    releaseSnapshotRequest =
        ReleaseSnapshotRequest.newBuilder()
            .setIndexName(grpcServer.getTestIndex())
            .setSnapshotId(SnapshotId.newBuilder().setIndexGen(3).setStateGen(-1))
            .build();
    releaseSnapshotResponse = grpcServer.getBlockingStub().releaseSnapshot(releaseSnapshotRequest);
    assertTrue(releaseSnapshotResponse.getSuccess());

    // Verify both index gens released
    GetAllSnapshotGenRequest getAllSnapshotGenRequest =
        GetAllSnapshotGenRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build();
    GetAllSnapshotGenResponse getAllSnapshotGenResponse =
        grpcServer.getBlockingStub().getAllSnapshotIndexGen(getAllSnapshotGenRequest);
    assertEquals(0, getAllSnapshotGenResponse.getIndexGensCount());
  }

  @Test
  public void testGetAllSnapshotIndexGen() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs =
        new GrpcServer.TestServer(grpcServer, true, Mode.STANDALONE);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    CommitRequest commitRequest =
        CommitRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build();
    grpcServer.getBlockingStub().commit(commitRequest);

    // create a snapshot
    CreateSnapshotRequest createSnapshotRequest =
        CreateSnapshotRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build();
    CreateSnapshotResponse createSnapshotResponse =
        grpcServer.getBlockingStub().createSnapshot(createSnapshotRequest);
    assertEquals(2, createSnapshotResponse.getSnapshotId().getIndexGen());

    // add more documents and another commit to create another index gen
    testAddDocs.addDocuments();
    grpcServer.getBlockingStub().commit(commitRequest);

    // create another snapshot
    createSnapshotResponse = grpcServer.getBlockingStub().createSnapshot(createSnapshotRequest);
    assertEquals(3, createSnapshotResponse.getSnapshotId().getIndexGen());

    GetAllSnapshotGenRequest getAllSnapshotGenRequest =
        GetAllSnapshotGenRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build();
    GetAllSnapshotGenResponse getAllSnapshotGenResponse =
        grpcServer.getBlockingStub().getAllSnapshotIndexGen(getAllSnapshotGenRequest);

    assertThat(
        getAllSnapshotGenResponse.getIndexGensList(), IsCollectionContaining.hasItems(2L, 3L));
  }

  @Test
  public void testAddDocsHasPrimaryId() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.PRIMARY);
    // 2 docs addDocuments
    AddDocumentResponse response = testAddDocs.addDocuments();
    assertFalse(response.getPrimaryId().isEmpty());
  }

  @Test
  public void testCommitHasPrimaryId() throws IOException, InterruptedException {
    GrpcServer.TestServer testAddDocs = new GrpcServer.TestServer(grpcServer, true, Mode.PRIMARY);
    // 2 docs addDocuments
    testAddDocs.addDocuments();
    CommitRequest commitRequest =
        CommitRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build();
    CommitResponse response = grpcServer.getBlockingStub().commit(commitRequest);
    assertFalse(response.getPrimaryId().isEmpty());
  }

  @Test
  public void testReady() throws IOException {
    LuceneServerGrpc.LuceneServerBlockingStub blockingStub = grpcServer.getBlockingStub();
    String index1 = "index1";
    String index2 = "index2";
    String index3 = "index3";

    // Create all indices
    for (String indexName : List.of(index1, index2, index3)) {
      CreateIndexResponse createIndexResponse =
          blockingStub.createIndex(CreateIndexRequest.newBuilder().setIndexName(indexName).build());
      String expectedResponse =
          String.format("Created Index name: %s", indexName, grpcServer.getIndexDir());
      assertEquals(expectedResponse, createIndexResponse.getResponse());
    }

    // Start indices 2 and 3
    for (String indexName : List.of(index2, index3)) {
      StartIndexRequest startIndexRequest =
          StartIndexRequest.newBuilder().setIndexName(indexName).setMode(Mode.STANDALONE).build();
      StartIndexResponse startIndexResponse = blockingStub.startIndex(startIndexRequest);
      assertEquals(0, startIndexResponse.getNumDocs());
    }

    grpcServer.getGlobalState().getIndex(index3).getShard(0).writer.close();

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
    LuceneServerConfiguration configuration =
        new LuceneServerConfiguration(new ByteArrayInputStream(configStr.getBytes()));
    LuceneServerImpl.initQueryCache(configuration);
    assertNull(IndexSearcher.getDefaultQueryCache());

    configStr = String.join("\n", "queryCache:", "  enabled: true");
    configuration = new LuceneServerConfiguration(new ByteArrayInputStream(configStr.getBytes()));
    LuceneServerImpl.initQueryCache(configuration);
    queryCache = IndexSearcher.getDefaultQueryCache();
    assertTrue(queryCache instanceof NrtQueryCache);
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
  public void testCancellationDefaultDisabled() {
    assertFalse(DeadlineUtils.getCancellationEnabled());
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
