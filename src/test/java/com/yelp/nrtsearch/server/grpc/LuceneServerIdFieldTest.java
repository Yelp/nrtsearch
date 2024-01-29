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
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.LuceneServerTestConfigurationFactory;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LuceneServerIdFieldTest {

  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Rule public ExpectedException thrown = ExpectedException.none();

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
    CollectorRegistry collectorRegistry = new CollectorRegistry();
    grpcServer = setUpGrpcServer(collectorRegistry);
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
        null,
        Collections.emptyList());
  }

  @Test
  public void testAddUpdateAndSearch() throws IOException, InterruptedException {
    GrpcServer.TestServer testServer =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsBasicWithId.json");
    // 2 docs addDocuments
    testServer.addDocuments();
    assertFalse(testServer.error);
    assertTrue(testServer.completed);

    // add 2 more docs
    testServer.addDocuments();
    assertFalse(testServer.error);
    assertTrue(testServer.completed);

    StatsResponse stats =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    // there are only 2 documents
    assertEquals(2, stats.getNumDocs());

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
    testServer.addDocuments("addDocsUpdated.csv");
    assertFalse(testServer.error);
    assertTrue(testServer.completed);
    stats =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    // there are 4 documents in total
    assertEquals(4, stats.getNumDocs());

    // Query for first document
    SearchResponse searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(List.of("doc_id", "vendor_name"))
                    .setQuery(
                        Query.newBuilder()
                            .setPhraseQuery(
                                PhraseQuery.newBuilder()
                                    .setField("vendor_name")
                                    .addTerms("first")
                                    .addTerms("vendor")
                                    .build())
                            .build())
                    .build());
    assertEquals(1, searchResponse.getTotalHits().getValue());
    assertEquals(1, searchResponse.getHitsList().size());
    assertEquals(
        "1",
        searchResponse.getHits(0).getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());

    // Term Query for first document
    searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(List.of("doc_id", "vendor_name"))
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("doc_id").setTextValue("1").build())
                            .build())
                    .build());
    assertEquals(1, searchResponse.getTotalHits().getValue());
    assertEquals(1, searchResponse.getHitsList().size());
    assertEquals(
        "1",
        searchResponse.getHits(0).getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());

    // TermInSetQuery for first and third document
    searchResponse =
        grpcServer
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(grpcServer.getTestIndex())
                    .setStartHit(0)
                    .setTopHits(10)
                    .addAllRetrieveFields(List.of("doc_id", "vendor_name"))
                    .setQuery(
                        Query.newBuilder()
                            .setTermInSetQuery(
                                TermInSetQuery.newBuilder()
                                    .setField("doc_id")
                                    .setTextTerms(
                                        TermInSetQuery.TextTerms.newBuilder()
                                            .addAllTerms(List.of("1", "3"))
                                            .build())
                                    .build())
                            .build())
                    .build());
    assertEquals(2, searchResponse.getTotalHits().getValue());
    assertEquals(2, searchResponse.getHitsList().size());
    assertEquals(
        "1",
        searchResponse.getHits(0).getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        "3",
        searchResponse.getHits(1).getFieldsMap().get("doc_id").getFieldValue(0).getTextValue());
  }

  @Test(expected = StatusRuntimeException.class)
  public void testDuplicateIdFieldInIndexState() throws IOException, InterruptedException {
    GrpcServer.TestServer testServer =
        new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    new GrpcServer.IndexAndRoleManager(grpcServer)
        .createStartIndexAndRegisterFields(
            Mode.STANDALONE, 0, false, "registerFieldsBasicWithId.json");
    // 2 docs addDocuments
    testServer.addDocuments();
    assertFalse(testServer.error);
    assertTrue(testServer.completed);

    // add 2 more docs
    testServer.addDocuments();
    assertFalse(testServer.error);
    assertTrue(testServer.completed);

    StatsResponse stats =
        grpcServer
            .getBlockingStub()
            .stats(StatsRequest.newBuilder().setIndexName(grpcServer.getTestIndex()).build());

    // there are only 2 documents
    assertEquals(2, stats.getNumDocs());

    try {
      // update schema: add a new field
      grpcServer
          .getBlockingStub()
          .updateFields(
              FieldDefRequest.newBuilder()
                  .setIndexName("test_index")
                  .addField(
                      Field.newBuilder()
                          .setName("new_text_field")
                          .setType(FieldType._ID)
                          .setStore(true)
                          .build())
                  .build());
    } catch (RuntimeException e) {
      String message =
          "INVALID_ARGUMENT: error while trying to UpdateFieldsHandler for index: test_index\n"
              + "Index can only register one id field, found: doc_id and new_text_field";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test(expected = StatusRuntimeException.class)
  public void testMultiValued() throws Exception {
    try {
      registerFields(List.of(getFieldBuilder("doc_id", true, true, true, false)));
    } catch (RuntimeException e) {
      String message =
          "INVALID_ARGUMENT: error while trying to RegisterFields for index: test_index\n"
              + "field: doc_id cannot have multivalued fields or tokenization as it's an _ID field";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test(expected = StatusRuntimeException.class)
  public void testTokenized() throws Exception {
    try {
      registerFields(List.of(getFieldBuilder("doc_id", true, true, false, true)));
    } catch (RuntimeException e) {
      String message =
          "INVALID_ARGUMENT: error while trying to RegisterFields for index: test_index\n"
              + "field: doc_id cannot have multivalued fields or tokenization as it's an _ID field";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test(expected = StatusRuntimeException.class)
  public void testStoreAndDocValuesFalse() throws IOException {
    try {
      registerFields(List.of(getFieldBuilder("doc_id", false, false, false, false)));
    } catch (RuntimeException e) {
      String message =
          "INVALID_ARGUMENT: error while trying to RegisterFields for index: test_index\n"
              + "field: doc_id is an _ID field and should be retrievable by either store=true or storeDocValues=true";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test
  public void testStoreTrueAndDocValuesFalse() throws IOException {
    registerFields(List.of(getFieldBuilder("doc_id", false, true, false, false)));
  }

  @Test
  public void testStoreFalseAndDocValuesTrue() throws IOException {
    registerFields(List.of(getFieldBuilder("doc_id", true, false, false, false)));
  }

  @Test(expected = StatusRuntimeException.class)
  public void testMultipleDocIds() throws Exception {
    try {
      registerFields(
          List.of(
              getFieldBuilder("doc_id", true, true, false, false),
              getFieldBuilder("doc_id_2", true, true, false, false)));
    } catch (RuntimeException e) {
      String message =
          "INVALID_ARGUMENT: error while trying to RegisterFields for index: test_index\n"
              + "Index can only register one id field, found: doc_id and doc_id_2";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  private Field getFieldBuilder(
      String fieldName,
      boolean storeDocValues,
      boolean store,
      boolean multiValued,
      boolean tokenized) {
    return Field.newBuilder()
        .setName(fieldName)
        .setStoreDocValues(storeDocValues)
        .setStore(store)
        .setType(FieldType._ID)
        .setMultiValued(multiValued)
        .setTokenize(tokenized)
        .build();
  }

  private void registerFields(List<Field> fields) throws IOException {
    new GrpcServer.TestServer(grpcServer, false, Mode.STANDALONE);
    String indexName = grpcServer.getTestIndex();
    grpcServer
        .getBlockingStub()
        .createIndex(CreateIndexRequest.newBuilder().setIndexName(indexName).build());
    FieldDefRequest.Builder builder = FieldDefRequest.newBuilder().setIndexName(indexName);
    for (Field field : fields) {
      builder.addField(field);
    }
    grpcServer.getBlockingStub().registerFields(builder.build());
  }
}
