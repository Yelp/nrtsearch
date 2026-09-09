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

import com.yelp.nrtsearch.test_utils.TestDocumentHelper;
import com.yelp.nrtsearch.test_utils.TestResourceHelper;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NrtsearchServerIdFieldTest {

  private static final String TEST_INDEX = "test_index";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private TestServer server;

  @After
  public void tearDown() {
    TestServer.cleanupAll();
  }

  @Before
  public void setUp() throws IOException {
    server = TestServer.builder(folder).build();
  }

  @Test
  public void testAddUpdateAndSearch() throws Exception {
    LuceneServerGrpc.LuceneServerBlockingStub stub = server.getClient().getBlockingStub();
    stub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.startIndex(StartIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.registerFields(
        TestResourceHelper.getFieldsFromResourceFile("/registerFieldsBasicWithId.json").toBuilder()
            .setIndexName(TEST_INDEX)
            .build());

    // 2 docs addDocuments
    TestDocumentHelper.addDocuments(
        server.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    stub.refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    // add 2 more docs
    TestDocumentHelper.addDocuments(
        server.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    stub.refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    StatsResponse stats = stub.stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    // there are only 2 documents (ID field deduplicates)
    assertEquals(2, stats.getNumDocs());

    // update schema: add a new field
    stub.updateFields(
        FieldDefRequest.newBuilder()
            .setIndexName(TEST_INDEX)
            .addField(
                Field.newBuilder()
                    .setName("new_text_field")
                    .setType(FieldType.TEXT)
                    .setStoreDocValues(true)
                    .setSearch(true)
                    .setMultiValued(true)
                    .build())
            .build());

    // 2 docs addDocuments with updated schema
    TestDocumentHelper.addDocuments(
        server.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocsUpdated.csv"));
    stub.refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    stats = stub.stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    // there are 4 documents in total
    assertEquals(4, stats.getNumDocs());

    // Query for first document
    SearchResponse searchResponse =
        stub.search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
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
        stub.search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
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
        stub.search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
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
  public void testDuplicateIdFieldInIndexState() throws Exception {
    LuceneServerGrpc.LuceneServerBlockingStub stub = server.getClient().getBlockingStub();
    stub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.startIndex(StartIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.registerFields(
        TestResourceHelper.getFieldsFromResourceFile("/registerFieldsBasicWithId.json").toBuilder()
            .setIndexName(TEST_INDEX)
            .build());

    // 2 docs addDocuments
    TestDocumentHelper.addDocuments(
        server.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    stub.refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    // add 2 more docs
    TestDocumentHelper.addDocuments(
        server.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocs.csv"));
    stub.refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    StatsResponse stats = stub.stats(StatsRequest.newBuilder().setIndexName(TEST_INDEX).build());
    // there are only 2 documents (ID field deduplicates)
    assertEquals(2, stats.getNumDocs());

    try {
      // update schema: add a new field with _ID type (duplicate)
      stub.updateFields(
          FieldDefRequest.newBuilder()
              .setIndexName(TEST_INDEX)
              .addField(
                  Field.newBuilder()
                      .setName("new_text_field")
                      .setType(FieldType._ID)
                      .setStore(true)
                      .build())
              .build());
    } catch (RuntimeException e) {
      String message =
          "INTERNAL: Error handling updateFields request\n"
              + "Index can only register one id field, found: doc_id and new_text_field";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test(expected = StatusRuntimeException.class)
  public void testMultiValued() throws Exception {
    try {
      registerFields(List.of(getFieldBuilder("doc_id", true, true, true)));
    } catch (RuntimeException e) {
      String message =
          "INTERNAL: Error handling registerFields request\n"
              + "field: doc_id cannot have multivalued fields as it's an _ID field";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test(expected = StatusRuntimeException.class)
  public void testStoreAndDocValuesFalse() throws IOException {
    try {
      registerFields(List.of(getFieldBuilder("doc_id", false, false, false)));
    } catch (RuntimeException e) {
      String message =
          "INTERNAL: Error handling registerFields request\n"
              + "field: doc_id is an _ID field and should be retrievable by either store=true or storeDocValues=true";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  @Test
  public void testStoreTrueAndDocValuesFalse() throws IOException {
    registerFields(List.of(getFieldBuilder("doc_id", false, true, false)));
  }

  @Test
  public void testStoreFalseAndDocValuesTrue() throws IOException {
    registerFields(List.of(getFieldBuilder("doc_id", true, false, false)));
  }

  @Test(expected = StatusRuntimeException.class)
  public void testMultipleDocIds() throws Exception {
    try {
      registerFields(
          List.of(
              getFieldBuilder("doc_id", true, true, false),
              getFieldBuilder("doc_id_2", true, true, false)));
    } catch (RuntimeException e) {
      String message =
          "INTERNAL: Error handling registerFields request\n"
              + "Index can only register one id field, found: doc_id and doc_id_2";
      assertEquals(message, e.getMessage());
      throw e;
    }
  }

  private Field getFieldBuilder(
      String fieldName, boolean storeDocValues, boolean store, boolean multiValued) {
    return Field.newBuilder()
        .setName(fieldName)
        .setStoreDocValues(storeDocValues)
        .setStore(store)
        .setType(FieldType._ID)
        .setMultiValued(multiValued)
        .build();
  }

  private void registerFields(List<Field> fields) throws IOException {
    LuceneServerGrpc.LuceneServerBlockingStub stub = server.getClient().getBlockingStub();
    stub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    FieldDefRequest.Builder builder = FieldDefRequest.newBuilder().setIndexName(TEST_INDEX);
    for (Field field : fields) {
      builder.addField(field);
    }
    stub.registerFields(builder.build());
  }
}
