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

import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.FieldDefCreator;
import com.yelp.nrtsearch.server.field.FieldDefProvider;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.plugins.FieldTypePlugin;
import com.yelp.nrtsearch.server.plugins.Plugin;
import com.yelp.nrtsearch.test_utils.TestDocumentHelper;
import com.yelp.nrtsearch.test_utils.TestResourceHelper;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CustomFieldTypeTest {

  private static final String TEST_INDEX = "test_index";

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private TestServer server;

  @After
  public void tearDown() {
    TestServer.cleanupAll();
  }

  @Before
  public void setUp() throws IOException {
    server =
        TestServer.builder(folder)
            .withPlugins(Collections.singletonList(new TestFieldTypePlugin()))
            .build();
  }

  static class TestFieldDef extends IndexableFieldDef<Integer> {

    public TestFieldDef(
        String name, Field requestField, FieldDefCreator.FieldDefCreatorContext context) {
      super(name, requestField, context, Integer.class, null);
    }

    @Override
    protected DocValuesType parseDocValuesType(Field requestField) {
      return DocValuesType.NUMERIC;
    }

    @Override
    public LoadedDocValues<Integer> getDocValues(LeafReaderContext context) throws IOException {
      NumericDocValues numericDocValues = DocValues.getNumeric(context.reader(), getName());
      return new LoadedDocValues.SingleInteger(numericDocValues);
    }

    @Override
    public void parseDocumentField(
        Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
      int val = Integer.parseInt(fieldValues.get(0)) + 10;
      org.apache.lucene.document.Field field = new NumericDocValuesField(getName(), val);
      document.add(field);
    }

    @Override
    public String getType() {
      return "custom_field_type";
    }
  }

  static class TestFieldTypePlugin extends Plugin implements FieldTypePlugin {
    @Override
    public Map<String, FieldDefProvider<? extends FieldDef>> getFieldTypes() {
      Map<String, FieldDefProvider<? extends FieldDef>> typeMap = new HashMap<>();
      typeMap.put("custom_field_type", TestFieldDef::new);
      return typeMap;
    }
  }

  @Test
  public void testCustomFieldDef() throws Exception {
    LuceneServerGrpc.LuceneServerBlockingStub stub = server.getClient().getBlockingStub();
    stub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.startIndex(StartIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.registerFields(
        TestResourceHelper.getFieldsFromResourceFile("/registerFieldsCustomType.json").toBuilder()
            .setIndexName(TEST_INDEX)
            .build());
    TestDocumentHelper.addDocuments(
        server.getClient().getAsyncStub(),
        TestResourceHelper.getCsvDocumentStream(TEST_INDEX, "/addDocsCustomType.csv"));
    stub.refresh(RefreshRequest.newBuilder().setIndexName(TEST_INDEX).build());

    SearchResponse searchResponse =
        stub.search(
            SearchRequest.newBuilder()
                .setIndexName(TEST_INDEX)
                .addRetrieveFields("doc_id")
                .addRetrieveFields("int_field")
                .addRetrieveFields("custom_field")
                .setStartHit(0)
                .setTopHits(10)
                .setQuery(Query.newBuilder().build())
                .build());
    assertEquals(2, searchResponse.getHitsCount());
    assertEquals(
        "1", searchResponse.getHits(0).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        100,
        searchResponse.getHits(0).getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
    assertEquals(
        110,
        searchResponse.getHits(0).getFieldsOrThrow("custom_field").getFieldValue(0).getIntValue());
    assertEquals(
        "2", searchResponse.getHits(1).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue());
    assertEquals(
        1001,
        searchResponse.getHits(1).getFieldsOrThrow("int_field").getFieldValue(0).getIntValue());
    assertEquals(
        1011,
        searchResponse.getHits(1).getFieldsOrThrow("custom_field").getFieldValue(0).getIntValue());
  }

  @Test(expected = StatusRuntimeException.class)
  public void testNoTypeProperty() throws Exception {
    LuceneServerGrpc.LuceneServerBlockingStub stub = server.getClient().getBlockingStub();
    stub.createIndex(CreateIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.startIndex(StartIndexRequest.newBuilder().setIndexName(TEST_INDEX).build());
    stub.registerFields(
        TestResourceHelper.getFieldsFromResourceFile("/registerFieldsCustomTypeInvalid.json")
            .toBuilder()
            .setIndexName(TEST_INDEX)
            .build());
  }
}
