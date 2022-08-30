/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Analyzer;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.suggest.document.CompletionAnalyzer;
import org.junit.ClassRule;
import org.junit.Test;

public class ContextSuggestFieldDefTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private static Gson gson = new Gson();
  private static final String FIELD_NAME = "context_suggest_name";
  private static final String FIELD_TYPE = "CONTEXT_SUGGEST";
  private static final List<String> CONTEXT_SUGGEST_VALUES =
      List.of(
          gson.toJson(
              Map.of(
                  "value", "test one", "contexts", List.of("context1", "context2"), "weight", 123)),
          gson.toJson(
              Map.of(
                  "value",
                  "test two",
                  "contexts",
                  List.of("context1", "context2"),
                  "weight",
                  123)));

  private Map<String, MultiValuedField> getFieldsMapForOneDocument(String value) {
    Map<String, AddDocumentRequest.MultiValuedField> fieldsMap =
        Map.of(
            FIELD_NAME, AddDocumentRequest.MultiValuedField.newBuilder().addValue(value).build());
    return fieldsMap;
  }

  private List<AddDocumentRequest> buildDocuments(String indexName, List<String> csfValues) {
    List<AddDocumentRequest> documentRequests = new ArrayList<>();
    for (String value : csfValues) {
      documentRequests.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(indexName)
              .putAllFields(getFieldsMapForOneDocument(value))
              .build());
    }
    return documentRequests;
  }

  public FieldDef getFieldDef(String testIndex, String fieldName) throws IOException {
    return getGrpcServer().getGlobalState().getIndex(testIndex).getField(fieldName);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsContextSuggest.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    List<AddDocumentRequest> documents = buildDocuments(name, CONTEXT_SUGGEST_VALUES);
    addDocuments(documents.stream());
  }

  @Test
  public void validSearchAndIndexAnalyzerWhenFieldAnalyzerIsProvided() {
    Analyzer standardAnalyzer = Analyzer.newBuilder().setPredefined("standard").build();
    Analyzer analyzer = Analyzer.newBuilder().setPredefined("simple").build();
    Field field =
        Field.newBuilder()
            .setSearchAnalyzer(standardAnalyzer) // should be ignored as analyzer takes precedence
            .setIndexAnalyzer(standardAnalyzer) // should be ignored as analyzer takes precedence
            .setAnalyzer(analyzer)
            .build();
    ContextSuggestFieldDef contextSuggestFieldDef = new ContextSuggestFieldDef("test_field", field);
    assertEquals(
        CompletionAnalyzer.class, contextSuggestFieldDef.getSearchAnalyzer().get().getClass());
    assertEquals(
        CompletionAnalyzer.class, contextSuggestFieldDef.getIndexAnalyzer().get().getClass());
  }

  @Test
  public void validSearchAndIndexAnalyzerWhenSearchAndIndexAnalyzersAreProvided() {
    Analyzer searchAnalyzer = Analyzer.newBuilder().setPredefined("bg.Bulgarian").build();
    Analyzer indexAnalyzer = Analyzer.newBuilder().setPredefined("en.English").build();
    Field field =
        Field.newBuilder()
            .setSearchAnalyzer(searchAnalyzer)
            .setIndexAnalyzer(indexAnalyzer)
            .build();
    ContextSuggestFieldDef contextSuggestFieldDef = new ContextSuggestFieldDef("test_field", field);
    assertSame(
        BulgarianAnalyzer.class, contextSuggestFieldDef.getSearchAnalyzer().get().getClass());
    assertSame(EnglishAnalyzer.class, contextSuggestFieldDef.getIndexAnalyzer().get().getClass());
  }

  @Test
  public void validDefaultSearchAndIndexAnalyzerNoAnalyzersAreProvided() {
    Field field = Field.newBuilder().build();
    ContextSuggestFieldDef contextSuggestFieldDef = new ContextSuggestFieldDef("test_field", field);
    assertSame(StandardAnalyzer.class, contextSuggestFieldDef.getSearchAnalyzer().get().getClass());
    assertSame(StandardAnalyzer.class, contextSuggestFieldDef.getIndexAnalyzer().get().getClass());
  }

  @Test
  public void validContextSuggestFieldDefTest() throws IOException {
    FieldDef contextSuggestFieldDef = getFieldDef(DEFAULT_TEST_INDEX, FIELD_NAME);
    assertEquals(FIELD_TYPE, contextSuggestFieldDef.getType());
    assertEquals(FIELD_NAME, contextSuggestFieldDef.getName());

    String expectedCSF1 = CONTEXT_SUGGEST_VALUES.get(0);

    // TODO: Test this suggest field using the suggest api instead of the search api
    SearchResponse searchResponse =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(DEFAULT_TEST_INDEX)
                    .addRetrieveFields(FIELD_NAME)
                    .setStartHit(0)
                    .setTopHits(10)
                    .setQuery(Query.newBuilder().build())
                    .build());

    String fieldValue1 =
        searchResponse.getHits(0).getFieldsOrThrow(FIELD_NAME).getFieldValue(0).getTextValue();

    assertEquals(expectedCSF1, fieldValue1);
  }
}
