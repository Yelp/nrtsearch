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

import static com.yelp.nrtsearch.server.luceneserver.search.collectors.MyTopSuggestDocsCollector.SUGGEST_KEY_FIELD_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.Analyzer;
import com.yelp.nrtsearch.server.grpc.CompletionQuery;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.ClassRule;
import org.junit.Test;

public class ContextSuggestFieldDefTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private static final String SINGLE_VALUED_FIELD_NAME = "context_suggest_name";
  private static final String MULTI_VALUED_FIELD_NAME = "context_suggest_name_multi_valued";
  private static final String FIELD_TYPE = "CONTEXT_SUGGEST";

  public FieldDef getFieldDef(String testIndex, String fieldName) throws IOException {
    return getGrpcServer().getGlobalState().getIndex(testIndex).getField(fieldName);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/field/registerFieldsContextSuggest.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    this.addDocsFromJsonResourceFile(name, "/addContextSuggestDocs.jsonl");
  }

  @Test
  public void validSearchAndIndexAnalyzerWhenFieldAnalyzerIsProvided() {
    Analyzer standardAnalyzer = Analyzer.newBuilder().setPredefined("standard").build();
    Analyzer analyzer = Analyzer.newBuilder().setPredefined("classic").build();
    Field field =
        Field.newBuilder()
            .setSearchAnalyzer(standardAnalyzer) // should be ignored as analyzer takes precedence
            .setIndexAnalyzer(standardAnalyzer) // should be ignored as analyzer takes precedence
            .setAnalyzer(analyzer)
            .build();
    ContextSuggestFieldDef contextSuggestFieldDef = new ContextSuggestFieldDef("test_field", field);
    assertEquals(
        ClassicAnalyzer.class, contextSuggestFieldDef.getSearchAnalyzer().get().getClass());
    assertEquals(ClassicAnalyzer.class, contextSuggestFieldDef.getIndexAnalyzer().get().getClass());
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
    FieldDef contextSuggestFieldDef = getFieldDef(DEFAULT_TEST_INDEX, SINGLE_VALUED_FIELD_NAME);
    assertEquals(FIELD_TYPE, contextSuggestFieldDef.getType());
    assertEquals(SINGLE_VALUED_FIELD_NAME, contextSuggestFieldDef.getName());
  }

  @Test
  public void validCompletionQuerySuggestionsSingleValued_FilterByText() {
    Query query =
        Query.newBuilder()
            .setCompletionQuery(
                CompletionQuery.newBuilder()
                    .setField(SINGLE_VALUED_FIELD_NAME)
                    .setText("Tasty")
                    .build())
            .build();

    SearchResponse searchResponse =
        getGrpcServer().getBlockingStub().search(getRequestWithQuery(query));

    assertEquals(1, searchResponse.getHitsCount());

    Set<String> hitsIds = getHitsIds(searchResponse);
    assertTrue(hitsIds.contains("3"));
    String suggestKeyValueFromHit =
        searchResponse
            .getHitsList()
            .get(0)
            .getFieldsMap()
            .get(SUGGEST_KEY_FIELD_NAME)
            .getFieldValue(0)
            .getTextValue();
    assertEquals("Tasty Burger", suggestKeyValueFromHit);
  }

  @Test
  public void validCompletionQuerySuggestionsSingleValued_FilterByTextAndContext() {
    Query query =
        Query.newBuilder()
            .setCompletionQuery(
                CompletionQuery.newBuilder()
                    .setField(SINGLE_VALUED_FIELD_NAME)
                    .setText("Fantastic")
                    .addContexts("a")
                    .build())
            .build();

    SearchResponse searchResponse =
        getGrpcServer().getBlockingStub().search(getRequestWithQuery(query));

    assertEquals(1, searchResponse.getHitsCount());

    Set<String> hitsIds = getHitsIds(searchResponse);
    assertTrue(hitsIds.contains("5"));
    String suggestKeyValueFromHit =
        searchResponse
            .getHitsList()
            .get(0)
            .getFieldsMap()
            .get(SUGGEST_KEY_FIELD_NAME)
            .getFieldValue(0)
            .getTextValue();
    assertEquals("Fantastic Fries", suggestKeyValueFromHit);
  }

  @Test
  public void validCompletionQuerySuggestionsMultiValued_FilterByText() {
    Query query =
        Query.newBuilder()
            .setCompletionQuery(
                CompletionQuery.newBuilder()
                    .setField(MULTI_VALUED_FIELD_NAME)
                    .setText("Fries")
                    .build())
            .build();

    SearchResponse searchResponse =
        getGrpcServer().getBlockingStub().search(getRequestWithQuery(query));

    assertEquals(2, searchResponse.getHitsCount());

    Set<String> hitsIds = getHitsIds(searchResponse);
    assertTrue(hitsIds.contains("4"));
    assertTrue(hitsIds.contains("5"));
  }

  @Test
  public void validCompletionQuerySuggestionsMultiValued_FilterByTextAndContext() {
    Query query =
        Query.newBuilder()
            .setCompletionQuery(
                CompletionQuery.newBuilder()
                    .setField(MULTI_VALUED_FIELD_NAME)
                    .setText("Burger")
                    .addContexts("b")
                    .build())
            .build();

    SearchResponse searchResponse =
        getGrpcServer().getBlockingStub().search(getRequestWithQuery(query));

    assertEquals(1, searchResponse.getHitsCount());
    String suggestKeyValueFromHit =
        searchResponse
            .getHitsList()
            .get(0)
            .getFieldsMap()
            .get(SUGGEST_KEY_FIELD_NAME)
            .getFieldValue(0)
            .getTextValue();
    assertEquals("Burger Fries", suggestKeyValueFromHit);

    Set<String> hitsIds = getHitsIds(searchResponse);
    assertTrue(hitsIds.contains("4"));
  }

  private Set<String> getHitsIds(SearchResponse searchResponse) {
    return searchResponse.getHitsList().stream()
        .map(hit -> hit.getFieldsMap().get("id").getFieldValue(0).getTextValue())
        .collect(Collectors.toSet());
  }

  private SearchRequest getRequestWithQuery(Query query) {
    return SearchRequest.newBuilder()
        .setIndexName(DEFAULT_TEST_INDEX)
        .addAllRetrieveFields(Set.of("id", SINGLE_VALUED_FIELD_NAME, MULTI_VALUED_FIELD_NAME))
        .setStartHit(0)
        .setTopHits(5)
        .setQuery(query)
        .build();
  }
}
