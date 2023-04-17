/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.grpc.Analyzer;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.MatchOperator;
import com.yelp.nrtsearch.server.grpc.MultiMatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiMatchQuery.MatchType;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.luceneserver.ServerTestCase;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class MatchCrossFieldsQueryTest extends ServerTestCase {
  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  protected List<String> getIndices() {
    return Collections.singletonList(DEFAULT_TEST_INDEX);
  }

  protected FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/query/registerFieldsCrossFields.json");
  }

  protected void initIndex(String name) throws Exception {
    addDocsFromResourceFile(name, "/search/query/addCrossFieldsDocs.csv");
  }

  @Test
  public void testOneFieldOneTerm() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .build());
    assertIds(response, 1, 6);
  }

  @Test
  public void testOneFieldMultiTerm() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .build());
    assertIds(response, 1, 2, 5, 6);
  }

  @Test
  public void testOneFieldBoost() {
    SearchResponse responseNotBoosted =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .build());
    SearchResponse responseBoosted =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .putFieldBoosts("field1", 2.0f)
                .build());
    assertEquals(2, responseBoosted.getHitsCount());
    assertEquals(2, responseNotBoosted.getHitsCount());
    assertEquals(
        responseBoosted.getHits(0).getScore(),
        responseNotBoosted.getHits(0).getScore() * 2.0f,
        0.00001);
    assertEquals(
        responseBoosted.getHits(1).getScore(),
        responseNotBoosted.getHits(1).getScore() * 2.0f,
        0.00001);
  }

  @Test
  public void testOneFieldMust() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .setOperator(MatchOperator.MUST)
                .build());
    assertIds(response, 1);
  }

  @Test
  public void testOneFieldMinShouldMatch() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .setOperator(MatchOperator.SHOULD)
                .setMinimumNumberShouldMatch(2)
                .build());
    assertIds(response, 1);
    response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .setOperator(MatchOperator.SHOULD)
                .setMinimumNumberShouldMatch(3)
                .build());
    assertIds(response, 1);
  }

  @Test
  public void testMultiFieldOneTerm() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .build());
    assertIds(response, 1, 2, 3, 6);
  }

  @Test
  public void testMultiFieldMultiTerm() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .build());
    assertIds(response, 1, 2, 3, 4, 5, 6);
  }

  @Test
  public void testMultiFieldMust() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .setOperator(MatchOperator.MUST)
                .build());
    assertIds(response, 1, 2, 3);

    response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .addFields("field3")
                .setOperator(MatchOperator.MUST)
                .build());
    assertIds(response, 1, 2, 3, 4, 6);
  }

  @Test
  public void testMultiFieldMinShouldMatch() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .setOperator(MatchOperator.SHOULD)
                .setMinimumNumberShouldMatch(2)
                .build());
    assertIds(response, 1, 2, 3);

    response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .addFields("field3")
                .setOperator(MatchOperator.SHOULD)
                .setMinimumNumberShouldMatch(2)
                .build());
    assertIds(response, 1, 2, 3, 4, 6);
  }

  @Test
  public void testPerFieldBoost() {
    SearchResponse responseNotBoosted =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term4")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .build());
    SearchResponse responseBoosted =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term4")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .putFieldBoosts("field1", 2.0f)
                .build());
    assertEquals(2, responseBoosted.getHitsCount());
    assertEquals(2, responseNotBoosted.getHitsCount());
    Map<String, Double> notBoostedScoreMap = new HashMap<>();
    Map<String, Double> boostedScoreMap = new HashMap<>();
    for (int i = 0; i < responseNotBoosted.getHitsCount(); ++i) {
      String key =
          responseNotBoosted.getHits(i).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
      notBoostedScoreMap.put(key, responseNotBoosted.getHits(i).getScore());
    }
    for (int i = 0; i < responseBoosted.getHitsCount(); ++i) {
      String key =
          responseBoosted.getHits(i).getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue();
      boostedScoreMap.put(key, responseBoosted.getHits(i).getScore());
    }
    assertEquals(boostedScoreMap.get("0"), notBoostedScoreMap.get("0"), 0.00001);
    assertEquals(boostedScoreMap.get("4"), notBoostedScoreMap.get("4") * 2.0f, 0.00001);
  }

  @Test
  public void testAtomFields() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1.atom")
                .addFields("field2.atom")
                .build());
    assertIds(response, 1);

    response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1.atom")
                .addFields("field2.atom")
                .build());
    assertIds(response, 2, 6);
  }

  @Test
  public void testSetAnalyzer() {
    Analyzer analyzer = Analyzer.newBuilder().setPredefined("core.Keyword").build();
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term1 term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .setAnalyzer(analyzer)
                .build());
    assertIds(response);

    response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("term2")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .setAnalyzer(analyzer)
                .build());
    assertIds(response, 1, 2, 3, 6);
  }

  @Test
  public void testFieldNotExist() {
    try {
      doQuery(
          MultiMatchQuery.newBuilder()
              .setQuery("term2")
              .setType(MatchType.CROSS_FIELDS)
              .addFields("unknown")
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(
          e.getMessage()
              .contains("field \"unknown\" is unknown: it was not registered with registerField"));
    }
  }

  @Test
  public void testFieldNotAnalyzed() {
    try {
      doQuery(
          MultiMatchQuery.newBuilder()
              .setQuery("term2")
              .setType(MatchType.CROSS_FIELDS)
              .addFields("not_analyzed")
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Field must be analyzable: not_analyzed"));
    }
  }

  @Test
  public void testFieldNotSearchable() {
    try {
      doQuery(
          MultiMatchQuery.newBuilder()
              .setQuery("term2")
              .setType(MatchType.CROSS_FIELDS)
              .addFields("field1.not_searchable")
              .build());
      fail();
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("Field must be searchable: field1.not_searchable"));
    }
  }

  @Test
  public void testNoAnalyzedTokens() {
    SearchResponse response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .build());
    assertIds(response);

    response =
        doQuery(
            MultiMatchQuery.newBuilder()
                .setQuery("")
                .setType(MatchType.CROSS_FIELDS)
                .addFields("field1")
                .addFields("field2")
                .build());
    assertIds(response);
  }

  @Test
  public void testNoFields() {
    try {
      MatchCrossFieldsQuery.build(
          "test",
          Collections.emptyList(),
          Collections.emptyMap(),
          MatchOperator.SHOULD,
          0,
          0.0f,
          null);
    } catch (IllegalArgumentException e) {
      assertEquals("No fields specified", e.getMessage());
    }
  }

  private SearchResponse doQuery(MultiMatchQuery multiMatchQuery) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(DEFAULT_TEST_INDEX)
                .setTopHits(10)
                .addRetrieveFields("doc_id")
                .setQuery(Query.newBuilder().setMultiMatchQuery(multiMatchQuery).build())
                .build());
  }

  private void assertIds(SearchResponse response, int... ids) {
    Set<Integer> uniqueIds = new HashSet<>();
    for (int id : ids) {
      uniqueIds.add(id);
    }
    assertEquals(uniqueIds.size(), response.getHitsCount());

    Set<Integer> responseIds = new HashSet<>();
    for (Hit hit : response.getHitsList()) {
      responseIds.add(
          Integer.parseInt(hit.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue()));
    }
    assertEquals(uniqueIds, responseIds);
  }
}
