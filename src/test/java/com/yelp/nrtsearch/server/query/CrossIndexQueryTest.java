/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.*;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Tests for {@link CrossIndexQuery} support in {@link QueryNodeMapper}. Sets up two indices:
 * primary_index (biz_id_primary, name, rating) and secondary_index (biz_id_secondary, time_slot,
 * covers). Tests join filtering, empty matches, score modes, boolean composition, and error cases.
 */
public class CrossIndexQueryTest extends ServerTestCase {
  private static final String PRIMARY_INDEX = "primary_index";
  private static final String SECONDARY_INDEX = "secondary_index";

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public List<String> getIndices() {
    return Arrays.asList(PRIMARY_INDEX, SECONDARY_INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    if (name.equals(PRIMARY_INDEX)) {
      return FieldDefRequest.newBuilder(
              getFieldsFromResourceFile("/registerFieldsCrossIndexPrimary.json"))
          .setIndexName(name)
          .build();
    } else {
      return FieldDefRequest.newBuilder(
              getFieldsFromResourceFile("/registerFieldsCrossIndexSecondary.json"))
          .setIndexName(name)
          .build();
    }
  }

  @Override
  public void initIndex(String name) throws Exception {
    if (name.equals(PRIMARY_INDEX)) {
      initPrimaryIndex();
    } else if (name.equals(SECONDARY_INDEX)) {
      initSecondaryIndex();
    }
  }

  /**
   * Primary index: 5 businesses (biz_1 through biz_5).
   *
   * <ul>
   *   <li>biz_1: "Pizza Palace", rating=5
   *   <li>biz_2: "Sushi Spot", rating=4
   *   <li>biz_3: "Taco Town", rating=3
   *   <li>biz_4: "Burger Bar", rating=2
   *   <li>biz_5: "Noodle Nook", rating=1
   * </ul>
   */
  private void initPrimaryIndex() throws Exception {
    String[][] docs = {
      {"biz_1", "Pizza Palace", "5"},
      {"biz_2", "Sushi Spot", "4"},
      {"biz_3", "Taco Town", "3"},
      {"biz_4", "Burger Bar", "2"},
      {"biz_5", "Noodle Nook", "1"},
    };
    for (String[] doc : docs) {
      addDocuments(
          java.util.stream.Stream.of(
              AddDocumentRequest.newBuilder()
                  .setIndexName(PRIMARY_INDEX)
                  .putFields(
                      "biz_id_primary",
                      AddDocumentRequest.MultiValuedField.newBuilder().addValue(doc[0]).build())
                  .putFields(
                      "name",
                      AddDocumentRequest.MultiValuedField.newBuilder().addValue(doc[1]).build())
                  .putFields(
                      "rating",
                      AddDocumentRequest.MultiValuedField.newBuilder().addValue(doc[2]).build())
                  .build()));
    }
  }

  /**
   * Secondary index: reservations for some businesses.
   *
   * <ul>
   *   <li>biz_1: time_slot=1200, covers=2
   *   <li>biz_1: time_slot=1800, covers=4
   *   <li>biz_2: time_slot=1300, covers=2
   *   <li>biz_3: time_slot=1900, covers=6
   * </ul>
   *
   * Note: biz_4 and biz_5 have NO reservations.
   */
  private void initSecondaryIndex() throws Exception {
    String[][] docs = {
      {"biz_1", "1200", "2"},
      {"biz_1", "1800", "4"},
      {"biz_2", "1300", "2"},
      {"biz_3", "1900", "6"},
    };
    for (String[] doc : docs) {
      addDocuments(
          java.util.stream.Stream.of(
              AddDocumentRequest.newBuilder()
                  .setIndexName(SECONDARY_INDEX)
                  .putFields(
                      "biz_id_secondary",
                      AddDocumentRequest.MultiValuedField.newBuilder().addValue(doc[0]).build())
                  .putFields(
                      "time_slot",
                      AddDocumentRequest.MultiValuedField.newBuilder().addValue(doc[1]).build())
                  .putFields(
                      "covers",
                      AddDocumentRequest.MultiValuedField.newBuilder().addValue(doc[2]).build())
                  .build()));
    }
  }

  /** Basic join: match all secondary docs → only businesses with reservations returned. */
  @Test
  public void testBasicJoinFilter() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(
                                        Query.newBuilder()
                                            .setMatchAllQuery(MatchAllQuery.newBuilder()))
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_NONE))
                            .build())
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    // Only biz_1, biz_2, biz_3 have reservations in the secondary index
    assertEquals(Set.of("biz_1", "biz_2", "biz_3"), returnedBizIds);
  }

  /** Inner query with filter: only reservations with covers >= 4. */
  @Test
  public void testJoinWithInnerFilter() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(
                                        Query.newBuilder()
                                            .setRangeQuery(
                                                RangeQuery.newBuilder()
                                                    .setField("covers")
                                                    .setLower("4")
                                                    .setUpper("100")))
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_NONE))
                            .build())
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    // biz_1 has covers=4, biz_3 has covers=6
    assertEquals(Set.of("biz_1", "biz_3"), returnedBizIds);
  }

  /** Inner query matches nothing → primary returns no results. */
  @Test
  public void testEmptySecondaryMatch() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(
                                        Query.newBuilder()
                                            .setTermQuery(
                                                TermQuery.newBuilder()
                                                    .setField("biz_id_secondary")
                                                    .setTextValue("nonexistent")))
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_NONE))
                            .build())
                    .build());

    assertEquals(0, response.getHitsCount());
  }

  /** Non-existent secondary index → error. */
  @Test
  public void testInvalidSecondaryIndex() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(PRIMARY_INDEX)
                  .setTopHits(10)
                  .setQuery(
                      Query.newBuilder()
                          .setCrossIndexQuery(
                              CrossIndexQuery.newBuilder()
                                  .setIndex("no_such_index")
                                  .setSecondaryField("biz_id_secondary")
                                  .setPrimaryField("biz_id_primary")
                                  .setQuery(
                                      Query.newBuilder()
                                          .setMatchAllQuery(MatchAllQuery.newBuilder()))
                                  .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_NONE))
                          .build())
                  .build());
      fail("Expected exception for non-existent secondary index");
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains("not found"));
    }
  }

  /** secondary_field does not exist in secondary index → error. */
  @Test
  public void testNonExistentSecondaryField() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(PRIMARY_INDEX)
                  .setTopHits(10)
                  .setQuery(
                      Query.newBuilder()
                          .setCrossIndexQuery(
                              CrossIndexQuery.newBuilder()
                                  .setIndex(SECONDARY_INDEX)
                                  .setSecondaryField("no_such_field")
                                  .setPrimaryField("biz_id_primary")
                                  .setQuery(
                                      Query.newBuilder()
                                          .setMatchAllQuery(MatchAllQuery.newBuilder()))
                                  .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_NONE))
                          .build())
                  .build());
      fail("Expected exception for non-existent secondary_field in secondary index");
    } catch (StatusRuntimeException e) {
      assertTrue(
          "Error should mention the missing field: " + e.getMessage(),
          e.getMessage().contains("no_such_field") && e.getMessage().contains("unknown"));
    }
  }

  /** primary_field does not exist in primary index → error. */
  @Test
  public void testNonExistentPrimaryField() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(PRIMARY_INDEX)
                  .setTopHits(10)
                  .setQuery(
                      Query.newBuilder()
                          .setCrossIndexQuery(
                              CrossIndexQuery.newBuilder()
                                  .setIndex(SECONDARY_INDEX)
                                  .setSecondaryField("biz_id_secondary")
                                  .setPrimaryField("no_such_field")
                                  .setQuery(
                                      Query.newBuilder()
                                          .setMatchAllQuery(MatchAllQuery.newBuilder()))
                                  .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_NONE))
                          .build())
                  .build());
      fail("Expected exception for non-existent primary_field in primary index");
    } catch (StatusRuntimeException e) {
      assertTrue(
          "Error should mention the missing field: " + e.getMessage(),
          e.getMessage().contains("no_such_field") && e.getMessage().contains("unknown"));
    }
  }

  /** CrossIndexQuery used as a FILTER clause in a BooleanQuery alongside a primary query. */
  @Test
  public void testBooleanCompositionWithFilter() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setBooleanQuery(
                                BooleanQuery.newBuilder()
                                    // MUST: rating >= 4
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setOccur(BooleanClause.Occur.MUST)
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setRangeQuery(
                                                        RangeQuery.newBuilder()
                                                            .setField("rating")
                                                            .setLower("4")
                                                            .setUpper("5"))))
                                    // FILTER: must have reservation
                                    .addClauses(
                                        BooleanClause.newBuilder()
                                            .setOccur(BooleanClause.Occur.FILTER)
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setCrossIndexQuery(
                                                        CrossIndexQuery.newBuilder()
                                                            .setIndex(SECONDARY_INDEX)
                                                            .setSecondaryField("biz_id_secondary")
                                                            .setPrimaryField("biz_id_primary")
                                                            .setQuery(
                                                                Query.newBuilder()
                                                                    .setMatchAllQuery(
                                                                        MatchAllQuery.newBuilder()))
                                                            .setScoreMode(
                                                                CrossIndexQuery.JoinScoreMode
                                                                    .JOIN_SCORE_NONE))))))
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    // rating >= 4: biz_1 (5), biz_2 (4)
    // has reservation: biz_1, biz_2, biz_3
    // intersection: biz_1, biz_2
    assertEquals(Set.of("biz_1", "biz_2"), returnedBizIds);
  }

  /**
   * Helper to build a FunctionScoreQuery that scores each secondary doc by its covers value. This
   * produces varying scores so that MAX, MIN, AVG, and TOTAL are distinguishable.
   *
   * <p>Secondary data: biz_1 has covers=2 (score 2.0) and covers=4 (score 4.0), biz_2 has covers=2
   * (score 2.0), biz_3 has covers=6 (score 6.0).
   */
  private Query coversScoreQuery() {
    return Query.newBuilder()
        .setFunctionScoreQuery(
            FunctionScoreQuery.newBuilder()
                .setQuery(Query.newBuilder().setMatchAllQuery(MatchAllQuery.newBuilder()))
                .setScript(
                    Script.newBuilder().setLang("js").setSource("doc['covers'].value").build()))
        .build();
  }

  /**
   * Test with JOIN_SCORE_MAX using varying scores. biz_1: max(2.0, 4.0) = 4.0, biz_2: max(2.0) =
   * 2.0, biz_3: max(6.0) = 6.0.
   */
  @Test
  public void testScoreModeMax() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(coversScoreQuery())
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_MAX))
                            .build())
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    assertEquals(Set.of("biz_1", "biz_2", "biz_3"), returnedBizIds);

    Map<String, Double> scores = extractBizIdScores(response);
    assertEquals(4.0, scores.get("biz_1"), 0.001);
    assertEquals(2.0, scores.get("biz_2"), 0.001);
    assertEquals(6.0, scores.get("biz_3"), 0.001);
  }

  /**
   * Test with JOIN_SCORE_TOTAL using varying scores. biz_1: 2.0 + 4.0 = 6.0, biz_2: 2.0, biz_3:
   * 6.0.
   */
  @Test
  public void testScoreModeTotal() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(coversScoreQuery())
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_TOTAL))
                            .build())
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    assertEquals(Set.of("biz_1", "biz_2", "biz_3"), returnedBizIds);

    Map<String, Double> scores = extractBizIdScores(response);
    assertEquals(6.0, scores.get("biz_1"), 0.001);
    assertEquals(2.0, scores.get("biz_2"), 0.001);
    assertEquals(6.0, scores.get("biz_3"), 0.001);
  }

  /**
   * Test with JOIN_SCORE_AVG using varying scores. biz_1: (2.0 + 4.0) / 2 = 3.0, biz_2: 2.0 / 1 =
   * 2.0, biz_3: 6.0 / 1 = 6.0.
   */
  @Test
  public void testScoreModeAvg() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(coversScoreQuery())
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_AVG))
                            .build())
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    assertEquals(Set.of("biz_1", "biz_2", "biz_3"), returnedBizIds);

    Map<String, Double> scores = extractBizIdScores(response);
    assertEquals(3.0, scores.get("biz_1"), 0.001);
    assertEquals(2.0, scores.get("biz_2"), 0.001);
    assertEquals(6.0, scores.get("biz_3"), 0.001);
  }

  /**
   * Test with JOIN_SCORE_MIN using varying scores. biz_1: min(2.0, 4.0) = 2.0, biz_2: min(2.0) =
   * 2.0, biz_3: min(6.0) = 6.0.
   */
  @Test
  public void testScoreModeMin() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(coversScoreQuery())
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_MIN))
                            .build())
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    assertEquals(Set.of("biz_1", "biz_2", "biz_3"), returnedBizIds);

    Map<String, Double> scores = extractBizIdScores(response);
    assertEquals(2.0, scores.get("biz_1"), 0.001);
    assertEquals(2.0, scores.get("biz_2"), 0.001);
    assertEquals(6.0, scores.get("biz_3"), 0.001);
  }

  /** Test that inner query targeting a specific biz returns only that business. */
  @Test
  public void testSingleBusinessJoin() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(
                                        Query.newBuilder()
                                            .setTermQuery(
                                                TermQuery.newBuilder()
                                                    .setField("biz_id_secondary")
                                                    .setTextValue("biz_2")))
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_NONE))
                            .build())
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    assertEquals(Set.of("biz_2"), returnedBizIds);
  }

  /**
   * Test JOIN_SCORE_TOTAL with a filtered inner query to verify scores reflect the correct count of
   * matching secondary docs after filtering. Only covers >= 4 matches: biz_1 (covers=4, score 1.0)
   * and biz_3 (covers=6, score 1.0).
   */
  @Test
  public void testScoreTotalWithInnerFilter() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(
                                        Query.newBuilder()
                                            .setRangeQuery(
                                                RangeQuery.newBuilder()
                                                    .setField("covers")
                                                    .setLower("4")
                                                    .setUpper("100")))
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_TOTAL))
                            .build())
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    assertEquals(Set.of("biz_1", "biz_3"), returnedBizIds);

    // Each has exactly 1 matching secondary doc after the covers >= 4 filter
    Map<String, Double> scores = extractBizIdScores(response);
    assertEquals(1.0, scores.get("biz_1"), 0.001);
    assertEquals(1.0, scores.get("biz_3"), 0.001);
  }

  /** max_terms within limit — query succeeds. Secondary has 4 docs total. */
  @Test
  public void testMaxTermsWithinLimit() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(PRIMARY_INDEX)
                    .setTopHits(10)
                    .addRetrieveFields("biz_id_primary")
                    .setQuery(
                        Query.newBuilder()
                            .setCrossIndexQuery(
                                CrossIndexQuery.newBuilder()
                                    .setIndex(SECONDARY_INDEX)
                                    .setSecondaryField("biz_id_secondary")
                                    .setPrimaryField("biz_id_primary")
                                    .setQuery(
                                        Query.newBuilder()
                                            .setMatchAllQuery(MatchAllQuery.newBuilder()))
                                    .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_NONE)
                                    .setMaxTerms(10))
                            .build())
                    .build());

    Set<String> returnedBizIds = extractBizIds(response);
    assertEquals(Set.of("biz_1", "biz_2", "biz_3"), returnedBizIds);
  }

  /** max_terms exceeded — error returned. Secondary has 4 docs, limit set to 2. */
  @Test
  public void testMaxTermsExceeded() {
    try {
      getGrpcServer()
          .getBlockingStub()
          .search(
              SearchRequest.newBuilder()
                  .setIndexName(PRIMARY_INDEX)
                  .setTopHits(10)
                  .setQuery(
                      Query.newBuilder()
                          .setCrossIndexQuery(
                              CrossIndexQuery.newBuilder()
                                  .setIndex(SECONDARY_INDEX)
                                  .setSecondaryField("biz_id_secondary")
                                  .setPrimaryField("biz_id_primary")
                                  .setQuery(
                                      Query.newBuilder()
                                          .setMatchAllQuery(MatchAllQuery.newBuilder()))
                                  .setScoreMode(CrossIndexQuery.JoinScoreMode.JOIN_SCORE_NONE)
                                  .setMaxTerms(2))
                          .build())
                  .build());
      fail("Expected exception for max_terms exceeded");
    } catch (StatusRuntimeException e) {
      assertTrue(
          "Error should mention exceeding max_terms: " + e.getMessage(),
          e.getMessage().contains("exceeding max_terms"));
    }
  }

  private Set<String> extractBizIds(SearchResponse response) {
    return response.getHitsList().stream()
        .map(hit -> hit.getFieldsMap().get("biz_id_primary"))
        .map(fieldValue -> fieldValue.getFieldValue(0).getTextValue())
        .collect(Collectors.toSet());
  }

  private Map<String, Double> extractBizIdScores(SearchResponse response) {
    return response.getHitsList().stream()
        .collect(
            Collectors.toMap(
                hit -> hit.getFieldsMap().get("biz_id_primary").getFieldValue(0).getTextValue(),
                hit -> (double) hit.getScore()));
  }
}
