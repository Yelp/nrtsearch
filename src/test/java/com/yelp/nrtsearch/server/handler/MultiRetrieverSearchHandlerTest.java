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
package com.yelp.nrtsearch.server.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FlatMapBlender;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Integration tests for multi-retriever search via {@link SearchHandler}. Each test verifies that
 * the multi-retriever path produces results equivalent (or strictly correct) relative to the legacy
 * single-query path for the same logical query.
 *
 * <p>Index schema: doc_id (ATOM), text_field (TEXT), int_field (INT), category (ATOM), vector_field
 * (VECTOR, 3-dim cosine).
 *
 * <p>Documents (10 total):
 *
 * <pre>
 *   id=0  text="alpha beta"  int=10  category="a"  vector=[1.0, 0.0, 0.0]
 *   id=1  text="alpha gamma" int=20  category="b"  vector=[0.0, 1.0, 0.0]
 *   id=2  text="beta delta"  int=30  category="a"  vector=[0.0, 0.0, 1.0]
 *   id=3  text="gamma delta" int=40  category="b"  vector=[0.7, 0.7, 0.0]
 *   id=4  text="epsilon"     int=50  category="a"  vector=[0.0, 0.7, 0.7]
 *   id=5  text="alpha delta" int=60  category="b"  vector=[0.7, 0.0, 0.7]
 *   id=6  text="zeta"        int=70  category="a"  vector=[0.5, 0.5, 0.7]
 *   id=7  text="alpha zeta"  int=80  category="b"  vector=[0.3, 0.6, 0.7]
 *   id=8  text="beta zeta"   int=90  category="a"  vector=[0.6, 0.3, 0.7]
 *   id=9  text="gamma zeta"  int=100 category="b"  vector=[0.4, 0.4, 0.8]
 * </pre>
 */
public class MultiRetrieverSearchHandlerTest extends ServerTestCase {

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final String INDEX = DEFAULT_TEST_INDEX;
  private static final List<String> RETRIEVE_FIELDS = List.of("doc_id");

  // Fixed blender: flat_map with SCORE_ORDER_DEDUP (default merge mode)
  private static final Blender FLAT_MAP_BLENDER =
      Blender.newBuilder().setFlatMap(FlatMapBlender.newBuilder().build()).setTopHits(10).build();

  @Override
  protected List<String> getIndices() {
    return Collections.singletonList(INDEX);
  }

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/retriever/registerFieldsMultiRetriever.json");
  }

  @Override
  public void initIndex(String name) throws Exception {
    String[][] docs = {
      {"0", "alpha beta", "10", "a", "[1.0, 0.0, 0.0]"},
      {"1", "alpha gamma", "20", "b", "[0.0, 1.0, 0.0]"},
      {"2", "beta delta", "30", "a", "[0.0, 0.0, 1.0]"},
      {"3", "gamma delta", "40", "b", "[0.7, 0.7, 0.0]"},
      {"4", "epsilon", "50", "a", "[0.0, 0.7, 0.7]"},
      {"5", "alpha delta", "60", "b", "[0.7, 0.0, 0.7]"},
      {"6", "zeta", "70", "a", "[0.5, 0.5, 0.7]"},
      {"7", "alpha zeta", "80", "b", "[0.3, 0.6, 0.7]"},
      {"8", "beta zeta", "90", "a", "[0.6, 0.3, 0.7]"},
      {"9", "gamma zeta", "100", "b", "[0.4, 0.4, 0.8]"},
    };
    List<AddDocumentRequest> requests = new ArrayList<>();
    for (String[] d : docs) {
      requests.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields("doc_id", MultiValuedField.newBuilder().addValue(d[0]).build())
              .putFields("text_field", MultiValuedField.newBuilder().addValue(d[1]).build())
              .putFields("int_field", MultiValuedField.newBuilder().addValue(d[2]).build())
              .putFields("category", MultiValuedField.newBuilder().addValue(d[3]).build())
              .putFields("vector_field", MultiValuedField.newBuilder().addValue(d[4]).build())
              .build());
    }
    addDocuments(requests.stream());
  }

  // ---- helpers ---------------------------------------------------------------

  private SearchResponse legacyTextSearch(Query query, int topHits) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(INDEX)
                .setTopHits(topHits)
                .addAllRetrieveFields(RETRIEVE_FIELDS)
                .setQuery(query)
                .build());
  }

  private SearchResponse multiRetrieverSearch(MultiRetrieverRequest multiRetriever) {
    return getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(INDEX)
                .addAllRetrieveFields(RETRIEVE_FIELDS)
                .setMultiRetriever(multiRetriever)
                .build());
  }

  private static Set<String> docIds(SearchResponse response) {
    return response.getHitsList().stream()
        .map(h -> h.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue())
        .collect(Collectors.toSet());
  }

  private static List<String> docIdList(SearchResponse response) {
    return response.getHitsList().stream()
        .map(h -> h.getFieldsOrThrow("doc_id").getFieldValue(0).getTextValue())
        .collect(Collectors.toList());
  }

  private static Query termQuery(String field, String value) {
    return Query.newBuilder()
        .setTermQuery(TermQuery.newBuilder().setField(field).setTextValue(value).build())
        .build();
  }

  private static Query shouldQuery(Query... clauses) {
    BooleanQuery.Builder bq = BooleanQuery.newBuilder();
    for (Query clause : clauses) {
      bq.addClauses(
          BooleanClause.newBuilder().setQuery(clause).setOccur(BooleanClause.Occur.SHOULD).build());
    }
    return Query.newBuilder().setBooleanQuery(bq.build()).build();
  }

  private static KnnQuery knnQuery(List<Float> vector, int k, int numCandidates) {
    return KnnQuery.newBuilder()
        .setField("vector_field")
        .addAllQueryVector(vector)
        .setK(k)
        .setNumCandidates(numCandidates)
        .build();
  }

  // ---- Test 1: single text retriever vs legacy text query --------------------

  /**
   * A single text retriever with a term query should return the same documents as the legacy
   * single-query path.
   */
  @Test
  public void testSingleTextRetriever_matchesLegacy() {
    Query query = termQuery("text_field", "alpha");

    SearchResponse legacy = legacyTextSearch(query, 10);

    SearchResponse multi =
        multiRetrieverSearch(
            MultiRetrieverRequest.newBuilder()
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("text")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(query).build())
                        .build())
                .setBlender(FLAT_MAP_BLENDER)
                .build());

    assertEquals("hit counts differ", legacy.getHitsCount(), multi.getHitsCount());
    assertEquals("doc id sets differ", docIds(legacy), docIds(multi));
  }

  // ---- Test 2: single text retriever – different term (distinct from test 1) -

  /** Same pattern as test 1 but with a different term, ensuring no shared state between tests. */
  @Test
  public void testSingleTextRetriever_differentTerm() {
    Query query = termQuery("text_field", "gamma");

    SearchResponse legacy = legacyTextSearch(query, 10);

    SearchResponse multi =
        multiRetrieverSearch(
            MultiRetrieverRequest.newBuilder()
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("text")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(query).build())
                        .build())
                .setBlender(FLAT_MAP_BLENDER)
                .build());

    assertEquals(legacy.getHitsCount(), multi.getHitsCount());
    assertEquals(docIds(legacy), docIds(multi));
  }

  // ---- Test 3: 1 kNN + 1 text retriever vs legacy kNN + query ----------------

  /**
   * A kNN retriever combined with a text retriever should return the same document set as the
   * legacy hybrid kNN+query path (single request with both knn and query set).
   */
  @Test
  public void testKnnPlusTextRetriever_matchesLegacyHybrid() {
    List<Float> queryVector = List.of(0.5f, 0.5f, 0.7f);
    Query textQuery = termQuery("text_field", "alpha");
    KnnQuery knn = knnQuery(queryVector, 5, 10);
    int topHits = 10;

    SearchResponse legacy =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .setTopHits(topHits)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setQuery(textQuery)
                    .addKnn(knn)
                    .build());

    SearchResponse multi =
        multiRetrieverSearch(
            MultiRetrieverRequest.newBuilder()
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("knn")
                        .setTopHits(knn.getK())
                        .setKnnRetriever(KnnRetriever.newBuilder().setKnnQuery(knn).build())
                        .build())
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("text")
                        .setTopHits(topHits)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(textQuery).build())
                        .build())
                .setBlender(FLAT_MAP_BLENDER)
                .build());

    assertEquals("hit counts differ", legacy.getHitsCount(), multi.getHitsCount());
    assertEquals("doc id sets differ", docIds(legacy), docIds(multi));
  }

  // ---- Test 4: with facets ---------------------------------------------------

  /**
   * When facets are requested alongside a multi-retriever, facet counts should be non-empty and the
   * hit results should still match the single-retriever equivalent.
   */
  @Test
  public void testSingleTextRetriever_withFacets() {
    Query query = termQuery("category", "a");

    SearchResponse legacy =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .setTopHits(10)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setQuery(query)
                    .addFacets(
                        Facet.newBuilder()
                            .setName("category_facet")
                            .setDim("category")
                            .setTopN(5)
                            .build())
                    .build());

    SearchResponse multi =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(
                                Retriever.newBuilder()
                                    .setName("text")
                                    .setTopHits(10)
                                    .setTextRetriever(
                                        TextRetriever.newBuilder().setQuery(query).build())
                                    .build())
                            .setBlender(FLAT_MAP_BLENDER)
                            .build())
                    .addFacets(
                        Facet.newBuilder()
                            .setName("category_facet")
                            .setDim("category")
                            .setTopN(5)
                            .build())
                    .build());

    // Hit results match
    assertEquals(legacy.getHitsCount(), multi.getHitsCount());
    assertEquals(docIds(legacy), docIds(multi));

    // Facet results are present and non-empty
    assertFalse("Expected facet results", multi.getFacetResultList().isEmpty());
    assertEquals(1, multi.getFacetResultCount());
    assertTrue(multi.getFacetResult(0).getLabelValuesCount() > 0);

    // Facet counts agree between legacy and multi-retriever paths
    assertEquals(
        legacy.getFacetResult(0).getLabelValuesCount(),
        multi.getFacetResult(0).getLabelValuesCount());
  }

  // ---- Test 5: with collectors -----------------------------------------------

  /**
   * When additional collectors are requested alongside a multi-retriever, collector results should
   * be populated and the hit results should match the single-retriever equivalent.
   */
  @Test
  public void testSingleTextRetriever_withCollector() {
    Query query = termQuery("text_field", "zeta");
    Collector termsCollector =
        Collector.newBuilder()
            .setTerms(TermsCollector.newBuilder().setField("category").setSize(5).build())
            .build();

    SearchResponse legacy =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .setTopHits(10)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setQuery(query)
                    .putCollectors("cat_terms", termsCollector)
                    .build());

    SearchResponse multi =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(
                                Retriever.newBuilder()
                                    .setName("text")
                                    .setTopHits(10)
                                    .setTextRetriever(
                                        TextRetriever.newBuilder().setQuery(query).build())
                                    .build())
                            .setBlender(FLAT_MAP_BLENDER)
                            .build())
                    .putCollectors("cat_terms", termsCollector)
                    .build());

    // Hit results match
    assertEquals(legacy.getHitsCount(), multi.getHitsCount());
    assertEquals(docIds(legacy), docIds(multi));

    // Collector result is present
    assertTrue(
        "Expected collector result for 'cat_terms'", multi.containsCollectorResults("cat_terms"));
    assertTrue(legacy.containsCollectorResults("cat_terms"));
  }

  // ---- Test 6: facets with 2 retrievers (no double-counting) -----------------

  /**
   * With two retrievers whose result sets overlap (doc 7 matches both "alpha" and "zeta"), the
   * facet counts must reflect the union of matched documents — each document counted exactly once.
   *
   * <p>Retriever 1: text_field="alpha" → docs {0,1,5,7} <br>
   * Retriever 2: text_field="zeta" → docs {6,7,8,9} <br>
   * Union (doc 7 deduped): {0,1,5,6,7,8,9} → category="a": {0,6,8}=3, category="b": {1,5,7,9}=4
   *
   * <p>The legacy equivalent is a single SHOULD boolean query over both terms.
   */
  @Test
  public void testTwoTextRetrievers_facetsNoDoubleCount() {
    Query alphaQuery = termQuery("text_field", "alpha");
    Query zetaQuery = termQuery("text_field", "zeta");
    Query unionQuery = shouldQuery(alphaQuery, zetaQuery);
    Facet categoryFacet =
        Facet.newBuilder().setName("category_facet").setDim("category").setTopN(5).build();

    SearchResponse legacy =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .setTopHits(10)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setQuery(unionQuery)
                    .addFacets(categoryFacet)
                    .build());

    SearchResponse multi =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(
                                Retriever.newBuilder()
                                    .setName("alpha")
                                    .setTopHits(10)
                                    .setTextRetriever(
                                        TextRetriever.newBuilder().setQuery(alphaQuery).build())
                                    .build())
                            .addRetrievers(
                                Retriever.newBuilder()
                                    .setName("zeta")
                                    .setTopHits(10)
                                    .setTextRetriever(
                                        TextRetriever.newBuilder().setQuery(zetaQuery).build())
                                    .build())
                            .setBlender(FLAT_MAP_BLENDER)
                            .build())
                    .addFacets(categoryFacet)
                    .build());

    // Facet label counts must be identical — no double-counting of doc 7
    assertEquals(1, multi.getFacetResultCount());
    FacetResult multiCatFacet = multi.getFacetResult(0);
    FacetResult legacyCatFacet = legacy.getFacetResult(0);
    assertEquals(legacyCatFacet.getLabelValuesCount(), multiCatFacet.getLabelValuesCount());
    // Build label→value maps and compare directly
    java.util.Map<String, Double> multiCounts = new java.util.HashMap<>();
    multiCatFacet.getLabelValuesList().forEach(lv -> multiCounts.put(lv.getLabel(), lv.getValue()));
    java.util.Map<String, Double> legacyCounts = new java.util.HashMap<>();
    legacyCatFacet
        .getLabelValuesList()
        .forEach(lv -> legacyCounts.put(lv.getLabel(), lv.getValue()));
    assertEquals("Facet counts differ (possible double-counting)", legacyCounts, multiCounts);
  }

  // ---- Test 7: collectors with 2 retrievers (no double-counting) -------------

  /**
   * With two retrievers whose result sets overlap, the terms-collector counts must match the legacy
   * single SHOULD query — each document contributing to the bucket exactly once.
   *
   * <p>Same union as test 6: category="a" bucket=3, category="b" bucket=4.
   */
  @Test
  public void testTwoTextRetrievers_collectorNoDoubleCount() {
    Query alphaQuery = termQuery("text_field", "alpha");
    Query zetaQuery = termQuery("text_field", "zeta");
    Query unionQuery = shouldQuery(alphaQuery, zetaQuery);
    Collector termsCollector =
        Collector.newBuilder()
            .setTerms(TermsCollector.newBuilder().setField("category").setSize(5).build())
            .build();

    SearchResponse legacy =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .setTopHits(10)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setQuery(unionQuery)
                    .putCollectors("cat_terms", termsCollector)
                    .build());

    SearchResponse multi =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(
                                Retriever.newBuilder()
                                    .setName("alpha")
                                    .setTopHits(10)
                                    .setTextRetriever(
                                        TextRetriever.newBuilder().setQuery(alphaQuery).build())
                                    .build())
                            .addRetrievers(
                                Retriever.newBuilder()
                                    .setName("zeta")
                                    .setTopHits(10)
                                    .setTextRetriever(
                                        TextRetriever.newBuilder().setQuery(zetaQuery).build())
                                    .build())
                            .setBlender(FLAT_MAP_BLENDER)
                            .build())
                    .putCollectors("cat_terms", termsCollector)
                    .build());

    // Collector bucket counts must be identical — no double-counting of doc 7
    assertTrue(multi.containsCollectorResults("cat_terms"));
    CollectorResult multiResult = multi.getCollectorResultsOrThrow("cat_terms");
    CollectorResult legacyResult = legacy.getCollectorResultsOrThrow("cat_terms");
    java.util.Map<String, Integer> multiCounts = new java.util.HashMap<>();
    multiResult
        .getBucketResult()
        .getBucketsList()
        .forEach(b -> multiCounts.put(b.getKey(), b.getCount()));
    java.util.Map<String, Integer> legacyCounts = new java.util.HashMap<>();
    legacyResult
        .getBucketResult()
        .getBucketsList()
        .forEach(b -> legacyCounts.put(b.getKey(), b.getCount()));
    assertEquals("Collector counts differ (possible double-counting)", legacyCounts, multiCounts);
  }

  // ---- Test 8: three retrievers -----------------------------------------------

  /**
   * Three text retrievers whose result sets partially overlap. After SCORE_ORDER_DEDUP blending the
   * union must contain exactly the deduplicated set of all matched documents.
   *
   * <p>Retriever "alpha": text_field="alpha" → docs {0,1,5,7} <br>
   * Retriever "zeta": text_field="zeta" → docs {6,7,8,9} <br>
   * Retriever "beta": text_field="beta" → docs {0,2,8} <br>
   * Union (deduped): {0,1,2,5,6,7,8,9} = 8 unique docs
   *
   * <p>The legacy equivalent is a single three-clause SHOULD boolean query.
   */
  @Test
  public void testThreeTextRetrievers_matchesLegacyUnion() {
    Query alphaQuery = termQuery("text_field", "alpha");
    Query zetaQuery = termQuery("text_field", "zeta");
    Query betaQuery = termQuery("text_field", "beta");
    Query unionQuery = shouldQuery(alphaQuery, zetaQuery, betaQuery);

    SearchResponse legacy =
        getGrpcServer()
            .getBlockingStub()
            .search(
                SearchRequest.newBuilder()
                    .setIndexName(INDEX)
                    .setTopHits(10)
                    .addAllRetrieveFields(RETRIEVE_FIELDS)
                    .setQuery(unionQuery)
                    .build());

    SearchResponse multi =
        multiRetrieverSearch(
            MultiRetrieverRequest.newBuilder()
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("alpha")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(alphaQuery).build())
                        .build())
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("zeta")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(zetaQuery).build())
                        .build())
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("beta")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(betaQuery).build())
                        .build())
                .setBlender(FLAT_MAP_BLENDER)
                .build());

    assertEquals("hit counts differ", legacy.getHitsCount(), multi.getHitsCount());
    assertEquals("doc id sets differ", docIds(legacy), docIds(multi));
  }

  // ---- Tests 9-12: FlatMapBlender merge modes ---------------------------------

  // Shared retrievers for merge-mode tests:
  //   "alpha": text_field="alpha" → docs {0,1,5,7}  (declaration order, rank 0-3)
  //   "zeta":  text_field="zeta"  → docs {6,7,8,9}  (declaration order, rank 0-3)
  //   doc 7 appears in both retrievers.

  private MultiRetrieverRequest.Builder twoRetrieverBuilder(
      Query alphaQuery, Query zetaQuery, FlatMapBlender.MergeMode mode) {
    return MultiRetrieverRequest.newBuilder()
        .addRetrievers(
            Retriever.newBuilder()
                .setName("alpha")
                .setTopHits(10)
                .setTextRetriever(TextRetriever.newBuilder().setQuery(alphaQuery).build())
                .build())
        .addRetrievers(
            Retriever.newBuilder()
                .setName("zeta")
                .setTopHits(10)
                .setTextRetriever(TextRetriever.newBuilder().setQuery(zetaQuery).build())
                .build())
        .setBlender(
            Blender.newBuilder()
                .setFlatMap(FlatMapBlender.newBuilder().setMergeMode(mode).build())
                .setTopHits(10)
                .build());
  }

  /**
   * APPEND mode concatenates all retriever results in declaration order without deduplication. Doc
   * 7 appears in both retrievers and must therefore appear twice in the result.
   */
  @Test
  public void testMergeMode_append() {
    Query alphaQuery = termQuery("text_field", "alpha");
    Query zetaQuery = termQuery("text_field", "zeta");

    SearchResponse response =
        multiRetrieverSearch(
            twoRetrieverBuilder(alphaQuery, zetaQuery, FlatMapBlender.MergeMode.APPEND).build());

    // alpha (4 docs) + zeta (4 docs) = 8 entries, doc 7 present twice
    assertEquals("APPEND must not deduplicate", 8, response.getHitsCount());
    List<String> ids = docIdList(response);
    assertEquals("APPEND: doc 7 must appear exactly twice", 2, Collections.frequency(ids, "7"));
    // First 4 entries are alpha's results, next 4 are zeta's
    assertEquals(
        "APPEND: first half must be alpha docs",
        Set.of("0", "1", "5", "7"),
        new java.util.HashSet<>(ids.subList(0, 4)));
    assertEquals(
        "APPEND: second half must be zeta docs",
        Set.of("6", "7", "8", "9"),
        new java.util.HashSet<>(ids.subList(4, 8)));
  }

  /**
   * SCORE_ORDER_DEDUP deduplicates across retrievers keeping the highest score for each document,
   * then sorts by score descending. Doc 7 must appear exactly once with its highest score.
   */
  @Test
  public void testMergeMode_scoreOrderDedup() {
    Query alphaQuery = termQuery("text_field", "alpha");
    Query zetaQuery = termQuery("text_field", "zeta");

    SearchResponse response =
        multiRetrieverSearch(
            twoRetrieverBuilder(alphaQuery, zetaQuery, FlatMapBlender.MergeMode.SCORE_ORDER_DEDUP)
                .build());

    List<String> ids = docIdList(response);

    // 7 unique docs after deduplication
    assertEquals("SCORE_ORDER_DEDUP must deduplicate", 7, response.getHitsCount());
    assertEquals("doc 7 must appear exactly once", 1, Collections.frequency(ids, "7"));

    // Scores must be non-increasing
    List<Double> scores =
        response.getHitsList().stream()
            .map(SearchResponse.Hit::getScore)
            .collect(Collectors.toList());
    for (int i = 1; i < scores.size(); i++) {
      assertTrue(
          "Scores must be non-increasing at position " + i, scores.get(i) <= scores.get(i - 1));
    }
  }

  /**
   * RANK_ORDER_DEDUP deduplicates keeping the first (best-rank) occurrence and preserves the
   * interleaved rank order across retrievers. Doc 7 must appear once; its position must be wherever
   * it first appeared across both retriever lists.
   */
  @Test
  public void testMergeMode_rankOrderDedup() {
    Query alphaQuery = termQuery("text_field", "alpha");
    Query zetaQuery = termQuery("text_field", "zeta");

    SearchResponse response =
        multiRetrieverSearch(
            twoRetrieverBuilder(alphaQuery, zetaQuery, FlatMapBlender.MergeMode.RANK_ORDER_DEDUP)
                .build());

    List<String> ids = docIdList(response);

    // 7 unique docs after deduplication
    assertEquals("RANK_ORDER_DEDUP must deduplicate", 7, response.getHitsCount());
    assertEquals("doc 7 must appear exactly once", 1, Collections.frequency(ids, "7"));

    // All alpha docs and all zeta docs must be present
    assertTrue("All alpha docs must be present", ids.containsAll(List.of("0", "1", "5", "7")));
    assertTrue("All zeta docs must be present", ids.containsAll(List.of("6", "8", "9")));

    // Alpha's docs appear before any zeta-only doc that ranked after doc 7 in zeta,
    // because alpha is declared first and all its hits precede zeta's in rank-order iteration.
    // The first 4 positions must all be alpha docs (rank 0-3 of alpha come before zeta's rank 0+).
    assertEquals(
        "First 4 positions must be alpha's hits (rank order)",
        Set.of("0", "1", "5", "7"),
        new java.util.HashSet<>(ids.subList(0, 4)));
  }

  /**
   * PRIORITY_ORDER_DEDUP groups results by retriever in priority order. With priority
   * ["zeta","alpha"], all of zeta's unique docs come first, then alpha's docs that were not already
   * returned by zeta. Doc 7 belongs to zeta's group (higher priority).
   */
  @Test
  public void testMergeMode_priorityOrderDedup() {
    Query alphaQuery = termQuery("text_field", "alpha");
    Query zetaQuery = termQuery("text_field", "zeta");

    SearchResponse response =
        multiRetrieverSearch(
            twoRetrieverBuilder(
                    alphaQuery, zetaQuery, FlatMapBlender.MergeMode.PRIORITY_ORDER_DEDUP)
                .setBlender(
                    Blender.newBuilder()
                        .setFlatMap(
                            FlatMapBlender.newBuilder()
                                .setMergeMode(FlatMapBlender.MergeMode.PRIORITY_ORDER_DEDUP)
                                .addRetrieverPriorityOrder("zeta")
                                .addRetrieverPriorityOrder("alpha")
                                .build())
                        .setTopHits(10)
                        .build())
                .build());

    List<String> ids = docIdList(response);

    // 7 unique docs after deduplication
    assertEquals("PRIORITY_ORDER_DEDUP must deduplicate", 7, response.getHitsCount());
    assertEquals("doc 7 must appear exactly once", 1, Collections.frequency(ids, "7"));

    // First group: zeta docs {6,7,8,9} (4 docs from zeta)
    assertEquals(
        "First group must be zeta docs",
        Set.of("6", "7", "8", "9"),
        new java.util.HashSet<>(ids.subList(0, 4)));
    // Second group: alpha docs not in zeta = {0,1,5}
    assertEquals(
        "Second group must be alpha-only docs",
        Set.of("0", "1", "5"),
        new java.util.HashSet<>(ids.subList(4, 7)));
  }

  // ---- Test 13: blender top_hits ----------------------------------------------

  /**
   * When the blender's top_hits is set, the response must contain exactly that many hits taken from
   * the front of the full blended list — regardless of how many total matches exist.
   *
   * <p>Full union of "alpha" ∪ "zeta" (SCORE_ORDER_DEDUP) = 7 unique docs. With top_hits=3 the
   * blender must return the 3 highest-scoring docs from that list.
   */
  @Test
  public void testBlender_topHits() {
    Query alphaQuery = termQuery("text_field", "alpha");
    Query zetaQuery = termQuery("text_field", "zeta");

    // Full blended list (no pagination)
    SearchResponse full =
        multiRetrieverSearch(
            MultiRetrieverRequest.newBuilder()
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("alpha")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(alphaQuery).build())
                        .build())
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("zeta")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(zetaQuery).build())
                        .build())
                .setBlender(FLAT_MAP_BLENDER)
                .build());

    int topHits = 3;
    assertTrue("Need more hits than topHits to test pagination", full.getHitsCount() > topHits);

    // Blended with top_hits=3
    SearchResponse paged =
        multiRetrieverSearch(
            MultiRetrieverRequest.newBuilder()
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("alpha")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(alphaQuery).build())
                        .build())
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("zeta")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(zetaQuery).build())
                        .build())
                .setBlender(
                    Blender.newBuilder()
                        .setFlatMap(FlatMapBlender.newBuilder().build())
                        .setTopHits(topHits)
                        .build())
                .build());

    assertEquals("blender top_hits not respected", topHits, paged.getHitsCount());
    // The returned docs must be the first topHits docs of the full list
    List<String> fullIds = docIdList(full);
    List<String> pagedIds = docIdList(paged);
    assertEquals("paged docs must be prefix of full list", fullIds.subList(0, topHits), pagedIds);
  }

  // ---- Test 14: blender start_hit ---------------------------------------------

  /**
   * When the blender's start_hit is set (with top_hits), the response must contain exactly the
   * correct page of the blended list.
   *
   * <p>Full union: 7 docs. start_hit=2, top_hits=3 → docs at positions [2, 3, 4] of the full list.
   */
  @Test
  public void testBlender_startHit() {
    Query alphaQuery = termQuery("text_field", "alpha");
    Query zetaQuery = termQuery("text_field", "zeta");

    // Full blended list (no pagination)
    SearchResponse full =
        multiRetrieverSearch(
            MultiRetrieverRequest.newBuilder()
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("alpha")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(alphaQuery).build())
                        .build())
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("zeta")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(zetaQuery).build())
                        .build())
                .setBlender(FLAT_MAP_BLENDER)
                .build());

    int startHit = 2;
    int topHits = 3;
    assertTrue(
        "Need at least startHit+topHits hits in full list",
        full.getHitsCount() >= startHit + topHits);

    SearchResponse paged =
        multiRetrieverSearch(
            MultiRetrieverRequest.newBuilder()
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("alpha")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(alphaQuery).build())
                        .build())
                .addRetrievers(
                    Retriever.newBuilder()
                        .setName("zeta")
                        .setTopHits(10)
                        .setTextRetriever(TextRetriever.newBuilder().setQuery(zetaQuery).build())
                        .build())
                .setBlender(
                    Blender.newBuilder()
                        .setFlatMap(FlatMapBlender.newBuilder().build())
                        .setStartHit(startHit)
                        .setTopHits(topHits)
                        .build())
                .build());

    assertEquals("blender top_hits not respected", topHits, paged.getHitsCount());
    List<String> fullIds = docIdList(full);
    List<String> pagedIds = docIdList(paged);
    assertEquals(
        "paged docs must be the [startHit, startHit+topHits) slice of the full list",
        fullIds.subList(startHit, startHit + topHits),
        pagedIds);
  }

  // ---- Test 15: mutual exclusion validation ----------------------------------

  /**
   * Providing both a top-level query and a multi_retriever in the same request must be rejected
   * with a status error.
   */
  @Test(expected = io.grpc.StatusRuntimeException.class)
  public void testMutualExclusion_queryAndMultiRetriever() {
    getGrpcServer()
        .getBlockingStub()
        .search(
            SearchRequest.newBuilder()
                .setIndexName(INDEX)
                .setTopHits(5)
                .setQuery(termQuery("text_field", "alpha"))
                .setMultiRetriever(
                    MultiRetrieverRequest.newBuilder()
                        .addRetrievers(
                            Retriever.newBuilder()
                                .setName("r1")
                                .setTopHits(5)
                                .setTextRetriever(
                                    TextRetriever.newBuilder()
                                        .setQuery(termQuery("text_field", "alpha"))
                                        .build())
                                .build())
                        .setBlender(FLAT_MAP_BLENDER)
                        .build())
                .build());
  }
}
