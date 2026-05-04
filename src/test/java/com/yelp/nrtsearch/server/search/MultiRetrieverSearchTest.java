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
package com.yelp.nrtsearch.server.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest;
import com.yelp.nrtsearch.server.grpc.ProfileResult;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QueryRescorer;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.ScorelessRawMergeBlender;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.TermsCollector;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import com.yelp.nrtsearch.server.grpc.WeightedRrfBlender;
import com.yelp.nrtsearch.server.grpc.WeightedScoreOrderBlender;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.ClassRule;
import org.junit.Test;

public class MultiRetrieverSearchTest extends ServerTestCase {

  private static final double SCORE_DELTA = 1e-5;
  // Max RRF score a doc can get from a single retriever with k=60: 1/(60+1)
  private static final double RRF_RANK1_K60 = 1.0 / 61.0;

  @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Override
  public FieldDefRequest getIndexDef(String name) throws IOException {
    return getFieldsFromResourceFile("/search/registerFieldsMultiRetriever.json");
  }

  @Override
  protected void initIndex(String name) throws Exception {
    List<AddDocumentRequest> docs = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      docs.add(
          AddDocumentRequest.newBuilder()
              .setIndexName(name)
              .putFields(
                  "doc_id", MultiValuedField.newBuilder().addValue(String.valueOf(i)).build())
              .putFields(
                  "text_field",
                  MultiValuedField.newBuilder().addValue("test document " + i).build())
              .putFields(
                  "vector_field",
                  MultiValuedField.newBuilder()
                      .addValue(String.format("[%f, %f, %f]", i * 0.1f, i * 0.2f, i * 0.3f))
                      .build())
              .build());
    }
    addDocuments(docs.stream());
  }

  private SearchRequest.Builder baseRequest() {
    return SearchRequest.newBuilder()
        .setIndexName(DEFAULT_TEST_INDEX)
        .setStartHit(0)
        .setTopHits(10);
  }

  private Retriever textRetriever(int topHits) {
    return Retriever.newBuilder()
        .setName("text")
        .setTextRetriever(
            TextRetriever.newBuilder()
                .setQuery(
                    Query.newBuilder()
                        .setMatchQuery(
                            MatchQuery.newBuilder().setField("text_field").setQuery("test").build())
                        .build())
                .setTopHits(topHits)
                .build())
        .build();
  }

  private Retriever knnRetriever(int k) {
    return Retriever.newBuilder()
        .setName("knn")
        .setKnnRetriever(
            KnnRetriever.newBuilder()
                .setKnnQuery(
                    KnnQuery.newBuilder()
                        .setField("vector_field")
                        .addAllQueryVector(List.of(0.1f, 0.2f, 0.3f))
                        .setNumCandidates(10)
                        .setK(k)
                        .build())
                .build())
        .build();
  }

  /**
   * Rescorer that replaces every hit's score with {@code constant} (queryWeight=0 drops original).
   */
  private static Rescorer constantScoreRescorer(double constant, int windowSize, String name) {
    return Rescorer.newBuilder()
        .setName(name)
        .setWindowSize(windowSize)
        .setQueryRescorer(
            QueryRescorer.newBuilder()
                .setQueryWeight(0.0f)
                .setRescoreQueryWeight(1.0f)
                .setRescoreQuery(
                    Query.newBuilder()
                        .setFunctionScoreQuery(
                            FunctionScoreQuery.newBuilder()
                                .setScript(
                                    Script.newBuilder()
                                        .setLang("js")
                                        .setSource(String.valueOf(constant))))
                        .build()))
        .build();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void assertSearchError(SearchRequest request, String expectedMessage) {
    try {
      getGrpcServer().getBlockingStub().search(request);
      fail("Expected error");
    } catch (StatusRuntimeException e) {
      assertTrue(e.getMessage().contains(expectedMessage));
    }
  }

  private static Set<Integer> docIds(SearchResponse response) {
    Set<Integer> ids = new HashSet<>();
    for (Hit hit : response.getHitsList()) ids.add(hit.getLuceneDocId());
    return ids;
  }

  @Test
  public void testValidationErrors() {
    MultiRetrieverRequest oneRetriever =
        MultiRetrieverRequest.newBuilder().addRetrievers(textRetriever(5)).build();

    assertSearchError(
        baseRequest().setMultiRetriever(MultiRetrieverRequest.newBuilder().build()).build(),
        "MultiRetriever request must have at least one retriever");
    assertSearchError(
        baseRequest()
            .setQuery(
                Query.newBuilder()
                    .setMatchQuery(
                        MatchQuery.newBuilder().setField("text_field").setQuery("test").build())
                    .build())
            .setMultiRetriever(oneRetriever)
            .build(),
        "Query should not be set along with a MultiRetriever request");
    assertSearchError(
        baseRequest().setQueryText("test").setMultiRetriever(oneRetriever).build(),
        "QueryText should not be set along with a MultiRetriever request");
    assertSearchError(
        baseRequest()
            .addKnn(
                KnnQuery.newBuilder()
                    .setField("vector_field")
                    .addAllQueryVector(List.of(0.1f, 0.2f, 0.3f))
                    .setNumCandidates(10)
                    .setK(5)
                    .build())
            .setMultiRetriever(oneRetriever)
            .build(),
        "Knn Query should not be set along with a MultiRetriever request");
    assertSearchError(
        baseRequest()
            .setQuerySort(
                QuerySortField.newBuilder()
                    .setFields(
                        SortFields.newBuilder()
                            .addSortedFields(SortType.newBuilder().setFieldName("doc_id").build())
                            .build())
                    .build())
            .setMultiRetriever(oneRetriever)
            .build(),
        "QuerySort is not supported with MultiRetriever requests");
    assertSearchError(
        baseRequest()
            .setMultiRetriever(
                MultiRetrieverRequest.newBuilder()
                    .addRetrievers(textRetriever(5))
                    .setBlender(Blender.newBuilder().build())
                    .build())
            .build(),
        "Unsupported blender type");
  }

  // TODO: Remove after adding Facets and Collectors support
  @Test
  public void testAggregationsNotSupported() {
    MultiRetrieverRequest twoRetrievers =
        MultiRetrieverRequest.newBuilder()
            .addRetrievers(textRetriever(5))
            .addRetrievers(knnRetriever(5))
            .build();

    assertSearchError(
        baseRequest()
            .setMultiRetriever(twoRetrievers)
            .addFacets(Facet.newBuilder().setName("doc_id").setTopN(5).build())
            .build(),
        "Facets are not supported with MultiRetriever requests");
    assertSearchError(
        baseRequest()
            .setMultiRetriever(twoRetrievers)
            .putCollectors(
                "terms",
                Collector.newBuilder()
                    .setTerms(TermsCollector.newBuilder().setField("doc_id").setSize(5).build())
                    .build())
            .build(),
        "Collectors are not supported with MultiRetriever requests");
  }

  /**
   * Single text retriever, k=60. Score at rank r = 1/(60+r) exactly. All 10 docs match "test",
   * returned in strictly descending score order with doc_id field populated.
   */
  @Test
  public void testTextOnlyRrfScores() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                baseRequest()
                    .addRetrieveFields("doc_id")
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(textRetriever(10))
                            .setBlender(
                                Blender.newBuilder()
                                    .setWeightedRrf(
                                        WeightedRrfBlender.newBuilder().setRankConstant(60))
                                    .build())
                            .build())
                    .build());

    assertEquals(10, response.getHitsCount());
    assertEquals(10, response.getTotalHits().getValue());
    assertEquals(10, docIds(response).size());

    List<Hit> hits = response.getHitsList();
    double prevScore = Double.MAX_VALUE;
    for (int i = 0; i < hits.size(); i++) {
      double score = hits.get(i).getScore();
      assertEquals(1.0 / (60 + i + 1), score, SCORE_DELTA);
      assertTrue(score < prevScore);
      prevScore = score;
      assertFalse(hits.get(i).getFieldsOrThrow("doc_id").getFieldValueList().isEmpty());
    }
  }

  /**
   * Text (10 hits) + KNN (k=5). All KNN hits are also text hits → 10 unique docs. Docs appearing in
   * both retrievers get contributions from two sources, so their RRF score > 1/(60+1) (the max from
   * a single retriever). The top 5 must therefore be the 5 KNN docs.
   */
  @Test
  public void testTextAndKnnRrfScoresAndOrdering() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                baseRequest()
                    .setTopHits(10)
                    .addRetrieveFields("doc_id")
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(textRetriever(10))
                            .addRetrievers(knnRetriever(5))
                            .setBlender(
                                Blender.newBuilder()
                                    .setWeightedRrf(
                                        WeightedRrfBlender.newBuilder().setRankConstant(60))
                                    .build())
                            .build())
                    .build());

    assertEquals(10, response.getHitsCount());
    assertEquals(10, docIds(response).size());

    List<Hit> hits = response.getHitsList();
    for (int i = 0; i < hits.size(); i++) {
      assertTrue(hits.get(i).getScore() > 0);
      if (i > 0) assertTrue(hits.get(i).getScore() <= hits.get(i - 1).getScore());
      assertFalse(hits.get(i).getFieldsOrThrow("doc_id").getFieldValueList().isEmpty());
    }
    for (int i = 0; i < 5; i++) assertTrue(hits.get(i).getScore() > RRF_RANK1_K60);
    for (int i = 5; i < 10; i++) assertTrue(hits.get(i).getScore() <= RRF_RANK1_K60);

    // Top-5 docs appear in both retrievers → both retriever scores present
    for (int i = 0; i < 5; i++) {
      assertTrue(hits.get(i).getRetrieverScoresMap().containsKey("text"));
      assertTrue(hits.get(i).getRetrieverScoresMap().containsKey("knn"));
      assertTrue(hits.get(i).getRetrieverScoresMap().get("text") > 0);
      assertEquals(1.0, hits.get(i).getRetrieverScoresMap().get("knn"), SCORE_DELTA);
    }
    // Bottom-5 docs appear only in the text retriever → only "text" score present
    for (int i = 5; i < 10; i++) {
      assertTrue(hits.get(i).getRetrieverScoresMap().containsKey("text"));
      assertFalse(hits.get(i).getRetrieverScoresMap().containsKey("knn"));
    }
  }

  /**
   * KNN-only, cosine=1.0 for all docs (parallel vectors), boost=1. WeightedScoreOrder MAX produces
   * score=1.0 for every hit. k=5 → exactly 5 unique hits.
   */
  @Test
  public void testKnnWeightedScoreOrderScores() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                baseRequest()
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(knnRetriever(5))
                            .setBlender(
                                Blender.newBuilder()
                                    .setWeightedScoreOrder(
                                        WeightedScoreOrderBlender.newBuilder()
                                            .setScoreMode(WeightedScoreOrderBlender.ScoreMode.MAX)
                                            .build())
                                    .build())
                            .build())
                    .build());

    assertEquals(5, response.getHitsCount());
    assertEquals(5, docIds(response).size());
    for (Hit hit : response.getHitsList()) assertEquals(1.0, hit.getScore(), SCORE_DELTA);
  }

  /** Deduplicates across retrievers and assigns score=0 to all hits. */
  @Test
  public void testScorelessRawMerge() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                baseRequest()
                    .setTopHits(10)
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(textRetriever(10))
                            .addRetrievers(knnRetriever(5))
                            .setBlender(
                                Blender.newBuilder()
                                    .setScorelessRawMerge(
                                        ScorelessRawMergeBlender.newBuilder().build())
                                    .build())
                            .build())
                    .build());

    assertEquals(10, response.getHitsCount());
    assertEquals(10, docIds(response).size());
    for (Hit hit : response.getHitsList()) assertEquals(0.0, hit.getScore(), SCORE_DELTA);
  }

  /**
   * L1 rescorer on the text retriever replaces all scores with 7.0 before blending.
   * WeightedScoreOrder MAX with boost=1 sees post-L1 scores, so every blended hit must have
   * score=7.0.
   */
  @Test
  public void testL1RescorerAppliedBeforeBlend() {
    Retriever textWithL1 =
        Retriever.newBuilder()
            .setName("text")
            .setTextRetriever(
                TextRetriever.newBuilder()
                    .setQuery(
                        Query.newBuilder()
                            .setMatchQuery(
                                MatchQuery.newBuilder()
                                    .setField("text_field")
                                    .setQuery("test")
                                    .build())
                            .build())
                    .setTopHits(10)
                    .build())
            .setRescorer(constantScoreRescorer(7.0, 10, "l1_text"))
            .build();

    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                baseRequest()
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(textWithL1)
                            .setBlender(
                                Blender.newBuilder()
                                    .setWeightedScoreOrder(
                                        WeightedScoreOrderBlender.newBuilder()
                                            .setScoreMode(WeightedScoreOrderBlender.ScoreMode.MAX)
                                            .build())
                                    .build())
                            .build())
                    .build());

    assertEquals(10, response.getHitsCount());
    for (Hit hit : response.getHitsList()) assertEquals(7.0, hit.getScore(), SCORE_DELTA);
  }

  /**
   * L2 rescorer on the SearchRequest runs post-blend. Constant score=3.0 replaces every blended
   * hit's score. Rescorer timing must appear in diagnostics.
   */
  @Test
  public void testL2RescorerAppliedPostBlend() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                baseRequest()
                    .addRescorers(constantScoreRescorer(3.0, 20, "l2_rescorer"))
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(textRetriever(10))
                            .addRetrievers(knnRetriever(5))
                            .setBlender(
                                Blender.newBuilder()
                                    .setWeightedRrf(
                                        WeightedRrfBlender.newBuilder().setRankConstant(60))
                                    .build())
                            .build())
                    .build());

    assertEquals(10, response.getHitsCount());
    for (Hit hit : response.getHitsList()) assertEquals(3.0, hit.getScore(), SCORE_DELTA);
    assertTrue(response.getDiagnostics().getRescorersTimeMsMap().containsKey("l2_rescorer"));
    assertTrue(response.getDiagnostics().getRescoreTimeMs() >= 0);
  }

  @Test
  public void testMultiRetrieverDiagnostics() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                baseRequest()
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(textRetriever(10))
                            .addRetrievers(knnRetriever(5))
                            .setBlender(
                                Blender.newBuilder()
                                    .setWeightedRrf(
                                        WeightedRrfBlender.newBuilder().setRankConstant(60))
                                    .build())
                            .build())
                    .build());

    assertTrue(response.getDiagnostics().hasMultiRetrieverDiagnostics());
    SearchResponse.Diagnostics.MultiRetrieverDiagnostics diag =
        response.getDiagnostics().getMultiRetrieverDiagnostics();
    assertNotNull(diag.getRetrieverDiagnosticsOrDefault("text", null));
    assertNotNull(diag.getRetrieverDiagnosticsOrDefault("knn", null));
    assertTrue(diag.getRetrieverDiagnosticsOrThrow("text").getSearchTimeMs() >= 0);
    assertTrue(diag.getRetrieverDiagnosticsOrThrow("knn").getSearchTimeMs() >= 0);
    assertTrue(diag.getBlenderTimeMs() >= 0);
    assertTrue(response.getDiagnostics().getFirstPassSearchTimeMs() >= 0);
  }

  @Test
  public void testProfilingPopulatesPerRetrieverResults() {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(
                baseRequest()
                    .setProfile(true)
                    .setMultiRetriever(
                        MultiRetrieverRequest.newBuilder()
                            .addRetrievers(textRetriever(10))
                            .addRetrievers(knnRetriever(5))
                            .setBlender(
                                Blender.newBuilder()
                                    .setWeightedRrf(
                                        WeightedRrfBlender.newBuilder().setRankConstant(60))
                                    .build())
                            .build())
                    .build());

    assertTrue(response.hasProfileResult());
    ProfileResult profile = response.getProfileResult();
    assertTrue(profile.hasMultiRetrieverProfileResult());
    assertTrue(
        profile
            .getMultiRetrieverProfileResult()
            .getRetrieverProfileResultsMap()
            .containsKey("text"));
    assertTrue(
        profile
            .getMultiRetrieverProfileResult()
            .getRetrieverProfileResultsMap()
            .containsKey("knn"));

    ProfileResult textProfile =
        profile.getMultiRetrieverProfileResult().getRetrieverProfileResultsOrThrow("text");
    assertFalse(textProfile.getParsedQuery().isEmpty());
    assertFalse(textProfile.getRewrittenQuery().isEmpty());
  }

  @Test
  public void testJsonMultiRetrieverRrfRequest() throws Exception {
    SearchResponse response =
        getGrpcServer()
            .getBlockingStub()
            .search(getSearchRequestFromResourceFile("/search/multiRetrieverRrfRequest.json"));

    assertEquals(10, response.getHitsCount());
    assertEquals(10, docIds(response).size());
    List<Hit> hits = response.getHitsList();
    for (int i = 0; i < hits.size(); i++) {
      assertTrue(hits.get(i).getScore() > 0);
      if (i > 0) assertTrue(hits.get(i).getScore() <= hits.get(i - 1).getScore());
      assertFalse(hits.get(i).getFieldsOrThrow("doc_id").getFieldValueList().isEmpty());
      assertFalse(hits.get(i).getFieldsOrThrow("text_field").getFieldValueList().isEmpty());
    }
    for (int i = 0; i < 5; i++) assertTrue(hits.get(i).getScore() > RRF_RANK1_K60);
    for (int i = 5; i < 10; i++) assertTrue(hits.get(i).getScore() <= RRF_RANK1_K60);
  }
}
