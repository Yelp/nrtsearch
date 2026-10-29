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
package com.yelp.nrtsearch.server.warming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest;
import com.yelp.nrtsearch.server.grpc.AddDocumentRequest.MultiValuedField;
import com.yelp.nrtsearch.server.grpc.Blender;
import com.yelp.nrtsearch.server.grpc.FieldDefRequest;
import com.yelp.nrtsearch.server.grpc.FunctionFilterQuery;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.MatchQuery;
import com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import com.yelp.nrtsearch.server.grpc.WeightedRrfBlender;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/** Runs simplified warming requests against a real index, to check they pass validation. */
public class WarmingUtilsSearchTest extends ServerTestCase {

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
              .putFields(
                  "category", MultiValuedField.newBuilder().addValue(i <= 5 ? "a" : "b").build())
              .build());
    }
    addDocuments(docs.stream());
  }

  private static KnnQuery.Builder knnQuery(int k) {
    return KnnQuery.newBuilder()
        .setField("vector_field")
        .addAllQueryVector(List.of(0.1f, 0.2f, 0.3f))
        .setNumCandidates(10)
        .setK(k);
  }

  private static Query categoryFilterWithScript() {
    return Query.newBuilder()
        .setFunctionScoreQuery(
            FunctionScoreQuery.newBuilder()
                .setQuery(
                    Query.newBuilder()
                        .setTermQuery(
                            TermQuery.newBuilder().setField("category").setTextValue("a")))
                .setScript(Script.newBuilder().setLang("js").setSource("10")))
        .build();
  }

  private SearchResponse searchSimplified(SearchRequest.Builder request) {
    SearchRequest simplified = WarmingUtils.simplifySearchRequestForWarming(request);
    return getGrpcServer().getBlockingStub().search(simplified);
  }

  @Test
  public void testSimplifiedMultiRetrieverRequest() {
    SearchRequest.Builder request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(10)
            .setMultiRetriever(
                MultiRetrieverRequest.newBuilder()
                    .addRetrievers(
                        Retriever.newBuilder()
                            .setName("text")
                            .setTextRetriever(
                                TextRetriever.newBuilder()
                                    .setQuery(
                                        Query.newBuilder()
                                            .setFunctionScoreQuery(
                                                FunctionScoreQuery.newBuilder()
                                                    .setQuery(
                                                        Query.newBuilder()
                                                            .setMatchQuery(
                                                                MatchQuery.newBuilder()
                                                                    .setField("text_field")
                                                                    .setQuery("test")))
                                                    .setScript(
                                                        Script.newBuilder()
                                                            .setLang("js")
                                                            .setSource("10"))))
                                    .setTopHits(10)))
                    .addRetrievers(
                        Retriever.newBuilder()
                            .setName("knn")
                            .setKnnRetriever(
                                KnnRetriever.newBuilder()
                                    .setKnnQuery(
                                        knnQuery(5).setFilter(categoryFilterWithScript()))))
                    .setBlender(
                        Blender.newBuilder()
                            .setWeightedRrf(
                                WeightedRrfBlender.newBuilder().setRankConstant(60).build())));

    // fails validation if simplification sets an empty query
    SearchResponse response = searchSimplified(request);
    assertEquals(10, response.getHitsCount());
  }

  @Test
  public void testSimplifiedKnnOnlyRequestHasNoMatchAllClause() {
    SearchRequest.Builder request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(10)
            .addKnn(knnQuery(3));

    // with an empty query added as a match all clause, every doc would be a hit
    SearchResponse response = searchSimplified(request);
    assertEquals(3, response.getHitsCount());
    assertEquals(3, response.getTotalHits().getValue());
  }

  @Test
  public void testSimplifiedKnnOnlyRequestWithScriptFilter() {
    SearchRequest.Builder request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(10)
            .addKnn(knnQuery(10).setFilter(categoryFilterWithScript()));

    // the script is stripped and the category filter kept
    SearchResponse response = searchSimplified(request);
    assertEquals(5, response.getHitsCount());
  }

  @Test
  public void testSimplifiedKnnOnlyRequestWithEmptiedFilter() {
    SearchRequest.Builder request =
        SearchRequest.newBuilder()
            .setIndexName(DEFAULT_TEST_INDEX)
            .setTopHits(10)
            .addKnn(
                knnQuery(3)
                    .setFilter(
                        Query.newBuilder()
                            .setFunctionFilterQuery(
                                FunctionFilterQuery.newBuilder()
                                    .setScript(Script.newBuilder().setLang("js").setSource("1")))));

    SearchRequest simplified = WarmingUtils.simplifySearchRequestForWarming(request);
    assertFalse(simplified.getKnn(0).hasFilter());
    SearchResponse response = getGrpcServer().getBlockingStub().search(simplified);
    assertEquals(3, response.getHitsCount());
  }
}
