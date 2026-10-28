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
package com.yelp.nrtsearch.server.warming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.Collector;
import com.yelp.nrtsearch.server.grpc.ConstantScoreQuery;
import com.yelp.nrtsearch.server.grpc.DisjunctionMaxQuery;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FetchTask;
import com.yelp.nrtsearch.server.grpc.FunctionFilterQuery;
import com.yelp.nrtsearch.server.grpc.FunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.Highlight;
import com.yelp.nrtsearch.server.grpc.InnerHit;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.KnnRetriever;
import com.yelp.nrtsearch.server.grpc.LoggingHits;
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest;
import com.yelp.nrtsearch.server.grpc.NestedQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.RuntimeField;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SortFields;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.grpc.TermQuery;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import com.yelp.nrtsearch.server.grpc.VirtualField;
import java.util.List;
import org.junit.Test;

public class WarmingUtilsTest {

  @Test
  public void testSimplifySearchRequestForWarming() {
    // Create a mock SearchRequest.Builder
    SearchRequest.Builder searchRequestBuilder =
        SearchRequest.newBuilder()
            .setQuery(
                Query.newBuilder()
                    .setBooleanQuery(
                        BooleanQuery.newBuilder()
                            .addClauses(
                                BooleanClause.newBuilder()
                                    .setQuery(
                                        Query.newBuilder()
                                            .setTermQuery(
                                                TermQuery.newBuilder()
                                                    .setField("field")
                                                    .setTextValue("value"))))))
            .addAllRetrieveFields(List.of("field1", "field2"))
            .setQuerySort(QuerySortField.newBuilder().build())
            .addFacets(Facet.newBuilder().build())
            .putCollectors("c1", Collector.newBuilder().build())
            .setHighlight(Highlight.newBuilder().build())
            .addFetchTasks(FetchTask.newBuilder().build())
            .addRescorers(Rescorer.newBuilder().build())
            .setProfile(true);

    // Simplify the search request
    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    // Verify that the query remains intact
    assertNotNull(simplifiedRequest.getQuery());
    assertEquals(searchRequestBuilder.getQuery(), simplifiedRequest.getQuery());

    // Verify that other fields are cleared
    assertEquals(0, simplifiedRequest.getRetrieveFieldsCount());
    assertFalse(simplifiedRequest.hasQuerySort());
    assertEquals(0, simplifiedRequest.getFacetsCount());
    assertEquals(0, simplifiedRequest.getCollectorsCount());
    assertFalse(simplifiedRequest.hasHighlight());
    assertEquals(0, simplifiedRequest.getFetchTasksCount());
    assertEquals(0, simplifiedRequest.getRescorersCount());
    assertFalse(simplifiedRequest.getProfile());
  }

  @Test
  public void testSimplifySearchRequestForWarmingWithFunctionScoreQuery() {
    SearchRequest.Builder searchRequestBuilder;
    searchRequestBuilder =
        SearchRequest.newBuilder()
            .setQuery(
                Query.newBuilder()
                    .setFunctionScoreQuery(
                        FunctionScoreQuery.newBuilder()
                            .setQuery(
                                Query.newBuilder()
                                    .setTermQuery(
                                        TermQuery.newBuilder()
                                            .setField("field")
                                            .setTextValue("value")))));

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertNotNull(simplifiedRequest.getQuery());
    assertEquals(
        Query.newBuilder()
            .setTermQuery(TermQuery.newBuilder().setField("field").setTextValue("value"))
            .build(),
        simplifiedRequest.getQuery());
  }

  @Test
  public void testSimplifySearchRequestForWarmingWithMultiFunctionScoreQuery() {
    SearchRequest.Builder searchRequestBuilder;
    searchRequestBuilder =
        SearchRequest.newBuilder()
            .setQuery(
                Query.newBuilder()
                    .setMultiFunctionScoreQuery(
                        MultiFunctionScoreQuery.newBuilder()
                            .addFunctions(
                                MultiFunctionScoreQuery.FilterFunction.newBuilder()
                                    .setScript(
                                        Script.newBuilder().setLang("js").setSource("11 > 10"))
                                    .setFilter(
                                        Query.newBuilder()
                                            .setTermQuery(
                                                TermQuery.newBuilder()
                                                    .setField("field")
                                                    .setTextValue("value"))))));

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertNotNull(simplifiedRequest.getQuery());
    assertEquals(
        Query.newBuilder()
            .setTermQuery(TermQuery.newBuilder().setField("field").setTextValue("value"))
            .build(),
        simplifiedRequest.getQuery());
  }

  @Test
  public void testSimplifySearchRequestForWarmingWithFunctionFilterQuery() {
    SearchRequest.Builder searchRequestBuilder;
    searchRequestBuilder =
        SearchRequest.newBuilder()
            .setQuery(
                Query.newBuilder()
                    .setFunctionFilterQuery(
                        FunctionFilterQuery.newBuilder()
                            .setScript(Script.newBuilder().setLang("js").setSource("10"))));

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertNotNull(simplifiedRequest.getQuery());
    assertEquals(Query.newBuilder().build(), simplifiedRequest.getQuery());
  }

  @Test
  public void testSimplifySearchRequestForDisjunctionMaxQuery() {
    SearchRequest.Builder searchRequestBuilder;
    searchRequestBuilder =
        SearchRequest.newBuilder()
            .setQuery(
                Query.newBuilder()
                    .setDisjunctionMaxQuery(
                        DisjunctionMaxQuery.newBuilder()
                            .addDisjuncts(
                                Query.newBuilder()
                                    .setFunctionScoreQuery(
                                        FunctionScoreQuery.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("field")
                                                            .setTextValue("value")))))));

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertNotNull(simplifiedRequest.getQuery());
    assertEquals(
        Query.newBuilder()
            .setDisjunctionMaxQuery(
                DisjunctionMaxQuery.newBuilder()
                    .addDisjuncts(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("field").setTextValue("value"))))
            .build(),
        simplifiedRequest.getQuery());
  }

  @Test
  public void testSimplifySearchRequestForWarmingWithNestedQuery() {
    SearchRequest.Builder searchRequestBuilder =
        SearchRequest.newBuilder()
            .setQuery(
                Query.newBuilder()
                    .setNestedQuery(
                        NestedQuery.newBuilder()
                            .setQuery(
                                Query.newBuilder()
                                    .setFunctionScoreQuery(
                                        FunctionScoreQuery.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("field")
                                                            .setTextValue("value")))))
                            .setPath("nested_path")));

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertNotNull(simplifiedRequest.getQuery());
    assertEquals(
        Query.newBuilder()
            .setNestedQuery(
                NestedQuery.newBuilder()
                    .setQuery(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("field").setTextValue("value")))
                    .setPath("nested_path"))
            .build(),
        simplifiedRequest.getQuery());
  }

  @Test
  public void testSimplifySearchRequestForWarmingWithConstantScoreQuery() {
    SearchRequest.Builder searchRequestBuilder =
        SearchRequest.newBuilder()
            .setQuery(
                Query.newBuilder()
                    .setConstantScoreQuery(
                        ConstantScoreQuery.newBuilder()
                            .setFilter(
                                Query.newBuilder()
                                    .setFunctionScoreQuery(
                                        FunctionScoreQuery.newBuilder()
                                            .setQuery(
                                                Query.newBuilder()
                                                    .setTermQuery(
                                                        TermQuery.newBuilder()
                                                            .setField("field")
                                                            .setTextValue("value")))))));

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertNotNull(simplifiedRequest.getQuery());
    assertEquals(
        Query.newBuilder()
            .setConstantScoreQuery(
                ConstantScoreQuery.newBuilder()
                    .setFilter(
                        Query.newBuilder()
                            .setTermQuery(
                                TermQuery.newBuilder().setField("field").setTextValue("value"))))
            .build(),
        simplifiedRequest.getQuery());
  }

  @Test
  public void testSimplifySearchRequestForWarmingClearsFetchOnlyFields() {
    SearchRequest.Builder searchRequestBuilder =
        SearchRequest.newBuilder()
            .setQuery(termQuery())
            .addVirtualFields(VirtualField.newBuilder().setName("vf"))
            .addRuntimeFields(RuntimeField.newBuilder().setName("rf"))
            .putInnerHits("inner", InnerHit.newBuilder().setQueryNestedPath("nested").build())
            .setLoggingHits(LoggingHits.newBuilder().setName("logger"))
            .setExplain(true);

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertEquals(termQuery(), simplifiedRequest.getQuery());
    assertEquals(0, simplifiedRequest.getVirtualFieldsCount());
    assertEquals(0, simplifiedRequest.getRuntimeFieldsCount());
    assertEquals(0, simplifiedRequest.getInnerHitsCount());
    assertFalse(simplifiedRequest.hasLoggingHits());
    assertFalse(simplifiedRequest.getExplain());
  }

  @Test
  public void testSimplifySearchRequestForWarmingKeepsQueryUnset() {
    // setting an empty query would add a match all clause to a knn search, and fail validation for
    // a multi retriever request
    KnnQuery knnQuery =
        KnnQuery.newBuilder().setField("vector").setK(10).setNumCandidates(100).build();
    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(SearchRequest.newBuilder().addKnn(knnQuery));

    assertFalse(simplifiedRequest.hasQuery());
    assertEquals(List.of(knnQuery), simplifiedRequest.getKnnList());
  }

  @Test
  public void testSimplifySearchRequestForWarmingWithKnnFilter() {
    SearchRequest.Builder searchRequestBuilder =
        SearchRequest.newBuilder()
            .setQuery(termQuery())
            .addKnn(
                KnnQuery.newBuilder()
                    .setField("vector")
                    .setK(10)
                    .setNumCandidates(100)
                    .setFilter(functionScoreQuery(termQuery())));

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertEquals(termQuery(), simplifiedRequest.getQuery());
    assertEquals(termQuery(), simplifiedRequest.getKnn(0).getFilter());
    assertEquals(10, simplifiedRequest.getKnn(0).getK());
    assertEquals(100, simplifiedRequest.getKnn(0).getNumCandidates());
  }

  @Test
  public void testSimplifySearchRequestForWarmingClearsEmptyKnnFilter() {
    // a function filter query is removed, the vector search then runs without a filter
    SearchRequest.Builder searchRequestBuilder =
        SearchRequest.newBuilder()
            .addKnn(
                KnnQuery.newBuilder()
                    .setField("vector")
                    .setK(10)
                    .setNumCandidates(100)
                    .setFilter(
                        Query.newBuilder()
                            .setFunctionFilterQuery(
                                FunctionFilterQuery.newBuilder()
                                    .setScript(Script.newBuilder().setLang("js").setSource("1")))));

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertFalse(simplifiedRequest.hasQuery());
    assertFalse(simplifiedRequest.getKnn(0).hasFilter());
  }

  @Test
  public void testSimplifySearchRequestForWarmingWithMultiRetriever() {
    Rescorer rescorer = Rescorer.newBuilder().setWindowSize(10).build();
    SortFields sortFields =
        SortFields.newBuilder().addSortedFields(SortType.newBuilder().setFieldName("f")).build();
    SearchRequest.Builder searchRequestBuilder =
        SearchRequest.newBuilder()
            .setMultiRetriever(
                MultiRetrieverRequest.newBuilder()
                    .addRetrievers(
                        Retriever.newBuilder()
                            .setName("text")
                            .setBoost(2.0f)
                            .setRescorer(rescorer)
                            .setSortedFields(sortFields)
                            .setTextRetriever(
                                TextRetriever.newBuilder()
                                    .setQuery(functionScoreQuery(termQuery()))
                                    .setTopHits(10)))
                    .addRetrievers(
                        Retriever.newBuilder()
                            .setName("knn")
                            .setRescorer(rescorer)
                            .setKnnRetriever(
                                KnnRetriever.newBuilder()
                                    .setKnnQuery(
                                        KnnQuery.newBuilder()
                                            .setField("vector")
                                            .setK(5)
                                            .setNumCandidates(50)
                                            .setFilter(functionScoreQuery(termQuery())))))
                    .addRetrievers(
                        Retriever.newBuilder()
                            .setName("empty_text")
                            .setTextRetriever(TextRetriever.newBuilder().setTopHits(10))));

    SearchRequest simplifiedRequest =
        WarmingUtils.simplifySearchRequestForWarming(searchRequestBuilder);

    assertFalse(simplifiedRequest.hasQuery());
    assertTrue(simplifiedRequest.hasMultiRetriever());
    List<Retriever> retrievers = simplifiedRequest.getMultiRetriever().getRetrieversList();
    assertEquals(3, retrievers.size());
    for (Retriever retriever : retrievers) {
      assertFalse(retriever.hasRescorer());
      assertFalse(retriever.hasSortedFields());
    }

    Retriever text = retrievers.get(0);
    assertEquals("text", text.getName());
    assertEquals(2.0f, text.getBoost(), 0);
    assertEquals(termQuery(), text.getTextRetriever().getQuery());
    assertEquals(10, text.getTextRetriever().getTopHits());

    KnnQuery knnQuery = retrievers.get(1).getKnnRetriever().getKnnQuery();
    assertEquals(termQuery(), knnQuery.getFilter());
    assertEquals(5, knnQuery.getK());

    assertFalse(retrievers.get(2).getTextRetriever().hasQuery());
  }

  private static Query termQuery() {
    return Query.newBuilder()
        .setTermQuery(TermQuery.newBuilder().setField("field").setTextValue("value"))
        .build();
  }

  private static Query functionScoreQuery(Query query) {
    return Query.newBuilder()
        .setFunctionScoreQuery(
            FunctionScoreQuery.newBuilder()
                .setQuery(query)
                .setScript(Script.newBuilder().setLang("js").setSource("10")))
        .build();
  }
}
