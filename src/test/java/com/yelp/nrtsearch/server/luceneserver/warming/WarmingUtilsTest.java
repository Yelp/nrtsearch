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
package com.yelp.nrtsearch.server.luceneserver.warming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

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
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.NestedQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.QuerySortField;
import com.yelp.nrtsearch.server.grpc.Rescorer;
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TermQuery;
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
}
