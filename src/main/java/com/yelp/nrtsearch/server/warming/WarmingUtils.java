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

import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.ChildFilter;
import com.yelp.nrtsearch.server.grpc.ConstantScoreQuery;
import com.yelp.nrtsearch.server.grpc.DisjunctionMaxQuery;
import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.MultiRetrieverRequest;
import com.yelp.nrtsearch.server.grpc.NestedQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.TextRetriever;

public class WarmingUtils {

  public static SearchRequest simplifySearchRequestForWarming(
      SearchRequest.Builder searchRequestBuilder) {
    if (searchRequestBuilder.hasQuery()) {
      searchRequestBuilder.setQuery(stripScriptQuery(searchRequestBuilder.getQuery()));
    }
    for (int i = 0; i < searchRequestBuilder.getChildFiltersCount(); i++) {
      ChildFilter childFilter = searchRequestBuilder.getChildFilters(i);
      if (childFilter.hasFilter()) {
        searchRequestBuilder.setChildFilters(
            i, childFilter.toBuilder().setFilter(stripScriptQuery(childFilter.getFilter())));
      }
    }
    for (int i = 0; i < searchRequestBuilder.getKnnCount(); i++) {
      searchRequestBuilder.setKnn(i, stripKnnQuery(searchRequestBuilder.getKnn(i)));
    }
    if (searchRequestBuilder.hasMultiRetriever()) {
      searchRequestBuilder.setMultiRetriever(
          stripMultiRetriever(searchRequestBuilder.getMultiRetriever()));
    }
    SearchRequest searchRequest =
        searchRequestBuilder
            .clearQuerySort()
            .clearRescorers()
            .clearRetrieveFields()
            .clearFetchTasks()
            .clearFacets()
            .clearCollectors()
            .clearHighlight()
            .clearInnerHits()
            .clearVirtualFields()
            .clearRuntimeFields()
            .clearLoggingHits()
            .setExplain(false)
            .setProfile(false)
            .build();
    return searchRequest;
  }

  /**
   * Strip scripts from each text retriever query and knn filter, and remove the per retriever
   * rescorers and sorts. The blender is kept, since it only merges the retriever hits.
   */
  private static MultiRetrieverRequest stripMultiRetriever(MultiRetrieverRequest multiRetriever) {
    MultiRetrieverRequest.Builder multiRetrieverBuilder = multiRetriever.toBuilder();
    for (int i = 0; i < multiRetriever.getRetrieversCount(); i++) {
      Retriever.Builder retrieverBuilder =
          multiRetriever.getRetrievers(i).toBuilder().clearRescorer().clearSortedFields();
      switch (retrieverBuilder.getRetrieverTypeCase()) {
        case TEXTRETRIEVER -> {
          TextRetriever textRetriever = retrieverBuilder.getTextRetriever();
          if (textRetriever.hasQuery()) {
            retrieverBuilder.setTextRetriever(
                textRetriever.toBuilder().setQuery(stripScriptQuery(textRetriever.getQuery())));
          }
        }
        case KNNRETRIEVER ->
            retrieverBuilder.setKnnRetriever(
                retrieverBuilder.getKnnRetriever().toBuilder()
                    .setKnnQuery(stripKnnQuery(retrieverBuilder.getKnnRetriever().getKnnQuery())));
        default -> {}
      }
      multiRetrieverBuilder.setRetrievers(i, retrieverBuilder.build());
    }
    return multiRetrieverBuilder.build();
  }

  private static KnnQuery stripKnnQuery(KnnQuery knnQuery) {
    if (!knnQuery.hasFilter()) {
      return knnQuery;
    }
    Query filter = stripScriptQuery(knnQuery.getFilter());
    // an empty filter would be built as a match all filter, so search without one instead
    if (filter.getQueryNodeCase() == Query.QueryNodeCase.QUERYNODE_NOT_SET) {
      return knnQuery.toBuilder().clearFilter().build();
    }
    return knnQuery.toBuilder().setFilter(filter).build();
  }

  private static Query stripScriptQuery(Query query) {
    if (query.hasFunctionScoreQuery()) {
      return stripScriptQuery(query.getFunctionScoreQuery().getQuery());
    }
    if (query.hasFunctionFilterQuery()) {
      return Query.newBuilder().build();
    }
    if (query.hasMultiFunctionScoreQuery()) {
      MultiFunctionScoreQuery multiFunctionScoreQuery = query.getMultiFunctionScoreQuery();
      for (MultiFunctionScoreQuery.FilterFunction function :
          multiFunctionScoreQuery.getFunctionsList()) {
        if (function.hasScript()) {
          return stripScriptQuery(function.getFilter());
        }
      }
    }

    Query.Builder queryBuilder = query.toBuilder();
    switch (query.getQueryNodeCase()) {
      case BOOLEANQUERY -> queryBuilder.setBooleanQuery(stripBooleanQuery(query.getBooleanQuery()));
      case DISJUNCTIONMAXQUERY ->
          queryBuilder.setDisjunctionMaxQuery(
              stripDisjunctionMaxQuery(query.getDisjunctionMaxQuery()));
      case NESTEDQUERY -> queryBuilder.setNestedQuery(stripNestedQuery(query.getNestedQuery()));
      case CONSTANTSCOREQUERY ->
          queryBuilder.setConstantScoreQuery(
              stripConstantScoreQuery(query.getConstantScoreQuery()));
      default -> {}
    }
    // Add other cases as needed
    return queryBuilder.build();
  }

  private static BooleanQuery stripBooleanQuery(BooleanQuery booleanQuery) {
    BooleanQuery.Builder booleanQueryBuilder = booleanQuery.toBuilder();
    for (int i = 0; i < booleanQuery.getClausesCount(); i++) {
      BooleanClause clause = booleanQuery.getClauses(i);
      BooleanClause.Builder clauseBuilder = clause.toBuilder();
      clauseBuilder.setQuery(stripScriptQuery(clause.getQuery()));
      booleanQueryBuilder.setClauses(i, clauseBuilder.build());
    }
    return booleanQueryBuilder.build();
  }

  private static DisjunctionMaxQuery stripDisjunctionMaxQuery(
      DisjunctionMaxQuery disjunctionMaxQuery) {
    DisjunctionMaxQuery.Builder disjunctionMaxQueryBuilder = disjunctionMaxQuery.toBuilder();
    for (int i = 0; i < disjunctionMaxQuery.getDisjunctsCount(); i++) {
      disjunctionMaxQueryBuilder.setDisjuncts(
          i, stripScriptQuery(disjunctionMaxQuery.getDisjuncts(i)));
    }
    return disjunctionMaxQueryBuilder.build();
  }

  private static NestedQuery stripNestedQuery(NestedQuery nestedQuery) {
    NestedQuery.Builder nestedQueryBuilder = nestedQuery.toBuilder();
    nestedQueryBuilder.setQuery(stripScriptQuery(nestedQuery.getQuery()));
    return nestedQueryBuilder.build();
  }

  private static ConstantScoreQuery stripConstantScoreQuery(ConstantScoreQuery constantScoreQuery) {
    ConstantScoreQuery.Builder constantScoreQueryBuilder = constantScoreQuery.toBuilder();
    constantScoreQueryBuilder.setFilter(stripScriptQuery(constantScoreQuery.getFilter()));
    return constantScoreQueryBuilder.build();
  }
}
