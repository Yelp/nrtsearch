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

import com.yelp.nrtsearch.server.grpc.BooleanClause;
import com.yelp.nrtsearch.server.grpc.BooleanQuery;
import com.yelp.nrtsearch.server.grpc.ConstantScoreQuery;
import com.yelp.nrtsearch.server.grpc.DisjunctionMaxQuery;
import com.yelp.nrtsearch.server.grpc.MultiFunctionScoreQuery;
import com.yelp.nrtsearch.server.grpc.NestedQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.SearchRequest;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.SearchHandler;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.SearchRequestProcessor;
import java.io.IOException;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarmingUtils {
  private static final Logger logger = LoggerFactory.getLogger(WarmingUtils.class);

  public static void handleStrippedWarmingQuery(
      IndexState indexState, SearchRequest searchRequest) {
    SearchContext searchContext;
    SearcherTaxonomyManager.SearcherAndTaxonomy s = null;
    ShardState shardState = indexState.getShard(0);

    try {
      s =
          SearchHandler.getSearcherAndTaxonomy(
              searchRequest,
              indexState,
              shardState,
              SearchResponse.Diagnostics.newBuilder(),
              indexState.getSearchThreadPoolExecutor());

      searchContext =
          SearchRequestProcessor.buildContextForWarmingQuery(
              searchRequest, indexState, shardState, s);

      s.searcher.search(searchContext.getQuery(), searchContext.getCollector().getWrappedManager());
    } catch (InterruptedException | IOException e) {
      logger.warn(e.getMessage(), e);
      throw new RuntimeException(e);
    } finally {
      try {
        if (s != null) {
          shardState.release(s);
        }
      } catch (IOException e) {
        logger.warn("Failed to release searcher reference previously acquired by acquire()", e);
        throw new RuntimeException(e);
      }
    }
  }

  public static Query stripScriptQuery(Query query) {
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
      case BOOLEANQUERY:
        queryBuilder.setBooleanQuery(stripBooleanQuery(query.getBooleanQuery()));
        break;
      case DISJUNCTIONMAXQUERY:
        queryBuilder.setDisjunctionMaxQuery(
            stripDisjunctionMaxQuery(query.getDisjunctionMaxQuery()));
        break;
      case NESTEDQUERY:
        queryBuilder.setNestedQuery(stripNestedQuery(query.getNestedQuery()));
        break;
      case CONSTANTSCOREQUERY:
        queryBuilder.setConstantScoreQuery(stripConstantScoreQuery(query.getConstantScoreQuery()));
        break;
        // Add other cases as needed
      default:
        break;
    }
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
