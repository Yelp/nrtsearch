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

import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.grpc.Retriever;
import com.yelp.nrtsearch.server.grpc.TextRetriever;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.query.QueryNodeMapper;
import com.yelp.nrtsearch.server.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.search.collectors.RelevanceCollector;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;

/**
 * Executes a single {@link TextRetriever} and returns a {@link SearcherResult} containing the top
 * documents.
 */
public class TextRetrieverExecutor implements Callable<SearcherResult> {

  private static final QueryNodeMapper QUERY_NODE_MAPPER = QueryNodeMapper.getInstance();
  private static final float DEFAULT_BOOST = 1.0f;

  private final TextRetriever textRetriever;
  private final float boost;
  private final IndexState indexState;
  private final CollectorCreatorContext collectorCreatorContext;
  private final DocLookup docLookup;

  public TextRetrieverExecutor(
      Retriever retriever,
      IndexState indexState,
      CollectorCreatorContext collectorCreatorContext,
      DocLookup docLookup) {
    this.textRetriever = retriever.getTextRetriever();
    this.boost = retriever.hasBoost() ? retriever.getBoost() : DEFAULT_BOOST;
    this.indexState = indexState;
    this.collectorCreatorContext = collectorCreatorContext;
    this.docLookup = docLookup;
  }

  @Override
  public SearcherResult call() throws Exception {
    Query query = QUERY_NODE_MAPPER.getQuery(textRetriever.getQuery(), docLookup);

    String queryNestedPath =
        IndexState.resolveQueryNestedPath(textRetriever.getQueryNestedPath(), docLookup);
    if (indexState.hasNestedChildFields()) {
      query = QUERY_NODE_MAPPER.applyQueryNestedPath(query, indexState, queryNestedPath);
    }

    query = collectorCreatorContext.getSearcherAndTaxonomy().searcher().rewrite(query);

    if (boost != DEFAULT_BOOST) {
      query = new BoostQuery(query, boost);
    }

    RelevanceCollector collector =
        new RelevanceCollector(collectorCreatorContext, Collections.emptyList());
    return collectorCreatorContext
        .getSearcherAndTaxonomy()
        .searcher()
        .search(query, collector.getWrappedManager());
  }
}
