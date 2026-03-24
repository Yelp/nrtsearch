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
import java.util.concurrent.Callable;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;

/**
 * Executes a single {@link TextRetriever} and returns a {@link RetrieverResult} containing the top
 * documents and execution time.
 */
public class TextRetrieverExecutor implements Callable<RetrieverResult> {

  private static final QueryNodeMapper QUERY_NODE_MAPPER = QueryNodeMapper.getInstance();
  private static final float DEFAULT_BOOST = 1.0f;

  private final String name;
  private final TextRetriever textRetriever;
  private final float boost;
  private final int totalHitsThreshold;
  private final IndexState indexState;
  private final IndexSearcher indexSearcher;
  private final DocLookup docLookup;

  public TextRetrieverExecutor(
      Retriever retriever,
      IndexState indexState,
      IndexSearcher indexSearcher,
      DocLookup docLookup,
      int totalHitsThreshold) {
    this.name = retriever.getName();
    this.textRetriever = retriever.getTextRetriever();
    this.boost = retriever.hasBoost() ? retriever.getBoost() : DEFAULT_BOOST;
    this.totalHitsThreshold = totalHitsThreshold;
    this.indexState = indexState;
    this.indexSearcher = indexSearcher;
    this.docLookup = docLookup;
  }

  @Override
  public RetrieverResult call() throws Exception {
    long startNs = System.nanoTime();

    Query query = QUERY_NODE_MAPPER.getQuery(textRetriever.getQuery(), docLookup);

    String queryNestedPath =
        IndexState.resolveQueryNestedPath(textRetriever.getQueryNestedPath(), docLookup);
    if (indexState.hasNestedChildFields()) {
      query = QUERY_NODE_MAPPER.applyQueryNestedPath(query, indexState, queryNestedPath);
    }

    query = indexSearcher.rewrite(query);

    if (boost != DEFAULT_BOOST) {
      query = new BoostQuery(query, boost);
    }

    int topHits = textRetriever.getTopHits();
    TopDocs topDocs =
        indexSearcher.search(
            query, new TopScoreDocCollectorManager(topHits, null, totalHitsThreshold));

    double timeTakenMs = (System.nanoTime() - startNs) / 1_000_000.0;
    return new RetrieverResult(name, topDocs, timeTakenMs);
  }
}
