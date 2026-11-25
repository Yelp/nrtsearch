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
package com.yelp.nrtsearch.server.search.multiretriever;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/**
 * Holds the queries of a single retriever: the query as parsed from the request, which is known
 * when the retriever is planned, and the rewritten query, which is derived from the parsed query by
 * {@link #rewrite(IndexSearcher)}. Reading the rewritten query before it is derived fails, it never
 * triggers a rewrite.
 *
 * <p>Thread safe. Rewrites are serialized, so the parsed query is rewritten at most once, and the
 * rewritten query is published safely to threads that read it.
 */
public class RetrieverQuery {
  private final Query parsedQuery;

  /** Set once by {@link #rewrite(IndexSearcher)}, never changed after that. */
  private volatile Query rewrittenQuery;

  /**
   * @param parsedQuery query as parsed from the request, before rewrite
   */
  public RetrieverQuery(Query parsedQuery) {
    this.parsedQuery = Objects.requireNonNull(parsedQuery);
  }

  /** Get the query as parsed from the request, before rewrite. */
  public Query getParsedQuery() {
    return parsedQuery;
  }

  /** Check if the rewritten query has been derived. */
  public boolean isRewritten() {
    return rewrittenQuery != null;
  }

  /**
   * Get the rewritten query.
   *
   * @throws IllegalStateException if {@link #rewrite(IndexSearcher)} has not completed yet
   */
  public Query getRewrittenQuery() {
    if (!isRewritten()) {
      throw new IllegalStateException("Rewritten query has not been set");
    }
    // Write-once, so the value cannot change once isRewritten() has returned true
    return rewrittenQuery;
  }

  /**
   * Derive the rewritten query by rewriting the parsed query with the given searcher. Write-once:
   * the parsed query is rewritten at most once. Concurrent callers are serialized, so a second call
   * fails without rewriting again.
   *
   * @param searcher searcher to rewrite the parsed query with
   * @return the rewritten query
   * @throws IllegalStateException if the query was already rewritten
   * @throws IOException if the rewrite fails
   */
  public synchronized Query rewrite(IndexSearcher searcher) throws IOException {
    Objects.requireNonNull(searcher);
    if (isRewritten()) {
      throw new IllegalStateException("Rewritten query has already been set");
    }
    Query rewritten = searcher.rewrite(parsedQuery);
    rewrittenQuery = rewritten;
    return rewritten;
  }
}
