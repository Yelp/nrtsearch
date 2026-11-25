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

import com.yelp.nrtsearch.server.search.multiretriever.MultiRetrieverContext;
import java.util.Objects;
import org.apache.lucene.search.Query;

/**
 * Holds the final query of a search, the one that is executed. It is either given the fully
 * rewritten query, or derived from the rewritten queries of the retrievers of a multi retriever
 * request. Deriving only combines queries that are already rewritten, it never triggers a rewrite.
 * The derived query is built once, by the first reader, and cached. Reading it before every
 * retriever has its rewritten query fails.
 */
public class FinalQuery {
  private final MultiRetrieverContext multiRetrieverContext;
  private volatile Query query;

  private FinalQuery(Query query, MultiRetrieverContext multiRetrieverContext) {
    this.query = query;
    this.multiRetrieverContext = multiRetrieverContext;
  }

  /** Final query that is already fully rewritten, as for a request without retrievers. */
  public static FinalQuery of(Query query) {
    Objects.requireNonNull(query);
    return new FinalQuery(query, null);
  }

  /**
   * Final query derived from the rewritten queries of the retrievers.
   *
   * @param multiRetrieverContext retrievers to union, in request order
   */
  public static FinalQuery unionOf(MultiRetrieverContext multiRetrieverContext) {
    Objects.requireNonNull(multiRetrieverContext);
    return new FinalQuery(null, multiRetrieverContext);
  }

  /**
   * Get the final query.
   *
   * @throws IllegalStateException if a retriever has no rewritten query yet
   */
  public Query get() {
    Query result = query;
    if (result == null) {
      synchronized (this) {
        result = query;
        if (result == null) {
          result = multiRetrieverContext.unionRewrittenQueries();
          query = result;
        }
      }
    }
    return result;
  }
}
