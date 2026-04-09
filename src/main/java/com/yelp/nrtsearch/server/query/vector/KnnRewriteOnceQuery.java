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
package com.yelp.nrtsearch.server.query.vector;

import com.yelp.nrtsearch.server.query.MinThresholdQuery;
import java.io.IOException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * A {@link Query} wrapper that executes the underlying KNN vector search at most once per {@link
 * IndexSearcher} by caching the rewrite result alongside the searcher that produced it.
 *
 * <p>KNN queries (e.g. {@link NrtKnnFloatVectorQuery}) perform the expensive approximate nearest
 * neighbour search inside {@code rewrite()}. When such a query is embedded in a union (e.g. a
 * {@code BooleanQuery}) the union's {@code rewrite()} propagates down to every clause. If the union
 * is rewritten more than once with the same searcher — for example, once manually before submission
 * and once internally by {@link IndexSearcher} — the vector search would run each time without this
 * wrapper. The cache is invalidated whenever a different {@code IndexSearcher} instance is seen, so
 * reusing the same wrapper across shards or requests is safe: each distinct searcher triggers
 * exactly one KNN execution.
 *
 * <p>Thread safety: both cached fields are {@code volatile} and guarded by double-checked locking,
 * so concurrent first-time rewrites on the same instance are safe.
 *
 * <p>Use {@link #getWrapped()} to access the original KNN query for diagnostics (e.g. {@link
 * WithVectorTotalHits}).
 */
public class KnnRewriteOnceQuery extends Query {

  private final Query knnQuery;
  private volatile IndexSearcher cachedSearcher;
  private volatile Query rewritten;

  /**
   * @param knnQuery the KNN query whose {@code rewrite()} result should be cached; must be a {@link
   *     WithVectorTotalHits} instance, or a {@link MinThresholdQuery} wrapping one
   * @throws IllegalArgumentException if {@code knnQuery} is not a recognised KNN vector query
   */
  public KnnRewriteOnceQuery(Query knnQuery) {
    Query inner = knnQuery instanceof MinThresholdQuery mtq ? mtq.getWrapped() : knnQuery;
    if (!(inner instanceof WithVectorTotalHits)) {
      throw new IllegalArgumentException(
          "Expected a KNN vector query (WithVectorTotalHits), got: "
              + knnQuery.getClass().getName());
    }
    this.knnQuery = knnQuery;
  }

  /** Returns the original wrapped KNN query, useful for diagnostics. */
  public Query getWrapped() {
    return knnQuery;
  }

  /**
   * Returns the cached rewrite result if {@code searcher} is the same instance that produced it.
   * Otherwise executes the wrapped KNN query's rewrite, caches the result against {@code searcher},
   * and returns it. Subsequent calls with the same searcher instance skip the KNN execution.
   */
  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    if (cachedSearcher != searcher) {
      synchronized (this) {
        if (cachedSearcher != searcher) {
          rewritten = knnQuery.rewrite(searcher);
          cachedSearcher = searcher;
        }
      }
    }
    return rewritten;
  }

  /**
   * Rewrites via {@link #rewrite} (using the cache when possible) then delegates {@code
   * createWeight} to the result. This correctly handles the case where {@code createWeight} is
   * called before {@code rewrite}: KNN queries do not implement {@code createWeight} on the
   * unrewritten form and would throw if called directly.
   */
  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return rewrite(searcher).createWeight(searcher, scoreMode, boost);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    knnQuery.visit(visitor);
  }

  @Override
  public String toString(String field) {
    return "KnnRewriteOnce(" + knnQuery.toString(field) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (!sameClassAs(o)) return false;
    return knnQuery.equals(((KnnRewriteOnceQuery) o).knnQuery);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + knnQuery.hashCode();
  }
}
