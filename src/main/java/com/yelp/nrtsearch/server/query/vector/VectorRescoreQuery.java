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

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.RescoreTopNQuery;
import org.apache.lucene.search.TotalHits;

/**
 * Query that rescores the hits of a knn vector query using a {@link DoubleValuesSource}, keeping
 * only the top n rescored hits. This is intended to rescore an oversampled quantized vector search
 * using the full precision vectors. Rewriting this query executes both the vector search and the
 * rescoring, using {@link RescoreTopNQuery}.
 *
 * <p>The {@link TotalHits} of the wrapped vector query are available through the {@link
 * WithVectorTotalHits} interface after rewrite.
 */
public class VectorRescoreQuery extends Query implements WithVectorTotalHits {
  private final Query knnQuery;
  private final DoubleValuesSource valuesSource;
  private final int n;

  /**
   * Constructor.
   *
   * @param knnQuery knn query producing the hits to rescore
   * @param valuesSource source of the new hit scores
   * @param n number of top rescored hits to keep
   */
  public VectorRescoreQuery(Query knnQuery, DoubleValuesSource valuesSource, int n) {
    if (n < 1) {
      throw new IllegalArgumentException("n must be >= 1");
    }
    this.knnQuery = Objects.requireNonNull(knnQuery);
    this.valuesSource = Objects.requireNonNull(valuesSource);
    this.n = n;
  }

  /** Get the wrapped knn query. */
  public Query getKnnQuery() {
    return knnQuery;
  }

  /** Get the number of top rescored hits to keep. */
  public int getN() {
    return n;
  }

  @Override
  public TotalHits getTotalHits() {
    if (knnQuery instanceof WithVectorTotalHits withVectorTotalHits) {
      return withVectorTotalHits.getTotalHits();
    }
    return null;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    Query rewrittenKnnQuery = knnQuery.rewrite(indexSearcher);
    // Preserve MatchNoDocsQuery, which signals that the vector search was skipped
    if (rewrittenKnnQuery instanceof MatchNoDocsQuery) {
      return rewrittenKnnQuery;
    }
    return new RescoreTopNQuery(rewrittenKnnQuery, valuesSource, n).rewrite(indexSearcher);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    knnQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName()
        + ":"
        + knnQuery.toString(field)
        + ":"
        + valuesSource
        + "["
        + n
        + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!sameClassAs(obj)) {
      return false;
    }
    VectorRescoreQuery other = (VectorRescoreQuery) obj;
    return n == other.n
        && Objects.equals(knnQuery, other.knnQuery)
        && Objects.equals(valuesSource, other.valuesSource);
  }

  @Override
  public int hashCode() {
    return Objects.hash(classHash(), knnQuery, valuesSource, n);
  }
}
