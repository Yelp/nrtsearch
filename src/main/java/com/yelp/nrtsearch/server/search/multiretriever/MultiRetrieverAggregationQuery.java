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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;

/**
 * Query used for the multi-retriever aggregation pass. Wraps a {@link DocIdSetCache} and a fallback
 * union query. If the cache is valid (not exceeded), iterates the cached per-segment bitsets
 * directly — avoiding re-execution of the union query. If the cache was invalidated during
 * collection, rewrites to the fallback query transparently.
 *
 * <p>All matched docs receive a constant score of 0.0f since aggregation does not use scores.
 */
public class MultiRetrieverAggregationQuery extends Query {
  private final DocIdSetCache cache;
  private final Query fallback;

  /**
   * @param cache the doc ID cache populated during retriever collection (may be exceeded)
   * @param fallback the union query to use when the cache is invalid
   */
  public MultiRetrieverAggregationQuery(DocIdSetCache cache, Query fallback) {
    this.cache = cache;
    this.fallback = fallback;
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (cache.isExceeded()) {
      return fallback.rewrite(indexSearcher);
    }
    cache.merge();
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    assert cache.isMerged() && !cache.isExceeded()
        : "createWeight called without rewrite(); cache must be merged and not exceeded";

    // If scores are needed, delegate to the fallback query — the cached bitset has no scores.
    if (scoreMode.needsScores()) {
      return fallback.createWeight(searcher, scoreMode, boost);
    }

    return new ConstantScoreWeight(this, 0f) {
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        BitSet bitSet = cache.getBitSet(context.ord);
        if (bitSet == null || bitSet.cardinality() == 0) {
          return null;
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            DocIdSetIterator iterator = new BitSetIterator(bitSet, bitSet.cardinality());
            return new ConstantScoreScorer(score(), scoreMode, iterator);
          }

          @Override
          public long cost() {
            return bitSet.cardinality();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }

  @Override
  public String toString(String field) {
    return "MultiRetrieverAggregationQuery(fallback=" + fallback.toString(field) + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof MultiRetrieverAggregationQuery o)) {
      return false;
    }
    return cache == o.cache && fallback.equals(o.fallback);
  }

  @Override
  public int hashCode() {
    return 31 * System.identityHashCode(cache) + fallback.hashCode();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}
