/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.query;

import com.yelp.nrtsearch.server.query.multifunction.MultiFunctionScoreQuery;
import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

/**
 * A query wrapper that filters documents based on a minimum score threshold. Only documents with
 * scores greater than or equal to the specified threshold will be included in the results. This is
 * useful for filtering out low-relevance documents from search results.
 *
 * <p>The query wraps another query and applies the threshold filter during scoring. Documents that
 * don't meet the threshold are excluded from the results entirely.
 */
public class MinThresholdQuery extends Query {
  private final Query wrapped;
  private final float threshold;

  /**
   * Creates a new MinThresholdQuery that wraps the given query with a minimum score threshold.
   *
   * @param wrapped the query to wrap and apply threshold filtering to
   * @param threshold the minimum score threshold; documents with scores below this value will be
   *     filtered out
   */
  public MinThresholdQuery(Query wrapped, float threshold) {
    this.wrapped = wrapped;
    this.threshold = threshold;
  }

  /**
   * Returns the wrapped query that this MinThresholdQuery is filtering.
   *
   * @return the wrapped query
   */
  public Query getWrapped() {
    return wrapped;
  }

  /**
   * Rewrites this query by rewriting the wrapped query. If the wrapped query rewrites to a
   * MatchNoDocsQuery, returns that directly since no documents will match anyway. Otherwise,
   * returns a new MinThresholdQuery with the rewritten wrapped query.
   *
   * @param searcher the IndexSearcher to use for rewriting
   * @return the rewritten query
   * @throws IOException if an I/O error occurs during rewriting
   */
  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    Query rewrittenWrappedQuery = wrapped.rewrite(searcher);
    if (rewrittenWrappedQuery instanceof MatchNoDocsQuery) {
      return rewrittenWrappedQuery;
    }
    if (rewrittenWrappedQuery == wrapped) {
      return this;
    }
    return new MinThresholdQuery(rewrittenWrappedQuery, threshold);
  }

  /**
   * Creates a Weight for this query that applies the minimum threshold filtering. The Weight
   * delegates to the wrapped query's Weight but filters out documents that don't meet the minimum
   * score threshold.
   *
   * @param searcher the IndexSearcher to create the weight for
   * @param scoreMode the scoring mode (exhaustive or top scores)
   * @param boost the boost factor to apply to scores
   * @return a Weight that applies threshold filtering
   * @throws IOException if an I/O error occurs during weight creation
   */
  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    final Weight wrappedWeight;
    if (scoreMode.isExhaustive()) {
      wrappedWeight = wrapped.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
    } else {
      wrappedWeight = wrapped.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);
    }
    return new MinValueWeight(wrappedWeight, threshold, this, boost);
  }

  /**
   * Returns a string representation of this query for debugging purposes.
   *
   * @param field the field name to use in the string representation
   * @return a string representation showing the wrapped query and threshold
   */
  @Override
  public String toString(String field) {
    return "MinThresholdQuery["
        + "wrapped="
        + wrapped.toString(field)
        + ", threshold="
        + threshold
        + ']';
  }

  /**
   * Accepts a QueryVisitor to traverse this query. This query is treated as a leaf node in the
   * query tree for visiting purposes.
   *
   * @param queryVisitor the visitor to accept
   */
  @Override
  public void visit(QueryVisitor queryVisitor) {
    queryVisitor.visitLeaf(this);
  }

  /**
   * Checks if this query is equal to another object. Two MinThresholdQuery instances are equal if
   * they have the same threshold and wrapped query.
   *
   * @param o the object to compare with
   * @return true if the objects are equal, false otherwise
   */
  @Override
  public boolean equals(Object o) {
    if (!sameClassAs(o)) {
      return false;
    }
    MinThresholdQuery other = (MinThresholdQuery) o;
    return threshold == other.threshold && Objects.equals(wrapped, other.wrapped);
  }

  /**
   * Returns a hash code for this query based on the wrapped query and threshold.
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return Objects.hash(wrapped, threshold);
  }

  /**
   * A Weight implementation that applies minimum threshold filtering to the wrapped Weight. This
   * Weight delegates most operations to the wrapped Weight but filters out documents that don't
   * meet the minimum score threshold during scoring and explanation.
   */
  static class MinValueWeight extends FilterWeight {
    private final float threshold;
    private final float boost;

    /**
     * Creates a new MinValueWeight that wraps the given Weight with threshold filtering.
     *
     * @param wrappedWeight the Weight to wrap and filter
     * @param threshold the minimum score threshold for filtering
     * @param query the query this Weight belongs to
     * @param boost the boost factor to apply to scores
     */
    protected MinValueWeight(Weight wrappedWeight, float threshold, Query query, float boost) {
      super(query, wrappedWeight);
      this.threshold = threshold;
      this.boost = boost;
    }

    /**
     * Explains why a document matches or doesn't match this query. If the wrapped query matches but
     * the score is below the threshold, returns a no-match explanation. If the score meets the
     * threshold, returns a match explanation with the boosted score.
     *
     * @param context the leaf reader context
     * @param doc the document ID to explain
     * @return an Explanation describing the match status and score calculation
     * @throws IOException if an I/O error occurs during explanation
     */
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Explanation explanation = in.explain(context, doc);
      if (explanation.isMatch()) {
        float score = explanation.getValue().floatValue();
        if (score >= threshold) {
          return Explanation.match(
              explanation.getValue().floatValue() * boost,
              "document score exceeds threshold",
              explanation);
        } else {
          return Explanation.noMatch(
              String.format(
                  "document found, but score [%f] is less than matching minimum threshold [%f]",
                  explanation.getValue().floatValue(), threshold),
              explanation);
        }
      }
      return explanation;
    }

    /**
     * Returns a ScorerSupplier that provides Scorers with minimum threshold filtering. The returned
     * ScorerSupplier wraps the underlying scorer with a MinScoreWrapper that filters out documents
     * below the threshold.
     *
     * @param context the leaf reader context
     * @return a ScorerSupplier that applies threshold filtering, or null if no scorer is available
     * @throws IOException if an I/O error occurs during scorer creation
     */
    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      ScorerSupplier inScorerSupplier = in.scorerSupplier(context);
      if (inScorerSupplier == null) {
        return null;
      }
      return new ScorerSupplier() {
        @Override
        public Scorer get(long leadCost) throws IOException {
          return new MultiFunctionScoreQuery.MinScoreWrapper(
              inScorerSupplier.get(leadCost), threshold, false, boost);
        }

        @Override
        public long cost() {
          return inScorerSupplier.cost();
        }
      };
    }
  }
}
