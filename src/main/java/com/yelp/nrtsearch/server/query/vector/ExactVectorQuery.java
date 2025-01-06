/*
 * Copyright 2024 Yelp Inc.
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
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.Weight;

/**
 * Query that does a brute force search to find documents with the best similarity to the query
 * vector.
 */
public abstract class ExactVectorQuery extends Query {
  protected final String field;
  private final VectorScorerSupplier vectorScorerFunction;

  /** Function for creating a {@link VectorScorer} for a given {@link LeafReader}. */
  @FunctionalInterface
  protected interface VectorScorerSupplier {
    /**
     * Get a {@link VectorScorer} for the given {@link LeafReader}.
     *
     * @param reader the reader to get the scorer for
     * @return the scorer
     * @throws IOException if an error occurs
     */
    VectorScorer get(LeafReader reader) throws IOException;
  }

  /**
   * Constructor.
   *
   * @param field the field to search
   * @param vectorScorerFunction function for creating a {@link VectorScorer} for a given {@link
   *     LeafReader}
   */
  protected ExactVectorQuery(String field, VectorScorerSupplier vectorScorerFunction) {
    this.field = field;
    this.vectorScorerFunction = vectorScorerFunction;
  }

  /**
   * Get the field to search.
   *
   * @return the field
   */
  public String getField() {
    return field;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new Weight(this) {
      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        VectorScorer vectorScorer = vectorScorerFunction.get(context.reader());
        if (vectorScorer == null) {
          return Explanation.noMatch("No vector found for field: " + field);
        }
        DocIdSetIterator iterator = vectorScorer.iterator();
        if (iterator.advance(doc) == doc) {
          float score = vectorScorer.score();
          return Explanation.match(score, "Found vector with similarity: " + score);
        }
        return Explanation.noMatch("No document vector for field: " + field);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        VectorScorer vectorScorer = vectorScorerFunction.get(context.reader());
        if (vectorScorer == null) {
          return null;
        }
        return new VectorValuesScorer(this, vectorScorer);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  /**
   * Scorer that scores documents based on the similarity of the document vector to the query
   * vector.
   */
  private static class VectorValuesScorer extends Scorer {
    private final VectorScorer vectorScorer;
    private final DocIdSetIterator iterator;

    /**
     * Constructor.
     *
     * @param weight the weight that created the scorer
     * @param vectorScorer the vector scorer to use
     */
    protected VectorValuesScorer(Weight weight, VectorScorer vectorScorer) {
      super(weight);
      this.vectorScorer = vectorScorer;
      this.iterator = vectorScorer.iterator();
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return Float.POSITIVE_INFINITY;
    }

    @Override
    public float score() throws IOException {
      return vectorScorer.score();
    }

    @Override
    public int docID() {
      return iterator.docID();
    }
  }

  /**
   * Query that does a brute force search to find documents with the best similarity to the query
   * vector, for fields using the float element type.
   */
  public static class ExactFloatVectorQuery extends ExactVectorQuery {
    private final float[] queryVector;

    /**
     * Constructor.
     *
     * @param field the field to search
     * @param queryVector the query vector
     */
    public ExactFloatVectorQuery(String field, float[] queryVector) {
      super(field, reader -> reader.getFloatVectorValues(field).scorer(queryVector));
      this.queryVector = queryVector;
    }

    @Override
    public String toString(String field) {
      return "ExactFloatVectorQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!sameClassAs(obj)) {
        return false;
      }
      ExactFloatVectorQuery other = (ExactFloatVectorQuery) obj;
      return Objects.equals(field, other.field) && Arrays.equals(queryVector, other.queryVector);
    }

    @Override
    public int hashCode() {
      return Objects.hash(classHash(), field, Arrays.hashCode(queryVector));
    }
  }

  /**
   * Query that does a brute force search to find documents with the best similarity to the query
   * vector, for fields using the byte element type.
   */
  public static class ExactByteVectorQuery extends ExactVectorQuery {
    private final byte[] queryVector;

    /**
     * Constructor.
     *
     * @param field the field to search
     * @param queryVector the query vector
     */
    public ExactByteVectorQuery(String field, byte[] queryVector) {
      super(field, reader -> reader.getByteVectorValues(field).scorer(queryVector));
      this.queryVector = queryVector;
    }

    @Override
    public String toString(String field) {
      return "ExactByteVectorQuery";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!sameClassAs(obj)) {
        return false;
      }
      ExactByteVectorQuery other = (ExactByteVectorQuery) obj;
      return Objects.equals(field, other.field) && Arrays.equals(queryVector, other.queryVector);
    }

    @Override
    public int hashCode() {
      return Objects.hash(classHash(), field, Arrays.hashCode(queryVector));
    }
  }
}
