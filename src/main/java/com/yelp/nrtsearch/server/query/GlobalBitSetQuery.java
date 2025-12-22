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

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;

public class GlobalBitSetQuery extends Query {
  private final FixedBitSet globalBitSet;

  public GlobalBitSetQuery(FixedBitSet globalBitSet) {
    this.globalBitSet = globalBitSet;
  }

  @Override
  public String toString(String field) {
    return "A Query that wraps BitSet: " + globalBitSet.toString();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {

      // --- 1. Implement scorerSupplier (The Modern Way) ---
      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        int docBase = context.docBase;
        int maxDoc = context.reader().maxDoc();

        // Optimization: Check intersection efficiently before creating any objects.
        // If the Global BitSet has no bits in this segment's range, return null.
        int firstGlobal = globalBitSet.nextSetBit(docBase);
        if (firstGlobal == DocIdSetIterator.NO_MORE_DOCS || firstGlobal >= docBase + maxDoc) {
          return null;
        }

        // Return the supplier
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) {
            // Create the actual iterator only when requested
            return new ConstantScoreScorer(
                score(), scoreMode, new SegmentBitSetIterator(globalBitSet, docBase, maxDoc));
          }

          @Override
          public long cost() {
            return globalBitSet.cardinality();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }

  // NOOP
  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  private static class SegmentBitSetIterator extends DocIdSetIterator {
    private final FixedBitSet globalBitSet;
    private final int docBase;
    private final int maxDoc;
    private int currentDoc = -1;

    SegmentBitSetIterator(FixedBitSet globalBitSet, int docBase, int maxDoc) {
      this.globalBitSet = globalBitSet;
      this.docBase = docBase;
      this.maxDoc = maxDoc;
    }

    @Override
    public int docID() {
      return currentDoc;
    }

    @Override
    public int nextDoc() {
      // "docBase + currentDoc + 1" is the next Global ID to check
      int nextGlobal = globalBitSet.nextSetBit(docBase + currentDoc + 1);

      if (nextGlobal == NO_MORE_DOCS || nextGlobal >= docBase + maxDoc) {
        return currentDoc = NO_MORE_DOCS;
      }
      return currentDoc = (nextGlobal - docBase);
    }

    @Override
    public int advance(int target) {
      int nextGlobal = globalBitSet.nextSetBit(docBase + target);
      if (nextGlobal == NO_MORE_DOCS || nextGlobal >= docBase + maxDoc) {
        return currentDoc = NO_MORE_DOCS;
      }
      return currentDoc = (nextGlobal - docBase);
    }

    @Override
    public long cost() {
      return maxDoc;
    }
  }
}
