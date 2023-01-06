/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.search.query;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.Bits;

/** Class to hold helper methods for developing lucene queries. */
public class QueryUtils {

  private QueryUtils() {}

  /**
   * {@link DoubleValues} implementation that allows the produced value to be externally set. Useful
   * way to provide _score value to some lucene methods.
   */
  public static class SettableDoubleValues extends DoubleValues {
    private double value = 0.0;

    public void setValue(double value) {
      this.value = value;
    }

    @Override
    public double doubleValue() throws IOException {
      return value;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
      return true;
    }
  }

  /**
   * Given a {@link ScorerSupplier}, return a {@link Bits} instance that will match all documents
   * contained in the set. Note that the returned {@link Bits} instance MUST be consumed in order.
   * source: <a
   * href="https://github.com/elastic/elasticsearch/blob/v7.2.0/server/src/main/java/org/elasticsearch/common/lucene/Lucene.java#L826">Lucene</a>
   */
  public static Bits asSequentialAccessBits(final int maxDoc, ScorerSupplier scorerSupplier)
      throws IOException {
    if (scorerSupplier == null) {
      return new Bits.MatchNoBits(maxDoc);
    }
    // Since we want bits, we need random-access
    final Scorer scorer = scorerSupplier.get(Long.MAX_VALUE); // this never returns null
    final TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
    final DocIdSetIterator iterator;
    if (twoPhase == null) {
      iterator = scorer.iterator();
    } else {
      iterator = twoPhase.approximation();
    }

    return new Bits() {

      int previous = -1;
      boolean previousMatched = false;

      @Override
      public boolean get(int index) {
        if (index < 0 || index >= maxDoc) {
          throw new IndexOutOfBoundsException(
              index + " is out of bounds: [" + 0 + "-" + maxDoc + "[");
        }
        if (index < previous) {
          throw new IllegalArgumentException(
              "This Bits instance can only be consumed in order. "
                  + "Got called on ["
                  + index
                  + "] while previously called on ["
                  + previous
                  + "]");
        }
        if (index == previous) {
          // we cache whether it matched because it is illegal to call
          // twoPhase.matches() twice
          return previousMatched;
        }
        previous = index;

        int doc = iterator.docID();
        if (doc < index) {
          try {
            doc = iterator.advance(index);
          } catch (IOException e) {
            throw new IllegalStateException("Cannot advance iterator", e);
          }
        }
        if (index == doc) {
          try {
            return previousMatched = twoPhase == null || twoPhase.matches();
          } catch (IOException e) {
            throw new IllegalStateException("Cannot validate match", e);
          }
        }
        return previousMatched = false;
      }

      @Override
      public int length() {
        return maxDoc;
      }
    };
  }
}
