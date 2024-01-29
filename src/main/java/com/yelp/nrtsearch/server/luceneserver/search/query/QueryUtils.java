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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.Scorable;
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

  /** Class to hold terms and positions produced by text analysis. */
  public static class TermsAndPositions {
    private final List<Term[]> termArrays;
    private final List<Integer> positions;

    /**
     * Constructor.
     *
     * @param termArrays terms by position
     * @param positions positions
     */
    public TermsAndPositions(List<Term[]> termArrays, List<Integer> positions) {
      this.termArrays = termArrays;
      this.positions = positions;
    }

    /** Get terms by position. */
    public List<Term[]> getTermArrays() {
      return termArrays;
    }

    /** Get positions. */
    public List<Integer> getPositions() {
      return positions;
    }
  }

  /**
   * {@link DoubleValues} implementation that allows the produced value to be provided by setting a
   * {@link Scorable}. Useful way to provide _score value to some lucene methods.
   */
  public static class ScorableDoubleValues extends DoubleValues {
    private Scorable scorer;

    public void setScorer(Scorable scorer) {
      this.scorer = scorer;
    }

    @Override
    public double doubleValue() throws IOException {
      return scorer.score();
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
      return scorer != null && scorer.docID() == doc;
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

  /**
   * Analyze text for a field with the given Analyzer.
   *
   * @param field field name
   * @param queryText query text
   * @param analyzer analyzer
   * @return terms and positions
   * @throws IOException
   */
  public static TermsAndPositions getTermsAndPositions(
      String field, String queryText, Analyzer analyzer) throws IOException {
    try (TokenStream stream = analyzer.tokenStream(field, queryText)) {
      return getTermsAndPositions(field, stream);
    }
  }

  /**
   * Extract terms and positions from a given analysis token stream.
   *
   * @param field field name
   * @param stream token stream
   * @return terms and positions
   * @throws IOException
   */
  public static TermsAndPositions getTermsAndPositions(String field, TokenStream stream)
      throws IOException {
    List<Term[]> termArrays = new ArrayList<>();
    List<Integer> positions = new ArrayList<>();

    List<Term> currentTerms = new ArrayList<>();
    TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
    PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
    PositionLengthAttribute posLenAtt = stream.addAttribute(PositionLengthAttribute.class);

    // no terms
    if (termAtt == null) {
      return new TermsAndPositions(termArrays, positions);
    }

    stream.reset();
    int position = -1;
    while (stream.incrementToken()) {
      if (posIncrAtt.getPositionIncrement() != 0) {
        if (!currentTerms.isEmpty()) {
          termArrays.add(currentTerms.toArray(new Term[0]));
          positions.add(position);
        }
        position += posIncrAtt.getPositionIncrement();
        currentTerms.clear();
      }
      int positionLength = posLenAtt.getPositionLength();
      if (positionLength > 1) {
        throw new IllegalArgumentException(
            "MatchPhrasePrefixQuery does not support graph type analyzers");
      }
      currentTerms.add(new Term(field, termAtt.getBytesRef()));
    }
    // no tokens in query text
    if (position == -1) {
      return new TermsAndPositions(termArrays, positions);
    }
    termArrays.add(currentTerms.toArray(new Term[0]));
    positions.add(position);

    return new TermsAndPositions(termArrays, positions);
  }
}
