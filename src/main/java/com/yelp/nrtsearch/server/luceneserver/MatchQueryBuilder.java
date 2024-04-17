/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.FuzzyParams;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;

public class MatchQueryBuilder extends QueryBuilder {

  private final int DEFAULT_LOW = 3;
  private final int DEFAULT_HIGH = 6;
  int prefixLength;
  int maxExpansions;
  boolean transpositions;
  FuzzyParams fuzzyParams;

  public MatchQueryBuilder(Analyzer analyzer, FuzzyParams fuzzyParams) {
    super(analyzer);
    this.fuzzyParams = fuzzyParams;
    this.prefixLength = fuzzyParams.getPrefixLength();
    this.maxExpansions = fuzzyParams.getMaxExpansions();
    this.transpositions = fuzzyParams.getTranspositions();
  }

  @Override
  protected Query newTermQuery(Term term) {
    FuzzyParams.FuzzinessCase fuzziness = fuzzyParams.getFuzzinessCase();
    int maxEdits;
    switch (fuzziness) {
      case AUTO -> maxEdits = computeMaxEditsFromTermLength(term);
      case MAXEDITS -> maxEdits = fuzzyParams.getMaxEdits();
      default -> {
        return super.newTermQuery(term); // If fuzziness is not set
      }
    }
    if (maxEdits == 0) {
      return super.newTermQuery(term);
    } else {
      return new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions);
    }
  }

  protected int computeMaxEditsFromTermLength(Term term) {
    int maxEdits;
    int low = fuzzyParams.getAuto().getLow();
    int high = fuzzyParams.getAuto().getHigh();
    int termLength = term.bytes().length;
    // If both values are not set, use default values
    if (low == 0 && high == 0) {
      low = DEFAULT_LOW;
      high = DEFAULT_HIGH;
    }
    if (low < 0) {
      throw new IllegalArgumentException("AutoFuzziness low value cannot be negative");
    }
    if (low >= high) {
      throw new IllegalArgumentException("AutoFuzziness low value should be < high value");
    }
    if (termLength >= 0 && termLength < low) {
      maxEdits = 0;
    } else if (termLength >= low && termLength < high) {
      maxEdits = 1;
    } else {
      maxEdits = 2;
    }
    return maxEdits;
  }
}
