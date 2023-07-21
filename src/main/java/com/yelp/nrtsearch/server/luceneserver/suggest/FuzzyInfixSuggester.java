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
package com.yelp.nrtsearch.server.luceneserver.suggest;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.suggest.BitsProducer;
import org.apache.lucene.search.suggest.document.CompletionQuery;
import org.apache.lucene.search.suggest.document.FuzzyCompletionQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.Operations;

/**
 * Suggests fuzzy-matched suggest items.
 *
 * <p>The similarity is measured by default Damerau-Levenshtein algorithm or classic Levenshtein
 * algorithm with false value in transpositions.
 *
 * <p>The maximum edit distance is controlled by maxEdit variable. It is recommended to set maxEdit
 * as 1 for faster lookup.
 */
public class FuzzyInfixSuggester extends CompletionInfixSuggester {

  private static final BitsProducer DEFAULT_BITS_PRODUCER = null;
  private int minFuzzyLength;
  private int nonFuzzyPrefix;
  private int maxEdits;
  private boolean transpositions;
  private boolean unicodeAware;

  public FuzzyInfixSuggester(
      Directory dir,
      Analyzer indexAnalyzer,
      Analyzer queryAnalyzer,
      int maxEdits,
      boolean transpositions,
      int nonFuzzyPrefix,
      int minFuzzyLength,
      boolean unicodeAware)
      throws IOException {
    super(dir, indexAnalyzer, queryAnalyzer);
    if (maxEdits < 0 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
      throw new IllegalArgumentException(
          "maxEdits must be between 0 and " + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE);
    }
    if (nonFuzzyPrefix < 0) {
      throw new IllegalArgumentException(
          "nonFuzzyPrefix must not be >= 0 (got " + nonFuzzyPrefix + ")");
    }
    if (minFuzzyLength < 0) {
      throw new IllegalArgumentException(
          "minFuzzyLength must not be >= 0 (got " + minFuzzyLength + ")");
    }

    this.maxEdits = maxEdits;
    this.transpositions = transpositions;
    this.nonFuzzyPrefix = nonFuzzyPrefix;
    this.minFuzzyLength = minFuzzyLength;
    this.unicodeAware = unicodeAware;
  }

  public FuzzyInfixSuggester(Directory dir, Analyzer analyzer) throws IOException {
    super(dir, analyzer, analyzer);
    this.maxEdits = FuzzyCompletionQuery.DEFAULT_MAX_EDITS;
    this.transpositions = FuzzyCompletionQuery.DEFAULT_TRANSPOSITIONS;
    this.nonFuzzyPrefix = FuzzyCompletionQuery.DEFAULT_NON_FUZZY_PREFIX;
    this.minFuzzyLength = FuzzyCompletionQuery.DEFAULT_MIN_FUZZY_LENGTH;
    this.unicodeAware = FuzzyCompletionQuery.DEFAULT_UNICODE_AWARE;
  }

  @Override
  protected CompletionQuery createCompletionQuery(CharSequence key) {
    return new FuzzyCompletionQuery(
        queryAnalyzer,
        new Term(SEARCH_TEXT_FIELD_NAME, new BytesRef(key)),
        DEFAULT_BITS_PRODUCER,
        maxEdits,
        transpositions,
        nonFuzzyPrefix,
        minFuzzyLength,
        unicodeAware,
        Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
  }
}
