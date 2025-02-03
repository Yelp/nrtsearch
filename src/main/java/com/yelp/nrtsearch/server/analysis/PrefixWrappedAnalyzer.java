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
package com.yelp.nrtsearch.server.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;

/**
 * An {@link AnalyzerWrapper} that wraps another analyzer and applies an Edge N-Gram token filter to
 * the token stream.
 */
public class PrefixWrappedAnalyzer extends AnalyzerWrapper {
  private final int minChars;
  private final int maxChars;
  private final Analyzer delegate;

  /**
   * Create a new {@link PrefixWrappedAnalyzer} that wraps the given {@link Analyzer} and sets
   * applies an Edge N-Gram token filter to the token stream.
   *
   * @param delegate the analyzer to wrap
   * @param minChars the minimum number of characters for the edge n-grams
   * @param maxChars the maximum number of characters for the edge n-grams
   */
  public PrefixWrappedAnalyzer(Analyzer delegate, int minChars, int maxChars) {
    super(delegate.getReuseStrategy());
    this.delegate = delegate;
    this.minChars = minChars;
    this.maxChars = maxChars;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return delegate;
  }

  @Override
  protected TokenStreamComponents wrapComponents(
      String fieldName, TokenStreamComponents components) {
    TokenFilter filter =
        new EdgeNGramTokenFilter(components.getTokenStream(), minChars, maxChars, false);
    return new TokenStreamComponents(components.getSource(), filter);
  }

  @Override
  public String toString() {
    return "PrefixWrappedAnalyzer(" + delegate.toString() + ")";
  }
}
