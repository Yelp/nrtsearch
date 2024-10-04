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
package com.yelp.nrtsearch.server.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;

/**
 * An {@link AnalyzerWrapper} that wraps another analyzer and sets a fixed position increment gap.
 */
public class PosIncGapAnalyzerWrapper extends AnalyzerWrapper {
  private final Analyzer delegate;
  private final int posIncGap;

  /**
   * Create a new {@link PosIncGapAnalyzerWrapper} that wraps the given {@link Analyzer} and sets
   * the given position increment gap.
   *
   * @param delegate the analyzer to wrap
   * @param posIncGap the position increment gap to set
   * @throws IllegalArgumentException if {@code posIncGap} is negative
   */
  public PosIncGapAnalyzerWrapper(Analyzer delegate, int posIncGap) {
    super(delegate.getReuseStrategy());
    this.delegate = delegate;
    this.posIncGap = posIncGap;
    if (posIncGap < 0) {
      throw new IllegalArgumentException("posIncGap must be >= 0");
    }
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return delegate;
  }

  @Override
  public int getPositionIncrementGap(String fieldName) {
    return posIncGap;
  }

  @Override
  public String toString() {
    return "PosIncGapAnalyzerWrapper(" + delegate.toString() + ")";
  }
}
