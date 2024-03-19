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
package com.yelp.nrtsearch.server.luceneserver.warming;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple implementation of <a href="https://en.wikipedia.org/wiki/Reservoir_sampling">reservoir
 * sampling</a> used for having a set of warming queries.
 */
public class ReservoirSampler {

  private static final SampleResult REJECTED_RESULT = new SampleResult(false, 0);

  private final long maxQueries;
  private final AtomicLong numQueries;

  /**
   * @param maxQueries Maximum number of warming queries.
   */
  public ReservoirSampler(long maxQueries) {
    this.maxQueries = maxQueries;
    this.numQueries = new AtomicLong(0);
  }

  /**
   * Decide if a query should be included for warming or not.
   *
   * @return a {@link SampleResult}. If the query must be included, {@link SampleResult#isSample()}
   *     is {@code true} and {@link SampleResult#getReplace()} points to the index of query in the
   *     list to replace current query with. If the query must not be included then {@code isSample}
   *     returns {@code false}.
   */
  public SampleResult sample() {
    long numQueries = this.numQueries.getAndIncrement();
    if (numQueries < maxQueries) {
      return new SampleResult(true, (int) numQueries);
    }
    int replace = (int) ThreadLocalRandom.current().nextLong(numQueries);
    if (replace < maxQueries) {
      return new SampleResult(true, replace);
    }
    return REJECTED_RESULT;
  }

  public static class SampleResult {
    private final boolean sample;
    private final int replace;

    public SampleResult(boolean sample, int replace) {
      this.sample = sample;
      this.replace = replace;
    }

    public boolean isSample() {
      return sample;
    }

    public int getReplace() {
      return replace;
    }
  }
}
