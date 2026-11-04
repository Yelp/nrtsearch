/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.tools.vector_bench;

import java.util.Arrays;
import java.util.Locale;

/**
 * Records request latencies and reports exact percentiles. Keeps every sample, which is fine for
 * benchmark runs of up to tens of millions of requests. Thread safe.
 */
public class LatencyRecorder {
  private long[] samples = new long[1024];
  private int count;
  private long errors;

  public synchronized void record(long nanos) {
    if (count == samples.length) {
      samples = Arrays.copyOf(samples, samples.length * 2);
    }
    samples[count++] = nanos;
  }

  public synchronized void recordError() {
    errors++;
  }

  public synchronized int count() {
    return count;
  }

  public synchronized long errors() {
    return errors;
  }

  /** Summary of the recorded latencies. */
  public synchronized Summary summary() {
    long[] sorted = Arrays.copyOf(samples, count);
    Arrays.sort(sorted);
    return new Summary(sorted, errors);
  }

  /** Remove all samples, for example after a warmup period. */
  public synchronized void reset() {
    count = 0;
    errors = 0;
  }

  /** Latency percentiles in milliseconds. */
  public record Summary(long[] sortedNanos, long errors) {
    public int count() {
      return sortedNanos.length;
    }

    public double percentileMs(double p) {
      if (sortedNanos.length == 0) {
        return Double.NaN;
      }
      int index = (int) Math.ceil(p / 100.0 * sortedNanos.length) - 1;
      index = Math.max(0, Math.min(sortedNanos.length - 1, index));
      return sortedNanos[index] / 1e6;
    }

    public double meanMs() {
      if (sortedNanos.length == 0) {
        return Double.NaN;
      }
      double total = 0;
      for (long n : sortedNanos) {
        total += n;
      }
      return total / sortedNanos.length / 1e6;
    }

    public double maxMs() {
      return sortedNanos.length == 0 ? Double.NaN : sortedNanos[sortedNanos.length - 1] / 1e6;
    }

    /** One line summary, for example "n=1000 err=0 mean=4.1 p50=3.8 p90=6.0 p99=11.2 max=20.3". */
    public String format() {
      return String.format(
          Locale.ROOT,
          "n=%d err=%d mean=%.2fms p50=%.2fms p90=%.2fms p99=%.2fms p99.9=%.2fms max=%.2fms",
          count(),
          errors,
          meanMs(),
          percentileMs(50),
          percentileMs(90),
          percentileMs(99),
          percentileMs(99.9),
          maxMs());
    }
  }
}
