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
package com.yelp.nrtsearch.server.remote.s3;

import java.util.concurrent.Semaphore;

/**
 * A concurrency limiter that uses AIMD (Additive Increase Multiplicative Decrease) to adapt the
 * number of concurrent operations based on observed failures.
 *
 * <p>On IO-related failures the limit is halved (multiplicative decrease, floor 1). After {@code
 * currentLimit} consecutive successes the limit is increased by 1 (additive increase, capped at the
 * configured maximum).
 *
 * <p>This is useful for S3 downloads to disk-IO-limited instances: start at a configured maximum
 * concurrency and automatically back off when disk write pressure causes failures, then gradually
 * recover as the disk catches up.
 */
public class AdaptiveConcurrencyLimiter implements ConcurrencyLimiter {

  private final int maxConcurrency;
  private final Semaphore semaphore;
  private volatile int currentLimit;
  private int consecutiveSuccesses;

  /**
   * Create a new AdaptiveConcurrencyLimiter.
   *
   * @param maxConcurrency the initial (and maximum) number of concurrent permits
   */
  public AdaptiveConcurrencyLimiter(int maxConcurrency) {
    if (maxConcurrency < 1) {
      throw new IllegalArgumentException("maxConcurrency must be >= 1");
    }
    this.maxConcurrency = maxConcurrency;
    this.currentLimit = maxConcurrency;
    this.consecutiveSuccesses = 0;
    this.semaphore = new Semaphore(maxConcurrency);
  }

  @Override
  public void acquire() throws InterruptedException {
    semaphore.acquire();
  }

  /**
   * Release a permit and adjust the limit based on whether the operation succeeded or failed.
   *
   * <p>If {@code failure} is null (success): release a permit and, after {@code currentLimit}
   * consecutive successes, increase the limit by 1 (up to {@code maxConcurrency}).
   *
   * <p>If {@code failure} is an IO-related throwable: release a permit, halve the current limit
   * (floor 1), and reset the consecutive-success counter.
   *
   * <p>If {@code failure} is a non-IO throwable: release a permit only (no limit change).
   *
   * @param failure the failure, or null on success
   */
  @Override
  public synchronized void release(Throwable failure) {
    semaphore.release();
    if (failure == null) {
      consecutiveSuccesses++;
      if (consecutiveSuccesses >= currentLimit && currentLimit < maxConcurrency) {
        currentLimit++;
        consecutiveSuccesses = 0;
        // Add the new permit since limit grew
        semaphore.release();
      }
    } else if (S3Backend.isIoRelatedFailure(failure)) {
      int newLimit = Math.max(1, currentLimit / 2);
      if (newLimit < currentLimit) {
        // Drain excess permits to enforce the reduced limit
        int excess = currentLimit - newLimit;
        semaphore.acquireUninterruptibly(excess);
        currentLimit = newLimit;
      }
      consecutiveSuccesses = 0;
    }
    // Non-IO failure: just the semaphore.release() above, no limit change
  }

  @Override
  public int getCurrentLimit() {
    return currentLimit;
  }
}
