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
 * A fixed-concurrency limiter that never adjusts its limit based on failures.
 *
 * <p>This preserves the pre-existing sliding-window behaviour from PR #981: a fixed semaphore is
 * used to bound concurrent transfers and failures cause no change to the limit.
 */
public class StaticConcurrencyLimiter implements ConcurrencyLimiter {

  private final int limit;
  private final Semaphore semaphore;

  /**
   * Create a new StaticConcurrencyLimiter.
   *
   * @param limit the fixed number of concurrent permits
   */
  public StaticConcurrencyLimiter(int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be >= 1");
    }
    this.limit = limit;
    this.semaphore = new Semaphore(limit);
  }

  @Override
  public void acquire() throws InterruptedException {
    semaphore.acquire();
  }

  @Override
  public void release(Throwable failure) {
    semaphore.release();
  }

  @Override
  public int getCurrentLimit() {
    return limit;
  }
}
