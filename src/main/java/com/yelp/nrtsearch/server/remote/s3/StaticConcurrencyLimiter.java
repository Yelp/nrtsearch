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
import java.util.concurrent.atomic.AtomicInteger;

/** A {@link ConcurrencyLimiter} that enforces a fixed maximum concurrency. */
public class StaticConcurrencyLimiter implements ConcurrencyLimiter {

  private final Semaphore semaphore;
  private final int limit;
  private final AtomicInteger inflight = new AtomicInteger(0);

  public StaticConcurrencyLimiter(int maxConcurrency) {
    if (maxConcurrency <= 0) {
      throw new IllegalArgumentException("maxConcurrency must be > 0");
    }
    this.limit = maxConcurrency;
    this.semaphore = new Semaphore(maxConcurrency);
  }

  @Override
  public void acquire() throws InterruptedException {
    semaphore.acquire();
    inflight.incrementAndGet();
  }

  @Override
  public void onSuccess() {
    inflight.decrementAndGet();
    semaphore.release();
  }

  @Override
  public void onError() {
    inflight.decrementAndGet();
    semaphore.release();
  }

  @Override
  public int getLimit() {
    return limit;
  }

  @Override
  public int getInflight() {
    return inflight.get();
  }
}
