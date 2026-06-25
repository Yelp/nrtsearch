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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class StaticConcurrencyLimiterTest {

  @Test
  public void testInvalidConcurrency() {
    try {
      new StaticConcurrencyLimiter(0);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new StaticConcurrencyLimiter(-1);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testLimitIsFixed() {
    StaticConcurrencyLimiter limiter = new StaticConcurrencyLimiter(5);
    assertEquals(5, limiter.getLimit());
    limiter.onSuccess();
    assertEquals(5, limiter.getLimit());
    limiter.onError();
    assertEquals(5, limiter.getLimit());
  }

  @Test
  public void testAcquireAndRelease() throws InterruptedException {
    StaticConcurrencyLimiter limiter = new StaticConcurrencyLimiter(2);
    assertEquals(0, limiter.getInflight());
    limiter.acquire();
    assertEquals(1, limiter.getInflight());
    limiter.acquire();
    assertEquals(2, limiter.getInflight());
    limiter.onSuccess();
    assertEquals(1, limiter.getInflight());
    limiter.onError();
    assertEquals(0, limiter.getInflight());
  }

  @Test
  public void testConcurrencyCapEnforced() throws InterruptedException {
    int limit = 3;
    StaticConcurrencyLimiter limiter = new StaticConcurrencyLimiter(limit);
    AtomicInteger maxObservedInflight = new AtomicInteger(0);
    AtomicInteger currentInflight = new AtomicInteger(0);
    int totalTasks = 20;
    CountDownLatch done = new CountDownLatch(totalTasks);
    ExecutorService executor = Executors.newFixedThreadPool(10);

    for (int i = 0; i < totalTasks; i++) {
      executor.submit(
          () -> {
            try {
              limiter.acquire();
              int current = currentInflight.incrementAndGet();
              maxObservedInflight.updateAndGet(m -> Math.max(m, current));
              Thread.sleep(5);
              currentInflight.decrementAndGet();
              limiter.onSuccess();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              done.countDown();
            }
          });
    }

    assertTrue(done.await(10, TimeUnit.SECONDS));
    executor.shutdown();
    assertTrue(maxObservedInflight.get() <= limit);
    assertEquals(0, limiter.getInflight());
  }
}
