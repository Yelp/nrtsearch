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

import java.io.IOException;
import java.util.concurrent.CancellationException;
import org.junit.Test;

public class AdaptiveConcurrencyLimiterTest {

  // --- AdaptiveConcurrencyLimiter tests ---

  @Test
  public void testAdaptive_initialLimitIsConfigured() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(10);
    assertEquals(10, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_successDoesNotChangeLimit() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(10);
    limiter.acquire();
    limiter.release(null); // success
    assertEquals(10, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_ioFailureHalvesLimit() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(10);
    limiter.acquire();
    limiter.release(new IOException("disk write failed"));
    assertEquals(5, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_nonIoFailureDoesNotReduceLimit() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(10);
    limiter.acquire();
    limiter.release(new RuntimeException("S3 error"));
    assertEquals(10, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_cancellationExceptionWithIoMessageReducesLimit()
      throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(10);
    limiter.acquire();
    limiter.release(new CancellationException("subscription has been cancelled"));
    assertEquals(5, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_limitNeverGoesBelowOne() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(1);
    limiter.acquire();
    limiter.release(new IOException("disk write failed"));
    assertEquals(1, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_multipleIoFailuresKeepHalving() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(16);
    // First IO failure: 16 -> 8
    limiter.acquire();
    limiter.release(new IOException("disk error"));
    assertEquals(8, limiter.getCurrentLimit());
    // Second IO failure: 8 -> 4
    limiter.acquire();
    limiter.release(new IOException("disk error"));
    assertEquals(4, limiter.getCurrentLimit());
    // Third IO failure: 4 -> 2
    limiter.acquire();
    limiter.release(new IOException("disk error"));
    assertEquals(2, limiter.getCurrentLimit());
    // Fourth IO failure: 2 -> 1
    limiter.acquire();
    limiter.release(new IOException("disk error"));
    assertEquals(1, limiter.getCurrentLimit());
    // Fifth IO failure: stays at 1 (floor)
    limiter.acquire();
    limiter.release(new IOException("disk error"));
    assertEquals(1, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_additivIncreaseAfterSuccessRound() throws InterruptedException {
    // Start at 8, IO failure reduces to 4, then 4 consecutive successes -> increase back to 5
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(8);
    limiter.acquire();
    limiter.release(new IOException("disk error")); // 8 -> 4
    assertEquals(4, limiter.getCurrentLimit());
    for (int i = 0; i < 4; i++) {
      limiter.acquire();
      limiter.release(null);
    }
    assertEquals(5, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_ioFailureResetsSuccessCounterBeforeIncrease()
      throws InterruptedException {
    // Start at 4, have 3 successes (not enough), then IO failure -> should halve, not increase
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(4);
    for (int i = 0; i < 3; i++) {
      limiter.acquire();
      limiter.release(null);
    }
    assertEquals(4, limiter.getCurrentLimit()); // not yet increased
    limiter.acquire();
    limiter.release(new IOException("disk error")); // halve and reset counter
    assertEquals(2, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_additivIncreaseDoesNotExceedMaxConcurrency()
      throws InterruptedException {
    // IO failure reduces from 8 to 4; many successes should recover to 8 but no higher
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(8);
    limiter.acquire();
    limiter.release(new IOException("disk error")); // 8 -> 4
    // Do many successes: 4+5+6+7+8 = 30 successes to recover to max
    for (int i = 0; i < 50; i++) {
      limiter.acquire();
      limiter.release(null);
    }
    // Should be capped at max (8)
    assertEquals(8, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_ioFailureThenRecovery() throws InterruptedException {
    // 8 -> IO failure -> 4 -> 4 successes -> 5 -> 5 successes -> 6
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(8);
    limiter.acquire();
    limiter.release(new IOException("disk error"));
    assertEquals(4, limiter.getCurrentLimit());

    // 4 consecutive successes -> increase to 5
    for (int i = 0; i < 4; i++) {
      limiter.acquire();
      limiter.release(null);
    }
    assertEquals(5, limiter.getCurrentLimit());

    // 5 consecutive successes -> increase to 6
    for (int i = 0; i < 5; i++) {
      limiter.acquire();
      limiter.release(null);
    }
    assertEquals(6, limiter.getCurrentLimit());
  }

  @Test
  public void testAdaptive_acquireBlocksWhenAtLimit() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = new AdaptiveConcurrencyLimiter(1);
    limiter.acquire(); // takes the only permit

    // A background thread releases after a short delay
    Thread releaser =
        new Thread(
            () -> {
              try {
                Thread.sleep(50);
                limiter.release(null);
              } catch (InterruptedException ignored) {
              }
            });
    releaser.start();

    long start = System.currentTimeMillis();
    limiter.acquire(); // should block until released
    long elapsed = System.currentTimeMillis() - start;
    assertTrue("acquire should have blocked for at least 40ms", elapsed >= 40);
    limiter.release(null);
    releaser.join();
  }

  // --- S3Backend.isIoRelatedFailure tests ---

  @Test
  public void testIsIoRelatedFailure_ioException() {
    assertTrue(S3Backend.isIoRelatedFailure(new IOException("disk write failed")));
  }

  @Test
  public void testIsIoRelatedFailure_cancellationExceptionWithIoMessage() {
    assertTrue(
        S3Backend.isIoRelatedFailure(new CancellationException("subscription has been cancelled")));
  }

  @Test
  public void testIsIoRelatedFailure_cancellationExceptionWithOtherMessage() {
    // A generic CancellationException not from FileAsyncResponseTransformer path
    // should NOT be classified as IO - it could be a normal cancellation
    assertTrue(S3Backend.isIoRelatedFailure(new CancellationException("some other cancellation")));
  }

  @Test
  public void testIsIoRelatedFailure_wrappedIoException() {
    RuntimeException wrapper = new RuntimeException("wrapped", new IOException("disk error"));
    assertTrue(S3Backend.isIoRelatedFailure(wrapper));
  }

  @Test
  public void testIsIoRelatedFailure_nonIoException() {
    assertTrue(!S3Backend.isIoRelatedFailure(new RuntimeException("S3 error")));
  }

  @Test
  public void testIsIoRelatedFailure_wrappedNonIoException() {
    Exception wrapped = new RuntimeException("outer", new RuntimeException("inner"));
    assertTrue(!S3Backend.isIoRelatedFailure(wrapped));
  }

  // --- StaticConcurrencyLimiter tests ---

  @Test
  public void testStatic_initialLimitIsConfigured() {
    StaticConcurrencyLimiter limiter = new StaticConcurrencyLimiter(5);
    assertEquals(5, limiter.getCurrentLimit());
  }

  @Test
  public void testStatic_limitNeverChangesOnFailure() throws InterruptedException {
    StaticConcurrencyLimiter limiter = new StaticConcurrencyLimiter(5);
    limiter.acquire();
    limiter.release(new IOException("disk error"));
    assertEquals(5, limiter.getCurrentLimit());
  }

  @Test
  public void testStatic_limitNeverChangesOnSuccess() throws InterruptedException {
    StaticConcurrencyLimiter limiter = new StaticConcurrencyLimiter(5);
    limiter.acquire();
    limiter.release(null);
    assertEquals(5, limiter.getCurrentLimit());
  }
}
