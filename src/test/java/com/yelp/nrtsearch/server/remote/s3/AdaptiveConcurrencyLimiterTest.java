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

import org.junit.Test;

public class AdaptiveConcurrencyLimiterTest {

  /**
   * Build a limiter with a very short window (1 ms) so tests can drive adjustments without real
   * wall-clock delays. warmupWindows=0 lets decrease fire immediately.
   */
  private AdaptiveConcurrencyLimiter fastLimiter(int initial, int min, int max) {
    return new AdaptiveConcurrencyLimiter(
        initial,
        min,
        max,
        0.3,
        0.1, // shortAlpha, longAlpha
        0.85,
        0.75, // decreaseThreshold, decreaseFactor
        1, // windowDurationMs — 1 ms so tests can cross boundaries easily
        0 // warmupWindows — no warmup so both increase and decrease can fire
        );
  }

  /** Same as fastLimiter but with a warmup period. */
  private AdaptiveConcurrencyLimiter fastLimiterWithWarmup(
      int initial, int min, int max, int warmupWindows) {
    return new AdaptiveConcurrencyLimiter(
        initial, min, max, 0.3, 0.1, 0.85, 0.75, 1, warmupWindows);
  }

  // ---- Construction ----

  @Test
  public void testInvalidArguments() {
    try {
      new AdaptiveConcurrencyLimiter(0, 1, 100, 0.3, 0.1, 0.85, 0.75, 1000, 3);
      fail();
    } catch (IllegalArgumentException e) {
      /* expected */
    }

    try {
      new AdaptiveConcurrencyLimiter(5, 0, 100, 0.3, 0.1, 0.85, 0.75, 1000, 3);
      fail();
    } catch (IllegalArgumentException e) {
      /* expected */
    }

    try {
      // maxLimit < minLimit
      new AdaptiveConcurrencyLimiter(5, 10, 5, 0.3, 0.1, 0.85, 0.75, 1000, 3);
      fail();
    } catch (IllegalArgumentException e) {
      /* expected */
    }

    try {
      new AdaptiveConcurrencyLimiter(5, 1, 100, 0.0, 0.1, 0.85, 0.75, 1000, 3);
      fail();
    } catch (IllegalArgumentException e) {
      /* expected */
    }

    try {
      // decreaseFactor >= 1
      new AdaptiveConcurrencyLimiter(5, 1, 100, 0.3, 0.1, 0.85, 1.0, 1000, 3);
      fail();
    } catch (IllegalArgumentException e) {
      /* expected */
    }

    try {
      // windowDurationMs < 1
      new AdaptiveConcurrencyLimiter(5, 1, 100, 0.3, 0.1, 0.85, 0.75, 0, 3);
      fail();
    } catch (IllegalArgumentException e) {
      /* expected */
    }
  }

  @Test
  public void testInitialState() {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(16, 1, 100);
    assertEquals(16, limiter.getLimit());
    assertEquals(0, limiter.getInflight());
  }

  @Test
  public void testInitialLimitClampedToMaxLimit() {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(200, 1, 100);
    assertEquals(100, limiter.getLimit());
  }

  @Test
  public void testInitialLimitClampedToMinLimit() {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(1, 5, 100);
    assertEquals(5, limiter.getLimit());
  }

  // ---- Inflight tracking ----

  @Test
  public void testInflightTracking() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(4, 1, 10);
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

  // ---- recordBytes drives window adjustments ----

  /**
   * Helper: sleep past the window boundary, then feed a burst of bytes so the window closes and an
   * EMA sample is recorded.
   */
  private void feedWindow(AdaptiveConcurrencyLimiter limiter, long bytesPerWindow)
      throws InterruptedException {
    Thread.sleep(2); // ensure at least one 1 ms window has elapsed
    limiter.recordBytes(bytesPerWindow);
  }

  @Test
  public void testLimitIncreasesWithStableThroughput() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(10, 1, 100);
    int initialLimit = limiter.getLimit();

    // Drive many windows at constant throughput — gradient stays at 1.0, should increase.
    for (int i = 0; i < 20; i++) {
      feedWindow(limiter, 10_000_000L); // 10 MB per window
    }

    assertTrue(
        "Limit should increase with stable throughput, was " + limiter.getLimit(),
        limiter.getLimit() > initialLimit);
  }

  @Test
  public void testLimitDoesNotExceedMaxLimit() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(20, 1, 25);

    for (int i = 0; i < 100; i++) {
      feedWindow(limiter, 10_000_000L);
    }

    assertTrue(
        "Limit must not exceed maxLimit, was " + limiter.getLimit(), limiter.getLimit() <= 25);
  }

  @Test
  public void testLimitDecreasesWithDegradingThroughput() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(20, 1, 100);

    // Bootstrap with high throughput so long EMA is high.
    for (int i = 0; i < 10; i++) {
      feedWindow(limiter, 100_000_000L);
    }
    int limitAfterRamp = limiter.getLimit();

    // Now drop to near-zero throughput — short EMA collapses below threshold.
    for (int i = 0; i < 20; i++) {
      feedWindow(limiter, 1L);
    }

    assertTrue(
        "Limit should decrease with degrading throughput; before="
            + limitAfterRamp
            + ", after="
            + limiter.getLimit(),
        limiter.getLimit() < limitAfterRamp);
  }

  @Test
  public void testLimitDoesNotGoBelowMinLimit() throws InterruptedException {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(5, 3, 100);

    for (int i = 0; i < 100; i++) {
      feedWindow(limiter, 1L);
    }

    assertTrue(
        "Limit must not go below minLimit, was " + limiter.getLimit(), limiter.getLimit() >= 3);
  }

  // ---- Warmup: no decrease allowed ----

  @Test
  public void testNoDecreaseInWarmup() throws InterruptedException {
    // warmupWindows=5 — decreases blocked for the first 5 windows.
    AdaptiveConcurrencyLimiter limiter = fastLimiterWithWarmup(16, 1, 100, 5);
    int initialLimit = limiter.getLimit();

    // Feed 3 windows with collapsing throughput — still within warmup.
    feedWindow(limiter, 100_000_000L); // bootstrap EMA high
    feedWindow(limiter, 1L); // near zero
    feedWindow(limiter, 1L); // near zero

    assertTrue(
        "Limit should not decrease during warmup, was " + limiter.getLimit(),
        limiter.getLimit() >= initialLimit);
  }

  // ---- Error signal ----

  @Test
  public void testErrorDecreasesLimit() {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(20, 1, 100);
    int before = limiter.getLimit();

    limiter.onError();

    int after = limiter.getLimit();
    assertTrue(
        "Error should decrease limit, before=" + before + ", after=" + after, after < before);
  }

  @Test
  public void testErrorHonorsMinLimit() {
    AdaptiveConcurrencyLimiter limiter = fastLimiter(2, 2, 100);
    limiter.onError();
    assertTrue(limiter.getLimit() >= 2);
  }

  // ---- Hard bounds with effective cap ----

  @Test
  public void testEffectiveMaxLimitCap() throws InterruptedException {
    // Simulates createConcurrencyLimiter capping adaptiveMaxLimit at maxConcurrency.
    int effectiveMax = Math.min(100, 15);
    AdaptiveConcurrencyLimiter limiter = fastLimiter(8, 1, effectiveMax);

    for (int i = 0; i < 100; i++) {
      feedWindow(limiter, 10_000_000L);
    }
    assertTrue(limiter.getLimit() <= 15);
  }
}
