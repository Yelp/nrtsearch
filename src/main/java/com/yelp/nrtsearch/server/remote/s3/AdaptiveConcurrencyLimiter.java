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
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ConcurrencyLimiter} that dynamically adjusts the concurrency limit using a
 * throughput-gradient algorithm driven by a sliding time window.
 *
 * <p>Throughput is measured as total bytes received across <em>all</em> concurrent transfers in the
 * current time window, fed continuously via {@link #recordBytes(long)} from a {@link
 * software.amazon.awssdk.transfer.s3.progress.TransferListener}. This is intentionally aggregate
 * rather than per-file: the CRT S3 client downloads large files as many parallel parts, so a
 * per-file sample conflates internal part-level parallelism with actual IO saturation. The
 * time-window approach averages cleanly across files of any size.
 *
 * <p>Every {@code windowDurationMs} a new throughput sample is computed (bytes / elapsed seconds)
 * and fed into two EMAs:
 *
 * <ul>
 *   <li>Short EMA (higher {@code shortAlpha}): tracks recent throughput.
 *   <li>Long EMA (lower {@code longAlpha}): tracks baseline throughput.
 * </ul>
 *
 * <p>The gradient {@code shortEma / longEma} drives limit adjustments:
 *
 * <ul>
 *   <li>Ratio {@code >= 1.0}: throughput stable or improving — increase the limit.
 *   <li>Ratio {@code < decreaseThreshold}: throughput degrading — decrease the limit.
 *   <li>Otherwise: hold.
 * </ul>
 *
 * <p>Errors (failed downloads) are treated as an immediate congestion signal and decrease the limit
 * multiplicatively regardless of the window schedule.
 *
 * <p>During a configurable warmup period (measured in windows) the limit only increases, giving the
 * system time to ramp up without being penalised by cold-start effects.
 *
 * <p>A hard {@code maxLimit} cap prevents the GC death spiral that occurs when too many in-flight
 * transfers accumulate on the heap simultaneously.
 *
 * <p>Permit management uses a {@link Semaphore} combined with an {@code AtomicInteger} "permit
 * debt" counter:
 *
 * <ul>
 *   <li>Limit increase: release extra permits to the semaphore immediately.
 *   <li>Limit decrease: accumulate debt; completion callbacks absorb permits against the debt
 *       rather than releasing them, gracefully draining in-flight operations to the new limit.
 * </ul>
 */
public class AdaptiveConcurrencyLimiter implements ConcurrencyLimiter {

  private static final Logger logger = LoggerFactory.getLogger(AdaptiveConcurrencyLimiter.class);

  private final int minLimit;
  private final int maxLimit;
  private final double shortAlpha;
  private final double longAlpha;
  private final double decreaseThreshold;
  private final double decreaseFactor;
  private final long windowDurationNanos;
  private final int warmupWindows;

  private final AtomicInteger currentLimit;
  private final AtomicInteger inflight = new AtomicInteger(0);
  private final AtomicInteger permitDebt = new AtomicInteger(0);

  // Bytes accumulated in the current window; updated by recordBytes() on EventLoop threads.
  private final AtomicLong windowBytes = new AtomicLong(0);

  // Guarded by `this`
  private double shortEma = 0;
  private double longEma = 0;
  private long windowStartNanos;
  private int windowsObserved = 0;
  private final Semaphore semaphore;

  /**
   * @param initialLimit starting concurrency
   * @param minLimit minimum concurrency (floor)
   * @param maxLimit maximum concurrency (hard ceiling — safety net against GC death spiral)
   * @param shortAlpha fast EMA smoothing factor (higher = more responsive, e.g. 0.3)
   * @param longAlpha slow EMA smoothing factor (e.g. 0.1)
   * @param decreaseThreshold gradient ratio below which the limit is decreased (e.g. 0.85)
   * @param decreaseFactor multiplicative factor applied on decrease (e.g. 0.75)
   * @param windowDurationMs duration of each throughput measurement window in milliseconds
   * @param warmupWindows number of initial windows during which only increases are allowed
   */
  public AdaptiveConcurrencyLimiter(
      int initialLimit,
      int minLimit,
      int maxLimit,
      double shortAlpha,
      double longAlpha,
      double decreaseThreshold,
      double decreaseFactor,
      int windowDurationMs,
      int warmupWindows) {
    if (initialLimit <= 0) throw new IllegalArgumentException("initialLimit must be > 0");
    if (minLimit <= 0) throw new IllegalArgumentException("minLimit must be > 0");
    if (maxLimit < minLimit) throw new IllegalArgumentException("maxLimit must be >= minLimit");
    if (shortAlpha <= 0 || shortAlpha > 1)
      throw new IllegalArgumentException("shortAlpha must be in (0, 1]");
    if (longAlpha <= 0 || longAlpha > 1)
      throw new IllegalArgumentException("longAlpha must be in (0, 1]");
    if (decreaseFactor <= 0 || decreaseFactor >= 1)
      throw new IllegalArgumentException("decreaseFactor must be in (0, 1)");
    if (windowDurationMs < 1) throw new IllegalArgumentException("windowDurationMs must be >= 1");
    if (warmupWindows < 0) throw new IllegalArgumentException("warmupWindows must be >= 0");

    this.minLimit = minLimit;
    this.maxLimit = maxLimit;
    this.shortAlpha = shortAlpha;
    this.longAlpha = longAlpha;
    this.decreaseThreshold = decreaseThreshold;
    this.decreaseFactor = decreaseFactor;
    this.windowDurationNanos = windowDurationMs * 1_000_000L;
    this.warmupWindows = warmupWindows;

    int clamped = Math.max(minLimit, Math.min(maxLimit, initialLimit));
    this.currentLimit = new AtomicInteger(clamped);
    this.semaphore = new Semaphore(clamped);
    this.windowStartNanos = System.nanoTime();
  }

  @Override
  public void acquire() throws InterruptedException {
    semaphore.acquire();
    inflight.incrementAndGet();
  }

  @Override
  public void onSuccess() {
    releasePermit();
  }

  @Override
  public void onError() {
    // Treat error as strong congestion: decrease immediately without waiting for the next window.
    synchronized (this) {
      int oldLimit = currentLimit.get();
      int newLimit = Math.max((int) (oldLimit * decreaseFactor), minLimit);
      if (newLimit < oldLimit) {
        applyLimitChange(oldLimit, newLimit);
        logger.info(
            "Adaptive concurrency: error signal, limit {} -> {} (inflight={})",
            oldLimit,
            newLimit,
            inflight.get());
      }
      // Reset short EMA toward long EMA to clear short-term optimism.
      if (longEma > 0) {
        shortEma = longEma;
      }
    }
    releasePermit();
  }

  /**
   * Record bytes transferred since the last callback, as reported by a {@link
   * software.amazon.awssdk.transfer.s3.progress.TransferListener}. Rolls the measurement window
   * when sufficient time has elapsed and triggers a limit adjustment if warranted.
   *
   * <p>Must be non-blocking; called on AwsEventLoop threads.
   */
  @Override
  public void recordBytes(long delta) {
    windowBytes.addAndGet(delta);
    long now = System.nanoTime();

    // Fast path: still inside the current window.
    // Read windowStartNanos without the lock first to avoid contention on every byte callback.
    if (now - windowStartNanos < windowDurationNanos) {
      return;
    }

    // Window has elapsed — try to become the thread that closes it.
    synchronized (this) {
      long elapsed = now - windowStartNanos;
      if (elapsed < windowDurationNanos) {
        // Another thread already rolled the window.
        return;
      }

      // Claim the accumulated bytes and reset for the next window.
      long bytes = windowBytes.getAndSet(0);
      windowStartNanos = now;
      windowsObserved++;

      double throughput = bytes / (elapsed / 1e9);

      // Bootstrap EMAs from the first real window.
      if (shortEma == 0) {
        shortEma = throughput;
        longEma = throughput;
      } else {
        shortEma = shortAlpha * throughput + (1 - shortAlpha) * shortEma;
        longEma = longAlpha * throughput + (1 - longAlpha) * longEma;
      }

      if (longEma == 0) {
        return;
      }

      double gradient = shortEma / longEma;
      boolean inWarmup = windowsObserved <= warmupWindows;
      int oldLimit = currentLimit.get();
      int newLimit;

      if (gradient >= 1.0) {
        int increase = Math.max(1, oldLimit / 10);
        newLimit = Math.min(oldLimit + increase, maxLimit);
      } else if (!inWarmup && gradient < decreaseThreshold) {
        newLimit = Math.max((int) (oldLimit * decreaseFactor), minLimit);
      } else {
        return;
      }

      if (newLimit == oldLimit) {
        return;
      }

      applyLimitChange(oldLimit, newLimit);
      logger.info(
          "Adaptive concurrency: limit {} -> {} (gradient={}, shortEma={} MB/s, longEma={} MB/s)",
          oldLimit,
          newLimit,
          String.format("%.3f", gradient),
          String.format("%.1f", shortEma / (1024.0 * 1024.0)),
          String.format("%.1f", longEma / (1024.0 * 1024.0)));
    }
  }

  @Override
  public int getLimit() {
    return currentLimit.get();
  }

  @Override
  public int getInflight() {
    return inflight.get();
  }

  /**
   * Apply a limit change. Must be called while holding {@code this} lock so that the limit and
   * semaphore state are updated atomically with respect to other adjustments.
   */
  private void applyLimitChange(int oldLimit, int newLimit) {
    currentLimit.set(newLimit);
    int delta = newLimit - oldLimit;
    if (delta > 0) {
      // Pay off any outstanding debt first; release the remainder to the semaphore.
      int debtPayoff = Math.min(delta, permitDebt.get());
      if (debtPayoff > 0) {
        permitDebt.addAndGet(-debtPayoff);
      }
      int toRelease = delta - debtPayoff;
      if (toRelease > 0) {
        semaphore.release(toRelease);
      }
    } else {
      // Accumulate debt; completion callbacks will absorb permits rather than releasing them.
      permitDebt.addAndGet(-delta);
    }
  }

  /** Release a permit, absorbing it against outstanding debt if any exists. */
  private void releasePermit() {
    inflight.decrementAndGet();
    while (true) {
      int debt = permitDebt.get();
      if (debt <= 0) {
        semaphore.release();
        return;
      }
      if (permitDebt.compareAndSet(debt, debt - 1)) {
        // Permit absorbed — do not release to the semaphore.
        return;
      }
      // CAS missed; retry.
    }
  }
}
