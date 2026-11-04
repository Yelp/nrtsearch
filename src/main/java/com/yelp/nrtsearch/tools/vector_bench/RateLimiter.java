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

import java.util.concurrent.locks.LockSupport;

/**
 * Hands out evenly spaced start times for a target rate, shared by all worker threads. Each request
 * has a scheduled start time, and latency is measured from it rather than from when the request was
 * actually sent, so a slow server can't hide its delays by slowing the sender down (coordinated
 * omission).
 */
public class RateLimiter {
  private final long intervalNanos;
  private long next;
  private final java.util.concurrent.atomic.AtomicLong late =
      new java.util.concurrent.atomic.AtomicLong();

  /**
   * @param perSecond target rate, or 0 or less for no limit
   */
  public RateLimiter(double perSecond) {
    this.intervalNanos = perSecond > 0 ? (long) (1e9 / perSecond) : 0;
    // the schedule starts at the first acquire, not at construction, so start up time isn't
    // counted as falling behind
    this.next = Long.MIN_VALUE;
  }

  public boolean isLimited() {
    return intervalNanos > 0;
  }

  /** Number of start times handed out more than one interval after they were due. */
  public long lateCount() {
    return late.get();
  }

  /** Reset the late count, for example after a warmup period. */
  public void resetLateCount() {
    late.set(0);
  }

  /**
   * Wait for the next start time.
   *
   * @return the scheduled start time to measure latency from
   */
  public long acquire() {
    if (intervalNanos == 0) {
      return System.nanoTime();
    }
    long scheduled;
    long now = System.nanoTime();
    synchronized (this) {
      if (next == Long.MIN_VALUE) {
        next = now;
      }
      scheduled = next;
      next += intervalNanos;
    }
    // A slot already past when asked for means every sender was busy when it came due: the
    // target rate isn't being reached. Waking a little late from the wait below (timer slack,
    // usually 1-2ms) is not counted.
    if (now - scheduled > intervalNanos) {
      late.incrementAndGet();
    }
    long wait;
    while ((wait = scheduled - System.nanoTime()) > 0) {
      LockSupport.parkNanos(wait);
    }
    return scheduled;
  }
}
