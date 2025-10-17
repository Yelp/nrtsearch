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
package com.yelp.nrtsearch.server.utils;

/**
 * Limit the number of bytes read in a time window. If the limit is exceeded, the thread will sleep
 * until the next window. The rate limiter is synchronized to be thread safe.
 */
public class GlobalWindowRateLimiter {
  private final long bytesPerWindow;
  private final long windowMillis;

  private long windowStartTime;
  private long bytesReadInWindow;

  /**
   * Constructor for GlobalWindowRateLimiter.
   *
   * @param bytesPerSecond maximum bytes per second
   * @param windowSeconds size of the time window in seconds
   */
  public GlobalWindowRateLimiter(long bytesPerSecond, int windowSeconds) {
    this.bytesPerWindow = bytesPerSecond * windowSeconds;
    this.windowMillis = windowSeconds * 1000L;
    this.windowStartTime = System.currentTimeMillis();
    this.bytesReadInWindow = 0;
  }

  /**
   * Acquire permission to read numBytes. If the limit is exceeded, the thread will sleep until the
   * next window.
   *
   * @param numBytes number of bytes to read
   */
  public synchronized void acquire(int numBytes) {
    long now = System.currentTimeMillis();

    // Start new window if needed
    if (now - windowStartTime >= windowMillis) {
      windowStartTime = now;
      bytesReadInWindow = 0;
    }

    bytesReadInWindow += numBytes;

    // If over limit, sleep until window reset
    if (bytesReadInWindow > bytesPerWindow) {
      long sleepMillis = windowMillis - (now - windowStartTime);
      if (sleepMillis > 0) {
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      windowStartTime = System.currentTimeMillis();
      bytesReadInWindow = numBytes;
    }
  }
}
