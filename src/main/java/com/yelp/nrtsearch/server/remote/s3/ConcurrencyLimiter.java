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

/**
 * Controls the number of concurrent S3 transfer operations.
 *
 * <p>Callers must call {@link #acquire()} before starting an operation and {@link
 * #release(Throwable)} after it completes (whether successfully or not).
 */
public interface ConcurrencyLimiter {

  /**
   * Acquire a permit, blocking until one is available.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  void acquire() throws InterruptedException;

  /**
   * Release a permit, optionally reporting the outcome of the operation.
   *
   * @param failure the throwable if the operation failed, or {@code null} on success
   */
  void release(Throwable failure);

  /**
   * Returns the current concurrency limit (number of permits that may be held simultaneously).
   *
   * @return current limit
   */
  int getCurrentLimit();
}
