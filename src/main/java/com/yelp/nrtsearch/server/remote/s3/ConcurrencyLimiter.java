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
 * Controls the number of concurrent in-flight operations, optionally adjusting the limit
 * dynamically based on observed performance signals.
 *
 * <p>{@link #acquire()} is called by the submitter thread before starting each operation. {@link
 * #onSuccess} and {@link #onError} are called by completion callbacks (which may run on different
 * threads) and must therefore be non-blocking.
 */
public interface ConcurrencyLimiter {

  /**
   * Acquire a permit to start an operation. Blocks until a permit is available.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  void acquire() throws InterruptedException;

  /**
   * Report that an operation completed successfully. Releases the permit acquired via {@link
   * #acquire()}. Must be non-blocking.
   */
  void onSuccess();

  /**
   * Record that {@code delta} bytes were transferred since the last call. Called continuously by a
   * {@link software.amazon.awssdk.transfer.s3.progress.TransferListener} as parts arrive, giving
   * implementations a real-time throughput signal that is unaffected by file-size variance.
   *
   * <p>The default implementation is a no-op; only adaptive implementations need to override it.
   * Must be non-blocking.
   *
   * @param delta bytes transferred since the last call (already computed as a delta, not
   *     cumulative)
   */
  default void recordBytes(long delta) {}

  /**
   * Report that an operation failed. Implementations should treat this as a congestion signal and
   * may decrease the concurrency limit. Must be non-blocking.
   */
  void onError();

  /** Returns the current concurrency limit. */
  int getLimit();

  /** Returns the number of permits currently in use (in-flight operations). */
  int getInflight();
}
