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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class GlobalWindowRateLimiterTest {

  @Test
  public void testConstructorInitialization() throws Exception {
    // Arrange
    long bytesPerSecond = 1024;
    int windowSeconds = 5;

    // Act
    GlobalWindowRateLimiter rateLimiter =
        new GlobalWindowRateLimiter(bytesPerSecond, windowSeconds);

    // Assert - using reflection to access private fields
    Field bytesPerWindowField = GlobalWindowRateLimiter.class.getDeclaredField("bytesPerWindow");
    bytesPerWindowField.setAccessible(true);
    long actualBytesPerWindow = (long) bytesPerWindowField.get(rateLimiter);

    Field windowMillisField = GlobalWindowRateLimiter.class.getDeclaredField("windowMillis");
    windowMillisField.setAccessible(true);
    long actualWindowMillis = (long) windowMillisField.get(rateLimiter);

    Field bytesReadInWindowField =
        GlobalWindowRateLimiter.class.getDeclaredField("bytesReadInWindow");
    bytesReadInWindowField.setAccessible(true);
    long actualBytesReadInWindow = (long) bytesReadInWindowField.get(rateLimiter);

    // Verify the fields were initialized correctly
    assertThat(actualBytesPerWindow).isEqualTo(bytesPerSecond * windowSeconds);
    assertThat(actualWindowMillis).isEqualTo(windowSeconds * 1000L);
    assertThat(actualBytesReadInWindow).isEqualTo(0);

    // We don't test windowStartTime exactly since it depends on the current time,
    // but we can verify it's been set to a reasonable value (not 0)
    Field windowStartTimeField = GlobalWindowRateLimiter.class.getDeclaredField("windowStartTime");
    windowStartTimeField.setAccessible(true);
    long actualWindowStartTime = (long) windowStartTimeField.get(rateLimiter);

    assertThat(actualWindowStartTime).isGreaterThan(0);
  }

  @Test
  public void testBasicAcquireFunctionality() throws Exception {
    // Arrange
    long bytesPerSecond = 10000; // High enough to not trigger rate limiting
    int windowSeconds = 5;
    GlobalWindowRateLimiter rateLimiter =
        new GlobalWindowRateLimiter(bytesPerSecond, windowSeconds);

    // Get access to the private field
    Field bytesReadInWindowField =
        GlobalWindowRateLimiter.class.getDeclaredField("bytesReadInWindow");
    bytesReadInWindowField.setAccessible(true);

    // Act & Assert

    // Initial state
    assertThat((long) bytesReadInWindowField.get(rateLimiter)).isEqualTo(0);

    // First acquire
    rateLimiter.acquire(100);
    assertThat((long) bytesReadInWindowField.get(rateLimiter)).isEqualTo(100);

    // Second acquire
    rateLimiter.acquire(200);
    assertThat((long) bytesReadInWindowField.get(rateLimiter)).isEqualTo(300);

    // Third acquire
    rateLimiter.acquire(50);
    assertThat((long) bytesReadInWindowField.get(rateLimiter)).isEqualTo(350);
  }

  @Test
  public void testWindowResetBehavior() throws Exception {
    // Arrange
    long bytesPerSecond = 1000;
    int windowSeconds = 5;
    GlobalWindowRateLimiter rateLimiter =
        new GlobalWindowRateLimiter(bytesPerSecond, windowSeconds);

    // Get access to the private fields
    Field windowStartTimeField = GlobalWindowRateLimiter.class.getDeclaredField("windowStartTime");
    windowStartTimeField.setAccessible(true);

    Field bytesReadInWindowField =
        GlobalWindowRateLimiter.class.getDeclaredField("bytesReadInWindow");
    bytesReadInWindowField.setAccessible(true);

    // Act & Assert

    // Initial state
    long initialWindowStartTime = (long) windowStartTimeField.get(rateLimiter);

    // Add some bytes
    rateLimiter.acquire(500);
    assertThat((long) bytesReadInWindowField.get(rateLimiter)).isEqualTo(500);

    // Simulate time passing by setting windowStartTime to a value in the past
    // that exceeds the window duration
    long pastTime = initialWindowStartTime - (windowSeconds * 1000 + 100);
    windowStartTimeField.set(rateLimiter, pastTime);

    // Acquire more bytes, which should trigger a window reset
    rateLimiter.acquire(200);

    // Verify window was reset
    long newWindowStartTime = (long) windowStartTimeField.get(rateLimiter);
    assertThat(newWindowStartTime).isNotEqualTo(pastTime);
    assertThat((long) bytesReadInWindowField.get(rateLimiter)).isEqualTo(200); // Only the new bytes
  }

  @Test
  public void testRateLimitingBehavior() throws Exception {
    // Arrange
    long bytesPerSecond = 100;
    int windowSeconds = 1;
    long bytesPerWindow = bytesPerSecond * windowSeconds;
    GlobalWindowRateLimiter rateLimiter =
        new GlobalWindowRateLimiter(bytesPerSecond, windowSeconds);

    // Get access to the private fields
    Field bytesReadInWindowField =
        GlobalWindowRateLimiter.class.getDeclaredField("bytesReadInWindow");
    bytesReadInWindowField.setAccessible(true);

    Field windowStartTimeField = GlobalWindowRateLimiter.class.getDeclaredField("windowStartTime");
    windowStartTimeField.setAccessible(true);

    // Act & Assert

    // First, fill up the window to just below the limit
    rateLimiter.acquire((int) (bytesPerWindow - 10));
    assertThat((long) bytesReadInWindowField.get(rateLimiter)).isEqualTo(bytesPerWindow - 10);

    // Store the current window start time
    long initialWindowStartTime = (long) windowStartTimeField.get(rateLimiter);

    // Now exceed the limit, which should trigger a window reset
    int excessBytes = 20;
    rateLimiter.acquire(excessBytes);

    // Verify that bytesReadInWindow was reset and now contains only the excess bytes
    assertThat((long) bytesReadInWindowField.get(rateLimiter)).isEqualTo(excessBytes);

    // Verify that the window start time was updated
    long newWindowStartTime = (long) windowStartTimeField.get(rateLimiter);
    assertThat(newWindowStartTime).isGreaterThan(initialWindowStartTime);
  }

  @Test
  public void testThreadSafety() throws Exception {
    // Arrange
    long bytesPerSecond = 10000; // High enough to not trigger rate limiting
    int windowSeconds = 5;
    GlobalWindowRateLimiter rateLimiter =
        new GlobalWindowRateLimiter(bytesPerSecond, windowSeconds);

    // Get access to the private field
    Field bytesReadInWindowField =
        GlobalWindowRateLimiter.class.getDeclaredField("bytesReadInWindow");
    bytesReadInWindowField.setAccessible(true);

    int numThreads = 10;
    int bytesPerThread = 100;
    int totalBytes = numThreads * bytesPerThread;

    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);

    // Act - Create and start threads that will all call acquire concurrently
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            try {
              startLatch.await(); // Wait for all threads to be ready
              rateLimiter.acquire(bytesPerThread);
              successCount.incrementAndGet();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              finishLatch.countDown();
            }
          });
    }

    // Release all threads at once
    startLatch.countDown();

    // Wait for all threads to finish
    finishLatch.await(5, TimeUnit.SECONDS);
    executorService.shutdown();

    // Assert
    // All threads should have successfully called acquire
    assertThat(successCount.get()).isEqualTo(numThreads);

    // The total bytes read should be the sum of all thread acquisitions
    assertThat((long) bytesReadInWindowField.get(rateLimiter)).isEqualTo(totalBytes);
  }
}
