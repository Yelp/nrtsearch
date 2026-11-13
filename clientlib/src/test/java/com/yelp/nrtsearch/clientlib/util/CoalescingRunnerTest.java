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
package com.yelp.nrtsearch.clientlib.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class CoalescingRunnerTest {

  @Test
  public void testRequestWhileRunningRunsAgain() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch inFirst = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger runs = new AtomicInteger();
    CoalescingRunner runner =
        new CoalescingRunner(
            executor,
            () -> {
              if (runs.incrementAndGet() == 1) {
                inFirst.countDown();
                try {
                  release.await();
                } catch (InterruptedException ignored) {
                }
              }
            });
    runner.request();
    assertTrue(inFirst.await(5, TimeUnit.SECONDS));
    for (int i = 0; i < 10; i++) {
      runner.request();
    }
    release.countDown();
    executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
    executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
    assertEquals(2, runs.get());
    executor.shutdownNow();
  }

  /**
   * Every request is followed by a run that started after it, under heavy concurrency: a run
   * records when it started, and each request checks a later run happened.
   */
  @Test
  public void testNoLostRequests() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    AtomicLong lastRunStart = new AtomicLong();
    AtomicLong clock = new AtomicLong();
    CoalescingRunner runner =
        new CoalescingRunner(executor, () -> lastRunStart.set(clock.incrementAndGet()));
    ExecutorService requesters = Executors.newFixedThreadPool(8);
    AtomicInteger lost = new AtomicInteger();
    for (int t = 0; t < 8; t++) {
      requesters.execute(
          () -> {
            for (int i = 0; i < 2000; i++) {
              long requestedAt = clock.incrementAndGet();
              runner.request();
              long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
              while (lastRunStart.get() <= requestedAt) {
                if (System.nanoTime() > deadline) {
                  lost.incrementAndGet();
                  break;
                }
                Thread.onSpinWait();
              }
            }
          });
    }
    requesters.shutdown();
    assertTrue(requesters.awaitTermination(120, TimeUnit.SECONDS));
    assertEquals(0, lost.get());
    executor.shutdownNow();
  }

  /** An error thrown by the task (e.g. an AssertionError) must not stop later runs. */
  @Test
  public void testErrorDoesNotStopRunner() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    AtomicInteger runs = new AtomicInteger();
    CoalescingRunner runner =
        new CoalescingRunner(
            executor,
            () -> {
              if (runs.incrementAndGet() == 1) {
                throw new AssertionError("boom");
              }
            });
    runner.request();
    executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
    assertEquals(1, runs.get());
    runner.request();
    executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
    assertEquals(2, runs.get());
    executor.shutdownNow();
  }

  /**
   * A follow up run is queued behind other tasks on the executor, not run in a loop, so a stream of
   * requests does not starve them.
   */
  @Test
  public void testFollowUpRunQueuedBehindOtherTasks() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch inFirst = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger runs = new AtomicInteger();
    AtomicInteger runsWhenOtherTaskRan = new AtomicInteger(-1);
    CoalescingRunner runner =
        new CoalescingRunner(
            executor,
            () -> {
              if (runs.incrementAndGet() == 1) {
                inFirst.countDown();
                try {
                  release.await();
                } catch (InterruptedException ignored) {
                }
              }
            });
    runner.request();
    assertTrue(inFirst.await(5, TimeUnit.SECONDS));
    executor.execute(() -> runsWhenOtherTaskRan.set(runs.get()));
    runner.request();
    release.countDown();
    executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
    executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
    assertEquals(2, runs.get());
    // the other task ran between the first run and the follow up
    assertEquals(1, runsWhenOtherTaskRan.get());
    executor.shutdownNow();
  }
}
