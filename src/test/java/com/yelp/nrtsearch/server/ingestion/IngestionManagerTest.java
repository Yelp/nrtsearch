/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.ingestion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class IngestionManagerTest {

  /** start() launches a thread and returns, taking effect only after a delay (like Kafka). */
  static class LaunchingIngestor extends AbstractIngestor {
    final AtomicBoolean running = new AtomicBoolean();
    final AtomicInteger starts = new AtomicInteger();
    final long startDelayMs;

    LaunchingIngestor(long startDelayMs) {
      super(null);
      this.startDelayMs = startDelayMs;
    }

    @Override
    public boolean supportsLeaderElection() {
      return true;
    }

    @Override
    public void start() {
      starts.incrementAndGet();
      try {
        Thread.sleep(startDelayMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      running.set(true);
    }

    @Override
    public void stop() {
      running.set(false);
    }
  }

  /** start() is the ingest loop and only runs once marked running (like Paimon). */
  static class LoopIngestor extends AbstractIngestor {
    final AtomicBoolean running = new AtomicBoolean();
    final AtomicBoolean exited = new AtomicBoolean();
    final long startDelayMs;

    LoopIngestor(long startDelayMs) {
      super(null);
      this.startDelayMs = startDelayMs;
    }

    @Override
    public void start() {
      try {
        Thread.sleep(startDelayMs);
        if (!running.compareAndSet(false, true)) {
          return;
        }
        while (running.get()) {
          Thread.sleep(5);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        exited.set(true);
      }
    }

    @Override
    public void stop() {
      // only stops a run that already began, like a compare-and-set on a running flag
      running.compareAndSet(true, false);
    }
  }

  /** stop() never returns. */
  static class HungStopIngestor extends AbstractIngestor {
    final CountDownLatch never = new CountDownLatch(1);

    HungStopIngestor() {
      super(null);
    }

    @Override
    public void start() {}

    @Override
    public void stop() {
      try {
        never.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** A stop() arriving before start() takes effect still ends the run. */
  @Test
  public void testStopBeforeStartTakesEffect() throws Exception {
    LaunchingIngestor ingestor = new LaunchingIngestor(200);
    IngestionManager manager = new IngestionManager(List.of(ingestor), 10_000);
    manager.onActive(1);
    // wait until start() has begun, then end the term while it is still starting
    while (ingestor.starts.get() == 0) {
      Thread.sleep(1);
    }
    manager.onInactive();
    assertFalse(ingestor.running.get());
    Thread.sleep(300);
    assertFalse(ingestor.running.get());
  }

  /** A loop that begins after the first stop() is stopped by a repeated stop(). */
  @Test
  public void testLoopStartingAfterFirstStopIsStopped() throws Exception {
    LoopIngestor ingestor = new LoopIngestor(300);
    IngestionManager manager = new IngestionManager(List.of(ingestor), 10_000);
    manager.onActive(1);
    Thread.sleep(50);
    manager.onInactive();
    assertTrue(ingestor.exited.get());
    assertFalse(ingestor.running.get());
    assertEquals(0, manager.getLeakedRunners());
  }

  /** Ending a term right after it began never leaves a run active. */
  @Test
  public void testImmediateStopNeverLeavesRunActive() throws Exception {
    for (int i = 0; i < 50; i++) {
      LaunchingIngestor ingestor = new LaunchingIngestor(0);
      IngestionManager manager = new IngestionManager(List.of(ingestor), 10_000);
      manager.onActive(i);
      manager.onInactive();
      Thread.sleep(2);
      assertFalse("iteration " + i, ingestor.running.get());
    }
  }

  /** A hung stop() is bounded, reported, and does not block the caller. */
  @Test
  public void testHungStopIsBounded() throws Exception {
    HungStopIngestor ingestor = new HungStopIngestor();
    IngestionManager manager = new IngestionManager(List.of(ingestor), 500);
    manager.onActive(1);
    Thread.sleep(50);
    long start = System.nanoTime();
    manager.onInactive();
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    assertTrue("elapsed " + elapsedMs, elapsedMs < 5_000);
    assertEquals(1, manager.getLeakedRunners());
    assertFalse(manager.isActive());
  }

  /** stop() takes a while, but finishes. */
  static class SlowStopIngestor extends LaunchingIngestor {
    SlowStopIngestor() {
      super(0);
    }

    @Override
    public void stop() {
      try {
        Thread.sleep(700);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      super.stop();
    }
  }

  /** Each ingestor gets the full stop wait: slow stops do not use up the time of the others. */
  @Test
  public void testIngestorsStoppedInParallel() throws Exception {
    SlowStopIngestor first = new SlowStopIngestor();
    SlowStopIngestor second = new SlowStopIngestor();
    IngestionManager manager = new IngestionManager(List.of(first, second), 1000);
    manager.onActive(1);
    while (!first.running.get() || !second.running.get()) {
      Thread.sleep(1);
    }
    manager.onInactive();
    assertEquals(0, manager.getLeakedRunners());
    assertFalse(first.running.get());
    assertFalse(second.running.get());
  }

  /** An interrupt of the calling (transition) thread ends the wait but is not re-set. */
  @Test
  public void testInterruptNotPropagated() throws Exception {
    LoopIngestor ingestor = new LoopIngestor(0);
    IngestionManager manager = new IngestionManager(List.of(ingestor), 10_000);
    manager.onActive(1);
    Thread.sleep(50);
    Thread.currentThread().interrupt();
    manager.onInactive();
    assertFalse(Thread.currentThread().isInterrupted());
  }

  /** Terms can follow each other; each starts a fresh run. */
  @Test
  public void testConsecutiveTerms() throws Exception {
    LaunchingIngestor ingestor = new LaunchingIngestor(0);
    IngestionManager manager = new IngestionManager(List.of(ingestor), 10_000);
    for (int term = 1; term <= 3; term++) {
      manager.onActive(term);
      while (!ingestor.running.get()) {
        Thread.sleep(1);
      }
      manager.onInactive();
      assertFalse(ingestor.running.get());
    }
    assertEquals(3, ingestor.starts.get());
  }
}
