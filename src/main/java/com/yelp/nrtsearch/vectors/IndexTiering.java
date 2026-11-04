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
package com.yelp.nrtsearch.vectors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hot/cold tiering for vector indexes. An index that gets no requests for {@code idleStopMs} is
 * committed (so S3 holds all of its data) and stopped, which frees its memory and local disk. The
 * next request to the index starts it again, restoring the data from S3.
 *
 * <p>Requests hold a read lock on their index, so an index is never stopped while a request is
 * using it. Starting and stopping take the write lock.
 */
public class IndexTiering {
  private static final Logger logger = LoggerFactory.getLogger(IndexTiering.class);

  private static class IndexEntry {
    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    // null until the state is known: checked with nrtsearch on first use
    volatile Boolean started;
    volatile long lastAccessMs;
  }

  private final Consumer<String> starter;
  private final Consumer<String> stopper;
  private final Predicate<String> startedChecker;
  private final long idleStopMs;
  private final Map<String, IndexEntry> entries = new ConcurrentHashMap<>();
  private LongSupplier clock = System::currentTimeMillis;

  /**
   * @param starter starts a stopped index
   * @param stopper commits and stops an index
   * @param startedChecker asks nrtsearch if an index is started, used the first time an index is
   *     seen (for example after the gateway restarts)
   * @param options gateway options
   */
  public IndexTiering(
      Consumer<String> starter,
      Consumer<String> stopper,
      Predicate<String> startedChecker,
      VectorService.Options options) {
    this.starter = starter;
    this.stopper = stopper;
    this.startedChecker = startedChecker;
    this.idleStopMs = options.idleStopMs;
  }

  /** Use a different clock, for tests. */
  void setClock(LongSupplier clock) {
    this.clock = clock;
  }

  public boolean isEnabled() {
    return idleStopMs > 0;
  }

  private IndexEntry entry(String index) {
    return entries.computeIfAbsent(
        index,
        k -> {
          IndexEntry e = new IndexEntry();
          e.lastAccessMs = clock.getAsLong();
          return e;
        });
  }

  /** Record that an index was just created and started. */
  public void markStarted(String index) {
    IndexEntry e = entry(index);
    e.started = true;
    e.lastAccessMs = clock.getAsLong();
  }

  public void forget(String index) {
    entries.remove(index);
  }

  /** If the index is known to be started. False if it is stopped or its state is not known. */
  public boolean isStarted(String index) {
    IndexEntry e = entries.get(index);
    return e != null && Boolean.TRUE.equals(e.started);
  }

  /** Run an action on an index, starting the index first if it is stopped. */
  public <T> T withIndex(String index, Supplier<T> action) {
    IndexEntry e = entry(index);
    e.lastAccessMs = clock.getAsLong();
    if (!isEnabled()) {
      return action.get();
    }
    while (true) {
      if (!Boolean.TRUE.equals(e.started)) {
        // start in its own block, so a failure leaves no lock held
        e.lock.writeLock().lock();
        try {
          ensureStarted(index, e);
        } finally {
          e.lock.writeLock().unlock();
        }
      }
      e.lock.readLock().lock();
      try {
        // the index may have been stopped between the start and taking the read lock
        if (Boolean.TRUE.equals(e.started)) {
          e.lastAccessMs = clock.getAsLong();
          return action.get();
        }
      } finally {
        e.lock.readLock().unlock();
      }
    }
  }

  /** Make sure the index is started. Called while holding the write lock. */
  private void ensureStarted(String index, IndexEntry e) {
    if (e.started == null) {
      e.started = startedChecker.test(index);
    }
    if (!e.started) {
      long t0 = System.nanoTime();
      starter.accept(index);
      e.started = true;
      logger.info("Started index {} in {} ms", index, (System.nanoTime() - t0) / 1_000_000);
    }
  }

  /**
   * Stop every started index that has been idle for longer than {@code idleStopMs}. Call this
   * periodically.
   *
   * @return names of the indexes that were stopped
   */
  public List<String> stopIdleIndexes() {
    List<String> stopped = new ArrayList<>();
    if (!isEnabled()) {
      return stopped;
    }
    long now = clock.getAsLong();
    for (Map.Entry<String, IndexEntry> mapEntry : entries.entrySet()) {
      IndexEntry e = mapEntry.getValue();
      // indexes in an unknown state are checked on their next request, not stopped here
      if (!Boolean.TRUE.equals(e.started) || now - e.lastAccessMs < idleStopMs) {
        continue;
      }
      // don't wait for requests in progress, try again next time
      if (!e.lock.writeLock().tryLock()) {
        continue;
      }
      try {
        if (Boolean.TRUE.equals(e.started) && now - e.lastAccessMs >= idleStopMs) {
          stopper.accept(mapEntry.getKey());
          e.started = false;
          stopped.add(mapEntry.getKey());
        }
      } catch (RuntimeException ex) {
        logger.warn("Could not stop idle index {}", mapEntry.getKey(), ex);
      } finally {
        e.lock.writeLock().unlock();
      }
    }
    return stopped;
  }
}
