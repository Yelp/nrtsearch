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
package com.yelp.nrtsearch.clientlib.leader;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the confirmed active primary of a multi-primary cluster by polling a {@link
 * LeaderResolver}, and notifies subscribers when it changes. One tracker can serve many channels
 * (e.g. one per replica index), so the leader is resolved once for all of them.
 *
 * <p>The tracker only moves to the same or a newer epoch, never back to an older leader. When no
 * leader can be confirmed, the last confirmed leader is kept.
 *
 * <p>Polls run on the tracker's single thread. Every published leader gets a sequence number, and
 * each subscriber only accepts a leader with a higher sequence than the last one it received, so a
 * subscriber always sees leaders in order and never receives an older leader after a newer one,
 * even though a new subscriber gets the known leader at once on the subscribing thread (not behind
 * a poll in progress).
 */
public class LeaderTracker implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(LeaderTracker.class);

  /** Receives leader changes. */
  public interface Subscriber {
    /** The confirmed leader changed (or is reported for the first time to this subscriber). */
    void onLeader(LeaderResolver.Resolved leader);

    /** No leader has been confirmed yet. */
    void onNoLeader();
  }

  private final LeaderResolver resolver;
  private final long pollIntervalMs;
  private final Runnable onClose;
  private final Map<Subscriber, Delivery> subscribers = new ConcurrentHashMap<>();
  private final ScheduledExecutorService executor;
  // merges refresh requests from many channels into one extra poll
  private final com.yelp.nrtsearch.clientlib.util.CoalescingRunner refresher;
  // guarded by this
  private LeaderResolver.Resolved current;
  // sequence of current, incremented on every publish; guarded by this
  private long currentSeq = 0;
  private boolean closed = false;
  private boolean started = false;

  /**
   * Constructor. Polling starts with {@link #start()}.
   *
   * @param resolver leader resolver
   * @param pollIntervalMs how often to re-resolve the leader
   * @param onClose run when the tracker closes, e.g. to close probe channels
   */
  public LeaderTracker(LeaderResolver resolver, long pollIntervalMs, Runnable onClose) {
    if (pollIntervalMs <= 0) {
      throw new IllegalArgumentException("pollIntervalMs must be > 0");
    }
    this.resolver = Objects.requireNonNull(resolver);
    this.pollIntervalMs = pollIntervalMs;
    this.onClose = Objects.requireNonNull(onClose);
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "LeaderTracker");
              t.setDaemon(true);
              return t;
            });
    this.refresher =
        new com.yelp.nrtsearch.clientlib.util.CoalescingRunner(executor, this::safePoll);
  }

  /** Start polling. Calling it again has no effect. */
  public synchronized void start() {
    if (started || closed) {
      return;
    }
    started = true;
    executor.scheduleWithFixedDelay(this::safePoll, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  /** Delivers to one subscriber in sequence order, dropping anything older than delivered. */
  private static final class Delivery {
    private final Subscriber subscriber;
    // guarded by this
    private long lastSeq = -1;

    Delivery(Subscriber subscriber) {
      this.subscriber = subscriber;
    }

    synchronized void leader(LeaderResolver.Resolved leader, long seq) {
      if (seq <= lastSeq) {
        return;
      }
      lastSeq = seq;
      subscriber.onLeader(leader);
    }

    synchronized void noLeader() {
      // only before any leader was delivered; a known leader is kept
      if (lastSeq <= 0) {
        lastSeq = 0;
        subscriber.onNoLeader();
      }
    }
  }

  /**
   * Add a subscriber. If a leader is known, it is delivered to the subscriber immediately, on the
   * calling thread; later updates are never delivered out of order with it.
   *
   * @param subscriber subscriber
   */
  public void subscribe(Subscriber subscriber) {
    Delivery delivery = new Delivery(subscriber);
    LeaderResolver.Resolved leader;
    long seq;
    synchronized (this) {
      if (closed) {
        return;
      }
      subscribers.put(subscriber, delivery);
      leader = current;
      seq = currentSeq;
    }
    // a poll publishing a newer leader after the snapshot delivers a higher sequence, which wins
    // regardless of which delivery runs first
    if (leader != null) {
      delivery.leader(leader, seq);
    }
  }

  public void unsubscribe(Subscriber subscriber) {
    subscribers.remove(subscriber);
  }

  /**
   * Re-resolve soon, e.g. when a channel reports a connection failure. Requests from many channels
   * at once are merged.
   */
  public void refresh() {
    refresher.request();
  }

  private void safePoll() {
    try {
      poll();
    } catch (Throwable t) {
      logger.warn("Error resolving leader", t);
    }
  }

  /**
   * Resolve once and notify subscribers of any change. Runs on the tracker thread (tests may call
   * it directly when the tracker is not started). Package private for tests.
   */
  void poll() {
    Optional<LeaderResolver.Resolved> resolved = resolver.resolve();
    LeaderResolver.Resolved publish = null;
    long publishSeq = 0;
    boolean noLeader = false;
    synchronized (this) {
      if (closed) {
        return;
      }
      if (resolved.isPresent()) {
        LeaderResolver.Resolved next = resolved.get();
        // only move to the same or a newer epoch, never back to an older leader
        if (current == null
            || next.epoch() > current.epoch()
            || (next.epoch() == current.epoch() && !next.equals(current))) {
          current = next;
          publish = next;
          publishSeq = ++currentSeq;
        }
      } else if (current == null) {
        noLeader = true;
      }
    }
    if (publish != null) {
      logger.info("Leader resolved: {}", publish);
      for (Delivery d : subscribers.values()) {
        d.leader(publish, publishSeq);
      }
    } else if (noLeader) {
      for (Delivery d : subscribers.values()) {
        d.noLeader();
      }
    }
  }

  /** Wait for queued work on the tracker thread (deliveries, refreshes) to finish, for tests. */
  void awaitIdle() throws Exception {
    executor.submit(() -> {}).get(10, TimeUnit.SECONDS);
  }

  /** Current confirmed leader. */
  public synchronized Optional<LeaderResolver.Resolved> getCurrent() {
    return Optional.ofNullable(current);
  }

  @Override
  public void close() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
    }
    executor.shutdownNow();
    subscribers.clear();
    onClose.run();
  }
}
