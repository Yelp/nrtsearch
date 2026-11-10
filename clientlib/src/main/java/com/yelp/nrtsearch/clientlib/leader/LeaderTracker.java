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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
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
  private final Set<Subscriber> subscribers = new CopyOnWriteArraySet<>();
  private final ScheduledExecutorService executor;
  // guarded by this
  private LeaderResolver.Resolved current;
  private boolean closed = false;

  /**
   * Constructor. Polling starts immediately.
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
  }

  /** Start polling. */
  public void start() {
    executor.scheduleWithFixedDelay(this::safePoll, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Add a subscriber; it is told the current leader (or that there is none) right away.
   *
   * @param subscriber subscriber
   */
  public void subscribe(Subscriber subscriber) {
    subscribers.add(subscriber);
    LeaderResolver.Resolved leader;
    synchronized (this) {
      leader = current;
    }
    if (leader != null) {
      subscriber.onLeader(leader);
    }
  }

  public void unsubscribe(Subscriber subscriber) {
    subscribers.remove(subscriber);
  }

  /** Re-resolve soon, e.g. when a channel reports a connection failure. */
  public void refresh() {
    try {
      executor.execute(this::safePoll);
    } catch (java.util.concurrent.RejectedExecutionException e) {
      // closed
    }
  }

  private void safePoll() {
    try {
      poll();
    } catch (Throwable t) {
      logger.warn("Error resolving leader", t);
    }
  }

  /** Resolve once and notify subscribers of any change. Package private for tests. */
  void poll() {
    Optional<LeaderResolver.Resolved> resolved = resolver.resolve();
    LeaderResolver.Resolved publish = null;
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
        }
      } else if (current == null) {
        noLeader = true;
      }
    }
    if (publish != null) {
      logger.info("Leader resolved: {}", publish);
      for (Subscriber s : subscribers) {
        s.onLeader(publish);
      }
    } else if (noLeader) {
      for (Subscriber s : subscribers) {
        s.onNoLeader();
      }
    }
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
