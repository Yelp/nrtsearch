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

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link NameResolver} for the active primary of a multi-primary cluster. Polls a {@link
 * LeaderResolver} and publishes the confirmed leader's address. When no leader can be confirmed,
 * the last confirmed address is kept (a channel is never pointed at an unconfirmed node); if none
 * has been confirmed yet, an error is reported so calls fail fast with UNAVAILABLE.
 */
public class LeaderNameResolver extends NameResolver {
  private static final Logger logger = LoggerFactory.getLogger(LeaderNameResolver.class);

  private final LeaderResolver resolver;
  private final long pollIntervalMs;
  private final String authority;
  private ScheduledExecutorService executor;
  private Listener2 listener;
  // guarded by this
  private LeaderResolver.Resolved current;

  /**
   * Constructor.
   *
   * @param resolver leader resolver
   * @param pollIntervalMs how often to re-resolve the leader
   * @param authority authority reported for the channel
   */
  public LeaderNameResolver(LeaderResolver resolver, long pollIntervalMs, String authority) {
    if (pollIntervalMs <= 0) {
      throw new IllegalArgumentException("pollIntervalMs must be > 0");
    }
    this.resolver = Objects.requireNonNull(resolver);
    this.pollIntervalMs = pollIntervalMs;
    this.authority = Objects.requireNonNull(authority);
  }

  @Override
  public String getServiceAuthority() {
    return authority;
  }

  @Override
  public synchronized void start(Listener2 listener) {
    this.listener = Objects.requireNonNull(listener);
    executor =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "LeaderNameResolver");
              t.setDaemon(true);
              return t;
            });
    executor.scheduleWithFixedDelay(this::poll, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
  }

  /** Re-resolve now, e.g. when the channel reports a connection failure. */
  @Override
  public void refresh() {
    ScheduledExecutorService e;
    synchronized (this) {
      e = executor;
    }
    if (e != null) {
      e.execute(this::poll);
    }
  }

  @Override
  public synchronized void shutdown() {
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
  }

  /** Resolve once and publish any change. Package private for tests. */
  void poll() {
    Optional<LeaderResolver.Resolved> resolved;
    try {
      resolved = resolver.resolve();
    } catch (RuntimeException e) {
      logger.warn("Error resolving leader", e);
      resolved = Optional.empty();
    }
    Listener2 l;
    LeaderResolver.Resolved publish = null;
    boolean reportError = false;
    synchronized (this) {
      l = listener;
      if (l == null) {
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
        reportError = true;
      }
    }
    if (publish != null) {
      logger.info("Leader resolved: {}", publish);
      l.onResult(
          ResolutionResult.newBuilder()
              .setAddresses(
                  Collections.singletonList(
                      new EquivalentAddressGroup(
                          new InetSocketAddress(
                              publish.address().getHost(), publish.address().getPort()))))
              .setAttributes(Attributes.EMPTY)
              .build());
    } else if (reportError) {
      l.onError(Status.UNAVAILABLE.withDescription("No confirmed leader yet"));
    }
  }

  /** Current confirmed leader, for tests and diagnostics. */
  public synchronized Optional<LeaderResolver.Resolved> getCurrent() {
    return Optional.ofNullable(current);
  }
}
