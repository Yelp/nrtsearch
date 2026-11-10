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

/**
 * {@link NameResolver} for the active primary of a multi-primary cluster, backed by a {@link
 * LeaderTracker}. Publishes the confirmed leader's address. When no leader can be confirmed, the
 * last confirmed address is kept (a channel is never pointed at an unconfirmed node); if none has
 * been confirmed yet, an error is reported so calls fail fast with UNAVAILABLE.
 *
 * <p>The tracker may be shared by several channels (each with its own resolver) or owned by this
 * resolver, in which case it is closed when the resolver shuts down.
 */
public class LeaderNameResolver extends NameResolver implements LeaderTracker.Subscriber {
  private final LeaderTracker tracker;
  private final boolean ownsTracker;
  private final String authority;
  private volatile Listener2 listener;

  /**
   * Constructor.
   *
   * @param tracker leader tracker
   * @param ownsTracker close the tracker when this resolver shuts down
   * @param authority authority reported for the channel
   */
  public LeaderNameResolver(LeaderTracker tracker, boolean ownsTracker, String authority) {
    this.tracker = Objects.requireNonNull(tracker);
    this.ownsTracker = ownsTracker;
    this.authority = Objects.requireNonNull(authority);
  }

  @Override
  public String getServiceAuthority() {
    return authority;
  }

  @Override
  public void start(Listener2 listener) {
    this.listener = Objects.requireNonNull(listener);
    tracker.subscribe(this);
    if (ownsTracker) {
      tracker.start();
    }
  }

  /** Re-resolve now, e.g. when the channel reports a connection failure. */
  @Override
  public void refresh() {
    tracker.refresh();
  }

  @Override
  public void shutdown() {
    tracker.unsubscribe(this);
    if (ownsTracker) {
      tracker.close();
    }
  }

  @Override
  public void onLeader(LeaderResolver.Resolved leader) {
    Listener2 l = listener;
    if (l == null) {
      return;
    }
    l.onResult(
        ResolutionResult.newBuilder()
            .setAddresses(
                Collections.singletonList(
                    new EquivalentAddressGroup(
                        new InetSocketAddress(
                            leader.address().getHost(), leader.address().getPort()))))
            .setAttributes(Attributes.EMPTY)
            .build());
  }

  @Override
  public void onNoLeader() {
    Listener2 l = listener;
    if (l != null) {
      l.onError(Status.UNAVAILABLE.withDescription("No confirmed leader yet"));
    }
  }

  /** Current confirmed leader, for diagnostics. */
  public Optional<LeaderResolver.Resolved> getCurrent() {
    return tracker.getCurrent();
  }
}
