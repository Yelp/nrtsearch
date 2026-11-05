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

import com.yelp.nrtsearch.clientlib.Node;
import com.yelp.nrtsearch.server.grpc.LuceneServerGrpc;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderRequest;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Provides a {@link NameResolver} that targets the active primary of a multi-primary cluster, found
 * by calling resolveLeader on the client service of seed nodes. For ingestion clients, which must
 * send writes to the active primary.
 */
public class LeaderNameResolverProvider extends NameResolverProvider {
  public static final String SCHEME = "nrtleader";

  /** Target to use with this provider. */
  public static final String TARGET = SCHEME + ":///leader";

  private static final long PROBE_TIMEOUT_MS = 2000;

  private final Supplier<Collection<Node>> seeds;
  private final long pollIntervalMs;

  /**
   * Constructor.
   *
   * @param seeds seed nodes (host and client port) to ask for the leader
   * @param pollIntervalMs how often to re-resolve the leader
   */
  public LeaderNameResolverProvider(Supplier<Collection<Node>> seeds, long pollIntervalMs) {
    this.seeds = seeds;
    this.pollIntervalMs = pollIntervalMs;
  }

  @Override
  protected boolean isAvailable() {
    return true;
  }

  @Override
  protected int priority() {
    return 6;
  }

  @Override
  public String getDefaultScheme() {
    return SCHEME;
  }

  @Override
  public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
    if (!SCHEME.equals(targetUri.getScheme())) {
      return null;
    }
    Map<Node, ManagedChannel> channels = new ConcurrentHashMap<>();
    LeaderResolver.Probe probe =
        node -> {
          ManagedChannel channel =
              channels.computeIfAbsent(
                  node,
                  n ->
                      ManagedChannelBuilder.forAddress(n.getHost(), n.getPort())
                          .usePlaintext()
                          .build());
          ResolveLeaderResponse response =
              LuceneServerGrpc.newBlockingStub(channel)
                  .withDeadlineAfter(PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                  .resolveLeader(ResolveLeaderRequest.newBuilder().build());
          return response;
        };
    LeaderResolver resolver = new LeaderResolver(seeds, probe, LeaderResolver::clientAddress);
    return new LeaderNameResolver(resolver, pollIntervalMs, "") {
      @Override
      public synchronized void shutdown() {
        super.shutdown();
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
      }
    };
  }
}
