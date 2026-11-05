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
package com.yelp.nrtsearch.server.grpc.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yelp.nrtsearch.clientlib.Node;
import com.yelp.nrtsearch.clientlib.leader.LeaderNameResolver;
import com.yelp.nrtsearch.clientlib.leader.LeaderResolver;
import com.yelp.nrtsearch.clientlib.leader.SeedSources;
import com.yelp.nrtsearch.server.grpc.ReplicationServerGrpc;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Provides a {@link NameResolver} that resolves the active primary of a multi-primary cluster by
 * calling resolveLeader on the replication service of seed nodes, and targets the confirmed
 * leader's replication port.
 */
public class LeaderRpcNameResolverProvider extends NameResolverProvider {
  public static final String SCHEME = "nrtleader";
  private static final long PROBE_TIMEOUT_MS = 2000;

  private final Supplier<Collection<Node>> seeds;
  private final long pollIntervalMs;

  /**
   * Constructor.
   *
   * @param seeds seed nodes (host and replication port) to ask for the leader
   * @param pollIntervalMs how often to re-resolve the leader
   */
  public LeaderRpcNameResolverProvider(Supplier<Collection<Node>> seeds, long pollIntervalMs) {
    this.seeds = seeds;
    this.pollIntervalMs = pollIntervalMs;
  }

  /** Seeds from a static host and replication port. */
  public static Supplier<Collection<Node>> staticSeeds(String host, int port) {
    return SeedSources.fixed(List.of(new Node(host, port)));
  }

  /**
   * Seeds from a discovery file (json list of host/port), read on every resolve.
   *
   * @param file discovery file
   * @param portOverride if > 0, replaces the port of every node
   */
  public static Supplier<Collection<Node>> fileSeeds(
      ObjectMapper mapper, String file, int portOverride) {
    return SeedSources.file(mapper, file, portOverride);
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
    ProbeChannels channels = new ProbeChannels();
    LeaderResolver resolver =
        new LeaderResolver(seeds, channels::probe, LeaderResolver::replicationAddress);
    return new LeaderNameResolver(resolver, pollIntervalMs, "") {
      @Override
      public synchronized void shutdown() {
        super.shutdown();
        channels.close();
      }
    };
  }

  /** Channels to seed and leader nodes, reused across polls. */
  private static class ProbeChannels {
    private final Map<Node, ManagedChannel> channels = new ConcurrentHashMap<>();

    com.yelp.nrtsearch.server.grpc.ResolveLeaderResponse probe(Node node) {
      ManagedChannel channel =
          channels.computeIfAbsent(
              node,
              n ->
                  ManagedChannelBuilder.forAddress(n.getHost(), n.getPort())
                      .usePlaintext()
                      .build());
      return ReplicationServerGrpc.newBlockingStub(channel)
          .withDeadlineAfter(PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
          .resolveLeader(ResolveLeaderRequest.newBuilder().build());
    }

    void close() {
      channels.values().forEach(ManagedChannel::shutdownNow);
      channels.clear();
    }
  }
}
