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
import com.yelp.nrtsearch.clientlib.leader.LeaderNameResolverProvider;
import com.yelp.nrtsearch.clientlib.leader.LeaderResolver;
import com.yelp.nrtsearch.clientlib.leader.SeedSources;
import com.yelp.nrtsearch.server.grpc.ReplicationServerGrpc;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderRequest;
import com.yelp.nrtsearch.server.grpc.ResolveLeaderResponse;
import io.grpc.ManagedChannel;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Leader name resolution for replicas: asks the replication service of seed nodes with
 * resolveLeader, and targets the confirmed leader's replication port. Shares the resolver and probe
 * channel handling with the client side {@link LeaderNameResolverProvider}.
 */
public final class LeaderRpcNameResolverProvider {
  public static final String SCHEME = LeaderNameResolverProvider.SCHEME;
  private static final long PROBE_TIMEOUT_MS = 2000;

  private LeaderRpcNameResolverProvider() {}

  /**
   * Create a provider for replicas.
   *
   * @param seeds seed nodes (host and replication port) to ask for the leader
   * @param pollIntervalMs how often to re-resolve the leader
   */
  public static LeaderNameResolverProvider create(
      Supplier<Collection<Node>> seeds, long pollIntervalMs) {
    return new LeaderNameResolverProvider(
        seeds,
        pollIntervalMs,
        LeaderRpcNameResolverProvider::callReplication,
        LeaderResolver::replicationAddress);
  }

  private static ResolveLeaderResponse callReplication(ManagedChannel channel) {
    return ReplicationServerGrpc.newBlockingStub(channel)
        .withDeadlineAfter(PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .resolveLeader(ResolveLeaderRequest.newBuilder().build());
  }

  /** Seeds from static hosts and a replication port. */
  public static Supplier<Collection<Node>> staticSeeds(List<String> hosts, int port) {
    return SeedSources.fixed(hosts.stream().map(h -> new Node(h, port)).toList());
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
}
