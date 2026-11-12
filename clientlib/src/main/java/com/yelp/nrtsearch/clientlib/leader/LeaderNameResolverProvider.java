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
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provides a {@link NameResolver} that targets the active primary of a multi-primary cluster, found
 * by calling resolveLeader on seed nodes. By default asks the client service and targets the
 * leader's client port, for ingestion clients, which must send writes to the active primary.
 */
public class LeaderNameResolverProvider extends NameResolverProvider {
  public static final String SCHEME = "nrtleader";

  /** Target to use with this provider. */
  public static final String TARGET = SCHEME + ":///leader";

  /** Timeout for each resolveLeader call. */
  public static final long PROBE_TIMEOUT_MS = 2000;

  private final Supplier<Collection<Node>> seeds;
  private final long pollIntervalMs;
  private final Function<ManagedChannel, ResolveLeaderResponse> call;
  private final Function<ResolveLeaderResponse, Node> leaderAddress;

  /**
   * Constructor, for the client service.
   *
   * @param seeds seed nodes (host and client port) to ask for the leader
   * @param pollIntervalMs how often to re-resolve the leader
   */
  public LeaderNameResolverProvider(Supplier<Collection<Node>> seeds, long pollIntervalMs) {
    this(
        seeds,
        pollIntervalMs,
        LeaderNameResolverProvider::callClient,
        LeaderResolver::clientAddress);
  }

  /**
   * Constructor.
   *
   * @param seeds seed nodes to ask for the leader
   * @param pollIntervalMs how often to re-resolve the leader
   * @param call calls resolveLeader on a channel to a seed
   * @param leaderAddress address to target, from the leader's answer
   */
  public LeaderNameResolverProvider(
      Supplier<Collection<Node>> seeds,
      long pollIntervalMs,
      Function<ManagedChannel, ResolveLeaderResponse> call,
      Function<ResolveLeaderResponse, Node> leaderAddress) {
    this.seeds = seeds;
    this.pollIntervalMs = pollIntervalMs;
    this.call = call;
    this.leaderAddress = leaderAddress;
  }

  /** Call resolveLeader on the client service. */
  public static ResolveLeaderResponse callClient(ManagedChannel channel) {
    return LuceneServerGrpc.newBlockingStub(channel)
        .withDeadlineAfter(PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .resolveLeader(ResolveLeaderRequest.newBuilder().build());
  }

  /**
   * Provider whose channels subscribe to a shared tracker, which they do not own (it is not closed
   * when a channel shuts down). E.g. one tracker for the channels of all replica indices.
   *
   * @param tracker shared, started tracker
   */
  public static NameResolverProvider forSharedTracker(LeaderTracker tracker) {
    return new NameResolverProvider() {
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
        return new LeaderNameResolver(tracker, false, "");
      }
    };
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
    return new LeaderNameResolver(newTracker(seeds, pollIntervalMs, call, leaderAddress), true, "");
  }

  /**
   * Create a leader tracker that calls resolveLeader over gRPC. It owns its probe channels and
   * threads, released when the tracker is closed.
   *
   * @param seeds seed nodes
   * @param pollIntervalMs how often to re-resolve the leader
   * @param call calls resolveLeader on a channel to a seed
   * @param leaderAddress address to target, from the leader's answer
   */
  public static LeaderTracker newTracker(
      Supplier<Collection<Node>> seeds,
      long pollIntervalMs,
      Function<ManagedChannel, ResolveLeaderResponse> call,
      Function<ResolveLeaderResponse, Node> leaderAddress) {
    ProbeChannels channels = new ProbeChannels();
    ExecutorService probeExecutor =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "LeaderTracker-probe");
              t.setDaemon(true);
              return t;
            });
    LeaderResolver resolver =
        LeaderResolver.grpc(seeds, channels, call, leaderAddress, probeExecutor);
    return new LeaderTracker(
        resolver,
        pollIntervalMs,
        () -> {
          // closed before shutting down threads, so a poll still running can not create channels
          channels.close();
          probeExecutor.shutdownNow();
        });
  }
}
